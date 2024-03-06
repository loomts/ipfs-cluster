// Package sharding implements a sharding ClusterDAGService places
// content in different shards while it's being added, creating
// a final Cluster DAG and pinning it.
package sharding

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"time"

	"github.com/ipfs-cluster/ipfs-cluster/adder"
	ec "github.com/ipfs-cluster/ipfs-cluster/adder/erasure"
	"github.com/ipfs-cluster/ipfs-cluster/api"

	humanize "github.com/dustin/go-humanize"
	dag "github.com/ipfs/boxo/ipld/merkledag"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	rpc "github.com/libp2p/go-libp2p-gorpc"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

var logger = logging.Logger("shardingdags")

// DAGService is an implementation of a ClusterDAGService which
// shards content while adding among several IPFS Cluster peers,
// creating a Cluster DAG to track and pin that content selectively
// in the IPFS daemons allocated to it.
type DAGService struct {
	adder.BaseDAGService

	ctx       context.Context
	rpcClient *rpc.Client

	addParams api.AddParams
	output    chan<- api.AddedOutput

	addedSet *cid.Set

	// Current shard being built
	currentShard *shard
	// Last flushed shard CID
	previousShard cid.Cid

	// shard tracking
	shards map[string]cid.Cid

	startTime time.Time
	totalSize uint64
	// erasure coding
	rs *ec.ReedSolomon
	// record blocksize to metadata, enable erasure find each block
	blockMeta []ECBlockMeta
}

// New returns a new ClusterDAGService, which uses the given rpc client to perform
// Allocate, IPFSStream and Pin requests to other cluster components.
func New(ctx context.Context, rpc *rpc.Client, opts api.AddParams, out chan<- api.AddedOutput) *DAGService {
	// use a default value for this regardless of what is provided.
	opts.Mode = api.PinModeRecursive
	return &DAGService{
		ctx:       ctx,
		rpcClient: rpc,
		addParams: opts,
		output:    out,
		addedSet:  cid.NewSet(),
		shards:    make(map[string]cid.Cid),
		startTime: time.Now(),
		rs:        ec.New(ctx, opts.DataShards, opts.ParityShards, int(opts.ShardSize)),
		blockMeta: make([]ECBlockMeta, 0, 1024),
	}
}

// Add puts the given node in its corresponding shard and sends it to the
// destination peers.
func (dgs *DAGService) Add(ctx context.Context, node ipld.Node) error {
	// FIXME: This will grow in memory
	if !dgs.addedSet.Visit(node.Cid()) {
		return nil
	}

	return dgs.ingestBlock(ctx, node)
}

// Close performs cleanup and should be called when the DAGService is not
// going to be used anymore.
func (dgs *DAGService) Close() error {
	if dgs.currentShard != nil {
		dgs.currentShard.Close()
	}
	return nil
}

// Finalize finishes sharding, creates the cluster DAG and pins it along
// with the meta pin for the root node of the content.
func (dgs *DAGService) Finalize(ctx context.Context, dataRoot api.Cid) (api.Cid, error) {
	shardMeta := make(map[string]cid.Cid, len(dgs.shards))
	for i, ci := range dgs.shards {
		shardMeta[i] = ci
	}
	if dgs.addParams.Erasure {
		parityCids := dgs.rs.GetParityShards()
		for id, ci := range parityCids {
			i, _ := strconv.Atoi(id)
			shardMeta[fmt.Sprintf("%d", i+len(dgs.shards))] = ci
		}
	}
	// clusterDAG
	clusterDAGNodes, err := MakeDAG(ctx, shardMeta)
	if err != nil {
		return dataRoot, err
	}
	blocks := make(chan api.NodeWithMeta, 256)
	go func() {
		defer close(blocks)
		for _, n := range clusterDAGNodes {
			select {
			case <-ctx.Done():
				logger.Error(ctx.Err())
				return //abort
			case blocks <- adder.IpldNodeToNodeWithMeta(n):
			}
		}
	}()

	// Stream these blocks and wait until we are done.
	bs := adder.NewBlockStreamer(ctx, dgs.rpcClient, []peer.ID{""}, blocks)
	select {
	case <-ctx.Done():
		return api.CidUndef, ctx.Err()
	case <-bs.Done():
	}

	if err := bs.Err(); err != nil {
		return dataRoot, err
	}

	clusterDAG := clusterDAGNodes[0].Cid()

	dgs.sendOutput(api.AddedOutput{
		Name:        fmt.Sprintf("%s-clusterDAG", dgs.addParams.Name),
		Cid:         api.NewCid(clusterDAG),
		Size:        dgs.totalSize,
		Allocations: nil,
	})

	// Pin the ClusterDAG
	clusterDAGPin := api.PinWithOpts(api.NewCid(clusterDAG), dgs.addParams.PinOptions)
	clusterDAGPin.ReplicationFactorMin = -1
	clusterDAGPin.ReplicationFactorMax = -1
	clusterDAGPin.MaxDepth = 0 // pin direct
	clusterDAGPin.Name = fmt.Sprintf("%s-clusterDAG", dgs.addParams.Name)
	clusterDAGPin.Type = api.ClusterDAGType
	clusterDAGPin.Reference = &dataRoot
	dataShardSize := dgs.rs.GetDataShardSize()
	logger.Errorf("+++++++ datashardsize %v:%v", dataRoot.Cid, dataShardSize)
	// record data shard size to metadata, enable erasure module know data size and the number.
	for i, size := range dataShardSize {
		clusterDAGPin.Metadata[i] = fmt.Sprintf("%d", size)
	}
	for _, bmeta := range dgs.blockMeta {
		clusterDAGPin.Metadata[bmeta.String()] = ""
	}
	// Update object with response.
	err = adder.Pin(ctx, dgs.rpcClient, clusterDAGPin)
	if err != nil {
		return dataRoot, err
	}

	// Pin the META pin
	metaPin := api.PinWithOpts(dataRoot, dgs.addParams.PinOptions)
	metaPin.Type = api.MetaType
	ref := api.NewCid(clusterDAG)
	metaPin.Reference = &ref
	metaPin.MaxDepth = 0 // irrelevant. Meta-pins are not pinned
	metaPin.Metadata = nil
	err = adder.Pin(ctx, dgs.rpcClient, metaPin)
	if err != nil {
		return dataRoot, err
	}

	// Log some stats
	dgs.logStats(metaPin.Cid, clusterDAGPin.Cid)

	// Consider doing this? Seems like overkill
	//
	// // Amend ShardPins to reference clusterDAG root hash as a Parent
	// shardParents := cid.NewSet()
	// shardParents.Add(clusterDAG)
	// for shardN, shard := range dgs.shardNodes {
	// 	pin := api.PinWithOpts(shard, dgs.addParams)
	// 	pin.Name := fmt.Sprintf("%s-shard-%s", pin.Name, shardN)
	// 	pin.Type = api.ShardType
	// 	pin.Parents = shardParents
	// 	// FIXME: We don't know anymore the shard pin maxDepth
	//      // so we'd need to get the pin first.
	// 	err := dgs.pin(pin)
	// 	if err != nil {
	// 		return err
	// 	}
	// }

	return dataRoot, nil
}

// Allocations returns the current allocations for the current shard.
func (dgs *DAGService) Allocations() []peer.ID {
	// FIXME: this is probably not safe in concurrency?  However, there is
	// no concurrent execution of any code in the DAGService I think.
	if dgs.currentShard != nil {
		return dgs.currentShard.Allocations()
	}
	return nil
}

// ingests a block to the current shard. If it get's full, it
// Flushes the shard and retries with a new one.
func (dgs *DAGService) ingestBlock(ctx context.Context, n ipld.Node) error {
	shard := dgs.currentShard

	// if we have no currentShard, create one
	if shard == nil {
		logger.Infof("new shard for '%s': #%d", dgs.addParams.Name, len(dgs.shards))
		var err error
		// important: shards use the DAGService context.
		shard, err = newShard(dgs.ctx, ctx, dgs.rpcClient, dgs.addParams.PinOptions, len(dgs.shards), dgs.addParams.Erasure)
		if err != nil {
			return err
		}
		dgs.currentShard = shard
	}

	logger.Debugf("ingesting block %s in shard %d (%s)", n.Cid(), len(dgs.shards), dgs.addParams.Name)

	// this is not same as n.Size()
	size := uint64(len(n.RawData()))
	// add the block to it if it fits and return
	if shard.Size()+size <= shard.Limit() {
		if dgs.addParams.Erasure {
			dgs.rs.SendBlockTo() <- ec.StatBlock{Node: n, Stat: ec.DefaultBlock}
			// record block metadata
			var nb ipld.Node
			switch n.(type) {
			case *dag.ProtoNode:
				nb = n.(*dag.ProtoNode)
			case *dag.RawNode:
				nb = n.(*dag.RawNode)
			default:
				logger.Errorf("ingestBlock unknown node type:%v", n)
			}
			meta := ECBlockMeta{
				ShardNo: len(dgs.shards),
				BlockNo: len(shard.blockMeta),
				Offset:  shard.Size(),
				Size:    size,
				Cid:     nb.Cid().String(),
			}
			shard.blockMeta = append(shard.blockMeta, meta)
		}
		shard.AddLink(ctx, n.Cid(), size)
		return dgs.currentShard.sendBlock(ctx, n)
	}

	logger.Debugf("shard %d full: block: %d. shard: %d. limit: %d",
		len(dgs.shards),
		size,
		shard.Size(),
		shard.Limit(),
	)

	// -------
	// Below: block DOES NOT fit in shard
	// Flush and retry

	// if shard is empty, error
	if shard.Size() == 0 {
		return errors.New("block doesn't fit in empty shard: shard size too small?")
	}

	_, err := dgs.FlushCurrentShard(ctx)
	if err != nil {
		return err
	}
	return dgs.ingestBlock(ctx, n) // <-- retry ingest
}

func (dgs *DAGService) logStats(metaPin, clusterDAGPin api.Cid) {
	duration := time.Since(dgs.startTime)
	seconds := uint64(duration) / uint64(time.Second)
	var rate string
	if seconds == 0 {
		rate = "∞ B"
	} else {
		rate = humanize.Bytes(dgs.totalSize / seconds)
	}

	statsFmt := `sharding session successful:
CID: %s
Name: %s
ClusterDAG: %s
Total shards: %d
Total size: %d
Total time: %s
Ingest Rate: %s/s
`

	logger.Infof(
		statsFmt,
		metaPin,
		dgs.addParams.Name,
		clusterDAGPin,
		len(dgs.shards),
		// humanize.Bytes(dgs.totalSize),
		dgs.totalSize,
		duration,
		rate,
	)

}

func (dgs *DAGService) sendOutput(ao api.AddedOutput) {
	if dgs.output != nil {
		dgs.output <- ao
	}
}

// flushes the dgs.currentShard and returns the LastLink()
func (dgs *DAGService) FlushCurrentShard(ctx context.Context) (api.Cid, error) {
	shard := dgs.currentShard
	if shard == nil {
		return api.CidUndef, errors.New("cannot flush a nil shard")
	}

	lens := len(dgs.shards)

	shardCid, err := shard.Flush(ctx, lens, dgs.previousShard)
	if err != nil {
		return api.NewCid(shardCid), err
	}
	// end of shard
	if dgs.addParams.Erasure {
		dgs.rs.SendBlockTo() <- ec.StatBlock{Cid: api.NewCid(shardCid), Stat: ec.ShardEndBlock}
	}
	dgs.totalSize += shard.Size()
	dgs.shards[fmt.Sprintf("%d", lens)] = shardCid
	for _, bmeta := range shard.blockMeta {
		dgs.blockMeta = append(dgs.blockMeta, bmeta)
	}
	dgs.previousShard = shardCid
	dgs.currentShard = nil
	dgs.sendOutput(api.AddedOutput{
		Name:        fmt.Sprintf("shard-%d", lens),
		Cid:         api.NewCid(shardCid),
		Size:        shard.Size(),
		Allocations: shard.Allocations(),
	})

	return api.NewCid(shard.LastLink()), nil
}

// AddMany calls Add for every given node.
func (dgs *DAGService) AddMany(ctx context.Context, nodes []ipld.Node) error {
	for _, node := range nodes {
		err := dgs.Add(ctx, node)
		if err != nil {
			return err
		}
	}
	return nil
}

func (dgs *DAGService) GetRS() *ec.ReedSolomon {
	return dgs.rs
}

func (dgs *DAGService) SetParity(name string) {
}
