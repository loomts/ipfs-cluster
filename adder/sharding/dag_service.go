// Package sharding implements a sharding ClusterDAGService places
// content in different shards while it's being added, creating
// a final Cluster DAG and pinning it.
package sharding

import (
	"context"
	"errors"
	"fmt"

	"time"

	"github.com/ipfs-cluster/ipfs-cluster/adder"
	rs "github.com/ipfs-cluster/ipfs-cluster/adder/reedsolomon"
	"github.com/ipfs-cluster/ipfs-cluster/api"

	humanize "github.com/dustin/go-humanize"
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
	rs *rs.ReedSolomon
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
		rs:        rs.New(ctx, rs.DefaultDataShards, rs.DefaultParityShards, int(opts.ShardSize)),
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

func (dgs *DAGService) pinShardsMeta(ctx context.Context, ref api.Cid, shard2cid map[string]cid.Cid, parity bool) (api.Cid, error) {
	// Pin the shards DAG
	shardsMetaDAG, err := makeDAG(ctx, shard2cid)
	if err != nil {
		return api.CidUndef, err
	}
	// PutDAG to ourselves
	blocks := make(chan api.NodeWithMeta, 256)
	go func() {
		defer close(blocks)
		for _, n := range shardsMetaDAG {
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
		return api.CidUndef, err
	}
	shardsMetaDAGCid := shardsMetaDAG[0].Cid()
	output := api.AddedOutput{}
	output.Name = fmt.Sprintf("%s-clusterDAG", dgs.addParams.Name)
	output.Cid = api.NewCid(shardsMetaDAGCid)
	output.Size = dgs.totalSize
	if parity {
		output.Name = fmt.Sprintf("%s-parity-clusterDAG", dgs.addParams.Name)
		output.Size = dgs.rs.TotalSize()
	}
	dgs.sendOutput(output)

	shardsMetaDAGPin := api.PinWithOpts(api.NewCid(shardsMetaDAGCid), dgs.addParams.PinOptions)
	shardsMetaDAGPin.ReplicationFactorMin = -1
	shardsMetaDAGPin.ReplicationFactorMax = -1
	shardsMetaDAGPin.MaxDepth = 0 // pin direct
	shardsMetaDAGPin.Name = fmt.Sprintf("%s-clusterDAG", dgs.addParams.Name)
	shardsMetaDAGPin.Type = api.ClusterDAGType
	shardsMetaDAGPin.Reference = &ref
	if parity {
		shardsMetaDAGPin.Name = fmt.Sprintf("%s-parity-clusterDAG", dgs.addParams.Name)
	}
	// Update object with response.
	err = adder.Pin(ctx, dgs.rpcClient, shardsMetaDAGPin)
	if err != nil {
		return api.CidUndef, err
	}
	return shardsMetaDAGPin.Cid, err
}

// Finalize finishes sharding, creates the cluster DAG and pins it along
// with the meta pin for the root node of the content.
func (dgs *DAGService) Finalize(ctx context.Context, dataRoot api.Cid) (api.Cid, error) {
	ref := dataRoot
	if dgs.addParams.Erasure {
		parityCids := dgs.rs.GetParityShards() // erasure: first pin parity shards then pin data shards and ref to parity shards.
		paritysCid, err := dgs.pinShardsMeta(ctx, dataRoot, parityCids, true)
		ref = paritysCid
		if err != nil {
			return dataRoot, err
		}
		logger.Errorf("pin parity shards metadata: %s", paritysCid)
	}
	// clusterDAG
	clusterDAG, err := dgs.pinShardsMeta(ctx, ref, dgs.shards, false)
	if err != nil {
		return dataRoot, err
	}
	logger.Errorf("pin data shards metadata: %s", clusterDAG)
	// Pin the META pin
	metaPin := api.PinWithOpts(dataRoot, dgs.addParams.PinOptions)
	metaPin.Type = api.MetaType
	ref = clusterDAG
	metaPin.Reference = &ref
	metaPin.MaxDepth = 0 // irrelevant. Meta-pins are not pinned
	err = adder.Pin(ctx, dgs.rpcClient, metaPin)
	if err != nil {
		return dataRoot, err
	}

	// Log some stats
	dgs.logStats(metaPin.Cid, clusterDAG)

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
	if shard.Size()+size < shard.Limit() {
		shard.AddLink(ctx, n.Cid(), size)
		if dgs.addParams.Erasure {
			dgs.rs.SendBlockTo() <- rs.StatBlock{Node: n, Stat: rs.DefaultBlock}
		}
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
ClusterDAG: %s
Total shards: %d
Total size: %s
Total time: %s
Ingest Rate: %s/s
`

	logger.Infof(
		statsFmt,
		metaPin,
		clusterDAGPin,
		len(dgs.shards),
		humanize.Bytes(dgs.totalSize),
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
	// end of shard
	if dgs.addParams.Erasure {
		dgs.rs.SendBlockTo() <- rs.StatBlock{Cid: api.NewCid(shardCid), Stat: rs.ShardEndBlock}
	}
	if err != nil {
		return api.NewCid(shardCid), err
	}
	dgs.totalSize += shard.Size()
	dgs.shards[fmt.Sprintf("%d", lens)] = shardCid
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

func (dgs *DAGService) GetRS() *rs.ReedSolomon {
	return dgs.rs
}

func (dgs *DAGService) SetParity(name string) {
}
