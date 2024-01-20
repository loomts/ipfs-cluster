package adder

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/ipfs-cluster/ipfs-cluster/api"
	"go.uber.org/multierr"

	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	rpc "github.com/libp2p/go-libp2p-gorpc"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

// ErrBlockAdder is returned when adding a to multiple destinations
// block fails on all of them.
var ErrBlockAdder = errors.New("failed to put block on all destinations")

// BlockStreamer helps streaming nodes to multiple destinations, as long as
// one of them is still working.
type BlockStreamer struct {
	dests     []peer.ID
	rpcClient *rpc.Client
	blocks    <-chan api.NodeWithMeta

	ctx    context.Context
	cancel context.CancelFunc
	errMu  sync.Mutex
	err    error
}

// NewBlockStreamer creates a BlockStreamer given an rpc client, allocated
// peers and a channel on which the blocks to stream are received.
func NewBlockStreamer(ctx context.Context, rpcClient *rpc.Client, dests []peer.ID, blocks <-chan api.NodeWithMeta) *BlockStreamer {
	bsCtx, cancel := context.WithCancel(ctx)

	bs := BlockStreamer{
		ctx:       bsCtx,
		cancel:    cancel,
		dests:     dests,
		rpcClient: rpcClient,
		blocks:    blocks,
		err:       nil,
	}

	go bs.streamBlocks()
	return &bs
}

// Done returns a channel which gets closed when the BlockStreamer has
// finished.
func (bs *BlockStreamer) Done() <-chan struct{} {
	return bs.ctx.Done()
}

func (bs *BlockStreamer) setErr(err error) {
	bs.errMu.Lock()
	bs.err = err
	bs.errMu.Unlock()
}

// Err returns any errors that happened after the operation of the
// BlockStreamer, for example when blocks could not be put to all nodes.
func (bs *BlockStreamer) Err() error {
	bs.errMu.Lock()
	defer bs.errMu.Unlock()
	return bs.err
}

func (bs *BlockStreamer) streamBlocks() {
	defer bs.cancel()

	// Nothing should be sent on out.
	// We drain though
	out := make(chan struct{})
	go func() {
		for range out {
		}
	}()

	errs := bs.rpcClient.MultiStream(
		bs.ctx,
		bs.dests,
		"IPFSConnector",
		"BlockStream",
		bs.blocks,
		out,
	)

	combinedErrors := multierr.Combine(errs...)

	// FIXME: replicate everywhere.
	if len(multierr.Errors(combinedErrors)) == len(bs.dests) {
		logger.Error(combinedErrors)
		bs.setErr(ErrBlockAdder)
	} else if combinedErrors != nil {
		logger.Warning("there were errors streaming blocks, but at least one destination succeeded")
		logger.Warning(combinedErrors)
	}
}

// IpldNodeToNodeWithMeta converts an ipld.Node to api.NodeWithMeta.
func IpldNodeToNodeWithMeta(n ipld.Node) api.NodeWithMeta {
	size, err := n.Size()
	if err != nil {
		logger.Warn(err)
	}

	return api.NodeWithMeta{
		Cid:     api.NewCid(n.Cid()),
		Data:    n.RawData(),
		CumSize: size,
	}
}

// BlockAllocate helps allocating blocks to peers.
func BlockAllocate(ctx context.Context, rpc *rpc.Client, pinOpts api.PinOptions) ([]peer.ID, error) {
	// Find where to allocate this file
	var allocsStr []peer.ID
	err := rpc.CallContext(
		ctx,
		"",
		"Cluster",
		"BlockAllocate",
		api.PinWithOpts(api.CidUndef, pinOpts),
		&allocsStr,
	)
	return allocsStr, err
}

// DefaultECAllocate choose one peer to send shard
// more suitable for EC(4,2), and 3 peers
func DefaultECAllocate(peers []peer.ID, dShards int, pShards int, idx int, isData bool) (peer.ID, error) {
	if len(peers) == 0 {
		return "", errors.New("no peers")
	}
	if len(peers) == 1 {
		return peers[0], nil
	}
	// Sort peers to ensure consistent allocation
	sort.Slice(peers, func(i, j int) bool {
		return peers[i].String() < peers[j].String()
	})

	// dPeerNum is the number of peers who store data shard, hopefully more than the number of parity peers.
	var dPeerNum int
	N := dShards + pShards
	if dShards*len(peers)%N == 0 {
		dPeerNum = dShards * len(peers) / N
	} else {
		dPeerNum = 1 + int(dShards*len(peers)/N)
		if len(peers) == dPeerNum {
			dPeerNum = len(peers) - 1
		}
	}
	pPeerNum := len(peers) - dPeerNum
	if isData {
		return peers[idx%dPeerNum], nil
	} else {
		return peers[dPeerNum+idx%pPeerNum], nil
	}
}

// ErasurePin helps sending local and remote RPC pin requests.(by setting dest and enable the promission of RPC Call)
func ErasurePin(ctx context.Context, rpc *rpc.Client, pin api.Pin) error {
	logger.Debugf("adder pinning %+v", pin)
	var pinResp api.Pin
	return rpc.CallContext(
		ctx,
		pin.Allocations[0], // use only one peer to pin
		"Cluster",
		"Pin",
		pin,
		&pinResp,
	)
}

// Pin helps sending local RPC pin requests.
func Pin(ctx context.Context, rpc *rpc.Client, pin api.Pin) error {
	if pin.ReplicationFactorMin < 0 {
		pin.Allocations = []peer.ID{}
	}
	logger.Debugf("adder pinning %+v", pin)
	var pinResp api.Pin
	return rpc.CallContext(
		ctx,
		"", // use ourself to pin
		"Cluster",
		"Pin",
		pin,
		&pinResp,
	)
}

// ErrDAGNotFound is returned whenever we try to get a block from the DAGService.
var ErrDAGNotFound = errors.New("dagservice: a Get operation was attempted while cluster-adding (this is likely a bug)")

// BaseDAGService partially implements an ipld.DAGService.
// It provides the methods which are not needed by ClusterDAGServices
// (Get*, Remove*) so that they can save adding this code.
type BaseDAGService struct {
}

// Get always returns errNotFound
func (dag BaseDAGService) Get(ctx context.Context, key cid.Cid) (ipld.Node, error) {
	return nil, ErrDAGNotFound
}

// GetMany returns an output channel that always emits an error
func (dag BaseDAGService) GetMany(ctx context.Context, keys []cid.Cid) <-chan *ipld.NodeOption {
	out := make(chan *ipld.NodeOption, 1)
	out <- &ipld.NodeOption{Err: ErrDAGNotFound}
	close(out)
	return out
}

// Remove is a nop
func (dag BaseDAGService) Remove(ctx context.Context, key cid.Cid) error {
	return nil
}

// RemoveMany is a nop
func (dag BaseDAGService) RemoveMany(ctx context.Context, keys []cid.Cid) error {
	return nil
}
