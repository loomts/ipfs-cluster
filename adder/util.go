package adder

import (
	"context"
	"errors"
	"hash/fnv"
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

// DefaultECAllocate selects a peer to send a shard.
// It uses a round-robin strategy to allocate shards to peers, use hash value sorts peers, and assigns different order of peers for data shards and parity shards.
func DefaultECAllocate(ctx context.Context, rpc *rpc.Client, hashName string, d, p, idx int, isData bool) (peer.ID, error) {
	if d <= 0 || p <= 0 || idx < 0 {
		return "", errors.New("invalid input")
	}

	var peers []peer.ID
	err := rpc.CallContext(
		ctx,
		"",
		"Consensus",
		"Peers",
		struct{}{},
		&peers,
	)
	if err != nil {
		return "", err
	}

	if len(peers) == 0 {
		return "", errors.New("error no peers")
	}

	if len(peers) == 1 {
		return peers[0], nil
	}

	// Sort peers by hash
	h := fnv.New32()
	hashPeers := make(map[peer.ID]uint32)
	for _, peer := range peers {
		h.Reset()
		h.Write([]byte(hashName + peer.String()))
		hashPeers[peer] = h.Sum32()
	}
	sort.Slice(peers, func(i, j int) bool {
		return hashPeers[peers[i]] < hashPeers[peers[j]]
	})
	// Choose the peer based on hash, either in positive or reverse order
	i := idx % len(peers)
	if isData {
		i = len(peers) - i - 1
	}

	return peers[i], nil
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
