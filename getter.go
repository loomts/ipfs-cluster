package ipfscluster

import (
	"context"

	"github.com/ipfs-cluster/ipfs-cluster/api"
	"github.com/ipfs/boxo/ipld/merkledag"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	ipldlegacy "github.com/ipfs/go-ipld-legacy"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/codec/raw"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/multicodec"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

var ipldDecoder *ipldlegacy.Decoder

// create an ipld registry specific to this package
func init() {
	mcReg := multicodec.Registry{}
	mcReg.RegisterDecoder(cid.DagProtobuf, dagpb.Decode)
	mcReg.RegisterDecoder(cid.Raw, raw.Decode)
	mcReg.RegisterDecoder(cid.DagCBOR, dagcbor.Decode)
	ls := cidlink.LinkSystemUsingMulticodecRegistry(mcReg)

	ipldDecoder = ipldlegacy.NewDecoderWithLS(ls)
	ipldDecoder.RegisterCodec(cid.DagProtobuf, dagpb.Type.PBNode, merkledag.ProtoNodeConverter)
	ipldDecoder.RegisterCodec(cid.Raw, basicnode.Prototype.Bytes, merkledag.RawNodeConverter)
}

// simple dag service, just for erasure coding file get
type dagSession struct {
	ctx      context.Context
	blockGet func(ctx context.Context, ci api.Cid) ([]byte, error)
}

func NewDagGetter(ctx context.Context, f func(ctx context.Context, ci api.Cid) ([]byte, error)) format.DAGService {
	return &dagSession{
		ctx:      ctx,
		blockGet: f,
	}
}

func (ds *dagSession) Get(ctx context.Context, ci cid.Cid) (format.Node, error) {
	b, err := ds.blockGet(ctx, api.NewCid(ci))
	if err != nil {
		logger.Infof("Failed to get block %s, err: %s", ci, err)
	}
	return ds.decode(ctx, b)
}

func (ds *dagSession) GetMany(ctx context.Context, in []cid.Cid) <-chan *format.NodeOption {
	out := make(chan *format.NodeOption, len(in))
	cis := make([]cid.Cid, 0, len(in))

	go func() {
		defer close(out)

		for _, ci := range in {
			b, err := ds.blockGet(ctx, api.NewCid(ci))
			if err != nil {
				logger.Infof("Failed to get block %s, err: %s", ci, err)
				cis = append(cis, ci)
				continue
			}

			nd, err := ds.decode(ctx, b)
			if !sendOrDone(ctx, out, &format.NodeOption{Node: nd, Err: err}) {
				return
			}
		}

		if len(cis) == 0 {
			return
		}
	}()

	return out
}

func (ds *dagSession) Add(ctx context.Context, node format.Node) error {
	//TODO unreachable code
	panic("unreachable code")
}

func (ds *dagSession) AddMany(ctx context.Context, nodes []format.Node) error {
	//TODO unreachable code
	panic("unreachable code")
}

func (ds *dagSession) Remove(ctx context.Context, c cid.Cid) error {
	//TODO unreachable code
	panic("unreachable code")
}

func (ds *dagSession) RemoveMany(ctx context.Context, cids []cid.Cid) error {
	//TODO unreachable code
	panic("unreachable code")
}

func sendOrDone(ctx context.Context, out chan<- *format.NodeOption, no *format.NodeOption) bool {
	select {
	case out <- no:
		return true
	case <-ctx.Done():
		return false
	}
}

func (ds *dagSession) decode(ctx context.Context, rawb []byte) (format.Node, error) {
	b := blocks.NewBlock(rawb)
	nd, err := ipldDecoder.DecodeNode(ctx, b) // TODO figure out why sharding block cannot decode: (PBNode) invalid wireType, expected 2, got ?
	if err != nil {
		logger.Warnf("Failed to decode block: %s", err)
		return nil, err
	}
	return nd, err
	// return merkledag.DecodeProtobuf(rawb)
}
