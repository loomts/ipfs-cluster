package ipfscluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/ipfs-cluster/ipfs-cluster/adder/sharding"
	"github.com/ipfs-cluster/ipfs-cluster/api"
	"github.com/ipfs/boxo/ipld/merkledag"
	uio "github.com/ipfs/boxo/ipld/unixfs/io"
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

func NewDagGetter(ctx context.Context, f func(ctx context.Context, ci api.Cid) ([]byte, error)) *dagSession {
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
}

func (ds *dagSession) getFileWithTimeout(ctx context.Context, ci api.Cid, timeout time.Duration) ([]byte, error) {
	// try to get file directly
	done := make(chan struct{})
	var fileb []byte
	var err error
	go func() {
		defer close(done)
		var b []byte
		b, err = ds.blockGet(ctx, ci)
		if err != nil {
			return
		}
		var metaNode format.Node
		var r uio.DagReader
		metaNode, err = ipldDecoder.DecodeNode(ctx, blocks.NewBlock(b))
		if err != nil {
			return
		}
		r, err = uio.NewDagReader(ctx, metaNode, ds)
		if err == nil {
			fileb, err = io.ReadAll(r)
			if err != nil {
				logger.Errorf("cannot read Node: %s", err)
				return
			}
		}
	}()
	select {
	case <-done:
		if err == nil {
			logger.Info("get file directly success")
			return fileb, nil
		}
		logger.Errorf("read file directly failed: %s, try to reconstruct", err)
	case <-time.After(timeout):
		logger.Info("cannot get file directly: timeout 30s, try to reconstruct")
	}
	return nil, errors.New("cannot get file directly")
}

// try to get shard by dag
func (ds *dagSession) ECGetShards(ctx context.Context, ci api.Cid, dataShardNum int) ([][]byte, [][]byte, bool, error) {
	links, errs := ds.ECResolveLinks(ctx, ci, dataShardNum) // get sorted data shards
	if errs != nil {
		logger.Error(errs)
	}
	vects := make([][]byte, len(links))
	wg := sync.WaitGroup{}
	wg.Add(len(links))
	needReCon := false

	for i, sh := range links {
		go func(i int, sh *format.Link) {
			defer wg.Done()
			resultCh := make(chan []byte)
			errCh := make(chan error)
			go func() {
				vect, err := ds.ECLink2Raw(ctx, sh, i < dataShardNum)
				if err != nil {
					errCh <- err
					return
				}
				resultCh <- vect
			}()
			select {
			case vects[i] = <-resultCh:
				logger.Infof("get %dth shard successfully", i)
				return
			case err := <-errCh:
				logger.Errorf("cannot get %dth shard: %s", i, err)
			case <-time.After(time.Minute):
				logger.Errorf("cannot get %dth shard: timeout 1min", i)
			}
			needReCon = true
		}(i, sh)
	}
	wg.Wait()
	return vects[:dataShardNum:dataShardNum], vects[dataShardNum:], needReCon, nil
}

func (ds *dagSession) ECResolveLinks(ctx context.Context, clusterDAG api.Cid, dataShardLen int) ([]*format.Link, error) {
	clusterDAGBlock, err := ds.blockGet(ctx, clusterDAG)
	if err != nil {
		return nil, fmt.Errorf("cluster pin(%s) was not stored: %s", clusterDAG, err)
	}
	clusterDAGNode, err := sharding.CborDataToNode(clusterDAGBlock, "cbor")
	if err != nil {
		return nil, err
	}

	shards := clusterDAGNode.Links()
	links := make([]*format.Link, 0, len(shards))
	var errs error
	// traverse shards in order
	// shards -> 0,cid0
	for i := 0; i < len(shards); i++ {
		sh, _, err := clusterDAGNode.ResolveLink([]string{fmt.Sprintf("%d", i)})
		fmt.Printf("shard %d: %s\n", i, sh.Cid)
		if err != nil {
			err = fmt.Errorf("cannot resolve %dst data shard: %s", i, err)
			errors.Join(errs, err)
		}
		links = append(links, sh)
	}
	return links, errs
}

// convert shard link to []byte
func (ds *dagSession) ECLink2Raw(ctx context.Context, sh *format.Link, isDataLink bool) ([]byte, error) {
	shardBlock, err := ds.blockGet(ctx, api.NewCid(sh.Cid))
	if err != nil {
		return nil, fmt.Errorf("cannot get shard(%s)'s Block: %s", sh.Cid, err)
	}
	var nd format.Node
	if isDataLink {
		nd, err = sharding.CborDataToNode(shardBlock, "cbor")
	} else {
		nd, err = merkledag.DecodeProtobuf(shardBlock)
	}
	if err != nil {
		return nil, fmt.Errorf("cannot decode shard(%s): %s", sh.Cid, err)
	}
	vect := make([]byte, 0, len(nd.Links())*256*1024) // estimate size
	// fetch each link
	for _, link := range nd.Links() {
		if isDataLink && strings.HasPrefix(link.Cid.String(), "Qm") {
			// V0 cid("Qm...") is not data block
			continue
		}
		b, err := ds.blockGet(ctx, api.NewCid(link.Cid))
		if err != nil {
			return nil, fmt.Errorf("cannot fetch links' data of shard(%s): %s", sh.Cid, err)
		}
		vect = append(vect, b...)
	}
	return vect, nil
}
