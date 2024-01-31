package ipfscluster

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ipfs-cluster/ipfs-cluster/adder/sharding"
	"github.com/ipfs-cluster/ipfs-cluster/api"
	"github.com/ipfs/boxo/ipld/merkledag"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	ipldlegacy "github.com/ipfs/go-ipld-legacy"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

var ipldDecoder *ipldlegacy.Decoder

// create an ipld registry specific to this package
func init() {
	ipldDecoder = ipldlegacy.NewDecoder()
	ipldDecoder.RegisterCodec(cid.DagProtobuf, dagpb.Type.PBNode, merkledag.ProtoNodeConverter)
	ipldDecoder.RegisterCodec(cid.Raw, basicnode.Prototype.Bytes, merkledag.RawNodeConverter)
}

// simple dag service, just for erasure coding file get
type dagSession struct {
	ctx      context.Context
	blockGet func(ctx context.Context, ci api.Cid) ([]byte, error)
	fileGet  func(ctx context.Context, fpath string) ([]byte, error)
}

func NewDagGetter(ctx context.Context, bg func(ctx context.Context, ci api.Cid) ([]byte, error), fg func(ctx context.Context, fpath string) ([]byte, error)) *dagSession {
	return &dagSession{
		ctx:      ctx,
		blockGet: bg,
		fileGet:  fg,
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
			var nd format.Node
			nd, err = ds.decode(ctx, b)
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

// ECGetShards get both data shards and parity shards by root cid
func (ds *dagSession) ECGetShards(ctx context.Context, ci api.Cid, dShardNum int) ([][]byte, [][]byte, []int, error) {
	links, errs := ds.ResolveCborLinks(ctx, ci) // get sorted data shards
	if errs != nil {
		logger.Error(errs)
		return nil, nil, nil, errs
	}
	vects := make([][]byte, len(links))
	needCh := make(chan int, len(links)) // default RS(4,2) enable 1/3 shards broken
	wg := sync.WaitGroup{}
	wg.Add(len(links))

	for i, sh := range links {
		go func(i int, sh *format.Link) {
			defer wg.Done()
			resultCh := make(chan []byte)
			errCh := make(chan error)
			go func() {
				vect, err := ds.ECLink2Raw(ctx, sh, i, dShardNum)
				if err != nil {
					errCh <- err
					return
				}
				resultCh <- vect
			}()
			select {
			case vects[i] = <-resultCh:
				logger.Infof("get %dth shard successfully, len:%d", i, len(vects[i]))
				return
			case err := <-errCh:
				needCh <- i
				logger.Errorf("cannot get %dth shard: %s", i, err)
			case <-time.After(30 * time.Second):
				needCh <- i
				logger.Errorf("cannot get %dth shard: timeout 30s", i)
			}
		}(i, sh)
	}
	wg.Wait()
	close(needCh)
	needRepin := make([]int, 0, len(needCh))
	for i := range needCh {
		needRepin = append(needRepin, i)
	}
	sort.Ints(needRepin)
	return vects[:dShardNum:dShardNum], vects[dShardNum:], needRepin, nil
}

// ResolveCborLinks get sorted block links
func (ds *dagSession) ResolveCborLinks(ctx context.Context, shard api.Cid) ([]*format.Link, error) {
	clusterDAGBlock, err := ds.blockGet(ctx, shard)
	if err != nil {
		return nil, fmt.Errorf("cluster pin(%s) was not stored: %s", shard, err)
	}
	clusterDAGNode, err := sharding.CborDataToNode(clusterDAGBlock, "cbor")
	if err != nil {
		return nil, err
	}

	blks := clusterDAGNode.Links()
	links := make([]*format.Link, 0, len(blks))
	var errs error
	// traverse shard in order
	// blks -> 0,cid0; 1,cid1
	for i := 0; i < len(blks); i++ {
		sh, _, err := clusterDAGNode.ResolveLink([]string{fmt.Sprintf("%d", i)})
		if err != nil {
			err = fmt.Errorf("cannot resolve %dst data shard: %s", i, err)
			errors.Join(errs, err)
		}
		links = append(links, sh)
	}
	return links, errs
}

// convert shard to []byte
func (ds *dagSession) ECLink2Raw(ctx context.Context, sh *format.Link, idx int, dShards int) ([]byte, error) {
	shardBlock, err := ds.blockGet(ctx, api.NewCid(sh.Cid))
	if err != nil {
		return nil, fmt.Errorf("cannot get shard(%s)'s Block: %s", sh.Cid, err)
	}

	var links []*format.Link
	if idx < dShards {
		links, err = ds.ResolveCborLinks(ctx, api.NewCid(sh.Cid)) // get sorted data shards
		if err != nil {
			return nil, fmt.Errorf("cannot resolve shard(%s): %s", sh.Cid, err)
		}
	} else {
		// TODO consider how to optimize
		switch sh.Cid.Prefix().Codec {
		case cid.DagProtobuf:
			// the reason why not use uio.NewDagReader is because dag_service.Get will call context.Cancel and sometime decode failed
			nd, err := merkledag.DecodeProtobuf(shardBlock)
			if err != nil {
				return nil, fmt.Errorf("cannot decode shard(%s): %s", sh.Cid, err)
			}
			links = nd.Links()
		case cid.Raw:
			return shardBlock, nil
		default:
			return nil, fmt.Errorf("unsupported codec:%v", sh.Cid.Prefix().Codec)
		}
	}
	return ds.ResolveRoot(ctx, links, idx)
}

func (ds *dagSession) ResolveRoot(ctx context.Context, links []*format.Link, idx int) ([]byte, error) {
	vect := make([]byte, 0, len(links)*256*1024) // estimate size
	for _, link := range links {
		b, err := ds.blockGet(ctx, api.NewCid(link.Cid))

		// fmt.Printf("shard%d link:%s size:%d, prefix:%v\n", idx, link.Cid, len(b), link.Cid.Prefix())
		if err != nil {
			return nil, fmt.Errorf("cannot fetch block(%s): %s", link.Cid, err)
		}
		vect = append(vect, b...)
	}
	return vect, nil
}

func (ds *dagSession) Add(ctx context.Context, node format.Node) error {
	panic("unreachable code")
}

func (ds *dagSession) AddMany(ctx context.Context, nodes []format.Node) error {
	panic("unreachable code")
}

func (ds *dagSession) Remove(ctx context.Context, c cid.Cid) error {
	panic("unreachable code")
}

func (ds *dagSession) RemoveMany(ctx context.Context, cids []cid.Cid) error {
	panic("unreachable code")
}
