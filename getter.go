package ipfscluster

import (
	"bufio"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/ipfs-cluster/ipfs-cluster/adder/sharding"
	"github.com/ipfs-cluster/ipfs-cluster/api"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/ipld/merkledag"
	unixfile "github.com/ipfs/boxo/ipld/unixfs/file"
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
	ctx        context.Context
	dataShards [][]byte
	bmeta      map[string]sharding.ECBlockMeta
	blockGet   func(ctx context.Context, ci api.Cid) ([]byte, error)
}

func NewDagGetter(ctx context.Context, bg func(ctx context.Context, ci api.Cid) ([]byte, error)) *dagSession {
	return &dagSession{
		bmeta:    make(map[string]sharding.ECBlockMeta),
		ctx:      ctx,
		blockGet: bg,
	}
}

func (ds *dagSession) SetCache(dataShards [][]byte, bmeta map[int][]sharding.ECBlockMeta) {
	ds.dataShards = dataShards
	for _, meta := range bmeta {
		for _, m := range meta {
			ds.bmeta[m.Cid] = m
		}
	}
}

func (ds *dagSession) GetRawFile(ctx context.Context, ci cid.Cid) ([]byte, error) {
	root, err := ds.Get(ctx, ci)
	if err != nil {
		return nil, err
	}
	r, err := uio.NewDagReader(ctx, root, ds)
	if err != nil {
		return nil, err
	}
	blockSize := 256 * 1024
	b := make([]byte, 0, blockSize)
	for {
		n, err := r.Read(b[len(b):cap(b)])
		b = b[:len(b)+n]
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return b, err
		}

		if len(b) == cap(b) {
			// Add more capacity (let append pick how much).
			b = append(b, 0)[:len(b)]
		}
	}
}

func (ds *dagSession) GetArchivedFile(ctx context.Context, ci cid.Cid, name string) ([]byte, error) {
	root, err := ds.Get(ctx, ci)
	if err != nil {
		return nil, err
	}
	f, err := unixfile.NewUnixfsFile(ctx, ds, root)
	if err != nil {
		return nil, err
	}
	fmt.Println("new file successfully")
	return fileArchive(f, name)
}

func (ds *dagSession) Get(ctx context.Context, ci cid.Cid) (format.Node, error) {
	if ds.dataShards != nil {
		m := ds.bmeta[ci.String()]
		b := ds.dataShards[m.ShardNo][m.Offset : m.Offset+m.Size]
		return ds.decode(ctx, b, ci)
	}
	b, err := ds.blockGet(ctx, api.NewCid(ci))
	if err != nil {
		logger.Infof("Failed to get block %s, err: %s", ci, err)
	}
	return ds.decode(ctx, b, ci)
}

func (ds *dagSession) GetMany(ctx context.Context, keys []cid.Cid) <-chan *format.NodeOption {
	out := make(chan *format.NodeOption, len(keys))

	go func() {
		defer close(out)
		wg := sync.WaitGroup{}
		wg.Add(len(keys))
		for _, ci := range keys {
			go func(ci cid.Cid) {
				defer wg.Done()
				nd, err := ds.Get(ctx, ci)
				if err != nil {
					out <- &format.NodeOption{Err: fmt.Errorf("cannot get all blocks: %s", err)}
				}
				no := &format.NodeOption{Node: nd, Err: err}
				select {
				case out <- no:
				case <-ctx.Done():
					out <- &format.NodeOption{Err: fmt.Errorf("GetMany context timeout: %s", err)}
					return
				}
			}(ci)
		}
		wg.Wait()
	}()

	return out
}

func (ds *dagSession) decode(ctx context.Context, rawb []byte, ci cid.Cid) (format.Node, error) {
	b, err := blocks.NewBlockWithCid(rawb, ci)
	if err != nil {
		return nil, fmt.Errorf("cannot parse raw data with cid")
	}
	nd, err := ipldDecoder.DecodeNode(ctx, b)
	if err != nil {
		logger.Errorf("Failed to decode block: %s", err)
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
				start := time.Now()
				vect, err := ds.ECLink2Raw(ctx, sh, i, dShardNum)
				if err != nil {
					errCh <- err
					return
				}
				fetchTime := time.Since(start)
				shardType := "parity"
				if i < dShardNum {
					shardType = "data"
				}
				logger.Infof("use %s fetch %dth shard(%v) successfully, size:%d shard(%s)", fetchTime, i, shardType, len(vect), sh.Cid)
				resultCh <- vect
			}()
			select {
			case vects[i] = <-resultCh:
				return
			case err := <-errCh:
				needCh <- i
				logger.Errorf("cannot get %dth shard: %s", i, err)
			case <-time.After(5 * time.Minute):
				needCh <- i
				logger.Errorf("cannot get %dth shard: timeout 1min", i)
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
func (ds *dagSession) ECLink2Raw(ctx context.Context, sh *format.Link, idx int, dShardNum int) ([]byte, error) {
	if idx < dShardNum {
		links, err := ds.ResolveCborLinks(ctx, api.NewCid(sh.Cid)) // get sorted data shards
		if err != nil {
			return nil, fmt.Errorf("cannot resolve shard(%s): %s", sh.Cid, err)
		}
		vect := make([][]byte, len(links)) // estimate size
		wg := sync.WaitGroup{}
		wg.Add(len(links))
		for i, link := range links {
			go func(i int, ci api.Cid) {
				defer wg.Done()
				vect[i], err = ds.blockGet(ctx, ci)
				if err != nil {
					fmt.Printf("cannot fetch block(%s): %s\n", ci, err)
				}
			}(i, api.NewCid(link.Cid))
		}
		wg.Wait()
		b := make([]byte, 0, len(vect)*len(vect[0]))
		for _, v := range vect {
			b = append(b, v...)
		}
		return b, nil
	}
	// directly get parity shard
	return ds.GetRawFile(ctx, sh.Cid)
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

// https://github.com/ipfs/kubo/blob/a7c65184976e8717ac23d7efaa5b0d477ad15deb/core/commands/get.go#L93
func fileArchive(f files.Node, name string) ([]byte, error) {
	archive := false
	compression := 0
	// need to connect a writer to a reader
	piper, pipew := io.Pipe()
	checkErrAndClosePipe := func(err error) bool {
		if err != nil {
			_ = pipew.CloseWithError(err)
			return true
		}
		return false
	}

	// use a buffered writer to parallelize task
	DefaultBufSize := 1048576
	bufw := bufio.NewWriterSize(pipew, DefaultBufSize)

	// compression determines whether to use gzip compression.
	maybeGzw, err := newMaybeGzWriter(bufw, compression)
	if checkErrAndClosePipe(err) {
		return nil, err
	}

	closeGzwAndPipe := func() {
		if err := maybeGzw.Close(); checkErrAndClosePipe(err) {
			return
		}
		if err := bufw.Flush(); checkErrAndClosePipe(err) {
			return
		}
		pipew.Close() // everything seems to be ok.
	}

	if !archive && compression != gzip.NoCompression {
		// the case when the node is a file
		r := files.ToFile(f)
		if r == nil {
			return nil, errors.New("file is not regular")
		}

		go func() {
			if _, err := io.Copy(maybeGzw, r); checkErrAndClosePipe(err) {
				return
			}
			closeGzwAndPipe() // everything seems to be ok
		}()
	} else {
		// the case for 1. archive, and 2. not archived and not compressed, in which tar is used anyway as a transport format

		// construct the tar writer
		w, err := files.NewTarWriter(maybeGzw)
		if checkErrAndClosePipe(err) {
			return nil, err
		}

		go func() {
			// write all the nodes recursively
			if err := w.WriteFile(f, name); checkErrAndClosePipe(err) {
				return
			}
			w.Close()         // close tar writer
			closeGzwAndPipe() // everything seems to be ok
		}()
	}

	return io.ReadAll(piper)
}

func newMaybeGzWriter(w io.Writer, compression int) (io.WriteCloser, error) {
	if compression != gzip.NoCompression {
		return gzip.NewWriterLevel(w, compression)
	}
	return &identityWriteCloser{w}, nil
}

type identityWriteCloser struct {
	w io.Writer
}

func (i *identityWriteCloser) Write(p []byte) (int, error) {
	return i.w.Write(p)
}

func (i *identityWriteCloser) Close() error {
	return nil
}
