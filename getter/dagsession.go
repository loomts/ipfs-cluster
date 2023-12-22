package getter

import (
	"context"
	"fmt"
	"sync"

	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/ipld/merkledag"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	format "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("dagsession")

type dagSession struct {
	ctx context.Context

	ex exchange.Interface
	bs blockstore.Blockstore

	f    exchange.Fetcher
	once sync.Once
}

func NewDagSession(ctx context.Context, ex exchange.Interface, bs blockstore.Blockstore) format.NodeGetter {
	return &dagSession{
		ctx: ctx,
		ex:  ex,
		bs:  bs,
	}
}

func (ds *dagSession) Get(ctx context.Context, id cid.Cid) (format.Node, error) {
	// local blockstore
	fmt.Println("local blockstore")
	b, err := ds.bs.Get(ctx, id)

	if err != nil {
		log.Infof("Failed to get block from local blockstore(%s): %s", id, err)

		// peer blockstore
		b, err = ds.fetcher().GetBlock(ctx, id)
		if err != nil {
			log.Infof("Failed to get block from remote blockstore(%s): %s", id, err)
			return nil, err
		}
	}
	return ds.decode(b)
}

func (ds *dagSession) GetMany(ctx context.Context, in []cid.Cid) <-chan *format.NodeOption {
	out := make(chan *format.NodeOption, len(in))
	ids := make([]cid.Cid, 0, len(in))

	go func() {
		defer close(out)

		for _, id := range in {
			b, err := ds.bs.Get(ctx, id)
			if err != nil {
				log.Infof("Failed to get block from local blockstore(%s): %s", id, err)
				ids = append(ids, id)
				continue
			}

			nd, err := ds.decode(b)
			if !sendOrDone(ctx, out, &format.NodeOption{Node: nd, Err: err}) {
				return
			}
		}

		if len(ids) == 0 {
			return
		}

		bs, err := ds.fetcher().GetBlocks(ctx, ids)
		if err != nil {
			log.Infof("Failed to get blocks from remote blockstore(%s): %s", ids, err)
			return
		}

		for b := range bs {
			nd, err := ds.decode(b)
			if !sendOrDone(ctx, out, &format.NodeOption{Node: nd, Err: err}) {
				return
			}
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

func (ds *dagSession) fetcher() exchange.Fetcher {
	ds.once.Do(func() {
		ex, ok := ds.ex.(exchange.SessionExchange)
		if !ok {
			ds.f = ds.ex
			return
		}

		ds.f = ex.NewSession(ds.ctx)
	})

	return ds.f
}

func (ds *dagSession) decode(b blocks.Block) (format.Node, error) {
	fmt.Println("starting decode block")
	nd, err := merkledag.DecodeProtobufBlock(b)

	if err != nil {
		log.Warnf("Failed to decode block: %s", err)
		return nil, err
	}
	return nd, err
}
