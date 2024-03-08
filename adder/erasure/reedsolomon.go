// use dataShard make parityShard and send to parityCh
// TODO according to the size of cluster metrics, adjust the dataShards and parityShards
// TODO make a buffer pool enable concurrent encode
// TODO add cancel context from dag_service

package erasure

import (
	"context"
	"fmt"
	"sync"

	"github.com/pkg/errors"

	"github.com/ipfs-cluster/ipfs-cluster/api"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	rs "github.com/klauspost/reedsolomon"
)

var log = logging.Logger("erasure")

type BlockStat string

const (
	DefaultBlock  = BlockStat("defaultBlock")
	ShardEndBlock = BlockStat("shardEndBlock")
	FileEndBlock  = BlockStat("fileEndBlock")
)

type StatBlock struct {
	ipld.Node
	Stat BlockStat
	api.Cid
}

type Shard struct {
	api.Cid
	Idx     int
	Name    string
	RawData []byte
	Links   map[int]api.Cid // Links to blocks
}

type Batch struct {
	Idx       int
	NeedRepin []int // Shards[i] broken, need to repin
	Shards    [][]byte
}

type ReedSolomon struct {
	mu            sync.Mutex
	rs            rs.Encoder
	ctx           context.Context
	blocks        [][]byte
	batchCid      map[int]api.Cid
	dataShardSize map[string]uint64
	parityCids    map[string]cid.Cid // send to sharding dag service
	dataShards    int
	parityShards  int
	curShardI     int
	curShardJ     int
	shardSize     int
	parityLen     int
	dataLen       int
	parityCh      chan Shard
	blockCh       chan StatBlock
	receAllData   bool
}

func New(ctx context.Context, d int, p int, shardSize int) *ReedSolomon {
	rss, _ := rs.New(d, p)
	r := &ReedSolomon{
		mu:            sync.Mutex{},
		rs:            rss,
		ctx:           ctx,
		blocks:        newBlocks(d, p, shardSize),
		dataShardSize: make(map[string]uint64),
		batchCid:      make(map[int]api.Cid),
		parityCids:    make(map[string]cid.Cid),
		shardSize:     shardSize,
		dataShards:    d,
		parityShards:  p,
		curShardI:     0,
		curShardJ:     0,
		parityLen:     0,
		dataLen:       0,
		parityCh:      make(chan Shard, 1024),
		blockCh:       make(chan StatBlock, 1024),
		receAllData:   false,
	}
	if shardSize != 0 {
		go r.handleBlock()
	}
	return r
}

// handleBlock handle block from dag_service
func (r *ReedSolomon) handleBlock() {
	for {
		blk := <-r.blockCh
		r.mu.Lock()
		switch blk.Stat {
		case DefaultBlock:
			b := blk.RawData()
			if r.curShardJ+len(b) <= r.shardSize {
				r.blocks[r.curShardI] = append(r.blocks[r.curShardI], b...)
				r.curShardJ += len(b)
			} else {
				log.Errorf("unreachable code, block size:%d, curShardJ:%d, shardSize:%d", len(b), r.curShardJ, r.shardSize)
			}
		case ShardEndBlock:
			// fill with 0 to alignL this data shard
			r.dataShardSize[fmt.Sprintf("%d", r.dataLen)] = uint64(len(r.blocks[r.curShardI]))
			r.dataLen += 1
			r.batchCid[r.curShardI] = blk.Cid
			r.curShardI, r.curShardJ = r.curShardI+1, 0
			if r.curShardI == r.dataShards {
				r.EncodeL(false)
			}
		case FileEndBlock:
			r.EncodeL(true)
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()
	}
}

// EncodeL take dataShards shard as a batch, if shards no reach dataShards, fill with 0
func (r *ReedSolomon) EncodeL(isLast bool) {
	// no data, don't need to encode (hard to reach this condition)
	if isLast && r.curShardI == 0 && r.curShardJ == 0 {
		close(r.parityCh)
		r.receAllData = isLast
		return
	}
	r.alignL()
	err := r.rs.Encode(r.blocks)
	if err != nil {
		log.Errorf("reedsolomon encode error:%v", err)
		return
	}
	for _, b := range r.blocks[r.dataShards:] {
		// make a new slice to copy b and send out
		r.parityCh <- Shard{
			RawData: b,
			Name:    fmt.Sprintf("parity-shard-%d", r.parityLen),
			Links:   r.batchCid,
		}
		r.parityLen += 1
	}
	r.blocks = newBlocks(r.dataShards, r.parityShards, r.shardSize)
	if isLast {
		close(r.parityCh)
		r.receAllData = isLast
	}
}

func (r *ReedSolomon) alignL() {
	eachShard := 0
	for i := 0; i < r.curShardI; i++ {
		eachShard = max(len(r.blocks[i]), eachShard)
	}
	for i := 0; i < r.curShardI; i++ {
		r.blocks[i] = append(r.blocks[i], make([]byte, eachShard-len(r.blocks[i]))...)
	}
	// make more 0 shards
	for r.curShardI < (r.dataShards + r.parityShards) {
		r.blocks[r.curShardI] = append(r.blocks[r.curShardI], make([]byte, eachShard)...)
		r.curShardI += 1
	}
	r.curShardI, r.curShardJ = 0, 0
}

func newBlocks(d, p, shardSize int) [][]byte {
	b := make([][]byte, d+p)
	for i := range b[:d] {
		b[i] = make([]byte, 0, shardSize)
	}
	return b
}

func (r *ReedSolomon) ReceiveAll() bool {
	return r.receAllData
}

func (r *ReedSolomon) GetParityShards() map[string]cid.Cid {
	return r.parityCids
}

func (r *ReedSolomon) AddParityCid(key int, cid api.Cid) {
	r.parityCids[fmt.Sprintf("%d", key)] = cid.Cid
}

func (r *ReedSolomon) GetDataShardSize() map[string]uint64 {
	return r.dataShardSize
}

func (r *ReedSolomon) GetParity() <-chan Shard {
	return r.parityCh
}

func (r *ReedSolomon) SendBlockTo() chan<- StatBlock {
	return r.blockCh
}

// BatchRecon handle each batch, only nil shard need to reconstruct
func (r *ReedSolomon) BatchRecon(ctx context.Context, batchIdx int, batchDataShardSize []int, shardCh <-chan Shard) (error, Batch) {
	receShards := 0
	batchDataShards := len(batchDataShardSize)
	batch := make([][]byte, r.dataShards+r.parityShards)
	for {
		select {
		case <-ctx.Done():
			log.Error(ctx.Err())
			return fmt.Errorf("%d batch err:%s", batchIdx, ctx.Err()), Batch{}
		case shard := <-shardCh:
			if len(shard.RawData) == 0 {
				log.Infof("BatchRecon received invalid shard")
				continue
			}
			receShards++
			if shard.Idx >= batchDataShards && batchDataShards < r.dataShards {
				// last batch of dataVects maybe not enough, make parity idx < decode idx
				shard.Idx = r.dataShards + shard.Idx - batchDataShards
			}
			batch[shard.Idx] = shard.RawData

			if receShards >= batchDataShards {
				// create new slices, avoid disrupting the original array
				vects := make([][]byte, len(batch))
				for i := 0; i < len(vects); i++ {
					if len(batch[i]) > 0 {
						vects[i] = batch[i]
					}
				}

				// append 0 for RS decode
				needRepin, err := r.batchAlignAndCheck(batchDataShards, vects[:r.dataShards], vects[r.dataShards:])
				if err != nil {
					return fmt.Errorf("%d batch err:%s", batchIdx, err), Batch{}
				}
				err = r.rs.Reconstruct(vects)
				if err != nil {
					log.Errorf("reconstruct shards[%d~%d] error:%v", batchIdx*r.dataShards, batchIdx*r.dataShards+len(batchDataShardSize), err)
					return fmt.Errorf("%d batch err:%s", batchIdx, err), Batch{}
				}

				// remove appended 0
				for j := 0; j < len(vects); j++ {
					if j < batchDataShards {
						vects[j] = vects[j][:batchDataShardSize[j]:batchDataShardSize[j]]
					} else if j < r.dataShards {
						vects[j] = nil
					}
				}

				return nil, Batch{Idx: batchIdx, NeedRepin: needRepin, Shards: vects}
			}
		}
	}
}

func (r *ReedSolomon) batchAlignAndCheck(batchDataShards int, dVects [][]byte, pVects [][]byte) ([]int, error) {
	shardSize, need := 0, batchDataShards-r.dataShards // minus full-0 Shards

	for _, shards := range [][][]byte{dVects, pVects} {
		for _, v := range shards {
			if len(v) > 0 {
				shardSize = max(shardSize, len(v))
			} else {
				need++
			}
		}
	}

	log.Infof("batch info: dShards:%d, pShards:%d, needRecon:%d, shardSize:%d\n", len(dVects), len(pVects), need, shardSize)
	if need > r.parityShards {
		return nil, errors.New("data vects not enough, cannot reconstruct")
	}

	// append 0 to vect for align
	for i, v := range dVects {
		if len(v) > 0 && len(v) < shardSize {
			dVects[i] = append(v, make([]byte, shardSize-len(v))...)
		}
		if i >= batchDataShards {
			dVects[i] = make([]byte, shardSize)
		}
	}

	needRepin := make([]int, 0, r.parityShards)
	for i, vects := range [][][]byte{dVects, pVects} {
		for j := range vects {
			if len(vects[j]) == 0 {
				if i == 0 {
					needRepin = append(needRepin, j)
					log.Infof("dataShard %d is nil\n", j)
				} else {
					needRepin = append(needRepin, j+batchDataShards)
					log.Infof("parityShard %d is nil\n", j+batchDataShards)
				}
			}
		}
	}
	return needRepin, nil
}
