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

type ParityShard struct {
	RawData  []byte
	Name     string
	Links    map[int]api.Cid // Links to data shards
	LinkSize map[api.Cid]int
}

type Shard struct {
	api.Cid
	Name    string
	RawData []byte
	Links   map[int]api.Cid // Links to blocks
}

type Batch struct {
	vects [][]byte
	need  []int
	has   []int
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
		sb := <-r.blockCh
		switch sb.Stat {
		case DefaultBlock:
			r.mu.Lock()
			b := sb.RawData()
			if r.curShardJ+len(b) <= r.shardSize {
				r.blocks[r.curShardI] = append(r.blocks[r.curShardI], b...)
				r.curShardJ += len(b)
			} else {
				log.Errorf("unreachable code, block size:%d, curShardJ:%d, shardSize:%d", len(b), r.curShardJ, r.shardSize)
			}
			r.mu.Unlock()
		case ShardEndBlock:
			r.mu.Lock()
			// fill with 0 to alignL this data shard
			r.dataShardSize[fmt.Sprintf("%d", r.dataLen)] = uint64(len(r.blocks[r.curShardI]))
			r.dataLen += 1
			r.batchCid[r.curShardI] = sb.Cid
			r.curShardI, r.curShardJ = r.curShardI+1, 0
			if r.curShardI == r.dataShards {
				r.EncodeL(false)
			}
			r.mu.Unlock()
		case FileEndBlock:
			r.mu.Lock()
			r.EncodeL(true)
			r.mu.Unlock()
			return
		}
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

// SplitAndRecon split data and parity vects and alignL to batch and reconstruct
func (r *ReedSolomon) SplitAndRecon(dataVects [][]byte, parityVects [][]byte, dShardSize []int) error {
	d := r.dataShards
	p := r.parityShards
	if len(parityVects)%p != 0 {
		return errors.New("parity vects length must be multiple of erasure coding parityShards")
	}
	var err error
	wg := sync.WaitGroup{}
	wg.Add(len(parityVects) / p)
	for i := 0; i < len(parityVects)/p; i++ {
		// batch reconstruct
		i := i
		go func(dVects [][]byte, pVects [][]byte, dSize []int) {
			defer wg.Done()
			shardSize, err := r.getShardSizeAndCheck(dVects, pVects)
			if err != nil {
				log.Errorf("reconstruct error:%v", err)
				return
			}
			// create new slices, avoid disrupting the original array
			vects := make([][]byte, 0, len(dVects))
			vects = append(vects, dVects...)
			for j := range vects {
				// fill with 0 to alignL
				if vects[j] != nil && len(vects[j]) != 0 && len(vects[j]) < shardSize {
					vects[j] = append(vects[j], make([]byte, shardSize-len(vects[j]))...)
				}
			}
			// last batch of dataVects maybe not enough, append zero vects
			for len(vects) < d {
				vects = append(vects, make([]byte, shardSize))
			}
			vects = append(vects, pVects...)

			for j := range vects {
				fmt.Println("batch", i, "vects", j, "len", len(vects[j]))
				if len(vects[j]) == 0 || vects[j] == nil {
					fmt.Printf("====batch%d -> vects[%d] is nil\n", i, j)
				}
			}
			er := r.rs.Reconstruct(vects)
			if err != nil {
				err = errors.Wrap(err, er.Error())
				log.Errorf("reconstruct shards[%d~%d] error:%v", i*d, min((i+1)*d, len(dataVects)), err)
				return
			}
			// remove diff
			for j := 0; j < len(dVects); j++ {
				dVects[j] = vects[j][:dSize[j]]
			}
			for j := 0; j < len(pVects); j++ {
				pVects[j] = vects[j+d][:]
			}
		}(dataVects[i*d:min((i+1)*d, len(dataVects))], parityVects[i*p:(i+1)*p], dShardSize[i*d:min((i+1)*d, len(dataVects))])
	}
	wg.Wait()
	return err
}

func (r *ReedSolomon) getShardSizeAndCheck(dVects [][]byte, pVects [][]byte) (int, error) {
	shardSize := 0
	need := 0
	for _, v := range pVects {
		if v == nil || len(v) == 0 {
			need++
		} else {
			shardSize = len(v)
		}
	}
	for _, v := range dVects {
		if v == nil || len(v) == 0 {
			need++
		} else {
			shardSize = max(shardSize, len(v))
		}
	}
	fmt.Printf("need:%d, shardSize:%d\n", need, shardSize)
	if need > r.parityShards {
		return shardSize, errors.New("data vects not enough, cannot reconstruct")
	}
	return shardSize, nil
}
