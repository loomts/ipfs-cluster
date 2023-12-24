// Package reedsolomon
// use dataShard make parityShard and send to parityCh
// Consider this scenario: Three machines & dataShards:parityShards = 6:3 -> 1/2 redundancy and tolerate one machine failure
// suppose the scenario that file totalShard%dataShards!=0 -> totalShard=k*dataShards+mShards(mShards<dataShards), then we will use Encode generate (k+1)*parityShards.
// TODO(loomt) according to the size of cluster metrics, adjust the dataShards and parityShards
// TODO(loomt) make a buffer pool enable concurrent encode
// TODO(loomt) add cancle context from dag_service

package reedsolomon

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"sync"
	"unsafe"

	ipld "github.com/ipfs/go-ipld-format"

	"github.com/ipfs-cluster/ipfs-cluster/api"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/templexxx/reedsolomon"
)

var log = logging.Logger("reedsolomon")

type BlockStat string

const (
	DefaultDataShards   = 6
	DefaultParityShards = 3
	DefaultBlock        = BlockStat("defaultBlock")
	ShardEndBlock       = BlockStat("shardEndBlock")
	FileEndBlock        = BlockStat("fileEndBlock")
)

type StatBlock struct {
	ipld.Node
	Stat BlockStat
	api.Cid
}

type ParityShard struct {
	RawData []byte
	Name    string
	Links   map[int]api.Cid // Links to data shards
}

type ReedSolomon struct {
	mu           sync.Mutex
	rs           *reedsolomon.RS
	ctx          context.Context
	blocks       [][]byte
	batchCid     map[int]api.Cid
	parityCids   map[string]cid.Cid // send to sharding dag service
	dataShards   int
	parityShards int
	curShardI    int
	curShardJ    int
	shardSize    int
	totalSize    int
	parityLen    int
	parityCh     chan Shard
	blockCh      chan StatBlock
	receAllData  bool
}

func New(ctx context.Context, d int, p int, shardSize int) *ReedSolomon {
	rs, _ := reedsolomon.New(d, p)
	b := make([][]byte, d+p)
	for i := range b {
		b[i] = make([]byte, shardSize)
	}
	r := &ReedSolomon{
		mu:           sync.Mutex{},
		rs:           rs,
		ctx:          ctx,
		blocks:       b,
		batchCid:     make(map[int]api.Cid),
		parityCids:   make(map[string]cid.Cid),
		shardSize:    shardSize,
		dataShards:   d,
		parityShards: p,
		curShardI:    0,
		curShardJ:    0,
		totalSize:    0,
		parityCh:     make(chan Shard, 1024),
		blockCh:      make(chan StatBlock, 1024),
		parityLen:    0,
		receAllData:  false,
	}
	go r.handleBlock()
	return r
}

// handleBlock handle block from dag_service
func (rs *ReedSolomon) handleBlock() {
	for {
		sb := <-rs.blockCh
		switch sb.Stat {
		case DefaultBlock:
			rs.mu.Lock()
			b := sb.RawData()
			if rs.curShardJ+len(b) < rs.shardSize {
				copy(rs.blocks[rs.curShardI][rs.curShardJ:], b)
				rs.curShardJ += len(b)
			} else {
				log.Errorf("unreachable code, block size:%d, curShardJ:%d, shardSize:%d", len(b), rs.curShardJ, rs.shardSize)
			}
			rs.mu.Unlock()
		case ShardEndBlock:
			rs.mu.Lock()
			// fill with 0 to align this data shard
			copy(rs.blocks[rs.curShardI][rs.curShardJ:], bytes.Repeat([]byte{0}, rs.shardSize-rs.curShardJ))
			rs.batchCid[rs.curShardI] = sb.Cid
			rs.curShardI, rs.curShardJ = rs.curShardI+1, 0
			if rs.curShardI == rs.dataShards {
				log.Errorf("encode false")
				rs.Encode(false)
			}
			rs.mu.Unlock()
		case FileEndBlock:
			rs.mu.Lock()
			log.Errorf("encode true")
			rs.Encode(true)
			rs.mu.Unlock()
			return
		}
	}
}

// Encode take dataShards shard as a batch, if shards no reach dataShards, fill with 0
func (rs *ReedSolomon) Encode(isLast bool) {
	if isLast && rs.curShardI == 0 && rs.curShardJ == 0 {
		// no data, don't need to encode
		log.Error("close paritych")
		close(rs.parityCh)
		rs.receAllData = isLast
		return
	}
	rs.align()
	err := rs.rs.Encode(rs.blocks)
	if err != nil {
		log.Errorf("reedsolomon encode error:%v", err)
		return
	}
	for _, b := range rs.blocks[rs.dataShards:] {
		// make a new slice to copy b and send out
		out := make([]byte, len(b))
		copy(out, b)
		rs.parityCh <- Shard{
			RawData: out,
			Name:    fmt.Sprintf("parity-shard-%d", rs.parityLen),
			Links:   rs.batchCid,
		}
		rs.totalSize += rs.shardSize
		rs.parityLen += 1
	}
	if isLast {
		log.Error("close paritych")
		close(rs.parityCh)
		rs.receAllData = isLast
	}
}

func (rs *ReedSolomon) ReConstruct(data [][]byte, replaceRows []int, parity [][]byte) error {
	err := rs.rs.Replace(data, replaceRows, parity)
	if err != nil {
		return err
	}
	return nil
}

func (rs *ReedSolomon) align() {
	// fill bytes with 0 to align shard all that followed
	total := (rs.dataShards-rs.curShardI)*rs.shardSize + rs.shardSize - rs.curShardJ
	CBytes := (*[1 << 30]byte)(unsafe.Pointer(&rs.blocks[rs.curShardI][rs.curShardJ]))[:total:total]
	CBytes = bytes.Repeat([]byte{0}, len(CBytes))
	rs.curShardI, rs.curShardJ = 0, 0
}

func (rs *ReedSolomon) ReceiveAll() bool {
	return rs.receAllData
}

func (rs *ReedSolomon) TotalSize() uint64 {
	return uint64(rs.totalSize)
}

func (rs *ReedSolomon) GetParityShards() map[string]cid.Cid {
	return rs.parityCids
}

func (rs *ReedSolomon) AddParityCid(key int, cid api.Cid) {
	rs.parityCids[strconv.Itoa(key)] = cid.Cid
}

func (rs *ReedSolomon) GetParityFrom() <-chan Shard {
	return rs.parityCh
}

func (rs *ReedSolomon) SendBlockTo() chan<- StatBlock {
	return rs.blockCh
}
