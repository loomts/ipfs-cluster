// Package reedsolomon
// use dataShard make parityShard and send to parityCh
// Consider this scenario: Three machines & dataShards:parityShards = 6:3 -> 1/2 redundancy and tolerate one machine failure
// suppose the scenario that file totalShard%dataShards!=0 -> totalShard=k*dataShards+mShards(mShards<dataShards), then we will use Encode generate (k+1)*parityShards.
// TODO(loomt) according to the size of cluster metrics, adjust the dataShards and parityShards
// TODO(loomt) this version is encode once, but split to 6*n and encode n times better
// TODO(loomt) make a buffer pool enable concurrent encode
// TODO(loomt) try to use io.Reader Seeker reset iterator and avoid copy from blocks
// TODO(loomt) add cancle context from dag_service
package reedsolomon

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"unsafe"

	"github.com/ipfs-cluster/ipfs-cluster/api"
	logging "github.com/ipfs/go-log/v2"
	"github.com/templexxx/reedsolomon"
)

var log = logging.Logger("reedsolomon")

const (
	DefaultDataShards   = 6
	DefaultParityShards = 3
)

type ReedSolomon struct {
	lock   sync.Mutex
	rs     *reedsolomon.RS
	ctx    context.Context
	blocks [][]byte
	s2c    map[int]api.Cid

	dataShards   int
	parityShards int
	curShardI    int
	curShardJ    int
	shardSize    int
	parityLen    int
	parityCh     chan ParityShard
	remainShards int
	receAllData  bool
}

type ParityShard struct {
	RawData []byte
	Name    string
	Links   map[int]api.Cid // Links to data shards
}

func New(ctx context.Context, d int, p int, shardSize int) *ReedSolomon {
	rs, _ := reedsolomon.New(d, p)
	b := make([][]byte, d+p)
	for i := range b {
		b[i] = make([]byte, shardSize)
	}
	return &ReedSolomon{
		lock:         sync.Mutex{},
		rs:           rs,
		ctx:          ctx,
		blocks:       b,
		s2c:          make(map[int]api.Cid),
		shardSize:    shardSize,
		dataShards:   d,
		parityShards: p,
		curShardI:    0,
		curShardJ:    0,
		parityCh:     make(chan ParityShard, 1024),
		parityLen:    0,
		remainShards: 0,
		receAllData:  false,
	}
}

func (rs *ReedSolomon) GetParityCh() <-chan ParityShard {
	return rs.parityCh
}

// Encode take dataShards shard as a batch, if shards no reach dataShards, fill with 0
func (rs *ReedSolomon) Encode(isLast bool) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	if isLast && rs.curShardI == 0 && rs.curShardJ == 0 {
		// no data, no need to encode
		rs.curShardI = 0
		rs.receAllData = isLast
		close(rs.parityCh)
		return
	}
	rs.align()
	rs.remainShards += rs.parityShards
	err := rs.rs.Encode(rs.blocks)
	if err != nil {
		log.Errorf("reedsolomon encode error:%v", err)
		return
	}
	for _, b := range rs.blocks[rs.dataShards:] {
		// make a new slice to copy b and send out
		out := make([]byte, len(b))
		copy(out, b)
		log.Infof("reedsolomon encode %s", fmt.Sprintf("parity-shard-%d", rs.parityLen))
		rs.parityCh <- ParityShard{
			RawData: out,
			Name:    fmt.Sprintf("parity-shard-%d", rs.parityLen),
			Links:   rs.s2c,
		}
		rs.parityLen += 1
		rs.remainShards -= 1
	}
	if isLast {
		close(rs.parityCh)
	}
	rs.s2c = make(map[int]api.Cid)
	rs.receAllData = isLast
}

func (rs *ReedSolomon) ReConstruct(data [][]byte, replaceRows []int, parity [][]byte) error {
	err := rs.rs.Replace(data, replaceRows, parity)
	if err != nil {
		return err
	}
	return nil
}

// HandleBlock receive block and end with a shard
// cid == CidUndef && b != nil -> block
// cid != CidUndef && b != nil -> last block of shard
// cid == CidUndef && b == nil -> end of file
func (rs *ReedSolomon) HandleBlock(b []byte, cid api.Cid) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	if rs.curShardJ+len(b) < rs.shardSize {
		copy(rs.blocks[rs.curShardI][rs.curShardJ:], b)
		rs.curShardJ += len(b)
	}
	if cid.Equals(api.CidUndef) && b == nil {
		// end of file
		go rs.Encode(true)
	} else if !cid.Equals(api.CidUndef) {
		rs.s2c[rs.curShardI] = cid
		rs.curShardI, rs.curShardJ = rs.curShardI+1, 0
		// last block of shard
		if rs.curShardI == rs.dataShards {
			go rs.Encode(false)
		}
	}
}

func (rs *ReedSolomon) align() {
	// align shards to the same size, fill the empty data shard with 0
	CBytes := (*[1 << 30]byte)(unsafe.Pointer(&rs.blocks[rs.curShardI][rs.curShardJ]))[: rs.shardSize-rs.curShardJ : rs.shardSize-rs.curShardJ]
	CBytes = bytes.Repeat([]byte{0}, len(CBytes))
	rs.curShardI, rs.curShardJ = 0, 0
}

func (rs *ReedSolomon) ReceAllData() bool {
	return rs.receAllData == true && rs.remainShards == 0 && len(rs.parityCh) == 0
}
