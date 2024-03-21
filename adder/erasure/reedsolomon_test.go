package erasure

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"testing"

	"github.com/ipfs-cluster/ipfs-cluster/adder/ipfsadd"
	"github.com/ipfs-cluster/ipfs-cluster/test"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/multiformats/go-multihash"

	"github.com/ipfs-cluster/ipfs-cluster/api"
	"github.com/ipfs/boxo/files"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

var nodeCh chan ipld.Node

// test more type in the future
func TestAll(t *testing.T) {
	blockSize := 256 * 1024
	shardSize := blockSize*2 + 1
	fileSize := shardSize*4 + 1
	nodeCh = make(chan ipld.Node, 1024)
	dir, sth := NewDirectory(t)
	defer sth.Clean(t)
	SplitAndRecon(t, test.NewMockDAGService(false), NewRandFile(fileSize), shardSize)
	SplitAndRecon(t, test.NewMockDAGService(false), dir, shardSize)
}

func NewNodeFromBytesAndDAGService(file files.Node, dg ipld.DAGService) (ipld.Node, error) {
	params := api.DefaultAddParams()
	iadder, err := ipfsadd.NewAdder(context.Background(), newMockCDAGServ(dg), nil)
	iadder.Trickle = params.Layout == "trickle"
	iadder.RawLeaves = params.RawLeaves
	iadder.Chunker = params.Chunker
	iadder.Progress = params.Progress
	iadder.NoCopy = params.NoCopy

	// Set up prefix
	prefix, err := merkledag.PrefixForCidVersion(params.CidVersion)
	if err != nil {
		return nil, fmt.Errorf("bad CID Version: %s", err)
	}

	hashFunCode, ok := multihash.Names[strings.ToLower(params.HashFun)]
	if !ok {
		return nil, errors.New("hash function name not known")
	}
	prefix.MhType = hashFunCode
	prefix.MhLength = -1
	iadder.CidBuilder = &prefix
	return iadder.AddAllAndPin(file)
}

func SplitAndRecon(t *testing.T, dgs ipld.DAGService, file files.Node, shardSize int) {
	fileSize, _ := file.Size()
	rs := New(context.Background(), 4, 2, shardSize)
	// send blocks and receive parityVects shards
	dataVects := make([][]byte, 1)
	root, err := NewNodeFromBytesAndDAGService(file, dgs)
	assert.Nil(t, err)
	curSize := 0
	sum := 0
	for {
		n := <-nodeCh
		s, _ := n.Size()
		if curSize+int(s) > shardSize {
			rs.SendBlockTo() <- StatBlock{Stat: ShardEndBlock}
			curSize = 0
			dataVects = append(dataVects, make([]byte, 0))
		}
		curSize += int(s)
		sum += int(s)
		dataVects[len(dataVects)-1] = append(dataVects[len(dataVects)-1], n.RawData()...)
		rs.SendBlockTo() <- StatBlock{Node: n, Stat: DefaultBlock}
		fmt.Printf("fileSize:%d, sum:%d, cursize:%d, shardSize:%d\n", fileSize, sum, curSize, shardSize)
		if root.Cid() == n.Cid() {
			if curSize > 0 {
				rs.SendBlockTo() <- StatBlock{Stat: ShardEndBlock}
			}
			rs.SendBlockTo() <- StatBlock{Stat: FileEndBlock}
			break
		}
	}
	parityVects := make([][]byte, 0)
	for {
		p := <-rs.GetParity()
		if p.Name == "" { // channel closed
			break
		}
		parityVects = append(parityVects, p.RawData)
	}

	// record the size of origin data shards, for verify
	dShardSize := make([]int, len(dataVects))
	for i, vect := range dataVects {
		dShardSize[i] = len(vect)
	}
	batchNum := (len(dataVects) + rs.dataShards - 1) / rs.dataShards
	for i := 0; i < batchNum; i++ {
		preData0 := make([]byte, len(dataVects[i*rs.dataShards]))
		copy(preData0, dataVects[i*rs.dataShards])
		dataVects[i*rs.dataShards] = nil
		shardCh := make(chan Shard, 6)
		dl := i * rs.dataShards
		dr := min((i+1)*rs.dataShards, len(dShardSize))
		pl := i * rs.parityShards
		pr := (i + 1) * rs.parityShards
		batchDataShards := dr - dl
		go func(i int) {
			defer close(shardCh)
			for j := dl; j < dr; j++ {
				fmt.Printf("send data shard:%d, len(%d)\n", j-dl, len(dataVects[j]))
				shardCh <- Shard{Idx: j - dl, RawData: dataVects[j]}
			}
			for j := pl; j < pr; j++ {
				fmt.Printf("send parity shard:%d, len(%d)\n", j-pl+batchDataShards, len(parityVects[j]))
				shardCh <- Shard{Idx: j - pl + batchDataShards, RawData: parityVects[j]}
			}
		}(i)
		err, batch := rs.BatchRecon(context.Background(),"", i, dShardSize[dl:dr], shardCh)
		assert.Nil(t, err)
		assert.Equal(t, preData0, batch.Shards[0])
	}
}

func NewRandFile(fileSize int) files.Node {
	data := make([]byte, fileSize)
	rand.Read(data)
	return files.NewMapDirectory(map[string]files.Node{"Reed-Solomon-test-file": files.NewBytesFile(data)})
}

func NewDirectory(t *testing.T) (files.Node, *test.ShardingTestHelper) {
	sth := test.NewShardingTestHelper()
	dir := sth.GetTreeSerialFile(t)
	return files.NewMapDirectory(map[string]files.Node{"Reed-Solomon-test-dir": dir}), sth
}

type mockCDAGServ struct {
	ipld.DAGService
}

func newMockCDAGServ(dgs ipld.DAGService) *mockCDAGServ {
	return &mockCDAGServ{
		DAGService: dgs,
	}
}

func (dgs *mockCDAGServ) Add(ctx context.Context, node ipld.Node) error {
	// send node to RS
	nodeCh <- node
	return dgs.DAGService.Add(ctx, node)
}

func (dgs *mockCDAGServ) Finalize(ctx context.Context, root api.Cid) (api.Cid, error) {
	return root, nil
}

func (dgs *mockCDAGServ) Allocations() []peer.ID {
	return nil
}

func (dgs *mockCDAGServ) GetRS() *ReedSolomon {
	return nil
}

func (dgs *mockCDAGServ) SetParity(name string) {
}

func (dgs *mockCDAGServ) FlushCurrentShard(ctx context.Context) (cid api.Cid, err error) {
	return api.CidUndef, err
}

func (dgs *mockCDAGServ) Close() error {
	return nil
}
