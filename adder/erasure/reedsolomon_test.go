package erasure

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"testing"

	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/stretchr/testify/assert"
)

func TestSplitAndRecon(t *testing.T) {
	fileSize := 1024*6 + 1
	blockSize := 1024
	shardSize := 1024 * 5
	originData := make([]byte, fileSize)
	_, err := rand.Read(originData)
	assert.Nil(t, err)
	rs := New(context.Background(), 4, 2, shardSize)
	// send blocks and receive parityVects shards
	dataVects := make([][]byte, 1)
	b := make([]byte, blockSize)
	r := bytes.NewReader(originData)
	curSize := 0
	for {
		end := false
		s, err := r.Read(b)
		if err == io.EOF {
			end = true
		}
		bb := make([]byte, s)
		copy(bb, b[:s])
		n := merkledag.NewRawNode(bb)
		if curSize+s > shardSize {
			rs.SendBlockTo() <- StatBlock{Stat: ShardEndBlock}
			curSize = 0
			dataVects = append(dataVects, make([]byte, 0))
		}
		curSize += s
		dataVects[len(dataVects)-1] = append(dataVects[len(dataVects)-1], bb...)
		rs.SendBlockTo() <- StatBlock{Node: n, Stat: DefaultBlock}
		if end {
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
	// verify
	for i, vect := range dataVects {
		assert.Equal(t, originData[i*shardSize:min((i+1)*shardSize, len(originData))], vect)
	}
	dShardSize := make([]int, len(dataVects))
	for i, vect := range dataVects {
		dShardSize[i] = len(vect)
	}
	dataVects[0] = nil
	err = rs.SplitAndRecon(dataVects, parityVects, dShardSize)
	assert.Nil(t, err)
	for i, vect := range dataVects {
		assert.Equal(t, originData[i*shardSize:min((i+1)*shardSize, len(originData))], vect)
	}
}
