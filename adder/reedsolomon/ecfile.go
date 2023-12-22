package reedsolomon

import "github.com/ipfs-cluster/ipfs-cluster/api"

type Shard struct {
	api.Cid
	Name    string
	RawData []byte
	Links   map[int]api.Cid // Links to blocks
}

type Data struct { // sharding node
	Name string
	api.Cid
	shards []Shard
}

type Parity struct {
	Name string
	api.Cid
	shard []Shard
}

type ECFile struct {
	Data
	Parity
}
