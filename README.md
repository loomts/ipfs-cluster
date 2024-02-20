## IPFS Cluster(Erasure Coding Support)
### Warning
This project is in progress.

### Motivation
IPFS-Cluster is a good project for data orchestration on IPFS. But it does not support erasure coding, which means that we need to use multiple memory for fault tolerance. But it can be solved by adding a [Reed-Solomon](https://github.com/klauspost/reedsolomon) module. See [discuss](https://discuss.ipfs.tech/t/is-there-an-implementation-of-ipfs-that-includes-erasure-coding-like-reed-solomon-right-now/17052/9).

### Overview
This work can be divided into three parts.
1. Data Addition: First is obtain data. Since data can only be accessed once, use `DAGService.Get` get the block data and send it to Erasure module during MerkleDAG traversal. Once Erasure module receives enough data shards, it use ReedSolomon encodes parity shards and send them to `adder`. Then adder reuses `single/dag_service` add them to IPFS as several individual files.
2. Shard Allocation: We need to decide which nodes are suitable to each shard. The implementation ensures that large number of peers store the data shards, and more than one peer stores the parity shards. And each shard **only** will store by one peer when added(but allocation may change when shard broken and recovery). See `DefaultECAllocate` for details. After determining allocation of shards, we use the RPC Call `IPFSConnector.BlockStream` to send blocks, and `Cluster.Pin` to pin **remotely** or locally. 
3. Data Recovery: We use `clusterPin` store the cid of data and parity shards as well as the size of data shards. During reconstruction, we set one minute as timeout and attempt to retrieve data and parity shards separately. If some shards are broken, we finally use ReedSolomon module to reconstruct and repin the file. **However, ReedSolomon has a limit**, only the sum of the number of existing data shards and party shards needs to greater than total data shards, can we reconstruct all data shards and piece together the complete data.

### Usage
It's exactly the same as ipfs cluster. First, we need to start the IPFS daemon and the IPFS cluster daemon, and then interact with the IPFS cluster daemon through ipfs-cluster-ctl. [See documentation](https://ipfscluster.io/documentation/deployment/setup/)

The only difference is that each binary executable needs to be replaced.

#### scripts

```zsh
# copy to ~/.bashrc || ~/.zshrc 

alias dctl="$GOPATH/src/ipfs-cluster/cmd/ipfs-cluster-ctl/ipfs-cluster-ctl"
alias dfollow="$GOPATH/src/ipfs-cluster/cmd/ipfs-cluster-follow/ipfs-cluster-follow"
alias dservice="$GOPATH/src/ipfs-cluster/cmd/ipfs-cluster-service/ipfs-cluster-service"

alias fctl="$GOPATH/src/ipfs-cluster/cmd/ipfs-cluster-ctl/ipfs-cluster-ctl --host /unix/$HOME/.ipfs-cluster-follow/ali/api-socket" #Communicate with the ipfs-cluster-follow

alias cctl="ipfs-cluster-ctl"
alias cfollow="ipfs-cluster-follow"
alias cservice="ipfs-cluster-service"

export GOLOG_LOG_LEVEL="info,subsystem1=warn,subsystem2=debug" # github.com/ipfs/go-log set log level

# fastly start the cluster using Docker
alias dctlmake='
cd $GOPATH/src/ipfs-cluster/cmd/ipfs-cluster-ctl && make
cd $GOPATH/src/ipfs-cluster

docker build -t ipfs-cluster-erasure -f Dockerfile-erasure .
docker-compose -f docker-compose-erasure.yml up -d

docker logs -f cluster0
'

# little change may let test fail
dctltest() {
  cd $GOPATH/src/ipfs-cluster
  make cmd/ipfs-cluster-ctl
  docker build -t ipfs-cluster-erasure -f Dockerfile-erasure .
  docker-compose -f docker-compose-erasure.yml up -d
  sleep 5

  # QmSxdRX48W7PeS4uNEmhcx4tAHt7rzjHWBwLHetefZ9AvJ is the cid of tmpfile
  ci="QmSxdRX48W7PeS4uNEmhcx4tAHt7rzjHWBwLHetefZ9AvJ"
  dctl pin rm $ci

  seq 1 250000 > tmpfile
  dctl add tmpfile -n tmpfile --erasure --shard-size 512000 # --data-shards 4 --parity-shards 2

  # find frist peer no equal cluster0 and store sharding data
  # awk '$1 == 1 && $2 != 0 {print $2}' means that find the peer that store one shard and it's id not cluster0(cluster0 expose port)
  x=$(dctl status --filter pinned | grep -A 2 tmpfile | awk -F'cluster' '{print $2}' | awk '{print $1}' | sort | uniq -c | awk '$1 == 3 && $2 != 0 {print $2}' | head -n 1)
  docker stop "cluster$x" "ipfs$x"

  dctl ipfs gc # clean ipfs cache
  sleep 5

  dctl ecget $ci
  diff $ci tmpfile > /dev/null
  if [ $? -eq 0 ]; then
      echo "Files are identical, test pass ^^"
  else
      echo "Files are different, test fail :D"
  fi
  dctl pin rm $ci
  rm $ci
  rm tmpfile
}

```

P.S. If you notice no disk space left, use `docker system df` to check docker cache :)

#### ipfs-cluster-ctl new command

`add <filepath> --erasure`: Added file by erasure coding, build `ipfs-cluster-ctl` and type `ipfs-cluster-ctl add -h` for details.
> P.S. Using --erasure also force enables raw-leaves and shard

`ecget`: Get erasure file by cid, if file was broken(canot get all shards) it will automatically recovery it. 
> P.S. Shell command can download file and directory directly. But rpc `Cluster.ECGet` can only retrieve tar archived []byte by stream, so you need to use `tar.Extractor` to extract it to FileSystem.

`ecrecovery`: Scan all erasure coding files pinned, if some files broken then try to recover.

### TODO
This project currently supports fundamental features about Erasure Code. However, there are something need to be optimizated:

1. At present, we use `sharding/dag_service` to store the original file and `single/dag_service` to store single files. A more elegant solution would be to create a new `adder` module to combine them.
2. Support for block-level erasure. ~~set shard size=defaultBlockSize is block-level erasure~~
3. When using `single/dag_service` to add parity shards as individual files, make `api.NodeWithMeta` and `sync.Once` as a slice is simple but stupid solution to prevent multiple invocations of Finalize. Each parity shard uses a unique `api.NodeWithMeta` and `sync.Once`, avoiding conflicts. However, this approach disrupts the original structure of `single/dag_service`, it return TODO1.

---
[![Made by](https://img.shields.io/badge/By-Protocol%20Labs-000000.svg?style=flat-square)](https://protocol.ai)
[![Main project](https://img.shields.io/badge/project-ipfs--cluster-ef5c43.svg?style=flat-square)](http://github.com/ipfs-cluster)
[![Discord](https://img.shields.io/badge/forum-discuss.ipfs.io-f9a035.svg?style=flat-square)](https://discuss.ipfs.io/c/help/help-ipfs-cluster/24)
[![Matrix channel](https://img.shields.io/badge/matrix-%23ipfs--cluster-3c8da0.svg?style=flat-square)](https://app.element.io/#/room/#ipfs-cluster:ipfs.io)
[![pkg.go.dev](https://pkg.go.dev/badge/github.com/ipfs-cluster/ipfs-cluster)](https://pkg.go.dev/github.com/ipfs-cluster/ipfs-cluster)
[![Go Report Card](https://goreportcard.com/badge/github.com/ipfs-cluster/ipfs-cluster)](https://goreportcard.com/report/github.com/ipfs-cluster/ipfs-cluster)
[![codecov](https://codecov.io/gh/ipfs-cluster/ipfs-cluster/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs-cluster/ipfs-cluster)

> Pinset orchestration for IPFS

<p align="center">
<img src="https://ipfscluster.io/cluster/png/IPFS_Cluster_color_no_text.png" alt="logo" width="300" height="300" />
</p>

[IPFS Cluster](https://ipfscluster.io) provides data orchestration across a swarm of IPFS daemons by allocating, replicating and tracking a global pinset distributed among multiple peers.

There are 3 different applications:

* A cluster peer application: `ipfs-cluster-service`, to be run along with `kubo` (`go-ipfs`) as a sidecar.
* A client CLI application: `ipfs-cluster-ctl`, which allows easily interacting with the peer's HTTP API.
* An additional "follower" peer application: `ipfs-cluster-follow`, focused on simplifying the process of configuring and running follower peers.

---

### Are you using IPFS Cluster?

Please participate in the [IPFS Cluster user registry](https://docs.google.com/forms/d/e/1FAIpQLSdWF5aXNXrAK_sCyu1eVv2obTaKVO3Ac5dfgl2r5_IWcizGRg/viewform).

---

## Table of Contents

- [IPFS Cluster(Erasure Coding Support)](#ipfs-clustererasure-coding-support)
  - [Warning](#warning)
  - [Motivation](#motivation)
  - [Overview](#overview)
  - [Usage](#usage)
    - [scripts](#scripts)
    - [ipfs-cluster-ctl new command](#ipfs-cluster-ctl-new-command)
  - [TODO](#todo)
  - [Are you using IPFS Cluster?](#are-you-using-ipfs-cluster)
- [Table of Contents](#table-of-contents)
- [Documentation](#documentation)
- [News \& Roadmap](#news--roadmap)
- [Install](#install)
- [Usage](#usage-1)
- [Contribute](#contribute)
- [License](#license)


## Documentation

Please visit https://ipfscluster.io/documentation/ to access user documentation, guides and any other resources, including detailed **download** and **usage** instructions.

## News & Roadmap

We regularly post project updates to https://ipfscluster.io/news/ .

The most up-to-date *Roadmap* is available at https://ipfscluster.io/roadmap/ .

## Install

Instructions for different installation methods (including from source) are available at https://ipfscluster.io/download .

## Usage

Extensive usage information is provided at https://ipfscluster.io/documentation/ , including:

* [Docs for `ipfs-cluster-service`](https://ipfscluster.io/documentation/reference/service/)
* [Docs for `ipfs-cluster-ctl`](https://ipfscluster.io/documentation/reference/ctl/)
* [Docs for `ipfs-cluster-follow`](https://ipfscluster.io/documentation/reference/follow/)

## Contribute

PRs accepted. As part of the IPFS project, we have some [contribution guidelines](https://ipfscluster.io/support/#contribution-guidelines).

## License

This library is dual-licensed under Apache 2.0 and MIT terms.

© 2022. Protocol Labs, Inc.
