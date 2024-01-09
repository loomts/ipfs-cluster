## IPFS Cluster(Erasure Coding Support)
### Warning
This project is in progress.

### Motivation
IPFS-Cluster is a good project for data orchestration on IPFS. But it does not support erasure coding, which means that we need to use multiple memory for fault tolerance. But it can be solved by adding a [Reed-Solomon](https://github.com/klauspost/reedsolomon) layer. See [discuss](https://discuss.ipfs.tech/t/is-there-an-implementation-of-ipfs-that-includes-erasure-coding-like-reed-solomon-right-now/17052/9).

### Overview

This work can be divided into three parts.
1. Data Addition: First is obtain data. Since data can only be accessed once, we must use `DAGService.Get` get the block data and send it to Erasure module during DAG traversal. Once Erasure module receives enough data shards, it use ReedSolomon encodes parity shards and send them to `adder`. Then adder reuses `single/dag_service` add them to IPFS as several individual files.
2. Shard Allocation: We need to decide which nodes are suitable to each shard. The implementation ensures that large number of peers store the data shards, and more than one peer stores the parity shards. See `ShardAllocate` for details. After determining allocation of shards, we use the RPC Call `IPFSConnector.BlockStream` to send blocks, and `Cluster.Pin` to pin **remotely** or locally. Therefore, I have enabled the `RPCTrusted` permission for `Cluster.Pin`.
3. Data Recovery: We use `clusterPin` store the cid of data and parity shards as well as the size of data shards. During reconstruction, we set a timeout and attempt to retrieve data and parity shards separately. If some data shards are broken, we finally use ReedSolomon module to reconstruct and repin the file.

### TODO
1. Send parity shards to one machine via a single stream.
2. Currently, we use the sharding `dag_service` to store the original file and `single/dag_service` to store single files. We need to create a new `adder` module to combine them.
3. `ECGet` can only retrieve data with one loop of links, and does not enable DFS traverse DAG.


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
  - [TODO](#todo)
  - [Are you using IPFS Cluster?](#are-you-using-ipfs-cluster)
- [Table of Contents](#table-of-contents)
- [Documentation](#documentation)
- [News \& Roadmap](#news--roadmap)
- [Install](#install)
- [Usage](#usage)
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
