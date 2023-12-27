## IPFS Cluster(Erasure Coding Support)
### Warning
1. This project is in progress.
2. Change the `Cluster.Pin` RPC promission to RPCTrusted in order to Pin Peers.

### Motivation
IPFS-Cluster is a good project for data orchestration on IPFS. But it unsupport erasure coding, which means that we should use multiple memory to fault tolerance. But it can be solve by adding a [Reed-Solomon]() layer. But Unforunterly, there is also something wrong with IFPS-Cluster sharding, see [discuss](https://discuss.ipfs.tech/t/ipfs-cluster-sharding-and-recursive-partial-pin/8035).

### Overview

This work can be devide into three part.
1. Add, figure out how to obtain data. Because data can only be accessed once, we can only get the blocks data. So I also send blocks to Reed-Solomon when send to IPFS, When the Reed-Solomon module receive 6 data shards(configable) it will generate 3 parity shards and send them to `adder`. While adder receive parity shards, it will use single/dag_service add them to IPFS.
2. Allocate, descide which shard send to which node. The implementation make more peers store the data shards, and more than one peer store the parity shards. See `ShardAllocate` for details. After figure who to store the shard, then use RPC Call `IPFSConnector.BlockStream` to send blocks, and `Cluster.Pin` to **remote** or local Pin. So I open the `RPCTrusted` promission for `Cluster.Pin`.
3. Get, readjust the shard struct to metaPin->clusterPin->parityClusterPin, metaPin point to clusterPin, clusterPin store the data shards cids and point to parityClusterPin, parityClusterPin store the parity shards cids. when get command receive, cluster will first use simple dag get try to get all data by `BlockGet`, then send out. If cannot get all data, then get data shards and parity separately, then use reedsolomon try to reconstruct.

### TODO
1. Now use the sharding dag_service to store the origin file and single dag_service to store single file. Many be is a method to combine them with a new dag_service.
2. After reconstructing, re add the data
3. Send parity shards to one machine by one stream 
4. batch reconstruct
5. check why total shard size not fit
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
