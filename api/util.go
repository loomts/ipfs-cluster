package api

import (
	"errors"
	"strings"

	gopath "github.com/ipfs/boxo/path"
	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

// PeersToStrings Encodes a list of peers.
func PeersToStrings(peers []peer.ID) []string {
	strs := make([]string, len(peers))
	for i, p := range peers {
		if p != "" {
			strs[i] = p.String()
		}
	}
	return strs
}

// StringsToPeers decodes peer.IDs from strings.
func StringsToPeers(strs []string) []peer.ID {
	peers := []peer.ID{}
	for _, p := range strs {
		pid, err := peer.Decode(p)
		if err != nil {
			continue
		}
		peers = append(peers, pid)
	}
	return peers
}

var ErrBadPath = errors.New("bad path")

func parseCidToPath(txt string) (gopath.Path, error) {
	ci, err := cid.Decode(txt)
	if err != nil {
		return nil, err
	}
	return gopath.FromCid(ci), nil
}

func ParsePath(txt string) (gopath.Path, error) {
	parts := strings.Split(txt, "/")
	if len(parts) == 1 {
		p, err := parseCidToPath(txt)
		if err == nil {
			return p, nil
		}
	}

	// if the path doesnt begin with a '/'
	// we expect this to start with a hash, and be an 'ipfs' path
	if parts[0] != "" {
		p, err := parseCidToPath(parts[0])
		if err != nil {
			return nil, ErrBadPath
		}
		// The case when the path starts with hash without a protocol prefix
		return p, nil
	}

	if len(parts) < 3 {
		return nil, ErrBadPath
	}

	if parts[1] == "ipfs" {
		p, err := parseCidToPath(parts[2])
		if err != nil {
			return nil, ErrBadPath
		}
		return p, nil
	} else if parts[1] != "ipns" && parts[1] != "ipld" {
		return nil, ErrBadPath
	}

	return gopath.NewPath(txt)
}
