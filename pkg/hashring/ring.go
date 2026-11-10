package hashring

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
)

const (
	// DefaultVirtualNodes is the number of virtual nodes per shard specified in design doc §2.2.
	DefaultVirtualNodes = 256
)

type vnode struct {
	token  uint32
	member string
}

// Ring implements a static consistent-hash ring with virtual nodes.
type Ring struct {
	vnodes       []vnode
	members      []string
	virtualNodes int
}

// NewRing creates a static consistent-hash ring for the given members.
func NewRing(members []string, virtualNodes int) *Ring {
	if virtualNodes <= 0 {
		virtualNodes = DefaultVirtualNodes
	}

	r := &Ring{
		members:      make([]string, len(members)),
		virtualNodes: virtualNodes,
	}
	copy(r.members, members)

	for _, member := range members {
		for v := 0; v < virtualNodes; v++ {
			token := hashKey(fmt.Sprintf("%s#vnode-%d", member, v))
			r.vnodes = append(r.vnodes, vnode{
				token:  token,
				member: member,
			})
		}
	}

	sort.Slice(r.vnodes, func(i, j int) bool {
		return r.vnodes[i].token < r.vnodes[j].token
	})

	return r
}

func hashKey(key string) uint32 {
	h := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint32(h[:4])
}

// GetShard returns the member responsible for the given key.
// The ring is static and does not rebalance or remap when a shard fails.
func (r *Ring) GetShard(key string) string {
	if len(r.vnodes) == 0 {
		return ""
	}

	token := hashKey(key)
	idx := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].token >= token
	})

	if idx == len(r.vnodes) {
		idx = 0
	}

	return r.vnodes[idx].member
}

// Members returns the list of configured members.
func (r *Ring) Members() []string {
	res := make([]string, len(r.members))
	copy(res, r.members)
	return res
}
