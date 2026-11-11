package raftnode

import (
	"context"
	"fmt"
	"testing"

	"github.com/jathurchan/raft/types"
	"github.com/mathusanm6/raftkv/pkg/cache"
	"github.com/mathusanm6/raftkv/pkg/eviction"
	raftkvv1 "github.com/mathusanm6/raftkv/proto/raftkv/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func applySets(t *testing.T, a *Applier, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		data, err := proto.Marshal(&raftkvv1.RaftCommand{
			CommandId: fmt.Sprintf("set-%d", i),
			Payload: &raftkvv1.RaftCommand_Set{
				Set: &raftkvv1.SetCommand{Key: fmt.Sprintf("k%03d", i), Value: "v", Version: 1},
			},
		})
		require.NoError(t, err)
		_, err = a.Apply(context.Background(), types.Index(i+1), data)
		require.NoError(t, err)
	}
}

func TestAccessReplicatorBuffersAndCapsCounts(t *testing.T) {
	node := &mockRaftNode{isLeader: true}
	r := NewAccessReplicator(node)

	for i := 0; i < 40; i++ {
		r.RecordAccess("hot")
	}
	r.RecordAccess("cold")
	r.Flush()

	require.Len(t, node.proposeCalls, 1)
	var cmd raftkvv1.RaftCommand
	require.NoError(t, proto.Unmarshal(node.proposeCalls[0], &cmd))

	counts := map[string]uint32{}
	for _, it := range cmd.GetAccess().GetItems() {
		counts[it.Key] = it.Count
	}
	require.Equal(t, map[string]uint32{"hot": AccessMaxCount, "cold": 1}, counts)

	// An empty buffer proposes nothing.
	r.Flush()
	require.Len(t, node.proposeCalls, 1)

	// Discarded counts are never proposed.
	r.RecordAccess("hot")
	r.Discard()
	r.Flush()
	require.Len(t, node.proposeCalls, 1)
}

// A follower that applies the leader's ACCESS batches keeps the hot keys, so on
// promotion it evicts the same victims the old leader would have.
func TestAccessBatchKeepsFollowerPoliciesInStep(t *testing.T) {
	const numKeys = 200
	hot := []string{"k000", "k001", "k002", "k003", "k004"}

	node := &mockRaftNode{isLeader: true}
	r := NewAccessReplicator(node)
	for _, k := range hot {
		for i := 0; i < 10; i++ {
			r.RecordAccess(k)
		}
	}
	r.Flush()
	require.Len(t, node.proposeCalls, 1)

	followers := make([]*eviction.WTinyLFU, 2)
	stores := make([]*cache.Store, 2)
	for i := range followers {
		followers[i] = eviction.NewWTinyLFU(100)
		stores[i] = cache.NewStore()
		a := NewApplier(stores[i], followers[i])
		applySets(t, a, numKeys)
		_, err := a.Apply(context.Background(), types.Index(numKeys+1), node.proposeCalls[0])
		require.NoError(t, err)
	}

	require.Equal(t, uint64(numKeys+1), stores[0].LastApplied(), "ACCESS must advance the applied index")

	victims := followers[0].SelectVictims(20)
	require.Equal(t, victims, followers[1].SelectVictims(20), "replicas must agree on victims")
	for _, k := range hot {
		require.NotContains(t, victims, k)
	}

	// v1 behaviour: a promoted leader rebuilds oldest-first and evicts hot keys.
	cold := eviction.NewColdStartWrapper(eviction.NewWTinyLFU(100), nil)
	keys := make([]string, numKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%03d", i)
	}
	cold.StartColdStart(keys)
	require.Subset(t, cold.SelectVictims(20), hot)
}

func TestAccessBatchReplayIsIdempotent(t *testing.T) {
	store := cache.NewStore()
	lfu := eviction.NewWTinyLFU(100)
	a := NewApplier(store, lfu)
	applySets(t, a, 3)

	data, err := proto.Marshal(&raftkvv1.RaftCommand{
		CommandId: "access-1",
		Payload: &raftkvv1.RaftCommand_Access{
			Access: &raftkvv1.AccessBatchCommand{Items: []*raftkvv1.AccessItem{{Key: "k000", Count: 3}}},
		},
	})
	require.NoError(t, err)

	_, err = a.Apply(context.Background(), types.Index(4), data)
	require.NoError(t, err)
	before := lfu.SelectVictims(3)

	// Replaying the same index must not record the hits again.
	_, err = a.Apply(context.Background(), types.Index(4), data)
	require.NoError(t, err)
	require.Equal(t, before, lfu.SelectVictims(3))
	require.Equal(t, uint64(4), store.LastApplied())
}
