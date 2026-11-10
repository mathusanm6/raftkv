package raftnode

import (
	"context"
	"testing"
	"time"

	"github.com/jathurchan/raft/raft"
	"github.com/jathurchan/raft/types"
	"github.com/mathusanm6/raftkv/pkg/cache"
	"github.com/mathusanm6/raftkv/pkg/eviction"
	raftkvv1 "github.com/mathusanm6/raftkv/proto/raftkv/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestApplierIdempotentReplayAndSnapshot(t *testing.T) {
	store := cache.NewStore()
	lru := eviction.NewLRU()
	applier := NewApplier(store, lru)

	ctx := context.Background()

	// Apply SET command at index 1
	setCmd := &raftkvv1.RaftCommand{
		CommandId: "cmd-1",
		Payload: &raftkvv1.RaftCommand_Set{
			Set: &raftkvv1.SetCommand{Key: "k1", Value: "v1", Version: 1},
		},
	}
	data, err := proto.Marshal(setCmd)
	require.NoError(t, err)

	_, err = applier.Apply(ctx, types.Index(1), data)
	require.NoError(t, err)
	require.Equal(t, uint64(1), store.LastApplied())

	val, ver, found := store.Get("k1")
	require.True(t, found)
	require.Equal(t, "v1", val)
	require.Equal(t, int64(1), ver)
	require.Equal(t, 1, lru.Len())

	// Reapply same index 1 with different value: MUST be skipped (idempotent)
	setCmd2 := &raftkvv1.RaftCommand{
		CommandId: "cmd-1-dup",
		Payload: &raftkvv1.RaftCommand_Set{
			Set: &raftkvv1.SetCommand{Key: "k1", Value: "v2-modified", Version: 2},
		},
	}
	data2, err := proto.Marshal(setCmd2)
	require.NoError(t, err)

	_, err = applier.Apply(ctx, types.Index(1), data2)
	require.NoError(t, err)

	val, ver, found = store.Get("k1")
	require.True(t, found)
	require.Equal(t, "v1", val, "Replayed index should be ignored")
	require.Equal(t, int64(1), ver)

	// Take snapshot
	idx, snapBytes, err := applier.Snapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, types.Index(1), idx)

	// Restore into new store and applier
	newStore := cache.NewStore()
	newLRU := eviction.NewLRU()
	newApplier := NewApplier(newStore, newLRU)

	err = newApplier.RestoreSnapshot(ctx, types.Index(1), types.Term(1), snapBytes)
	require.NoError(t, err)

	require.Equal(t, uint64(1), newStore.LastApplied())
	val, ver, found = newStore.Get("k1")
	require.True(t, found)
	require.Equal(t, "v1", val)
	require.Equal(t, 1, newLRU.Len())
}

// Mock Raft implementation for unit testing ReadIndex and Proposals
type mockRaftNode struct {
	raft.RPCHandler

	isLeader     bool
	commitIndex  types.Index
	lastApplied  types.Index
	readIndexErr error
	proposeErr   error
	proposeCalls [][]byte
}

func (m *mockRaftNode) SetNetworkManager(nm raft.NetworkManager) {}
func (m *mockRaftNode) Start() error                             { return nil }
func (m *mockRaftNode) Stop(ctx context.Context) error           { return nil }
func (m *mockRaftNode) Tick(ctx context.Context)                 {}

func (m *mockRaftNode) Propose(ctx context.Context, cmd []byte) (types.Index, types.Term, bool, error) {
	if m.proposeErr != nil {
		return 0, 0, m.isLeader, m.proposeErr
	}
	m.proposeCalls = append(m.proposeCalls, cmd)
	return m.commitIndex + 1, 1, m.isLeader, nil
}

func (m *mockRaftNode) ReadIndex(ctx context.Context) (types.Index, error) {
	if m.readIndexErr != nil {
		return 0, m.readIndexErr
	}
	return m.commitIndex, nil
}

func (m *mockRaftNode) Status() types.RaftStatus {
	role := types.RoleFollower
	if m.isLeader {
		role = types.RoleLeader
	}
	return types.RaftStatus{
		ID:          "mock-0",
		Role:        role,
		Term:        1,
		LeaderID:    "mock-0",
		CommitIndex: m.commitIndex,
		LastApplied: m.lastApplied,
	}
}

func (m *mockRaftNode) GetState() (types.Term, bool)          { return 1, m.isLeader }
func (m *mockRaftNode) GetLeaderID() types.NodeID             { return "mock-0" }
func (m *mockRaftNode) GetCommitIndex() types.Index           { return m.commitIndex }
func (m *mockRaftNode) ApplyChannel() <-chan types.ApplyMsg   { return nil }
func (m *mockRaftNode) LeaderChangeChannel() <-chan types.NodeID { return nil }

func TestReadIndexCoalescerNextRoundAndExecution(t *testing.T) {
	store := cache.NewStore()
	store.ApplySet("k1", "v1", 1, 10)
	lru := eviction.NewLRU()

	node := &mockRaftNode{
		isLeader:    true,
		commitIndex: 10,
		lastApplied: 10,
	}

	coalescer := NewReadIndexCoalescer(node, store, lru)
	defer coalescer.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	val, ver, found, err := coalescer.Read(ctx, "k1")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "v1", val)
	require.Equal(t, int64(1), ver)
}

func TestProposalManagerHardAndSoftLimits(t *testing.T) {
	store := cache.NewStore()
	lru := eviction.NewLRU()
	applier := NewApplier(store, lru)

	node := &mockRaftNode{
		isLeader:    true,
		commitIndex: 1,
		lastApplied: 1,
	}

	pm := NewProposalManager(node, store, applier, lru)

	// Fill store up to near hard limit
	// MaxMemoryBytes: 67,108,864
	// Set entryBytes to exceed hard limit
	store.MemoryTracker().Reset(cache.MaxMemoryBytes - cache.TombstoneReserveBytes)

	ctx := context.Background()
	err := pm.ProposeSet(ctx, "k1", "v1", 1)
	require.ErrorIs(t, err, ErrResourceExhausted, "Hard limit must reject SET proposals before consensus")
}
