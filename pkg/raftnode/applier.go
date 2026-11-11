package raftnode

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/jathurchan/raft/types"
	"github.com/mathusanm6/raftkv/pkg/cache"
	"github.com/mathusanm6/raftkv/pkg/eviction"
	raftkvv1 "github.com/mathusanm6/raftkv/proto/raftkv/v1"
	"google.golang.org/protobuf/proto"
)

// ProposalResult represents the completion notification for a leader proposal.
type ProposalResult struct {
	Applied bool
	Err     error
}

// Applier implements raft.Applier to bridge the consensus layer with the cache store.
type Applier struct {
	mu sync.Mutex

	store    *cache.Store
	eviction eviction.Policy

	// Pending proposal completion channels keyed by command ID
	waiters map[string]chan ProposalResult
}

// NewApplier creates an Applier backed by the cache store and eviction policy.
func NewApplier(store *cache.Store, evictionPolicy eviction.Policy) *Applier {
	return &Applier{
		store:    store,
		eviction: evictionPolicy,
		waiters:  make(map[string]chan ProposalResult),
	}
}

// RegisterWaiter registers a completion channel for a pending proposal.
func (a *Applier) RegisterWaiter(cmdID string, ch chan ProposalResult) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.waiters[cmdID] = ch
}

// UnregisterWaiter removes a waiter channel.
func (a *Applier) UnregisterWaiter(cmdID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.waiters, cmdID)
}

// AbortWaiters cancels all pending proposal waiters (e.g. on leadership step-down).
func (a *Applier) AbortWaiters(err error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for cmdID, ch := range a.waiters {
		select {
		case ch <- ProposalResult{Applied: false, Err: err}:
		default:
		}
		delete(a.waiters, cmdID)
	}
}

// Apply executes committed log entries deterministically on the cache state machine.
func (a *Applier) Apply(_ context.Context, index types.Index, command []byte) (any, error) {
	var cmd raftkvv1.RaftCommand
	if err := proto.Unmarshal(command, &cmd); err != nil {
		return nil, fmt.Errorf("failed to unmarshal RaftCommand at index %d: %w", index, err)
	}

	applyIdx := uint64(index)
	applied := false

	// Skip state changes if already applied (idempotent replay)
	if applyIdx > a.store.LastApplied() {
		switch p := cmd.Payload.(type) {
		case *raftkvv1.RaftCommand_Set:
			applied = a.store.ApplySet(p.Set.Key, p.Set.Value, p.Set.Version, applyIdx)
			if applied && a.eviction != nil {
				a.eviction.Add(p.Set.Key, 0)
			}

		case *raftkvv1.RaftCommand_Delete:
			applied = a.store.ApplyDelete(p.Delete.Key, p.Delete.Version, applyIdx)
			if applied && a.eviction != nil {
				a.eviction.Remove(p.Delete.Key)
			}

		case *raftkvv1.RaftCommand_Evict:
			evictedCount := a.store.ApplyEvict(p.Evict.Items, applyIdx)
			applied = evictedCount > 0
			if a.eviction != nil {
				for _, item := range p.Evict.Items {
					a.eviction.Remove(item.Key)
				}
			}

		case *raftkvv1.RaftCommand_Access:
			a.store.AdvanceApplied(applyIdx)
			applied = true
			if a.eviction != nil {
				for _, item := range p.Access.Items {
					for i := uint32(0); i < item.Count; i++ {
						a.eviction.RecordAccess(item.Key)
					}
				}
			}
		}
	}

	// Notify leader waiter if registered
	a.mu.Lock()
	if ch, ok := a.waiters[cmd.CommandId]; ok {
		select {
		case ch <- ProposalResult{Applied: applied, Err: nil}:
		default:
		}
		delete(a.waiters, cmd.CommandId)
	}
	a.mu.Unlock()

	return nil, nil
}

// Snapshot returns a point-in-time serialized snapshot of the state machine.
func (a *Applier) Snapshot(_ context.Context) (types.Index, []byte, error) {
	snap, err := a.store.Snapshot()
	if err != nil {
		return 0, nil, err
	}

	data, err := proto.Marshal(snap)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	return types.Index(snap.LastAppliedIndex), data, nil
}

// RestoreSnapshot restores the state machine from a snapshot.
func (a *Applier) RestoreSnapshot(
	_ context.Context,
	lastIncludedIndex types.Index,
	_ types.Term,
	snapshotData []byte,
) error {
	var snap raftkvv1.SnapshotData
	if err := proto.Unmarshal(snapshotData, &snap); err != nil {
		return fmt.Errorf("failed to unmarshal snapshot data: %w", err)
	}

	if err := a.store.RestoreSnapshot(&snap); err != nil {
		return fmt.Errorf("failed to restore snapshot into store: %w", err)
	}

	// Re-populate eviction policy in apply-index order
	if a.eviction != nil {
		a.eviction.Reset()

		entries := snap.Entries
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].ApplyIndex < entries[j].ApplyIndex
		})

		for _, e := range entries {
			a.eviction.Add(e.Key, 0)
		}
	}

	return nil
}
