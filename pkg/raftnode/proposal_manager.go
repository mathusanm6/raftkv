package raftnode

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jathurchan/raft/raft"
	"github.com/mathusanm6/raftkv/pkg/cache"
	"github.com/mathusanm6/raftkv/pkg/eviction"
	raftkvv1 "github.com/mathusanm6/raftkv/proto/raftkv/v1"
	"google.golang.org/protobuf/proto"
)

var (
	ErrResourceExhausted = errors.New("memory hard limit reached")
	ErrNotLeader         = errors.New("not the leader")
	ErrUnavailable       = errors.New("leader unavailable or election in progress")
	ErrKeyTooLarge       = errors.New("key exceeds maximum size of 64 bytes")
	ErrValueTooLarge     = errors.New("value exceeds maximum size of 512 bytes")
)

const EvictionBatchSize = 64

// ProposalManager coordinates client proposals, enforces memory limits,
// and schedules eviction batches.
type ProposalManager struct {
	mu sync.Mutex

	node     raft.Raft
	store    *cache.Store
	applier  *Applier
	eviction eviction.Policy

	inFlightSetBytes atomic.Int64
	evictionInFlight atomic.Bool
	inFlightVictims  map[string]bool
}

// NewProposalManager creates a new ProposalManager.
func NewProposalManager(
	node raft.Raft,
	store *cache.Store,
	applier *Applier,
	policy eviction.Policy,
) *ProposalManager {
	return &ProposalManager{
		node:            node,
		store:           store,
		applier:         applier,
		eviction:        policy,
		inFlightVictims: make(map[string]bool),
	}
}

func generateCommandID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// ProposeSet validates limits and proposes a SET operation.
func (pm *ProposalManager) ProposeSet(
	ctx context.Context,
	key, value string,
	version int64,
) error {
	if len(key) > cache.MaxKeyBytes {
		return ErrKeyTooLarge
	}
	if len(value) > cache.MaxValueBytes {
		return ErrValueTooLarge
	}

	entryBytes := cache.EntryAccountedBytes(key, value)

	// Hard limit check (100%): Check strictly BEFORE proposing
	currentUsed := pm.store.MemoryTracker().TotalAccountedBytes()
	inFlight := pm.inFlightSetBytes.Load()
	if currentUsed+inFlight+entryBytes > cache.MaxMemoryBytes {
		return ErrResourceExhausted
	}

	// Soft limit check (90%): Trigger asynchronous eviction batch if needed
	if currentUsed+inFlight+entryBytes >= cache.SoftLimitBytes {
		pm.maybeTriggerEviction()
	}

	pm.inFlightSetBytes.Add(entryBytes)
	defer pm.inFlightSetBytes.Add(-entryBytes)

	cmdID := generateCommandID()
	cmd := &raftkvv1.RaftCommand{
		CommandId: cmdID,
		Payload: &raftkvv1.RaftCommand_Set{
			Set: &raftkvv1.SetCommand{
				Key:     key,
				Value:   value,
				Version: version,
			},
		},
	}

	data, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	resultCh := make(chan ProposalResult, 1)
	pm.applier.RegisterWaiter(cmdID, resultCh)
	defer pm.applier.UnregisterWaiter(cmdID)

	_, _, isLeader, err := pm.node.Propose(ctx, data)
	if err != nil {
		return err
	}
	if !isLeader {
		return ErrNotLeader
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case res := <-resultCh:
		return res.Err
	}
}

// ProposeDelete proposes a DELETE operation.
func (pm *ProposalManager) ProposeDelete(
	ctx context.Context,
	key string,
	version int64,
) error {
	if len(key) > cache.MaxKeyBytes {
		return ErrKeyTooLarge
	}

	cmdID := generateCommandID()
	cmd := &raftkvv1.RaftCommand{
		CommandId: cmdID,
		Payload: &raftkvv1.RaftCommand_Delete{
			Delete: &raftkvv1.DeleteCommand{
				Key:     key,
				Version: version,
			},
		},
	}

	data, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	resultCh := make(chan ProposalResult, 1)
	pm.applier.RegisterWaiter(cmdID, resultCh)
	defer pm.applier.UnregisterWaiter(cmdID)

	_, _, isLeader, err := pm.node.Propose(ctx, data)
	if err != nil {
		return err
	}
	if !isLeader {
		return ErrNotLeader
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case res := <-resultCh:
		return res.Err
	}
}

// maybeTriggerEviction initiates an EVICT batch if one is not already running.
func (pm *ProposalManager) maybeTriggerEviction() {
	if pm.eviction == nil {
		return
	}

	if !pm.evictionInFlight.CompareAndSwap(false, true) {
		return
	}

	go pm.runEvictionBatch()
}

func (pm *ProposalManager) runEvictionBatch() {
	defer pm.evictionInFlight.Store(false)

	pm.mu.Lock()
	candidates := pm.eviction.SelectVictims(EvictionBatchSize)
	items := make([]*raftkvv1.EvictItem, 0, len(candidates))

	for _, k := range candidates {
		if !pm.inFlightVictims[k] {
			if _, ver, found := pm.store.Get(k); found {
				items = append(items, &raftkvv1.EvictItem{
					Key:     k,
					Version: ver,
				})
				pm.inFlightVictims[k] = true
			}
		}
	}
	pm.mu.Unlock()

	if len(items) == 0 {
		return
	}

	cmdID := generateCommandID()
	cmd := &raftkvv1.RaftCommand{
		CommandId: cmdID,
		Payload: &raftkvv1.RaftCommand_Evict{
			Evict: &raftkvv1.EvictBatchCommand{
				Items: items,
			},
		},
	}

	data, err := proto.Marshal(cmd)
	if err != nil {
		pm.clearInFlightVictims(items)
		return
	}

	resultCh := make(chan ProposalResult, 1)
	pm.applier.RegisterWaiter(cmdID, resultCh)
	defer pm.applier.UnregisterWaiter(cmdID)

	ctx, cancel := context.WithTimeout(context.Background(), defaultProposalTimeout)
	defer cancel()

	_, _, isLeader, err := pm.node.Propose(ctx, data)
	if err != nil || !isLeader {
		pm.clearInFlightVictims(items)
		return
	}

	select {
	case <-ctx.Done():
	case <-resultCh:
	}

	pm.clearInFlightVictims(items)
}

func (pm *ProposalManager) clearInFlightVictims(items []*raftkvv1.EvictItem) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	for _, it := range items {
		delete(pm.inFlightVictims, it.Key)
	}
}

const defaultProposalTimeout = 5 * time.Second
