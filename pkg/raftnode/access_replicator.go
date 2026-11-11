package raftnode

import (
	"context"
	"sync"
	"time"

	"github.com/jathurchan/raft/raft"
	raftkvv1 "github.com/mathusanm6/raftkv/proto/raftkv/v1"
	"google.golang.org/protobuf/proto"
)

const (
	AccessFlushInterval = 1 * time.Second
	AccessFlushMaxKeys  = 1024
	// AccessMaxCount matches the W-TinyLFU sketch's 4-bit counter maximum.
	AccessMaxCount = 15
)

// AccessReplicator buffers leader-observed cache hits and replicates them as
// ACCESS batches, so every replica's eviction policy keeps the same popularity
// data across leader changes (design doc §7.6, v2).
type AccessReplicator struct {
	mu     sync.Mutex
	node   raft.Raft
	counts map[string]uint32

	flushCh   chan struct{}
	stopCh    chan struct{}
	closeOnce sync.Once
}

// NewAccessReplicator creates an AccessReplicator. Call Run to start flushing.
func NewAccessReplicator(node raft.Raft) *AccessReplicator {
	return &AccessReplicator{
		node:    node,
		counts:  make(map[string]uint32),
		flushCh: make(chan struct{}, 1),
		stopCh:  make(chan struct{}),
	}
}

// RecordAccess buffers a hit. It implements the subset of eviction.Policy used
// by the read path, so the leader never updates its own policy directly.
func (r *AccessReplicator) RecordAccess(key string) {
	r.mu.Lock()
	if r.counts[key] < AccessMaxCount {
		r.counts[key]++
	}
	full := len(r.counts) >= AccessFlushMaxKeys
	r.mu.Unlock()

	if full {
		select {
		case r.flushCh <- struct{}{}:
		default:
		}
	}
}

// Run flushes the buffer every AccessFlushInterval, or sooner when it is full,
// until Stop is called.
func (r *AccessReplicator) Run() {
	ticker := time.NewTicker(AccessFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.Flush()
		case <-r.flushCh:
			r.Flush()
		}
	}
}

// Flush proposes the buffered counts as one ACCESS command. Failed proposals,
// for example after losing leadership, are dropped: the data is best effort.
func (r *AccessReplicator) Flush() {
	r.mu.Lock()
	if len(r.counts) == 0 {
		r.mu.Unlock()
		return
	}
	counts := r.counts
	r.counts = make(map[string]uint32, len(counts))
	r.mu.Unlock()

	items := make([]*raftkvv1.AccessItem, 0, len(counts))
	for k, c := range counts {
		items = append(items, &raftkvv1.AccessItem{Key: k, Count: c})
	}

	cmd := &raftkvv1.RaftCommand{
		CommandId: generateCommandID(),
		Payload: &raftkvv1.RaftCommand_Access{
			Access: &raftkvv1.AccessBatchCommand{Items: items},
		},
	}
	data, err := proto.Marshal(cmd)
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultProposalTimeout)
	defer cancel()
	_, _, _, _ = r.node.Propose(ctx, data)
}

// Discard drops buffered counts, e.g. when this node is no longer leader.
func (r *AccessReplicator) Discard() {
	r.mu.Lock()
	r.counts = make(map[string]uint32)
	r.mu.Unlock()
}

// Stop ends the flush loop.
func (r *AccessReplicator) Stop() {
	r.closeOnce.Do(func() { close(r.stopCh) })
}
