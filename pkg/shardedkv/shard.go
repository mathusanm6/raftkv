package shardedkv

import (
	"context"
	"errors"
	"sync"

	"github.com/mathusanm6/raftkv/pkg/cache"
	"github.com/mathusanm6/raftkv/pkg/eviction"
	raftkvv1 "github.com/mathusanm6/raftkv/proto/raftkv/v1"
)

var (
	ErrResourceExhausted = errors.New("memory limit reached, cannot evict")
	ErrKeyTooLarge       = errors.New("key exceeds maximum size of 64 bytes")
	ErrValueTooLarge     = errors.New("value exceeds maximum size of 512 bytes")
)

// Shard manages the in-memory state of one independent, unreplicated cache shard.
// Evictions are executed synchronously at 100% capacity.
type Shard struct {
	mu sync.Mutex

	store    *cache.Store
	eviction eviction.Policy
	maxBytes int64
	applySeq uint64
}

// NewShard creates a new Shard with the given eviction policy and memory limit.
func NewShard(policy eviction.Policy, maxBytes int64) *Shard {
	if maxBytes <= 0 {
		maxBytes = cache.MaxMemoryBytes
	}
	return &Shard{
		store:    cache.NewStore(),
		eviction: policy,
		maxBytes: maxBytes,
	}
}

// Get retrieves a key, recording access in the eviction policy on a hit.
func (s *Shard) Get(_ context.Context, key string) (string, int64, bool, error) {
	if len(key) > cache.MaxKeyBytes {
		return "", 0, false, ErrKeyTooLarge
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	val, ver, found := s.store.Get(key)
	if found && s.eviction != nil {
		s.eviction.RecordAccess(key)
	}
	return val, ver, found, nil
}

// Set stores a key-value pair, performing synchronous eviction if at 100% capacity.
func (s *Shard) Set(_ context.Context, key, value string, version int64) error {
	if len(key) > cache.MaxKeyBytes {
		return ErrKeyTooLarge
	}
	if len(value) > cache.MaxValueBytes {
		return ErrValueTooLarge
	}

	entryBytes := cache.EntryAccountedBytes(key, value)
	if entryBytes+cache.TombstoneReserveBytes > s.maxBytes {
		return ErrResourceExhausted
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.applySeq++

	// Check if key already exists; if so, replace might not increase memory much
	existingVal, existingVer, exists := s.store.Get(key)
	if exists && existingVer >= version {
		// Ignored per version rules
		return nil
	}

	// Tombstone version rule check
	if tombVer, tombExists := s.store.Tombstones().Lookup(key); tombExists && tombVer >= version {
		return nil
	}

	var netBytesIncrease int64
	if exists {
		netBytesIncrease = entryBytes - cache.EntryAccountedBytes(key, existingVal)
	} else {
		netBytesIncrease = entryBytes
	}

	// Evict synchronously at 100% capacity until there is sufficient room
	for s.store.MemoryTracker().TotalAccountedBytes()+netBytesIncrease > s.maxBytes {
		if s.eviction == nil {
			return ErrResourceExhausted
		}

		victims := s.eviction.SelectVictims(1)
		if len(victims) == 0 {
			return ErrResourceExhausted
		}

		victimKey := victims[0]
		vVal, vVer, vFound := s.store.Get(victimKey)
		if vFound {
			s.store.ApplyEvict([]*raftkvv1.EvictItem{
				{Key: victimKey, Version: vVer},
			}, s.applySeq)
			_ = vVal
		}
		s.eviction.Remove(victimKey)
	}

	applied := s.store.ApplySet(key, value, version, s.applySeq)
	if applied && s.eviction != nil {
		s.eviction.Add(key, 0)
	}

	return nil
}

// Delete removes a key and appends a tombstone to the FIFO table.
func (s *Shard) Delete(_ context.Context, key string, version int64) error {
	if len(key) > cache.MaxKeyBytes {
		return ErrKeyTooLarge
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.applySeq++
	applied := s.store.ApplyDelete(key, version, s.applySeq)
	if applied && s.eviction != nil {
		s.eviction.Remove(key)
	}
	return nil
}

// Store returns the underlying cache Store.
func (s *Shard) Store() *cache.Store {
	return s.store
}

// Eviction returns the eviction policy.
func (s *Shard) Eviction() eviction.Policy {
	return s.eviction
}
