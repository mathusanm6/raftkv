package cache

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"sync"

	raftkvv1 "github.com/mathusanm6/raftkv/proto/raftkv/v1"
)

// Entry represents a cached item with versioning and consensus apply index metadata.
type Entry struct {
	Key        string
	Value      string
	Version    int64
	ApplyIndex uint64
}

// Clone creates a shallow copy of an Entry (strings are immutable in Go).
func (e *Entry) Clone() *Entry {
	return &Entry{
		Key:        e.Key,
		Value:      e.Value,
		Version:    e.Version,
		ApplyIndex: e.ApplyIndex,
	}
}

// Store is the core deterministic replicated state machine.
type Store struct {
	mu          sync.RWMutex
	data        map[string]*Entry
	tombstones  *TombstoneRing
	mem         *MemoryTracker
	lastApplied uint64
}

// NewStore initializes a new cache Store.
func NewStore() *Store {
	return &Store{
		data:       make(map[string]*Entry),
		tombstones: NewTombstoneRing(),
		mem:        NewMemoryTracker(0),
	}
}

// Get retrieves an item by key. Returns (value, version, found).
func (s *Store) Get(key string) (string, int64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok := s.data[key]
	if !ok {
		return "", 0, false
	}
	return entry.Value, entry.Version, true
}

// ApplySet applies a SET operation.
// Must be called sequentially per Raft log order.
// Returns whether the write was applied.
func (s *Store) ApplySet(key, value string, version int64, applyIndex uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Version Rule 1: Ignored if the key's stored version >= request version.
	if existing, ok := s.data[key]; ok && existing.Version >= version {
		if applyIndex > s.lastApplied {
			s.lastApplied = applyIndex
		}
		return false
	}

	// Version Rule 2: Ignored if tombstone version >= request version.
	if tombstoneVer, exists := s.tombstones.Lookup(key); exists && tombstoneVer >= version {
		if applyIndex > s.lastApplied {
			s.lastApplied = applyIndex
		}
		return false
	}

	newBytes := EntryAccountedBytes(key, value)

	if existing, ok := s.data[key]; ok {
		oldBytes := EntryAccountedBytes(existing.Key, existing.Value)
		s.mem.AddEntryBytes(newBytes - oldBytes)
	} else {
		s.mem.AddEntryBytes(newBytes)
	}

	s.data[key] = &Entry{
		Key:        key,
		Value:      value,
		Version:    version,
		ApplyIndex: applyIndex,
	}

	if applyIndex > s.lastApplied {
		s.lastApplied = applyIndex
	}
	return true
}

// ApplyDelete applies a DELETE operation.
// Returns whether the delete applied and a tombstone was recorded.
func (s *Store) ApplyDelete(key string, version int64, applyIndex uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if applyIndex > s.lastApplied {
		s.lastApplied = applyIndex
	}

	// Case 1: Key exists in store
	if existing, ok := s.data[key]; ok {
		if existing.Version <= version {
			// Valid deletion: remove entry, update memory, record tombstone
			bytes := EntryAccountedBytes(existing.Key, existing.Value)
			delete(s.data, key)
			s.mem.AddEntryBytes(-bytes)
			s.tombstones.Record(key, version)
			return true
		}
		// Outdated delete: stored version is newer, do not delete and do not record tombstone
		return false
	}

	// Case 2: Key does not exist in store
	if tombVer, exists := s.tombstones.Lookup(key); !exists || tombVer < version {
		s.tombstones.Record(key, version)
		return true
	}

	return false
}

// ApplyEvict applies an EVICT batch.
// Entries are removed ONLY IF their stored version matches item.Version.
// This prevents evicting a key that was concurrently updated to a newer version.
func (s *Store) ApplyEvict(items []*raftkvv1.EvictItem, applyIndex uint64) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if applyIndex > s.lastApplied {
		s.lastApplied = applyIndex
	}

	evicted := 0
	for _, item := range items {
		if existing, ok := s.data[item.Key]; ok {
			if existing.Version == item.Version {
				bytes := EntryAccountedBytes(existing.Key, existing.Value)
				delete(s.data, item.Key)
				s.mem.AddEntryBytes(-bytes)
				evicted++
			}
		}
	}
	return evicted
}

// LastApplied returns the highest log index applied to this store.
func (s *Store) LastApplied() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastApplied
}

// MemoryTracker returns the memory tracker instance.
func (s *Store) MemoryTracker() *MemoryTracker {
	return s.mem
}

// Tombstones returns the tombstone ring.
func (s *Store) Tombstones() *TombstoneRing {
	return s.tombstones
}

// Len returns the number of active items in the store.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// Entries returns a copy of all current entries in the store.
func (s *Store) Entries() []*Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*Entry, 0, len(s.data))
	for _, e := range s.data {
		result = append(result, e.Clone())
	}
	return result
}

// Snapshot captures a full, consistent point-in-time snapshot.
func (s *Store) Snapshot() (*raftkvv1.SnapshotData, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := make([]*raftkvv1.SnapshotEntry, 0, len(s.data))
	for _, e := range s.data {
		entries = append(entries, &raftkvv1.SnapshotEntry{
			Key:        e.Key,
			Value:      e.Value,
			Version:    e.Version,
			ApplyIndex: e.ApplyIndex,
		})
	}

	tombstoneRecords := s.tombstones.Records()
	tombstones := make([]*raftkvv1.SnapshotTombstone, 0, len(tombstoneRecords))
	for _, t := range tombstoneRecords {
		tombstones = append(tombstones, &raftkvv1.SnapshotTombstone{
			Key:     t.Key,
			Version: t.Version,
		})
	}

	return &raftkvv1.SnapshotData{
		Entries:          entries,
		Tombstones:       tombstones,
		LastAppliedIndex: s.lastApplied,
		AccountedBytes:   s.mem.EntryBytes(),
	}, nil
}

// RestoreSnapshot restores the store from a snapshot.
func (s *Store) RestoreSnapshot(snap *raftkvv1.SnapshotData) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = make(map[string]*Entry, len(snap.Entries))
	var calculatedBytes int64

	for _, e := range snap.Entries {
		s.data[e.Key] = &Entry{
			Key:        e.Key,
			Value:      e.Value,
			Version:    e.Version,
			ApplyIndex: e.ApplyIndex,
		}
		calculatedBytes += EntryAccountedBytes(e.Key, e.Value)
	}

	tombstoneRecords := make([]TombstoneRecord, 0, len(snap.Tombstones))
	for _, t := range snap.Tombstones {
		tombstoneRecords = append(tombstoneRecords, TombstoneRecord{
			Key:     t.Key,
			Version: t.Version,
		})
	}
	s.tombstones.Restore(tombstoneRecords)

	s.mem.Reset(calculatedBytes)
	s.lastApplied = snap.LastAppliedIndex
	return nil
}

// StateHash calculates a deterministic SHA-256 hash of all store contents,
// tombstones, and accounted bytes to prove state equivalence across replicas.
func (s *Store) StateHash() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	h := sha256.New()
	for _, k := range keys {
		e := s.data[k]
		fmt.Fprintf(h, "E:%s=%s:%d:%d;", e.Key, e.Value, e.Version, e.ApplyIndex)
	}

	tombstones := s.tombstones.Records()
	for _, t := range tombstones {
		fmt.Fprintf(h, "T:%s=%d;", t.Key, t.Version)
	}

	fmt.Fprintf(h, "M:%d;A:%d;", s.mem.TotalAccountedBytes(), s.lastApplied)
	return fmt.Sprintf("%x", h.Sum(nil))
}
