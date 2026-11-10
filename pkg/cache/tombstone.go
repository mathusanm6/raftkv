package cache

import "sync"

const (
	// TombstoneCapacity is the maximum number of tombstone records in the FIFO table.
	TombstoneCapacity = 4096
)

// TombstoneRecord represents a single tombstone entry.
type TombstoneRecord struct {
	Key     string
	Version int64
}

// TombstoneRing is a fixed-capacity FIFO circular ring buffer for tombstone records.
// It includes a hash index for O(1) version checks and implements version-safe eviction
// to prevent newer tombstones from being erased when an older duplicate slot wraps around.
type TombstoneRing struct {
	mu    sync.RWMutex
	ring  [TombstoneCapacity]TombstoneRecord
	head  int              // Next write index in circular buffer
	count int              // Current number of elements stored (0 <= count <= TombstoneCapacity)
	index map[string]int64 // Key -> newest tombstone version
}

// NewTombstoneRing creates an empty 4,096-capacity tombstone ring.
func NewTombstoneRing() *TombstoneRing {
	return &TombstoneRing{
		index: make(map[string]int64),
	}
}

// Record records a tombstone for the given key and version.
// If the ring is full, the oldest slot is overwritten, safely evicting the key from
// the index only if the slot's version matches the index (preventing newer tombstones
// for the same key from being mistakenly purged).
func (tr *TombstoneRing) Record(key string, version int64) {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	if tr.count == TombstoneCapacity {
		// Ring is full, slot tr.head will be overwritten.
		oldRec := tr.ring[tr.head]
		// Version-safe eviction check:
		if tr.index[oldRec.Key] == oldRec.Version {
			delete(tr.index, oldRec.Key)
		}
	} else {
		tr.count++
	}

	tr.ring[tr.head] = TombstoneRecord{Key: key, Version: version}
	tr.head = (tr.head + 1) % TombstoneCapacity

	// Only update or insert if the version is >= existing version
	if currentVer, exists := tr.index[key]; !exists || version >= currentVer {
		tr.index[key] = version
	}
}

// Lookup returns the recorded tombstone version for key, if present.
func (tr *TombstoneRing) Lookup(key string) (int64, bool) {
	tr.mu.RLock()
	defer tr.mu.RUnlock()

	ver, exists := tr.index[key]
	return ver, exists
}

// Count returns the number of active entries currently in the ring.
func (tr *TombstoneRing) Count() int {
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	return tr.count
}

// Records returns all active tombstone records in insertion (FIFO) order.
// Used for snapshotting.
func (tr *TombstoneRing) Records() []TombstoneRecord {
	tr.mu.RLock()
	defer tr.mu.RUnlock()

	records := make([]TombstoneRecord, tr.count)
	if tr.count < TombstoneCapacity {
		// Ring has not wrapped yet; records are in [0, head)
		copy(records, tr.ring[:tr.count])
		return records
	}

	// Ring has wrapped. Oldest element is at head.
	n1 := copy(records, tr.ring[tr.head:])
	copy(records[n1:], tr.ring[:tr.head])
	return records
}

// Restore resets the tombstone ring with a list of records in chronological order.
func (tr *TombstoneRing) Restore(records []TombstoneRecord) {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	tr.ring = [TombstoneCapacity]TombstoneRecord{}
	tr.index = make(map[string]int64)
	tr.head = 0
	tr.count = 0

	for _, rec := range records {
		if tr.count == TombstoneCapacity {
			oldRec := tr.ring[tr.head]
			if tr.index[oldRec.Key] == oldRec.Version {
				delete(tr.index, oldRec.Key)
			}
		} else {
			tr.count++
		}
		tr.ring[tr.head] = rec
		tr.head = (tr.head + 1) % TombstoneCapacity
		if currentVer, exists := tr.index[rec.Key]; !exists || rec.Version >= currentVer {
			tr.index[rec.Key] = rec.Version
		}
	}
}
