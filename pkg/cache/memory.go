package cache

import "sync/atomic"

const (
	// MaxMemoryBytes is the total accounted cache capacity (64 MiB).
	MaxMemoryBytes int64 = 67108864

	// TombstoneReserveBytes is the memory reserved for the 4,096-entry tombstone table.
	// 4096 records * 136 bytes each = 557,056 bytes.
	TombstoneReserveBytes int64 = 557056

	// SoftLimitPercent is the threshold for leader-triggered eviction.
	SoftLimitPercent int64 = 90

	// SoftLimitBytes is 90% of MaxMemoryBytes (60,397,977 bytes).
	SoftLimitBytes int64 = (MaxMemoryBytes * SoftLimitPercent) / 100

	// FixedEntryOverhead is the per-entry fixed accounting overhead:
	// version (8 bytes) + fixed overhead (64 bytes) = 72 bytes.
	FixedEntryOverhead int64 = 72

	// MaxKeyBytes is the maximum allowed key size.
	MaxKeyBytes = 64

	// MaxValueBytes is the maximum allowed value size.
	MaxValueBytes = 512
)

// EntryAccountedBytes computes the deterministic accounted memory for a key-value entry:
// len(key) + len(value) + 8 (version) + 64 (fixed overhead).
func EntryAccountedBytes(key, value string) int64 {
	return int64(len(key)) + int64(len(value)) + FixedEntryOverhead
}

// MemoryTracker tracks deterministic cache memory across replicas.
type MemoryTracker struct {
	entryBytes atomic.Int64
}

// NewMemoryTracker creates a new tracker with initial entry bytes.
func NewMemoryTracker(initialEntryBytes int64) *MemoryTracker {
	m := &MemoryTracker{}
	m.entryBytes.Store(initialEntryBytes)
	return m
}

// AddEntryBytes adds delta to the tracked entry bytes.
func (m *MemoryTracker) AddEntryBytes(delta int64) {
	m.entryBytes.Add(delta)
}

// EntryBytes returns the total bytes used by cache entries.
func (m *MemoryTracker) EntryBytes() int64 {
	return m.entryBytes.Load()
}

// TotalAccountedBytes returns the entry bytes plus the reserved tombstone table bytes.
func (m *MemoryTracker) TotalAccountedBytes() int64 {
	return m.entryBytes.Load() + TombstoneReserveBytes
}

// Reset resets the tracker to a specific entry byte count.
func (m *MemoryTracker) Reset(entryBytes int64) {
	m.entryBytes.Store(entryBytes)
}
