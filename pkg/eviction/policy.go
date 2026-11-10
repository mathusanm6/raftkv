package eviction

// Policy defines the shared cache eviction interface specified in design doc §7.1.
// Implementations must be thread-safe.
type Policy interface {
	// RecordAccess records a read or write hit for popularity tracking.
	RecordAccess(key string)

	// Add records that a key was stored in the cache.
	Add(key string, sizeBytes int64)

	// Remove records that a key was deleted or evicted.
	Remove(key string)

	// SelectVictims selects up to n candidate keys to evict.
	// Does not mutate policy state until Remove is called upon commit.
	SelectVictims(n int) []string

	// Reset clears all popularity and frequency metadata.
	Reset()
}
