package client

import (
	"sync"
)

type versionRecord struct {
	version      int64
	acknowledged bool
}

// StaleTracker tracks DB write completions and delete acknowledgements to detect stale reads
// according to design doc §4.3.
type StaleTracker struct {
	mu      sync.RWMutex
	records map[string]versionRecord
}

// NewStaleTracker creates a new StaleTracker.
func NewStaleTracker() *StaleTracker {
	return &StaleTracker{
		records: make(map[string]versionRecord),
	}
}

// RecordDBWrite records that a DB update returned newVersion.
func (s *StaleTracker) RecordDBWrite(key string, version int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.records[key]
	if !ok || version >= existing.version {
		s.records[key] = versionRecord{
			version:      version,
			acknowledged: false,
		}
	}
}

// MarkDeleteAcknowledged records that the cache DELETE for version has been successfully applied.
func (s *StaleTracker) MarkDeleteAcknowledged(key string, version int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.records[key]
	if ok && existing.version == version {
		existing.acknowledged = true
		s.records[key] = existing
	}
}

// CheckStale compares a read version against the highest acknowledged/in-flight DB versions.
// Returns:
// - isStale: true if readVersion < highest DB version.
// - afterAck: true if the delete for that version had already been acknowledged (defect / lost tombstone).
func (s *StaleTracker) CheckStale(key string, readVersion int64) (isStale bool, afterAck bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, ok := s.records[key]
	if !ok {
		return false, false
	}

	if readVersion < rec.version {
		return true, rec.acknowledged
	}

	return false, false
}
