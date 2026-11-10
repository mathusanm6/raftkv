package eviction

import (
	"sync"
	"time"
)

// Clock provides an interface for time measurement, enabling deterministic unit testing.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

const (
	ColdStartDuration  = 30 * time.Second
	ColdStartReadLimit = 10000
)

// ColdStartWrapper decorates any Policy to handle the post-election cold-start phase.
// During the first 30 seconds or 10,000 reads, it evicts oldest-first by apply index,
// while simultaneously allowing the underlying policy to accumulate popularity data.
type ColdStartWrapper struct {
	mu         sync.Mutex
	underlying Policy
	clock      Clock

	inWarmup       bool
	warmupDeadline time.Time
	readCount      int

	applyOrder []string
	keyIndex   map[string]int // key -> index in applyOrder
}

// NewColdStartWrapper wraps a base eviction policy with cold-start management.
func NewColdStartWrapper(policy Policy, clock Clock) *ColdStartWrapper {
	if clock == nil {
		clock = realClock{}
	}
	return &ColdStartWrapper{
		underlying: policy,
		clock:      clock,
		keyIndex:   make(map[string]int),
	}
}

// StartColdStart initializes the cold-start phase using keys ordered by apply index.
func (c *ColdStartWrapper) StartColdStart(keysInApplyOrder []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.underlying.Reset()

	c.inWarmup = true
	c.warmupDeadline = c.clock.Now().Add(ColdStartDuration)
	c.readCount = 0

	c.applyOrder = make([]string, len(keysInApplyOrder))
	copy(c.applyOrder, keysInApplyOrder)

	c.keyIndex = make(map[string]int, len(keysInApplyOrder))
	for i, k := range c.applyOrder {
		c.keyIndex[k] = i
		c.underlying.Add(k, 0)
	}
}

// IsInWarmup returns whether the cold-start phase is currently active.
func (c *ColdStartWrapper) IsInWarmup() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.checkWarmupExpiryLocked()
	return c.inWarmup
}

func (c *ColdStartWrapper) checkWarmupExpiryLocked() {
	if !c.inWarmup {
		return
	}
	if c.readCount >= ColdStartReadLimit || c.clock.Now().After(c.warmupDeadline) {
		c.inWarmup = false
		c.applyOrder = nil
		c.keyIndex = nil
	}
}

// RecordAccess tracks reads/hits.
func (c *ColdStartWrapper) RecordAccess(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.underlying.RecordAccess(key)

	if c.inWarmup {
		c.readCount++
		c.checkWarmupExpiryLocked()
	}
}

// Add adds a key to the cache.
func (c *ColdStartWrapper) Add(key string, sizeBytes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.underlying.Add(key, sizeBytes)

	if c.inWarmup {
		if _, exists := c.keyIndex[key]; !exists {
			c.keyIndex[key] = len(c.applyOrder)
			c.applyOrder = append(c.applyOrder, key)
		}
	}
}

// Remove removes a key from the cache.
func (c *ColdStartWrapper) Remove(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.underlying.Remove(key)

	if c.inWarmup {
		if idx, exists := c.keyIndex[key]; exists {
			delete(c.keyIndex, key)
			// Fast removal from slice: mark as empty or rebuild if needed
			c.applyOrder[idx] = ""
		}
	}
}

// SelectVictims selects up to n candidate keys. If in warmup, selects oldest-first
// by apply index; otherwise delegates to the underlying policy.
func (c *ColdStartWrapper) SelectVictims(n int) []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.checkWarmupExpiryLocked()

	if !c.inWarmup {
		return c.underlying.SelectVictims(n)
	}

	if n <= 0 || len(c.applyOrder) == 0 {
		return nil
	}

	victims := make([]string, 0, n)
	// Iterate through applyOrder and gather non-empty keys
	for _, k := range c.applyOrder {
		if k != "" {
			victims = append(victims, k)
			if len(victims) == n {
				break
			}
		}
	}

	return victims
}

// Reset clears state.
func (c *ColdStartWrapper) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.underlying.Reset()
	c.inWarmup = false
	c.applyOrder = nil
	c.keyIndex = make(map[string]int)
	c.readCount = 0
}
