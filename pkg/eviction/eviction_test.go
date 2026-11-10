package eviction

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type mockClock struct {
	mu  sync.Mutex
	now time.Time
}

func (m *mockClock) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

func (m *mockClock) Advance(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = m.now.Add(d)
}

func TestLRUEviction(t *testing.T) {
	lru := NewLRU()

	// Add 3 keys
	lru.Add("k1", 100)
	lru.Add("k2", 100)
	lru.Add("k3", 100)

	// Access k1, making k2 the least recently used
	lru.RecordAccess("k1")

	victims := lru.SelectVictims(2)
	require.Equal(t, []string{"k2", "k3"}, victims)

	// Remove k2
	lru.Remove("k2")
	victims = lru.SelectVictims(2)
	require.Equal(t, []string{"k3", "k1"}, victims)
}

func TestLFUEviction(t *testing.T) {
	lfu := NewLFU()

	lfu.Add("k1", 100)
	lfu.Add("k2", 100)
	lfu.Add("k3", 100)

	// k1 accessed 3 times (freq 4), k2 accessed 1 time (freq 2), k3 freq 1
	lfu.RecordAccess("k1")
	lfu.RecordAccess("k1")
	lfu.RecordAccess("k1")
	lfu.RecordAccess("k2")

	// k3 has freq 1, k2 has freq 2, k1 has freq 4
	victims := lfu.SelectVictims(2)
	require.Equal(t, []string{"k3", "k2"}, victims)

	// Access k3 multiple times -> freq 5
	for i := 0; i < 4; i++ {
		lfu.RecordAccess("k3")
	}

	// Now k2 has lowest freq (2)
	victims = lfu.SelectVictims(1)
	require.Equal(t, []string{"k2"}, victims)
}

func TestWTinyLFUEviction(t *testing.T) {
	w := NewWTinyLFU(100)

	// Add frequent keys into main cache
	w.Add("hot1", 100)
	w.Add("hot2", 100)
	for i := 0; i < 10; i++ {
		w.RecordAccess("hot1")
		w.RecordAccess("hot2")
	}

	// Add cold key into window
	w.Add("cold", 100)

	// Select victims: cold key from window should be preferred for eviction over hot keys
	victims := w.SelectVictims(1)
	require.Equal(t, []string{"cold"}, victims)
}

func TestColdStartTransitionByTime(t *testing.T) {
	now := time.Now()
	clock := &mockClock{now: now}
	lru := NewLRU()
	cold := NewColdStartWrapper(lru, clock)

	keys := []string{"oldest", "middle", "newest"}
	cold.StartColdStart(keys)

	require.True(t, cold.IsInWarmup())

	// While in warmup, eviction is strictly oldest-first by apply order
	victims := cold.SelectVictims(2)
	require.Equal(t, []string{"oldest", "middle"}, victims)

	// Access "oldest" many times; in normal LRU it would become MRU,
	// but during warmup apply order still dictates eviction!
	for i := 0; i < 50; i++ {
		cold.RecordAccess("oldest")
	}
	victims = cold.SelectVictims(1)
	require.Equal(t, []string{"oldest"}, victims)

	// Advance time past 30 seconds
	clock.Advance(31 * time.Second)

	// Now warmup expires, LRU takes over: "oldest" was accessed recently, so least recently used is evicted
	require.False(t, cold.IsInWarmup())
	victims = cold.SelectVictims(1)
	require.Equal(t, []string{"middle"}, victims)
}

func TestColdStartTransitionByReadCount(t *testing.T) {
	now := time.Now()
	clock := &mockClock{now: now}
	lru := NewLRU()
	cold := NewColdStartWrapper(lru, clock)

	keys := []string{"k1", "k2", "k3"}
	cold.StartColdStart(keys)
	require.True(t, cold.IsInWarmup())

	// Perform 10,000 reads
	for i := 0; i < 10000; i++ {
		cold.RecordAccess("k1")
	}

	require.False(t, cold.IsInWarmup(), "Warmup should expire after 10,000 reads")
}

func TestConcurrentAccessWithRaceDetector(t *testing.T) {
	policies := []Policy{
		NewLRU(),
		NewLFU(),
		NewWTinyLFU(200),
		NewColdStartWrapper(NewLRU(), nil),
	}

	for _, p := range policies {
		p := p
		var wg sync.WaitGroup
		const numWorkers = 8
		const opsPerWorker = 500

		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for i := 0; i < opsPerWorker; i++ {
					k := fmt.Sprintf("k-%d-%d", workerID, i%20)
					p.Add(k, 100)
					p.RecordAccess(k)
					if i%5 == 0 {
						p.SelectVictims(3)
					}
					if i%10 == 0 {
						p.Remove(k)
					}
				}
			}(w)
		}
		wg.Wait()
	}
}
