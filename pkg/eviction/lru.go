package eviction

import (
	"container/list"
	"sync"
)

// LRU implements the Least Recently Used eviction policy.
type LRU struct {
	mu       sync.Mutex
	items    map[string]*list.Element
	evictSeq *list.List
}

// NewLRU creates a new LRU policy.
func NewLRU() *LRU {
	return &LRU{
		items:    make(map[string]*list.Element),
		evictSeq: list.New(),
	}
}

// RecordAccess marks a key as most recently used.
func (l *LRU) RecordAccess(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if elem, ok := l.items[key]; ok {
		l.evictSeq.MoveToFront(elem)
	}
}

// Add adds a key to the LRU policy or updates its recency.
func (l *LRU) Add(key string, _ int64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if elem, ok := l.items[key]; ok {
		l.evictSeq.MoveToFront(elem)
		return
	}

	elem := l.evictSeq.PushFront(key)
	l.items[key] = elem
}

// Remove deletes a key from the LRU policy.
func (l *LRU) Remove(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if elem, ok := l.items[key]; ok {
		l.evictSeq.Remove(elem)
		delete(l.items, key)
	}
}

// SelectVictims returns up to n keys from least recently used order.
func (l *LRU) SelectVictims(n int) []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	if n <= 0 || l.evictSeq.Len() == 0 {
		return nil
	}

	limit := n
	if limit > l.evictSeq.Len() {
		limit = l.evictSeq.Len()
	}

	victims := make([]string, 0, limit)
	elem := l.evictSeq.Back()
	for elem != nil && len(victims) < limit {
		victims = append(victims, elem.Value.(string))
		elem = elem.Prev()
	}
	return victims
}

// Reset clears all items in the LRU.
func (l *LRU) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.items = make(map[string]*list.Element)
	l.evictSeq.Init()
}

// Len returns the current count of items tracked by the LRU.
func (l *LRU) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.items)
}
