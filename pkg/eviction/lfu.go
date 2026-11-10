package eviction

import (
	"container/list"
	"sync"
)

type lfuItem struct {
	key      string
	freq     int
	bucket   *lfuBucket
	listElem *list.Element
}

type lfuBucket struct {
	freq     int
	items    *list.List
	listElem *list.Element
}

// LFU implements an O(1) Least Frequently Used eviction policy.
type LFU struct {
	mu         sync.Mutex
	items      map[string]*lfuItem
	buckets    map[int]*lfuBucket
	bucketList *list.List
	minFreq    int
}

// NewLFU creates a new LFU policy.
func NewLFU() *LFU {
	return &LFU{
		items:      make(map[string]*lfuItem),
		buckets:    make(map[int]*lfuBucket),
		bucketList: list.New(),
		minFreq:    0,
	}
}

// RecordAccess increments the frequency of a key.
func (f *LFU) RecordAccess(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	item, ok := f.items[key]
	if !ok {
		return
	}
	f.incrementItemLocked(item)
}

func (f *LFU) incrementItemLocked(item *lfuItem) {
	oldBucket := item.bucket
	newFreq := item.freq + 1

	// Find or create bucket for newFreq
	newBucket, ok := f.buckets[newFreq]
	if !ok {
		newBucket = &lfuBucket{
			freq:  newFreq,
			items: list.New(),
		}
		// Insert newBucket immediately after oldBucket in bucketList
		newBucket.listElem = f.bucketList.InsertAfter(newBucket, oldBucket.listElem)
		f.buckets[newFreq] = newBucket
	}

	// Remove item from oldBucket
	oldBucket.items.Remove(item.listElem)
	if oldBucket.items.Len() == 0 {
		f.bucketList.Remove(oldBucket.listElem)
		delete(f.buckets, oldBucket.freq)
		if f.minFreq == oldBucket.freq {
			f.minFreq = newFreq
		}
	}

	// Add item to newBucket
	item.freq = newFreq
	item.bucket = newBucket
	item.listElem = newBucket.items.PushFront(item)
}

// Add adds a key to the LFU with frequency 1 or increments its frequency if it exists.
func (f *LFU) Add(key string, _ int64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if item, ok := f.items[key]; ok {
		f.incrementItemLocked(item)
		return
	}

	// Find or create bucket 1
	bucket1, ok := f.buckets[1]
	if !ok {
		bucket1 = &lfuBucket{
			freq:  1,
			items: list.New(),
		}
		bucket1.listElem = f.bucketList.PushFront(bucket1)
		f.buckets[1] = bucket1
	}

	item := &lfuItem{
		key:    key,
		freq:   1,
		bucket: bucket1,
	}
	item.listElem = bucket1.items.PushFront(item)
	f.items[key] = item
	f.minFreq = 1
}

// Remove removes a key from the LFU policy.
func (f *LFU) Remove(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	item, ok := f.items[key]
	if !ok {
		return
	}

	bucket := item.bucket
	bucket.items.Remove(item.listElem)
	delete(f.items, key)

	if bucket.items.Len() == 0 {
		f.bucketList.Remove(bucket.listElem)
		delete(f.buckets, bucket.freq)
		if f.minFreq == bucket.freq {
			if f.bucketList.Len() > 0 {
				f.minFreq = f.bucketList.Front().Value.(*lfuBucket).freq
			} else {
				f.minFreq = 0
			}
		}
	}
}

// SelectVictims selects up to n candidate keys with the lowest frequencies.
func (f *LFU) SelectVictims(n int) []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	if n <= 0 || len(f.items) == 0 {
		return nil
	}

	victims := make([]string, 0, n)
	for bElem := f.bucketList.Front(); bElem != nil && len(victims) < n; bElem = bElem.Next() {
		bucket := bElem.Value.(*lfuBucket)
		// Extract from back of bucket (least recently accessed among same frequency)
		for iElem := bucket.items.Back(); iElem != nil && len(victims) < n; iElem = iElem.Prev() {
			victims = append(victims, iElem.Value.(*lfuItem).key)
		}
	}
	return victims
}

// Reset clears all items and buckets.
func (f *LFU) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.items = make(map[string]*lfuItem)
	f.buckets = make(map[int]*lfuBucket)
	f.bucketList.Init()
	f.minFreq = 0
}

// Len returns the number of items tracked.
func (f *LFU) Len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.items)
}
