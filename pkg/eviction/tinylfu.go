package eviction

import (
	"container/list"
	"hash/fnv"
	"sync"
)

// countMinSketch implements a 4-bit Count-Min Sketch with aging.
type countMinSketch struct {
	table      []uint8 // each byte stores a 4-bit counter (0-15)
	size       uint32  // power-of-two table length
	mask       uint32
	additions  int
	sampleSize int
}

func newCountMinSketch(capacity int) *countMinSketch {
	size := uint32(1)
	target := capacity * 4
	if target < 1024 {
		target = 1024
	}
	for size < uint32(target) {
		size <<= 1
	}

	return &countMinSketch{
		table:      make([]uint8, size),
		size:       size,
		mask:       size - 1,
		sampleSize: capacity * 10,
	}
}

func (s *countMinSketch) hash(key string, seed uint32) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	v := h.Sum32() ^ seed
	v = (v ^ (v >> 16)) * 0x45d9f3b
	v = (v ^ (v >> 16)) * 0x45d9f3b
	v = v ^ (v >> 16)
	return v & s.mask
}

func (s *countMinSketch) Add(key string) {
	for i := uint32(0); i < 4; i++ {
		idx := s.hash(key, i*0x9e3779b9)
		if s.table[idx] < 15 {
			s.table[idx]++
		}
	}
	s.additions++
	if s.additions >= s.sampleSize {
		s.decay()
	}
}

func (s *countMinSketch) Estimate(key string) uint8 {
	minCount := uint8(15)
	for i := uint32(0); i < 4; i++ {
		idx := s.hash(key, i*0x9e3779b9)
		if s.table[idx] < minCount {
			minCount = s.table[idx]
		}
	}
	return minCount
}

func (s *countMinSketch) decay() {
	for i := range s.table {
		s.table[i] >>= 1
	}
	s.additions = 0
}

func (s *countMinSketch) Reset() {
	for i := range s.table {
		s.table[i] = 0
	}
	s.additions = 0
}

// WTinyLFU implements the W-TinyLFU eviction policy:
// - Window LRU (1% capacity)
// - Main SLRU: Probation (20%) + Protected (80%)
// - Count-Min Sketch frequency tracker for admission control
type WTinyLFU struct {
	mu sync.Mutex

	windowCap    int
	probationCap int
	protectedCap int

	window    *list.List
	probation *list.List
	protected *list.List

	elements map[string]*list.Element
	location map[string]int // 0: window, 1: probation, 2: protected

	sketch *countMinSketch
}

const (
	locWindow    = 0
	locProbation = 1
	locProtected = 2
)

// NewWTinyLFU creates a new W-TinyLFU eviction policy configured for the given capacity.
func NewWTinyLFU(capacity int) *WTinyLFU {
	if capacity < 100 {
		capacity = 100
	}

	windowCap := capacity / 100
	if windowCap < 1 {
		windowCap = 1
	}

	mainCap := capacity - windowCap
	probationCap := (mainCap * 20) / 100
	if probationCap < 1 {
		probationCap = 1
	}
	protectedCap := mainCap - probationCap

	return &WTinyLFU{
		windowCap:    windowCap,
		probationCap: probationCap,
		protectedCap: protectedCap,
		window:       list.New(),
		probation:    list.New(),
		protected:    list.New(),
		elements:     make(map[string]*list.Element),
		location:     make(map[string]int),
		sketch:       newCountMinSketch(capacity),
	}
}

// RecordAccess increments frequency and adjusts recency across window and SLRU segments.
func (w *WTinyLFU) RecordAccess(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.sketch.Add(key)

	elem, exists := w.elements[key]
	if !exists {
		return
	}

	loc := w.location[key]
	switch loc {
	case locWindow:
		w.window.MoveToFront(elem)
	case locProbation:
		// Promote to protected
		w.probation.Remove(elem)
		newElem := w.protected.PushFront(key)
		w.elements[key] = newElem
		w.location[key] = locProtected

		// If protected overflows, demote its LRU to probation
		if w.protected.Len() > w.protectedCap {
			demotedElem := w.protected.Back()
			demotedKey := demotedElem.Value.(string)
			w.protected.Remove(demotedElem)

			probElem := w.probation.PushFront(demotedKey)
			w.elements[demotedKey] = probElem
			w.location[demotedKey] = locProbation
		}
	case locProtected:
		w.protected.MoveToFront(elem)
	}
}

// Add introduces a new key into the window LRU and records frequency.
func (w *WTinyLFU) Add(key string, _ int64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.sketch.Add(key)

	if elem, exists := w.elements[key]; exists {
		loc := w.location[key]
		if loc == locWindow {
			w.window.MoveToFront(elem)
		} else if loc == locProtected {
			w.protected.MoveToFront(elem)
		} else {
			w.probation.MoveToFront(elem)
		}
		return
	}

	elem := w.window.PushFront(key)
	w.elements[key] = elem
	w.location[key] = locWindow

	// If window exceeds capacity, migrate the oldest window element to probation
	if w.window.Len() > w.windowCap {
		demoted := w.window.Back()
		demotedKey := demoted.Value.(string)
		w.window.Remove(demoted)

		probElem := w.probation.PushFront(demotedKey)
		w.elements[demotedKey] = probElem
		w.location[demotedKey] = locProbation
	}
}

// Remove deletes a key from wherever it resides.
func (w *WTinyLFU) Remove(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	elem, exists := w.elements[key]
	if !exists {
		return
	}

	loc := w.location[key]
	switch loc {
	case locWindow:
		w.window.Remove(elem)
	case locProbation:
		w.probation.Remove(elem)
	case locProtected:
		w.protected.Remove(elem)
	}

	delete(w.elements, key)
	delete(w.location, key)
}

// SelectVictims selects up to n candidate keys by comparing window victims against
// probation victims using the TinyLFU frequency sketch.
func (w *WTinyLFU) SelectVictims(n int) []string {
	w.mu.Lock()
	defer w.mu.Unlock()

	if n <= 0 || len(w.elements) == 0 {
		return nil
	}

	victims := make([]string, 0, n)
	// We simulate eviction candidates without mutating the actual data structures
	// until Remove is invoked by Apply.
	excluded := make(map[string]bool)

	for len(victims) < n && len(excluded) < len(w.elements) {
		// Find window candidate
		var windowVictim string
		for elem := w.window.Back(); elem != nil; elem = elem.Prev() {
			k := elem.Value.(string)
			if !excluded[k] {
				windowVictim = k
				break
			}
		}

		// Find main (probation, then protected) candidate
		var mainVictim string
		for elem := w.probation.Back(); elem != nil; elem = elem.Prev() {
			k := elem.Value.(string)
			if !excluded[k] {
				mainVictim = k
				break
			}
		}
		if mainVictim == "" {
			for elem := w.protected.Back(); elem != nil; elem = elem.Prev() {
				k := elem.Value.(string)
				if !excluded[k] {
					mainVictim = k
					break
				}
			}
		}

		if windowVictim == "" && mainVictim == "" {
			break
		}

		if windowVictim != "" && mainVictim == "" {
			victims = append(victims, windowVictim)
			excluded[windowVictim] = true
			continue
		}

		if windowVictim == "" && mainVictim != "" {
			victims = append(victims, mainVictim)
			excluded[mainVictim] = true
			continue
		}

		// Both exist: compare frequencies via TinyLFU admission
		wFreq := w.sketch.Estimate(windowVictim)
		mFreq := w.sketch.Estimate(mainVictim)

		if wFreq > mFreq {
			// Window candidate wins admission; main victim is evicted
			victims = append(victims, mainVictim)
			excluded[mainVictim] = true
		} else {
			// Main victim wins; window candidate is evicted
			victims = append(victims, windowVictim)
			excluded[windowVictim] = true
		}
	}

	return victims
}

// Reset clears all segments, elements, and frequency sketch.
func (w *WTinyLFU) Reset() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.window.Init()
	w.probation.Init()
	w.protected.Init()
	w.elements = make(map[string]*list.Element)
	w.location = make(map[string]int)
	w.sketch.Reset()
}

// Len returns total elements tracked.
func (w *WTinyLFU) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.elements)
}
