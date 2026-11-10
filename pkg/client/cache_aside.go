package client

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

const (
	CacheReadTimeout      = 50 * time.Millisecond
	DeleteRetryInterval   = 200 * time.Millisecond
	DeleteRetryMaxTimeout = 60 * time.Second
)

// ReadResult holds the outcome of a cache-aside read operation.
type ReadResult struct {
	Value               string
	Version             int64
	IsHit               bool
	IsStale             bool
	IsStaleAfterAck     bool
	ExtraPostgresQuery  bool
	Err                 error
}

// ClientMetrics aggregates operational counts measured at the client per design doc §4 and benchmark plan §4.
type ClientMetrics struct {
	Hits                    int64
	Misses                  int64
	CacheErrors             int64
	CacheTimeouts           int64
	DBQueries               int64
	DBUpdates               int64
	StaleReadsInWindow      int64
	StaleReadsAfterAck      int64
	InvalidationsAbandoned  int64
}

type retryItem struct {
	key     string
	version int64
	start   time.Time
}

// CacheAsideClient coordinates client reads and writes across Cache and PostgreSQL.
type CacheAsideClient struct {
	cache        CacheClient
	db           DB
	staleTracker *StaleTracker

	// Metrics
	hits                   atomic.Int64
	misses                 atomic.Int64
	cacheErrors            atomic.Int64
	cacheTimeouts          atomic.Int64
	dbQueries              atomic.Int64
	dbUpdates              atomic.Int64
	staleReadsInWindow     atomic.Int64
	staleReadsAfterAck     atomic.Int64
	invalidationsAbandoned atomic.Int64

	// Retry loop for failed deletes
	mu          sync.Mutex
	pendingDels map[string]retryItem
	stopCh      chan struct{}
	closeOnce   sync.Once
}

// NewCacheAsideClient creates a CacheAsideClient.
func NewCacheAsideClient(cache CacheClient, db DB, tracker *StaleTracker) *CacheAsideClient {
	if tracker == nil {
		tracker = NewStaleTracker()
	}
	c := &CacheAsideClient{
		cache:        cache,
		db:           db,
		staleTracker: tracker,
		pendingDels:  make(map[string]retryItem),
		stopCh:       make(chan struct{}),
	}

	go c.deleteRetryLoop()
	return c
}

// Read implements the cache-aside read sequence specified in design doc §4.1:
// 1. GET key with 50 ms timeout.
// 2. On hit: return value, check staleness.
// 3. On miss/error/timeout: count miss, query PostgreSQL, async fill cache.
func (c *CacheAsideClient) Read(ctx context.Context, key string) ReadResult {
	cacheCtx, cancel := context.WithTimeout(ctx, CacheReadTimeout)
	val, ver, found, err := c.cache.Get(cacheCtx, key)
	cancel()

	if err == nil && found {
		c.hits.Add(1)
		isStale, afterAck := c.staleTracker.CheckStale(key, ver)
		if isStale {
			if afterAck {
				c.staleReadsAfterAck.Add(1)
			} else {
				c.staleReadsInWindow.Add(1)
			}
		}

		return ReadResult{
			Value:           val,
			Version:         ver,
			IsHit:           true,
			IsStale:         isStale,
			IsStaleAfterAck: afterAck,
		}
	}

	// Miss, error, or timeout
	c.misses.Add(1)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			c.cacheTimeouts.Add(1)
		} else {
			c.cacheErrors.Add(1)
		}
	}

	// Read from PostgreSQL
	c.dbQueries.Add(1)
	dbVal, dbVer, dbErr := c.db.Get(ctx, key)
	if dbErr != nil {
		return ReadResult{
			ExtraPostgresQuery: true,
			Err:                dbErr,
		}
	}

	// Async populate cache (errors ignored per design doc §4.1)
	go func(k, v string, version int64) {
		setCtx, setCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer setCancel()
		_ = c.cache.Set(setCtx, k, v, version)
	}(key, dbVal, dbVer)

	return ReadResult{
		Value:              dbVal,
		Version:            dbVer,
		IsHit:              false,
		ExtraPostgresQuery: true,
	}
}

// Write implements the cache-aside write sequence specified in design doc §4.2:
// 1. Update PostgreSQL: version = version + 1 RETURNING version.
// 2. DELETE from cache.
// 3. On failure, queue for retry every 200 ms up to 60 seconds.
func (c *CacheAsideClient) Write(ctx context.Context, key, value string) (int64, error) {
	c.dbUpdates.Add(1)
	newVersion, err := c.db.Update(ctx, key, value)
	if err != nil {
		return 0, err
	}

	c.staleTracker.RecordDBWrite(key, newVersion)

	// Attempt synchronous delete first
	delCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	err = c.cache.Delete(delCtx, key, newVersion)
	cancel()

	if err == nil {
		c.staleTracker.MarkDeleteAcknowledged(key, newVersion)
		return newVersion, nil
	}

	// Queue for retry
	c.mu.Lock()
	existing, ok := c.pendingDels[key]
	if !ok || newVersion > existing.version {
		c.pendingDels[key] = retryItem{
			key:     key,
			version: newVersion,
			start:   time.Now(),
		}
	}
	c.mu.Unlock()

	return newVersion, nil
}

func (c *CacheAsideClient) deleteRetryLoop() {
	ticker := time.NewTicker(DeleteRetryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.processPendingDeletes()
		}
	}
}

func (c *CacheAsideClient) processPendingDeletes() {
	c.mu.Lock()
	if len(c.pendingDels) == 0 {
		c.mu.Unlock()
		return
	}

	items := make([]retryItem, 0, len(c.pendingDels))
	for _, it := range c.pendingDels {
		items = append(items, it)
	}
	c.mu.Unlock()

	now := time.Now()
	for _, it := range items {
		if now.Sub(it.start) > DeleteRetryMaxTimeout {
			// Abandon retry
			c.mu.Lock()
			if curr, ok := c.pendingDels[it.key]; ok && curr.version == it.version {
				delete(c.pendingDels, it.key)
				c.invalidationsAbandoned.Add(1)
			}
			c.mu.Unlock()
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		err := c.cache.Delete(ctx, it.key, it.version)
		cancel()

		if err == nil {
			c.mu.Lock()
			if curr, ok := c.pendingDels[it.key]; ok && curr.version == it.version {
				delete(c.pendingDels, it.key)
				c.staleTracker.MarkDeleteAcknowledged(it.key, it.version)
			}
			c.mu.Unlock()
		}
	}
}

// Metrics returns the aggregated operational metrics.
func (c *CacheAsideClient) Metrics() ClientMetrics {
	return ClientMetrics{
		Hits:                   c.hits.Load(),
		Misses:                 c.misses.Load(),
		CacheErrors:            c.cacheErrors.Load(),
		CacheTimeouts:          c.cacheTimeouts.Load(),
		DBQueries:              c.dbQueries.Load(),
		DBUpdates:              c.dbUpdates.Load(),
		StaleReadsInWindow:     c.staleReadsInWindow.Load(),
		StaleReadsAfterAck:     c.staleReadsAfterAck.Load(),
		InvalidationsAbandoned: c.invalidationsAbandoned.Load(),
	}
}

// Close gracefully closes the client and shuts down retry workers.
func (c *CacheAsideClient) Close() error {
	var err error
	c.closeOnce.Do(func() {
		close(c.stopCh)
		err = c.cache.Close()
	})
	return err
}
