package client

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type mockCacheClient struct {
	mu           sync.Mutex
	data         map[string]mockCacheItem
	deleteErrors int
	deletes      []string
}

type mockCacheItem struct {
	val string
	ver int64
}

func newMockCacheClient() *mockCacheClient {
	return &mockCacheClient{
		data: make(map[string]mockCacheItem),
	}
}

func (m *mockCacheClient) Get(_ context.Context, key string) (string, int64, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	item, ok := m.data[key]
	if !ok {
		return "", 0, false, nil
	}
	return item.val, item.ver, true, nil
}

func (m *mockCacheClient) Set(_ context.Context, key, value string, version int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[key] = mockCacheItem{val: value, ver: version}
	return nil
}

func (m *mockCacheClient) Delete(_ context.Context, key string, version int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.deleteErrors > 0 {
		m.deleteErrors--
		return errors.New("transient delete error")
	}

	delete(m.data, key)
	m.deletes = append(m.deletes, key)
	return nil
}

func (m *mockCacheClient) Close() error {
	return nil
}

func TestFormatKeyAndGenerateValue(t *testing.T) {
	key := FormatKey(42)
	require.Equal(t, 64, len(key), "Key must be exactly 64 bytes")

	val := GenerateValue(1)
	require.Equal(t, 512, len(val), "Value must be exactly 512 bytes")
}

func TestZipfGeneratorSkew(t *testing.T) {
	const n = 1000
	zipf := NewZipfGenerator(n, 0.9, 133742)

	counts := make(map[int64]int)
	const samples = 20000

	for i := 0; i < samples; i++ {
		idx := zipf.Next()
		require.GreaterOrEqual(t, idx, int64(0))
		require.Less(t, idx, int64(n))
		counts[idx]++
	}

	// In Zipf 0.9, the top 10% most popular keys should receive >50% of the traffic
	top10PercentCount := 0
	for i := int64(0); i < n/10; i++ {
		top10PercentCount += counts[i]
	}

	ratio := float64(top10PercentCount) / float64(samples)
	require.Greater(t, ratio, 0.50, "Top 10% keys should capture >50% of requests in Zipf 0.9")
}

func TestCacheAsideReadMissThenHit(t *testing.T) {
	cache := newMockCacheClient()
	db := NewMockDB()
	db.SetInitial("key-1", "val-1", 1)

	tracker := NewStaleTracker()
	client := NewCacheAsideClient(cache, db, tracker)
	defer client.Close()

	ctx := context.Background()

	// 1. First Read: Cache Miss -> fetches from DB -> async sets cache
	res1 := client.Read(ctx, "key-1")
	require.NoError(t, res1.Err)
	require.False(t, res1.IsHit)
	require.Equal(t, "val-1", res1.Value)
	require.Equal(t, int64(1), res1.Version)

	// Allow async goroutine to populate cache
	time.Sleep(50 * time.Millisecond)

	// 2. Second Read: Cache Hit!
	res2 := client.Read(ctx, "key-1")
	require.NoError(t, res2.Err)
	require.True(t, res2.IsHit)
	require.Equal(t, "val-1", res2.Value)
	require.Equal(t, int64(1), res2.Version)
	require.False(t, res2.IsStale)

	metrics := client.Metrics()
	require.Equal(t, int64(1), metrics.Hits)
	require.Equal(t, int64(1), metrics.Misses)
	require.Equal(t, int64(1), metrics.DBQueries)
}

func TestCacheAsideWriteAndStaleReadDetection(t *testing.T) {
	cache := newMockCacheClient()
	db := NewMockDB()
	db.SetInitial("key-1", "val-1", 1)

	tracker := NewStaleTracker()
	client := NewCacheAsideClient(cache, db, tracker)
	defer client.Close()

	ctx := context.Background()

	// Seed cache with version 1
	_ = cache.Set(ctx, "key-1", "val-1", 1)

	// 1. Write updates DB to version 2, and deletes from cache
	newVer, err := client.Write(ctx, "key-1", "val-2")
	require.NoError(t, err)
	require.Equal(t, int64(2), newVer)

	// Read should now miss from cache and get version 2 from DB
	res := client.Read(ctx, "key-1")
	require.NoError(t, res.Err)
	require.False(t, res.IsHit)
	require.Equal(t, "val-2", res.Value)
	require.Equal(t, int64(2), res.Version)

	// 2. Simulate stale cache return: re-inject version 1 into cache
	_ = cache.Set(ctx, "key-1", "val-1", 1)

	// Read gets version 1 from cache -> must be detected as STALE AFTER ACK!
	resStale := client.Read(ctx, "key-1")
	require.NoError(t, resStale.Err)
	require.True(t, resStale.IsHit)
	require.Equal(t, "val-1", resStale.Value)
	require.True(t, resStale.IsStale)
	require.True(t, resStale.IsStaleAfterAck)

	metrics := client.Metrics()
	require.Equal(t, int64(1), metrics.StaleReadsAfterAck)
}

func TestCacheAsideDeleteRetry(t *testing.T) {
	cache := newMockCacheClient()
	cache.deleteErrors = 2 // First 2 delete calls fail

	db := NewMockDB()
	db.SetInitial("k-retry", "v1", 1)

	tracker := NewStaleTracker()
	client := NewCacheAsideClient(cache, db, tracker)
	defer client.Close()

	ctx := context.Background()
	_ = cache.Set(ctx, "k-retry", "v1", 1)

	// Write triggers Delete, which fails initially and enqueues to retry loop
	newVer, err := client.Write(ctx, "k-retry", "v2")
	require.NoError(t, err)
	require.Equal(t, int64(2), newVer)

	// Wait for retry loop (DeleteRetryInterval = 200ms) to successfully delete
	require.Eventually(t, func() bool {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		_, exists := cache.data["k-retry"]
		return !exists
	}, 1*time.Second, 50*time.Millisecond)
}
