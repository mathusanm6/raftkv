package shardedkv

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mathusanm6/raftkv/pkg/cache"
	"github.com/mathusanm6/raftkv/pkg/eviction"
	raftkvv1 "github.com/mathusanm6/raftkv/proto/raftkv/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestShardSynchronousEvictionAt100Percent(t *testing.T) {
	// Configure small max memory: enough for tombstone reserve + 2 entries
	entryBytes := cache.EntryAccountedBytes("key-1", "val-1")
	maxMemory := cache.TombstoneReserveBytes + (entryBytes * 2)

	lru := eviction.NewLRU()
	shard := NewShard(lru, maxMemory)

	ctx := context.Background()

	// 1. Add key-1
	err := shard.Set(ctx, "key-1", "val-1", 1)
	require.NoError(t, err)

	// 2. Add key-2
	err = shard.Set(ctx, "key-2", "val-2", 1)
	require.NoError(t, err)

	require.Equal(t, 2, shard.Store().Len())

	// Access key-1 to make key-2 the LRU victim
	_, _, found, err := shard.Get(ctx, "key-1")
	require.NoError(t, err)
	require.True(t, found)

	// 3. Add key-3: memory limit reached -> triggers synchronous eviction of key-2!
	err = shard.Set(ctx, "key-3", "val-3", 1)
	require.NoError(t, err)

	require.Equal(t, 2, shard.Store().Len())

	// key-2 must have been evicted
	_, _, found, err = shard.Get(ctx, "key-2")
	require.NoError(t, err)
	require.False(t, found, "key-2 should have been evicted")

	// key-1 and key-3 must exist
	_, _, found, err = shard.Get(ctx, "key-1")
	require.NoError(t, err)
	require.True(t, found)

	_, _, found, err = shard.Get(ctx, "key-3")
	require.NoError(t, err)
	require.True(t, found)
}

func TestShardVersionRules(t *testing.T) {
	lru := eviction.NewLRU()
	shard := NewShard(lru, 0)
	ctx := context.Background()

	// Set version 2
	err := shard.Set(ctx, "k1", "v2", 2)
	require.NoError(t, err)

	// Stale set version 1: must be ignored
	err = shard.Set(ctx, "k1", "v1", 1)
	require.NoError(t, err)

	val, ver, found, err := shard.Get(ctx, "k1")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "v2", val)
	require.Equal(t, int64(2), ver)

	// Delete with older version 1: must be ignored
	err = shard.Delete(ctx, "k1", 1)
	require.NoError(t, err)

	val, ver, found, err = shard.Get(ctx, "k1")
	require.NoError(t, err)
	require.True(t, found, "Key should still exist because delete was outdated")

	// Delete with version 2: succeeds
	err = shard.Delete(ctx, "k1", 2)
	require.NoError(t, err)

	_, _, found, err = shard.Get(ctx, "k1")
	require.NoError(t, err)
	require.False(t, found)

	// Re-set with version 2: ignored because tombstone is at version 2
	err = shard.Set(ctx, "k1", "v2-reborn", 2)
	require.NoError(t, err)

	_, _, found, err = shard.Get(ctx, "k1")
	require.NoError(t, err)
	require.False(t, found, "Set should be rejected by tombstone")

	// Re-set with version 3: succeeds
	err = shard.Set(ctx, "k1", "v3", 3)
	require.NoError(t, err)

	val, ver, found, err = shard.Get(ctx, "k1")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "v3", val)
}

func TestShardGRPCAndHTTP(t *testing.T) {
	lru := eviction.NewLRU()
	shard := NewShard(lru, 0)

	grpcSvc := NewGRPCServer(shard)

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	raftkvv1.RegisterRaftKVServer(grpcServer, grpcSvc)
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := raftkvv1.NewRaftKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 1. SET
	_, err = client.Set(ctx, &raftkvv1.SetRequest{
		Key:     "sharded-key",
		Value:   "sharded-val",
		Version: 1,
	})
	require.NoError(t, err)

	// 2. GET
	getResp, err := client.Get(ctx, &raftkvv1.GetRequest{Key: "sharded-key"})
	require.NoError(t, err)
	require.True(t, getResp.Found)
	require.Equal(t, "sharded-val", getResp.Value)
	require.Equal(t, int64(1), getResp.Version)

	// 3. HTTP Endpoints
	httpSvc := NewHTTPServer("shard-0", shard)
	handler := httpSvc.Handler()

	req := httptest.NewRequest(http.MethodGet, "/livez", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	req = httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	req = httptest.NewRequest(http.MethodGet, "/status", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var statusObj StatusResponse
	err = json.Unmarshal(rec.Body.Bytes(), &statusObj)
	require.NoError(t, err)
	require.Equal(t, "shard-0", statusObj.ShardID)
	require.Equal(t, 1, statusObj.EntryCount)
}

func BenchmarkShardSynchronousSet(b *testing.B) {
	lru := eviction.NewLRU()
	shard := NewShard(lru, 0)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("k-%d", i)
		_ = shard.Set(ctx, key, "val", int64(i+1))
	}
}
