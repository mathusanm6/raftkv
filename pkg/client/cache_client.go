package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/mathusanm6/raftkv/pkg/hashring"
	raftkvv1 "github.com/mathusanm6/raftkv/proto/raftkv/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var (
	ErrCacheUnavailable = errors.New("cache unavailable or no leader known")
)

// CacheClient defines the uniform cache access interface for both System A and System B.
type CacheClient interface {
	Get(ctx context.Context, key string) (value string, version int64, found bool, err error)
	Set(ctx context.Context, key, value string, version int64) error
	Delete(ctx context.Context, key string, version int64) error
	Close() error
}

// RaftKVClient implements CacheClient for System A (3-node Raft group with follower redirection).
type RaftKVClient struct {
	mu sync.RWMutex

	nodeAddrs  []string
	httpAddrs  []string
	conns      map[string]*grpc.ClientConn
	clients    map[string]raftkvv1.RaftKVClient
	leaderAddr string

	stopCh chan struct{}
}

// NewRaftKVClient initializes a client for System A.
func NewRaftKVClient(grpcAddrs []string, httpAddrs []string) (*RaftKVClient, error) {
	c := &RaftKVClient{
		nodeAddrs: grpcAddrs,
		httpAddrs: httpAddrs,
		conns:     make(map[string]*grpc.ClientConn),
		clients:   make(map[string]raftkvv1.RaftKVClient),
		stopCh:    make(chan struct{}),
	}

	for _, addr := range grpcAddrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			c.Close()
			return nil, err
		}
		c.conns[addr] = conn
		c.clients[addr] = raftkvv1.NewRaftKVClient(conn)
	}

	if len(grpcAddrs) > 0 {
		c.leaderAddr = grpcAddrs[0]
	}

	go c.leaderPollLoop()
	return c, nil
}

func (c *RaftKVClient) leaderPollLoop() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	httpClient := &http.Client{Timeout: 40 * time.Millisecond}

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			// If leader is empty, poll /status of all nodes
			c.mu.RLock()
			currentLeader := c.leaderAddr
			c.mu.RUnlock()

			if currentLeader == "" {
				for _, httpAddr := range c.httpAddrs {
					url := fmt.Sprintf("http://%s/status", httpAddr)
					resp, err := httpClient.Get(url)
					if err == nil && resp.StatusCode == http.StatusOK {
						var st struct {
							Role          string `json:"role"`
							LeaderAddress string `json:"leader_address"`
						}
						if json.NewDecoder(resp.Body).Decode(&st) == nil && st.LeaderAddress != "" {
							c.mu.Lock()
							c.leaderAddr = st.LeaderAddress
							c.mu.Unlock()
							_ = resp.Body.Close()
							break
						}
						_ = resp.Body.Close()
					}
				}
			}
		}
	}
}

func (c *RaftKVClient) getClient() (raftkvv1.RaftKVClient, string) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.leaderAddr != "" {
		if cl, ok := c.clients[c.leaderAddr]; ok {
			return cl, c.leaderAddr
		}
	}
	// Fallback to first available
	for addr, cl := range c.clients {
		return cl, addr
	}
	return nil, ""
}

func (c *RaftKVClient) updateLeader(leaderAddr string) {
	if leaderAddr == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	c.leaderAddr = leaderAddr
	if _, ok := c.clients[leaderAddr]; !ok {
		// Connect to new leader address if not already in pool
		conn, err := grpc.NewClient(leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			c.conns[leaderAddr] = conn
			c.clients[leaderAddr] = raftkvv1.NewRaftKVClient(conn)
		}
	}
}

func extractLeaderHint(err error) string {
	if st, ok := status.FromError(err); ok {
		if st.Code() == codes.FailedPrecondition {
			for _, detail := range st.Details() {
				if hint, ok := detail.(*raftkvv1.LeaderHint); ok {
					return hint.LeaderAddress
				}
			}
		}
	}
	return ""
}

// Get performs a read request with follower redirection retry.
func (c *RaftKVClient) Get(ctx context.Context, key string) (string, int64, bool, error) {
	client, _ := c.getClient()
	if client == nil {
		return "", 0, false, ErrCacheUnavailable
	}

	resp, err := client.Get(ctx, &raftkvv1.GetRequest{Key: key})
	if err != nil {
		newLeader := extractLeaderHint(err)
		if newLeader != "" {
			c.updateLeader(newLeader)
			// Retry once at the new leader within deadline
			c.mu.RLock()
			retryClient := c.clients[newLeader]
			c.mu.RUnlock()
			if retryClient != nil {
				resp, err = retryClient.Get(ctx, &raftkvv1.GetRequest{Key: key})
			}
		}
	}

	if err != nil {
		return "", 0, false, err
	}
	return resp.Value, resp.Version, resp.Found, nil
}

// Set performs a write request with follower redirection retry.
func (c *RaftKVClient) Set(ctx context.Context, key, value string, version int64) error {
	client, _ := c.getClient()
	if client == nil {
		return ErrCacheUnavailable
	}

	_, err := client.Set(ctx, &raftkvv1.SetRequest{
		Key:     key,
		Value:   value,
		Version: version,
	})
	if err != nil {
		newLeader := extractLeaderHint(err)
		if newLeader != "" {
			c.updateLeader(newLeader)
			c.mu.RLock()
			retryClient := c.clients[newLeader]
			c.mu.RUnlock()
			if retryClient != nil {
				_, err = retryClient.Set(ctx, &raftkvv1.SetRequest{
					Key:     key,
					Value:   value,
					Version: version,
				})
			}
		}
	}
	return err
}

// Delete performs an invalidation request with follower redirection retry.
func (c *RaftKVClient) Delete(ctx context.Context, key string, version int64) error {
	client, _ := c.getClient()
	if client == nil {
		return ErrCacheUnavailable
	}

	_, err := client.Delete(ctx, &raftkvv1.DeleteRequest{
		Key:     key,
		Version: version,
	})
	if err != nil {
		newLeader := extractLeaderHint(err)
		if newLeader != "" {
			c.updateLeader(newLeader)
			c.mu.RLock()
			retryClient := c.clients[newLeader]
			c.mu.RUnlock()
			if retryClient != nil {
				_, err = retryClient.Delete(ctx, &raftkvv1.DeleteRequest{
					Key:     key,
					Version: version,
				})
			}
		}
	}
	return err
}

// Close closes all client connections.
func (c *RaftKVClient) Close() error {
	close(c.stopCh)
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, conn := range c.conns {
		_ = conn.Close()
	}
	return nil
}

// ShardedKVClient implements CacheClient for System B (consistent-hash partitioned shards).
type ShardedKVClient struct {
	ring    *hashring.Ring
	conns   map[string]*grpc.ClientConn
	clients map[string]raftkvv1.RaftKVClient
}

// NewShardedKVClient creates a new ShardedKVClient.
func NewShardedKVClient(shardAddrs []string) (*ShardedKVClient, error) {
	ring := hashring.NewRing(shardAddrs, hashring.DefaultVirtualNodes)
	c := &ShardedKVClient{
		ring:    ring,
		conns:   make(map[string]*grpc.ClientConn),
		clients: make(map[string]raftkvv1.RaftKVClient),
	}

	for _, addr := range shardAddrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			c.Close()
			return nil, err
		}
		c.conns[addr] = conn
		c.clients[addr] = raftkvv1.NewRaftKVClient(conn)
	}

	return c, nil
}

func (s *ShardedKVClient) getClientFor(key string) raftkvv1.RaftKVClient {
	shard := s.ring.GetShard(key)
	return s.clients[shard]
}

// Get routes to the target shard via the consistent hash ring.
func (s *ShardedKVClient) Get(ctx context.Context, key string) (string, int64, bool, error) {
	client := s.getClientFor(key)
	if client == nil {
		return "", 0, false, ErrCacheUnavailable
	}

	resp, err := client.Get(ctx, &raftkvv1.GetRequest{Key: key})
	if err != nil {
		return "", 0, false, err
	}
	return resp.Value, resp.Version, resp.Found, nil
}

// Set routes to the target shard via the consistent hash ring.
func (s *ShardedKVClient) Set(ctx context.Context, key, value string, version int64) error {
	client := s.getClientFor(key)
	if client == nil {
		return ErrCacheUnavailable
	}

	_, err := client.Set(ctx, &raftkvv1.SetRequest{
		Key:     key,
		Value:   value,
		Version: version,
	})
	return err
}

// Delete routes to the target shard via the consistent hash ring.
func (s *ShardedKVClient) Delete(ctx context.Context, key string, version int64) error {
	client := s.getClientFor(key)
	if client == nil {
		return ErrCacheUnavailable
	}

	_, err := client.Delete(ctx, &raftkvv1.DeleteRequest{
		Key:     key,
		Version: version,
	})
	return err
}

// Close closes all shard connections.
func (s *ShardedKVClient) Close() error {
	for _, conn := range s.conns {
		_ = conn.Close()
	}
	return nil
}
