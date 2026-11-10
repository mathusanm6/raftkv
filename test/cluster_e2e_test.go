package test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jathurchan/raft/logger"
	"github.com/jathurchan/raft/raft"
	"github.com/jathurchan/raft/storage"
	"github.com/jathurchan/raft/types"
	"github.com/mathusanm6/raftkv/pkg/cache"
	"github.com/mathusanm6/raftkv/pkg/eviction"
	"github.com/mathusanm6/raftkv/pkg/raftnode"
	"github.com/mathusanm6/raftkv/pkg/server"
	raftkvv1 "github.com/mathusanm6/raftkv/proto/raftkv/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type clusterNode struct {
	id          types.NodeID
	raftPort    int
	clientPort  int
	httpPort    int
	dataDir     string
	node        raft.Raft
	store       *cache.Store
	applier     *raftnode.Applier
	coalescer   *raftnode.ReadIndexCoalescer
	grpcServer  *grpc.Server
	httpServer  *http.Server
	grpcLis     net.Listener
	stopTicker  chan struct{}
	running     bool
}

func (cn *clusterNode) Stop() {
	if !cn.running {
		return
	}
	cn.running = false
	close(cn.stopTicker)
	if cn.grpcServer != nil {
		cn.grpcServer.Stop()
	}
	if cn.grpcLis != nil {
		_ = cn.grpcLis.Close()
	}
	if cn.httpServer != nil {
		_ = cn.httpServer.Close()
	}
	if cn.coalescer != nil {
		cn.coalescer.Stop()
	}
	if cn.node != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = cn.node.Stop(ctx)
	}
}

type staticResolver struct {
	peers map[types.NodeID]string
}

func (r *staticResolver) ResolveClientAddress(id types.NodeID) string {
	return r.peers[id]
}

func createClusterNode(
	t *testing.T,
	id types.NodeID,
	raftPort, clientPort, httpPort int,
	peers map[types.NodeID]raft.PeerConfig,
	clientAddrs map[types.NodeID]string,
) *clusterNode {
	dataDir := t.TempDir()
	lg := logger.NewStdLogger("warn")

	storageOpts := storage.DefaultFileStorageOptions()
	storageOpts.SyncOnAppend = false // Faster for local unit testing

	st, err := storage.NewFileStorageWithOptions(storage.StorageConfig{Dir: dataDir}, storageOpts, lg)
	require.NoError(t, err)

	store := cache.NewStore()
	lru := eviction.NewLRU()
	coldStart := eviction.NewColdStartWrapper(lru, nil)
	applier := raftnode.NewApplier(store, coldStart)

	cfg := raft.Config{
		ID:    id,
		Peers: peers,
		Options: raft.Options{
			ElectionTickCount:           10, // Fast elections for tests (10 * 50ms = 500ms base)
			HeartbeatTickCount:          2,
			ElectionRandomizationFactor: 1.5,
			MaxLogEntriesPerRequest:     100,
			SnapshotThreshold:           1000,
			LogCompactionMinEntries:     200,
		},
		FeatureFlags: raft.FeatureFlags{
			EnableLeaderLease: false,
			EnablePreVote:     false,
		},
		TuningParams: raft.TuningParams{
			MaxApplyBatchSize:    64,
			MaxSnapshotChunkSize: 4 << 20,
		},
	}

	node, err := raft.NewRaftBuilder().
		WithConfig(cfg).
		WithStorage(st).
		WithApplier(applier).
		WithLogger(lg).
		Build()
	require.NoError(t, err)

	nm, err := raft.NewGRPCNetworkManager(
		id,
		fmt.Sprintf("127.0.0.1:%d", raftPort),
		peers,
		node,
		&atomic.Bool{},
		lg,
		raft.NewNoOpMetrics(),
		raft.NewStandardClock(),
		raft.DefaultGRPCNetworkManagerOptions(),
	)
	require.NoError(t, err)

	node.SetNetworkManager(nm)
	err = node.Start()
	require.NoError(t, err)

	stopTicker := make(chan struct{})
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopTicker:
				return
			case <-ticker.C:
				node.Tick(context.Background())
			}
		}
	}()

	go func() {
		for range node.ApplyChannel() {
		}
	}()

	coalescer := raftnode.NewReadIndexCoalescer(node, store, coldStart)
	proposer := raftnode.NewProposalManager(node, store, applier, coldStart)
	resolver := &staticResolver{peers: clientAddrs}

	grpcServer := grpc.NewServer()
	raftKVService := server.NewGRPCServer(node, coalescer, proposer, resolver)
	raftkvv1.RegisterRaftKVServer(grpcServer, raftKVService)

	grpcLis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", clientPort))
	require.NoError(t, err)

	go func() {
		_ = grpcServer.Serve(grpcLis)
	}()

	httpService := server.NewHTTPServer(node, store, coldStart, resolver)
	httpServer := &http.Server{
		Addr:    fmt.Sprintf("127.0.0.1:%d", httpPort),
		Handler: httpService.Handler(),
	}
	go func() {
		_ = httpServer.ListenAndServe()
	}()

	return &clusterNode{
		id:          id,
		raftPort:    raftPort,
		clientPort:  clientPort,
		httpPort:    httpPort,
		dataDir:     dataDir,
		node:        node,
		store:       store,
		applier:     applier,
		coalescer:   coalescer,
		grpcServer:  grpcServer,
		httpServer:  httpServer,
		grpcLis:     grpcLis,
		stopTicker:  stopTicker,
		running:     true,
	}
}

// TestCluster3NodeFailover tests a full 3-replica cluster under leader failure.
func TestCluster3NodeFailover(t *testing.T) {
	nodeIDs := []types.NodeID{"node-0", "node-1", "node-2"}

	// Find 9 available local ports (3 raft, 3 client, 3 http)
	ports := make([]int, 9)
	for i := 0; i < 9; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		ports[i] = lis.Addr().(*net.TCPAddr).Port
		_ = lis.Close()
	}

	raftPeers := make(map[types.NodeID]raft.PeerConfig)
	clientAddrs := make(map[types.NodeID]string)
	httpAddrs := make([]string, 3)
	grpcAddrs := make([]string, 3)

	for i, id := range nodeIDs {
		raftPort := ports[i*3]
		clientPort := ports[i*3+1]
		httpPort := ports[i*3+2]

		raftAddr := fmt.Sprintf("127.0.0.1:%d", raftPort)
		clientAddr := fmt.Sprintf("127.0.0.1:%d", clientPort)
		httpAddr := fmt.Sprintf("127.0.0.1:%d", httpPort)

		raftPeers[id] = raft.PeerConfig{ID: id, Address: raftAddr}
		clientAddrs[id] = clientAddr
		grpcAddrs[i] = clientAddr
		httpAddrs[i] = httpAddr
	}

	nodes := make(map[types.NodeID]*clusterNode)
	for i, id := range nodeIDs {
		nodes[id] = createClusterNode(
			t,
			id,
			ports[i*3],
			ports[i*3+1],
			ports[i*3+2],
			raftPeers,
			clientAddrs,
		)
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	// 1. Wait for cluster to reach quorum and elect an initial leader
	t.Log("Waiting for initial leader election among 3 nodes...")
	var initialLeader types.NodeID
	require.Eventually(t, func() bool {
		for _, id := range nodeIDs {
			status := nodes[id].node.Status()
			if status.Role == types.RoleLeader {
				initialLeader = id
				return true
			}
		}
		return false
	}, 15*time.Second, 100*time.Millisecond)

	t.Logf("Initial leader elected: %s", initialLeader)

	// Wait briefly for leader no-op to commit
	time.Sleep(300 * time.Millisecond)

	// 2. Connect client and write 20 keys
	clientConn, err := grpc.NewClient(clientAddrs[initialLeader], grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer clientConn.Close()

	kvClient := raftkvv1.NewRaftKVClient(clientConn)

	t.Log("Writing 20 key-value pairs through leader...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 1; i <= 20; i++ {
		key := fmt.Sprintf("bench-key-%04d", i)
		val := fmt.Sprintf("val-content-%d", i)
		_, err := kvClient.Set(ctx, &raftkvv1.SetRequest{
			Key:     key,
			Value:   val,
			Version: int64(i),
		})
		require.NoError(t, err)
	}

	// 3. Verify all 20 keys can be read via ReadIndex
	for i := 1; i <= 20; i++ {
		key := fmt.Sprintf("bench-key-%04d", i)
		expectedVal := fmt.Sprintf("val-content-%d", i)
		resp, err := kvClient.Get(ctx, &raftkvv1.GetRequest{Key: key})
		require.NoError(t, err)
		require.True(t, resp.Found)
		require.Equal(t, expectedVal, resp.Value)
	}
	t.Log("All 20 keys verified via linearizable ReadIndex")

	// 4. KILL THE LEADER
	t.Logf("Killing current leader %s...", initialLeader)
	nodes[initialLeader].Stop()

	// 5. Wait for remaining 2 nodes to elect a new leader
	t.Log("Waiting for surviving nodes to elect a new leader (quorum = 2)...")
	var newLeader types.NodeID
	require.Eventually(t, func() bool {
		for _, id := range nodeIDs {
			if id == initialLeader {
				continue
			}
			status := nodes[id].node.Status()
			if status.Role == types.RoleLeader {
				newLeader = id
				return true
			}
		}
		return false
	}, 15*time.Second, 100*time.Millisecond)

	t.Logf("New leader elected: %s", newLeader)
	require.NotEqual(t, initialLeader, newLeader)

	// Wait briefly for new leader no-op entry to commit
	time.Sleep(300 * time.Millisecond)

	// 6. Connect to the new leader and verify all original 20 keys survived with zero lost writes!
	newLeaderConn, err := grpc.NewClient(clientAddrs[newLeader], grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer newLeaderConn.Close()

	newKVClient := raftkvv1.NewRaftKVClient(newLeaderConn)

	t.Log("Verifying all 20 original keys from the new leader...")
	for i := 1; i <= 20; i++ {
		key := fmt.Sprintf("bench-key-%04d", i)
		expectedVal := fmt.Sprintf("val-content-%d", i)
		resp, err := newKVClient.Get(ctx, &raftkvv1.GetRequest{Key: key})
		require.NoError(t, err)
		require.True(t, resp.Found)
		require.Equal(t, expectedVal, resp.Value)
		require.Equal(t, int64(i), resp.Version)
	}
	t.Log("100% data preservation verified after leader failure!")

	// 7. Write 5 additional keys to prove cluster is fully functional with 2 of 3 nodes
	t.Log("Writing 5 new keys to the new leader with 2 active replicas...")
	for i := 21; i <= 25; i++ {
		key := fmt.Sprintf("bench-key-%04d", i)
		val := fmt.Sprintf("val-content-%d", i)
		_, err := newKVClient.Set(ctx, &raftkvv1.SetRequest{
			Key:     key,
			Value:   val,
			Version: int64(i),
		})
		require.NoError(t, err)
	}

	for i := 21; i <= 25; i++ {
		key := fmt.Sprintf("bench-key-%04d", i)
		resp, err := newKVClient.Get(ctx, &raftkvv1.GetRequest{Key: key})
		require.NoError(t, err)
		require.True(t, resp.Found)
	}
	t.Log("Cluster operations verified successfully with 2-of-3 quorum!")
}
