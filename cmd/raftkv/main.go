package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
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
	"google.golang.org/grpc"
)

type peerInfo struct {
	raftAddr   string
	clientAddr string
}

type staticPeerResolver struct {
	peers map[types.NodeID]peerInfo
}

func (r *staticPeerResolver) ResolveClientAddress(id types.NodeID) string {
	if p, ok := r.peers[id]; ok {
		return p.clientAddr
	}
	return ""
}

func parsePeers(peersFlag string) (map[types.NodeID]raft.PeerConfig, map[types.NodeID]peerInfo) {
	raftPeers := make(map[types.NodeID]raft.PeerConfig)
	peerInfos := make(map[types.NodeID]peerInfo)

	// Format: "node-0=node-0.raft:9001=node-0.raft:9000,node-1=..."
	for _, entry := range strings.Split(peersFlag, ",") {
		parts := strings.Split(strings.TrimSpace(entry), "=")
		if len(parts) >= 2 {
			id := types.NodeID(parts[0])
			raftAddr := parts[1]
			clientAddr := raftAddr
			if len(parts) >= 3 {
				clientAddr = parts[2]
			}
			raftPeers[id] = raft.PeerConfig{
				ID:      id,
				Address: raftAddr,
			}
			peerInfos[id] = peerInfo{
				raftAddr:   raftAddr,
				clientAddr: clientAddr,
			}
		}
	}
	return raftPeers, peerInfos
}

func main() {
	var (
		nodeIDStr    = flag.String("id", os.Getenv("POD_NAME"), "Node ID (defaults to POD_NAME env)")
		peersFlag    = flag.String("peers", "", "Comma-separated peer specifications: id=raftAddr=clientAddr")
		raftPort     = flag.Int("raft-port", 9001, "Port for Raft peer communication")
		clientPort   = flag.Int("client-port", 9000, "Port for client gRPC requests")
		httpPort     = flag.Int("http-port", 8080, "Port for HTTP management and metrics")
		dataDir      = flag.String("data-dir", "/data", "Path to persistent volume data directory")
		policyName   = flag.String("eviction-policy", "w-tinylfu", "Eviction policy: lru, lfu, or w-tinylfu")
		logLevel     = flag.String("log-level", "info", "Log level: debug, info, warn, error")
		syncOnAppend = flag.Bool("sync-on-append", true, "Force fsync on every log append")
	)
	flag.Parse()

	if *nodeIDStr == "" {
		*nodeIDStr = "raftkv-0"
	}
	id := types.NodeID(*nodeIDStr)

	lg := logger.NewStdLogger(*logLevel)
	lg.Infow("Starting raftkv node", "nodeID", id, "policy", *policyName)

	raftPeers, peerInfos := parsePeers(*peersFlag)
	if len(raftPeers) == 0 {
		// Default single-node / local setup
		raftAddr := fmt.Sprintf("127.0.0.1:%d", *raftPort)
		clientAddr := fmt.Sprintf("127.0.0.1:%d", *clientPort)
		raftPeers[id] = raft.PeerConfig{ID: id, Address: raftAddr}
		peerInfos[id] = peerInfo{raftAddr: raftAddr, clientAddr: clientAddr}
	}

	resolver := &staticPeerResolver{peers: peerInfos}

	// 1. Storage setup
	storageOpts := storage.DefaultFileStorageOptions()
	storageOpts.SyncOnAppend = *syncOnAppend

	st, err := storage.NewFileStorageWithOptions(storage.StorageConfig{
		Dir: *dataDir,
	}, storageOpts, lg)
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}

	// 2. Cache Store and Eviction Policy setup
	store := cache.NewStore()

	var basePolicy eviction.Policy
	switch strings.ToLower(*policyName) {
	case "lru":
		basePolicy = eviction.NewLRU()
	case "lfu":
		basePolicy = eviction.NewLFU()
	default:
		// Capacity for 64 MiB accounting is approx 102,700 items
		basePolicy = eviction.NewWTinyLFU(102700)
	}

	coldStartPolicy := eviction.NewColdStartWrapper(basePolicy, nil)
	applier := raftnode.NewApplier(store, coldStartPolicy)

	// 3. Raft Node Configuration
	cfg := raft.Config{
		ID:    id,
		Peers: raftPeers,
		Options: raft.Options{
			ElectionTickCount:           50,
			HeartbeatTickCount:          5,
			ElectionRandomizationFactor: 2.0,
			MaxLogEntriesPerRequest:     100,
			SnapshotThreshold:           100000,
			LogCompactionMinEntries:     200,
		},
		FeatureFlags: raft.FeatureFlags{
			EnableLeaderLease: false,
			EnablePreVote:     false,
		},
		TuningParams: raft.TuningParams{
			MaxApplyBatchSize:    64,
			MaxSnapshotChunkSize: 4 << 20, // 4 MiB chunks
		},
	}

	node, err := raft.NewRaftBuilder().
		WithConfig(cfg).
		WithStorage(st).
		WithApplier(applier).
		WithLogger(lg).
		Build()
	if err != nil {
		log.Fatalf("Failed to build Raft node: %v", err)
	}

	nm, err := raft.NewGRPCNetworkManager(
		id,
		fmt.Sprintf("0.0.0.0:%d", *raftPort),
		raftPeers,
		node,
		&atomic.Bool{},
		lg,
		raft.NewNoOpMetrics(),
		raft.NewStandardClock(),
		raft.DefaultGRPCNetworkManagerOptions(),
	)
	if err != nil {
		log.Fatalf("Failed to initialize GRPCNetworkManager: %v", err)
	}

	node.SetNetworkManager(nm)
	if err := node.Start(); err != nil {
		log.Fatalf("Failed to start Raft node: %v", err)
	}

	// 4. Background Raft Tick Loop (100 ms) and ApplyChannel drain
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		ticker := time.NewTicker(raft.NominalTickInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				node.Tick(ctx)
			}
		}
	}()

	go func() {
		// Keep ApplyChannel drained per design doc §2.1
		for range node.ApplyChannel() {
		}
	}()

	readCoalescer := raftnode.NewReadIndexCoalescer(node, store, coldStartPolicy)
	proposer := raftnode.NewProposalManager(node, store, applier, coldStartPolicy)

	// 5. Monitor Leadership Changes
	go func() {
		for leaderID := range node.LeaderChangeChannel() {
			if leaderID == id {
				lg.Infow("Became leader, initializing cold-start warmup", "nodeID", id)
				entries := store.Entries()
				sort.Slice(entries, func(i, j int) bool {
					return entries[i].ApplyIndex < entries[j].ApplyIndex
				})
				keys := make([]string, len(entries))
				for i, e := range entries {
					keys[i] = e.Key
				}
				coldStartPolicy.StartColdStart(keys)
			} else {
				// Leadership lost: abort waiters
				applier.AbortWaiters(raftnode.ErrNotLeader)
				readCoalescer.AbortAll(raftnode.ErrNotLeader)
			}
		}
	}()

	// 6. Client gRPC Server (:9000)
	grpcServer := grpc.NewServer()
	raftKVService := server.NewGRPCServer(node, readCoalescer, proposer, resolver)
	raftkvv1.RegisterRaftKVServer(grpcServer, raftKVService)

	grpcListener, err := net.Listen("tcp", fmt.Sprintf(":%d", *clientPort))
	if err != nil {
		log.Fatalf("Failed to listen on gRPC port %d: %v", *clientPort, err)
	}
	go func() {
		if err := grpcServer.Serve(grpcListener); err != nil && !errorsIsClosed(err) {
			lg.Errorw("gRPC server failed", "error", err)
		}
	}()

	// 7. Management HTTP Server (:8080)
	httpService := server.NewHTTPServer(node, store, coldStartPolicy, resolver)
	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", *httpPort),
		Handler: httpService.Handler(),
	}
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			lg.Errorw("HTTP management server failed", "error", err)
		}
	}()

	lg.Infow("raftkv servers running", "grpcPort", *clientPort, "httpPort", *httpPort, "raftPort", *raftPort)

	// 8. Graceful Shutdown on SIGTERM
	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, syscall.SIGTERM, syscall.SIGINT)
	<-stopCh

	lg.Infow("Shutting down raftkv gracefully...")
	grpcServer.GracefulStop()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	_ = httpServer.Shutdown(shutdownCtx)
	readCoalescer.Stop()
	_ = node.Stop(shutdownCtx)

	lg.Infow("Shutdown complete")
}

func errorsIsClosed(err error) bool {
	return strings.Contains(err.Error(), "use of closed network connection")
}
