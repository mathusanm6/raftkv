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
	"strings"
	"syscall"
	"time"

	"github.com/mathusanm6/raftkv/pkg/cache"
	"github.com/mathusanm6/raftkv/pkg/eviction"
	"github.com/mathusanm6/raftkv/pkg/shardedkv"
	raftkvv1 "github.com/mathusanm6/raftkv/proto/raftkv/v1"
	"google.golang.org/grpc"
)

func main() {
	var (
		shardID    = flag.String("id", os.Getenv("POD_NAME"), "Shard ID (defaults to POD_NAME env)")
		clientPort = flag.Int("client-port", 9000, "Port for client gRPC requests")
		httpPort   = flag.Int("http-port", 8080, "Port for HTTP management and health")
		policyName = flag.String("eviction-policy", "w-tinylfu", "Eviction policy: lru, lfu, or w-tinylfu")
		maxMemory  = flag.Int64("max-memory", cache.MaxMemoryBytes, "Max accounted memory in bytes (default 64 MiB)")
	)
	flag.Parse()

	if *shardID == "" {
		*shardID = "shard-0"
	}

	log.Printf("Starting shardedkv shard %s with policy %s and max memory %d bytes", *shardID, *policyName, *maxMemory)

	var policy eviction.Policy
	switch strings.ToLower(*policyName) {
	case "lru":
		policy = eviction.NewLRU()
	case "lfu":
		policy = eviction.NewLFU()
	default:
		// Capacity for 64 MiB is approx 102,700 items
		policy = eviction.NewWTinyLFU(102700)
	}

	shard := shardedkv.NewShard(policy, *maxMemory)

	// 1. Client gRPC Server (:9000)
	grpcServer := grpc.NewServer()
	shardedKVService := shardedkv.NewGRPCServer(shard)
	raftkvv1.RegisterRaftKVServer(grpcServer, shardedKVService)

	grpcListener, err := net.Listen("tcp", fmt.Sprintf(":%d", *clientPort))
	if err != nil {
		log.Fatalf("Failed to listen on gRPC port %d: %v", *clientPort, err)
	}
	go func() {
		if err := grpcServer.Serve(grpcListener); err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
			log.Printf("gRPC server error: %v", err)
		}
	}()

	// 2. HTTP Management Server (:8080)
	httpService := shardedkv.NewHTTPServer(*shardID, shard)
	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", *httpPort),
		Handler: httpService.Handler(),
	}
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	log.Printf("shardedkv shard %s running on gRPC :%d and HTTP :%d", *shardID, *clientPort, *httpPort)

	// 3. Graceful Shutdown on SIGTERM
	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, syscall.SIGTERM, syscall.SIGINT)
	<-stopCh

	log.Printf("Shutting down shardedkv shard %s...", *shardID)
	grpcServer.GracefulStop()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownCtx)

	log.Printf("Shutdown complete for %s", *shardID)
}
