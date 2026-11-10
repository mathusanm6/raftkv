package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mathusanm6/raftkv/pkg/client"
)

func main() {
	var (
		system       = flag.String("system", "a", "Target system: 'a' (raftkv) or 'b' (shardedkv)")
		grpcAddrs    = flag.String("grpc-addrs", "127.0.0.1:9000", "Comma-separated cache gRPC addresses")
		httpAddrs    = flag.String("http-addrs", "127.0.0.1:8080", "Comma-separated cache HTTP addresses")
		postgresURL  = flag.String("postgres-url", "", "PostgreSQL connection string (if empty, uses in-memory mock DB)")
		targetRate   = flag.Int("rate", 1000, "Fixed open-loop request rate per second")
		duration     = flag.Duration("duration", 60*time.Second, "Run duration (e.g. 60s, 5m, 15m)")
		seed         = flag.Int64("seed", 133742, "RNG seed for Zipf generator")
		outputCSV    = flag.String("output-csv", "", "Path to write CSV metrics log")
	)
	flag.Parse()

	log.Printf("Starting benchmark load generator for System %s at %d req/s for %v (seed=%d)",
		strings.ToUpper(*system), *targetRate, *duration, *seed)

	// 1. Database setup
	var db client.DB
	var err error
	if *postgresURL != "" {
		db, err = client.NewPostgresDB(*postgresURL, 100)
		if err != nil {
			log.Fatalf("Failed to connect to PostgreSQL: %v", err)
		}
	} else {
		log.Println("No postgres-url provided, using mock in-memory DB")
		mock := client.NewMockDB()
		// Seed 1,000 sample keys
		for i := int64(0); i < 1000; i++ {
			mock.SetInitial(client.FormatKey(i), client.GenerateValue(1), 1)
		}
		db = mock
	}
	defer db.Close()

	// 2. Cache Client setup
	grpcList := strings.Split(*grpcAddrs, ",")
	httpList := strings.Split(*httpAddrs, ",")

	var cacheClient client.CacheClient
	if strings.ToLower(*system) == "b" || strings.ToLower(*system) == "shardedkv" {
		cacheClient, err = client.NewShardedKVClient(grpcList)
	} else {
		cacheClient, err = client.NewRaftKVClient(grpcList, httpList)
	}
	if err != nil {
		log.Fatalf("Failed to initialize cache client: %v", err)
	}
	defer cacheClient.Close()

	// 3. Cache-Aside Client
	staleTracker := client.NewStaleTracker()
	asideClient := client.NewCacheAsideClient(cacheClient, db, staleTracker)
	defer asideClient.Close()

	// 4. CSV output writer
	var csvWriter *csv.Writer
	if *outputCSV != "" {
		f, err := os.Create(*outputCSV)
		if err != nil {
			log.Fatalf("Failed to create CSV output file: %v", err)
		}
		defer f.Close()
		csvWriter = csv.NewWriter(f)
		defer csvWriter.Flush()

		_ = csvWriter.Write([]string{
			"timestamp", "elapsed_sec", "requests_sent", "hits", "misses",
			"hit_ratio", "hr10", "timeouts", "errors", "pg_queries",
			"pg_updates", "stale_window", "stale_after_ack",
		})
	}

	loadGen := client.NewLoadGenerator(asideClient, client.LoadGenConfig{
		TargetRate: *targetRate,
		Duration:   *duration,
		Seed:       *seed,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigCh
		log.Println("Stopping load generator on signal...")
		cancel()
	}()

	fmt.Printf("%-6s | %-8s | %-6s | %-6s | %-8s | %-8s | %-8s | %-8s\n",
		"Sec", "Sent", "Hits", "Misses", "HR(t)", "HR10", "PG Qs", "Stale")
	fmt.Println(strings.Repeat("-", 75))

	err = loadGen.Run(ctx, func(s client.PerSecondStats) {
		fmt.Printf("%-6d | %-8d | %-6d | %-6d | %-8.3f | %-8.3f | %-8d | %-8d\n",
			s.ElapsedSeconds, s.RequestsSent, s.Hits, s.Misses, s.HitRatio, s.HR10,
			s.PostgresQueries, s.StaleReadsInWindow+s.StaleReadsAfterAck)

		if csvWriter != nil {
			_ = csvWriter.Write([]string{
				s.Timestamp.Format(time.RFC3339),
				fmt.Sprintf("%d", s.ElapsedSeconds),
				fmt.Sprintf("%d", s.RequestsSent),
				fmt.Sprintf("%d", s.Hits),
				fmt.Sprintf("%d", s.Misses),
				fmt.Sprintf("%.4f", s.HitRatio),
				fmt.Sprintf("%.4f", s.HR10),
				fmt.Sprintf("%d", s.Timeouts),
				fmt.Sprintf("%d", s.Errors),
				fmt.Sprintf("%d", s.PostgresQueries),
				fmt.Sprintf("%d", s.PostgresUpdates),
				fmt.Sprintf("%d", s.StaleReadsInWindow),
				fmt.Sprintf("%d", s.StaleReadsAfterAck),
			})
			csvWriter.Flush()
		}
	})

	if err != nil && err != context.Canceled {
		log.Printf("Load generator ended with error: %v", err)
	} else {
		log.Println("Load generator run completed successfully.")
	}
}
