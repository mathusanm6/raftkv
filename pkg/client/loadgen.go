package client

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
)

const (
	KeyCount     = 1000000
	ZipfExponent = 0.9
	ReadRatio    = 0.95
)

// FormatKey formats an integer index into an exact 64-byte key matching design doc §2.3:
// "Every key is exactly 64 bytes (zero-padded)".
func FormatKey(idx int64) string {
	// "key-" is 4 chars, %060d is 60 digits -> total 64 bytes
	return fmt.Sprintf("key-%060d", idx)
}

// GenerateValue generates an exact 512-byte value matching design doc §2.3.
func GenerateValue(version int64) string {
	prefix := fmt.Sprintf("val-v%d-", version)
	padLen := 512 - len(prefix)
	if padLen < 0 {
		return prefix[:512]
	}
	return prefix + strings.Repeat("x", padLen)
}

// LoadGenConfig configures the open-loop benchmark load generator.
type LoadGenConfig struct {
	TargetRate int           // Requests per second (e.g. 1000, 5000)
	Duration   time.Duration // Total run duration
	Seed       int64         // seed_i = 133742 + i
	KeyCount   int64         // 1,000,000
}

// PerSecondStats records client-measured metrics for each 1-second interval.
type PerSecondStats struct {
	Timestamp          time.Time
	ElapsedSeconds     int
	RequestsSent       int64
	Hits               int64
	Misses             int64
	HitRatio           float64
	HR10               float64
	Timeouts           int64
	Errors             int64
	PostgresQueries    int64
	PostgresUpdates    int64
	StaleReadsInWindow int64
	StaleReadsAfterAck int64
}

// LoadGenerator coordinates open-loop fixed-rate traffic dispatch.
type LoadGenerator struct {
	client *CacheAsideClient
	cfg    LoadGenConfig
	zipf   *ZipfGenerator
	rng    *rand.Rand
}

// NewLoadGenerator creates a LoadGenerator instance.
func NewLoadGenerator(client *CacheAsideClient, cfg LoadGenConfig) *LoadGenerator {
	if cfg.KeyCount <= 0 {
		cfg.KeyCount = KeyCount
	}
	if cfg.Seed == 0 {
		cfg.Seed = 133742
	}

	zipf := NewZipfGenerator(cfg.KeyCount, ZipfExponent, cfg.Seed)
	rng := rand.New(rand.NewSource(cfg.Seed + 1))

	return &LoadGenerator{
		client: client,
		cfg:    cfg,
		zipf:   zipf,
		rng:    rng,
	}
}

// Run executes the open-loop workload, invoking onStats every second with window metrics.
func (lg *LoadGenerator) Run(ctx context.Context, onStats func(PerSecondStats)) error {
	interval := time.Second / time.Duration(lg.cfg.TargetRate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	statsTicker := time.NewTicker(1 * time.Second)
	defer statsTicker.Stop()

	var wg sync.WaitGroup
	defer wg.Wait()

	start := time.Now()
	var deadline time.Time
	if lg.cfg.Duration > 0 {
		deadline = start.Add(lg.cfg.Duration)
	}

	var totalSent int64
	prevMetrics := lg.client.Metrics()
	var hr10History []float64

	elapsedSec := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case now := <-statsTicker.C:
			elapsedSec++
			curr := lg.client.Metrics()

			deltaHits := curr.Hits - prevMetrics.Hits
			deltaMisses := curr.Misses - prevMetrics.Misses
			deltaQueries := curr.DBQueries - prevMetrics.DBQueries
			deltaUpdates := curr.DBUpdates - prevMetrics.DBUpdates
			deltaTimeouts := curr.CacheTimeouts - prevMetrics.CacheTimeouts
			deltaErrors := curr.CacheErrors - prevMetrics.CacheErrors
			deltaStaleWindow := curr.StaleReadsInWindow - prevMetrics.StaleReadsInWindow
			deltaStaleAck := curr.StaleReadsAfterAck - prevMetrics.StaleReadsAfterAck

			var hr float64
			totalReads := deltaHits + deltaMisses
			if totalReads > 0 {
				hr = float64(deltaHits) / float64(totalReads)
			}

			hr10History = append(hr10History, hr)
			if len(hr10History) > 10 {
				hr10History = hr10History[len(hr10History)-10:]
			}
			var sumHR float64
			for _, h := range hr10History {
				sumHR += h
			}
			hr10 := sumHR / float64(len(hr10History))

			stat := PerSecondStats{
				Timestamp:          now,
				ElapsedSeconds:     elapsedSec,
				RequestsSent:       deltaHits + deltaMisses + deltaUpdates,
				Hits:               deltaHits,
				Misses:             deltaMisses,
				HitRatio:           hr,
				HR10:               hr10,
				Timeouts:           deltaTimeouts,
				Errors:             deltaErrors,
				PostgresQueries:    deltaQueries,
				PostgresUpdates:    deltaUpdates,
				StaleReadsInWindow: deltaStaleWindow,
				StaleReadsAfterAck: deltaStaleAck,
			}

			prevMetrics = curr
			if onStats != nil {
				onStats(stat)
			}

			if !deadline.IsZero() && now.After(deadline) {
				return nil
			}

		case <-ticker.C:
			totalSent++
			keyIdx := lg.zipf.Next()
			key := FormatKey(keyIdx)
			isWrite := lg.rng.Float64() >= ReadRatio

			wg.Add(1)
			go func(k string, write bool) {
				defer wg.Done()
				opCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()

				if write {
					val := GenerateValue(1)
					_, _ = lg.client.Write(opCtx, k, val)
				} else {
					_ = lg.client.Read(opCtx, k)
				}
			}(key, isWrite)
		}
	}
}
