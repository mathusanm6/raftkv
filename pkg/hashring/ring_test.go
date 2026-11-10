package hashring

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConsistentHashRingDistribution(t *testing.T) {
	shards := []string{"shard-0", "shard-1", "shard-2"}
	ring := NewRing(shards, DefaultVirtualNodes)

	require.Equal(t, shards, ring.Members())

	counts := make(map[string]int)
	const numKeys = 30000

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%08d", i)
		target := ring.GetShard(key)
		require.Contains(t, shards, target)
		counts[target]++
	}

	// Each shard should receive a fair share of keys (approx 33% +/- 10%)
	for _, shard := range shards {
		count := counts[shard]
		ratio := float64(count) / float64(numKeys)
		require.Greater(t, ratio, 0.23, fmt.Sprintf("%s received too few keys: %f", shard, ratio))
		require.Less(t, ratio, 0.43, fmt.Sprintf("%s received too many keys: %f", shard, ratio))
	}
}

func TestConsistentHashRingDeterminism(t *testing.T) {
	shards := []string{"shard-0", "shard-1", "shard-2"}
	ring1 := NewRing(shards, DefaultVirtualNodes)
	ring2 := NewRing(shards, DefaultVirtualNodes)

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("random-key-%d", i)
		require.Equal(t, ring1.GetShard(key), ring2.GetShard(key), "Rings must produce identical mapping")
	}
}
