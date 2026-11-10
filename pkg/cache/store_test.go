package cache

import (
	"fmt"
	"math/rand"
	"testing"

	raftkvv1 "github.com/mathusanm6/raftkv/proto/raftkv/v1"
	"github.com/stretchr/testify/require"
)

// TestStoreDeterminismAcrossReplicas proves that three independent stores applying the same
// command sequence end up in the exact same state (hash parity).
func TestStoreDeterminismAcrossReplicas(t *testing.T) {
	replica1 := NewStore()
	replica2 := NewStore()
	replica3 := NewStore()

	rng := rand.New(rand.NewSource(42))

	const numOps = 5000
	for i := uint64(1); i <= numOps; i++ {
		opType := rng.Intn(4)
		key := fmt.Sprintf("key-%04d", rng.Intn(100))
		val := fmt.Sprintf("val-%04d-%d", rng.Intn(100), i)
		version := int64(rng.Intn(20) + 1)

		switch opType {
		case 0, 1: // SET
			replica1.ApplySet(key, val, version, i)
			replica2.ApplySet(key, val, version, i)
			replica3.ApplySet(key, val, version, i)
		case 2: // DELETE
			replica1.ApplyDelete(key, version, i)
			replica2.ApplyDelete(key, version, i)
			replica3.ApplyDelete(key, version, i)
		case 3: // EVICT batch
			items := []*raftkvv1.EvictItem{
				{Key: key, Version: version},
				{Key: fmt.Sprintf("key-%04d", rng.Intn(100)), Version: int64(rng.Intn(20) + 1)},
			}
			replica1.ApplyEvict(items, i)
			replica2.ApplyEvict(items, i)
			replica3.ApplyEvict(items, i)
		}

		// Replicas must agree at every step
		require.Equal(t, replica1.StateHash(), replica2.StateHash())
		require.Equal(t, replica1.StateHash(), replica3.StateHash())
	}
}

// TestTombstoneDoubleDeleteSafety verifies that when a key is deleted twice with different
// versions, overwriting the older slot in the ring buffer does NOT remove the newer tombstone.
func TestTombstoneDoubleDeleteSafety(t *testing.T) {
	ring := NewTombstoneRing()

	// 1. Delete keyA at version 1
	ring.Record("keyA", 1)
	ver, exists := ring.Lookup("keyA")
	require.True(t, exists)
	require.Equal(t, int64(1), ver)

	// 2. Delete keyA at version 5
	ring.Record("keyA", 5)
	ver, exists = ring.Lookup("keyA")
	require.True(t, exists)
	require.Equal(t, int64(5), ver)

	// 3. Fill the remaining slots so the first slot (keyA, ver 1) is overwritten
	// TombstoneCapacity is 4096. We have 2 records in the ring now.
	for i := 0; i < TombstoneCapacity-2; i++ {
		ring.Record(fmt.Sprintf("filler-%d", i), 10)
	}

	// At this point, the ring has exactly 4096 entries.
	// Slot 0 has keyA:1, slot 1 has keyA:5.
	// Next record overwrites slot 0 (which had keyA:1).
	ring.Record("overflow", 99)

	// The lookup for keyA must STILL succeed and return version 5!
	ver, exists = ring.Lookup("keyA")
	require.True(t, exists, "keyA should still exist because version 5 is in slot 1")
	require.Equal(t, int64(5), ver)
}

// TestEvictVersionRaceCondition verifies that an EVICT batch does not remove a key that
// has been concurrently updated to a newer version.
func TestEvictVersionRaceCondition(t *testing.T) {
	store := NewStore()

	// Initial SET: version 1
	store.ApplySet("k1", "val1", 1, 1)

	// In-flight leader selected ("k1", version 1) for eviction.
	// Meanwhile, client updates "k1" to version 2.
	store.ApplySet("k1", "val2", 2, 2)

	// Now the eviction batch arrives with ("k1", version 1).
	evicted := store.ApplyEvict([]*raftkvv1.EvictItem{
		{Key: "k1", Version: 1},
	}, 3)

	require.Equal(t, 0, evicted, "Eviction should be ignored because version 1 != stored version 2")

	val, ver, found := store.Get("k1")
	require.True(t, found)
	require.Equal(t, "val2", val)
	require.Equal(t, int64(2), ver)

	// Now evicting with matching version 2 must succeed
	evicted = store.ApplyEvict([]*raftkvv1.EvictItem{
		{Key: "k1", Version: 2},
	}, 4)
	require.Equal(t, 1, evicted)

	_, _, found = store.Get("k1")
	require.False(t, found)
}

// TestMemoryAccountingPrecision verifies that adding and deleting entries accurately tracks memory.
func TestMemoryAccountingPrecision(t *testing.T) {
	store := NewStore()
	mem := store.MemoryTracker()

	require.Equal(t, TombstoneReserveBytes, mem.TotalAccountedBytes())

	key := "testkey" // len 7
	val := "testval" // len 7
	expectedEntryBytes := int64(len(key) + len(val) + 72)

	store.ApplySet(key, val, 1, 1)
	require.Equal(t, expectedEntryBytes, mem.EntryBytes())
	require.Equal(t, expectedEntryBytes+TombstoneReserveBytes, mem.TotalAccountedBytes())

	// Update with longer value
	val2 := "testval-longer" // len 14
	expectedEntryBytes2 := int64(len(key) + len(val2) + 72)
	store.ApplySet(key, val2, 2, 2)
	require.Equal(t, expectedEntryBytes2, mem.EntryBytes())

	// Delete
	store.ApplyDelete(key, 2, 3)
	require.Equal(t, int64(0), mem.EntryBytes())
	require.Equal(t, TombstoneReserveBytes, mem.TotalAccountedBytes())
}

// TestSnapshotRestoreResetsAllState verifies full serialization and restoration.
func TestSnapshotRestoreResetsAllState(t *testing.T) {
	store1 := NewStore()
	for i := 1; i <= 50; i++ {
		store1.ApplySet(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i), int64(i), uint64(i))
	}
	// Delete some to populate tombstones
	for i := 1; i <= 10; i++ {
		store1.ApplyDelete(fmt.Sprintf("k%d", i), int64(i), uint64(50+i))
	}

	snap, err := store1.Snapshot()
	require.NoError(t, err)

	store2 := NewStore()
	// Put dirty state in store2
	store2.ApplySet("dirty", "value", 999, 999)

	err = store2.RestoreSnapshot(snap)
	require.NoError(t, err)

	require.Equal(t, store1.StateHash(), store2.StateHash())
	require.Equal(t, store1.LastApplied(), store2.LastApplied())
	require.Equal(t, store1.MemoryTracker().TotalAccountedBytes(), store2.MemoryTracker().TotalAccountedBytes())
}
