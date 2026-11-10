# raftkv: Consensus-Backed Cache vs. Sharded Baseline

`raftkv` is a distributed, consensus-backed in-memory key-value cache built in Go on top of [`github.com/jathurchan/raft`](https://github.com/jathurchan/raft).

This project was built to empirically evaluate a classic distributed systems trade-off under strictly equal hardware resources (3 machines vs. 3 machines):

> **Should you spend 3 machines on replication (RaftKV) or on 3× partition capacity (an unreplicated consistent-hash ShardedKV)?**

To answer this, both systems run on Kubernetes in front of a 1,000,000-row PostgreSQL database under identical open-loop Zipfian workloads ($\theta = 0.9$), identical memory limits (64 MiB accounted cache per node), and identical cache-aside clients. We inject abrupt node terminations (`kubectl delete pod --force --grace-period=0`) to quantify the exact database spillover and data consistency trade-offs.

---

## Architecture

```
                 +-----------------------------------------------+
                 |        Open-Loop Load Generator (Job)         |
                 |     Zipf(0.9), 95% Reads / 5% Writes, 50ms    |
                 +-----------------------+-----------------------+
                                         |
                       +-----------------+-----------------+
                       |                                   |
                       v (gRPC :9000)                      v (gRPC :9000)
       +-------------------------------+   +-------------------------------+
       |      System A: raftkv         |   |      System B: shardedkv      |
       |  3 Replicas, 1 Raft Group     |   |   3 Unreplicated Shards       |
       |  - 102k key limit (1x cap)    |   |   - 308k key limit (3x cap)   |
       |  - Linearizable ReadIndex     |   |   - 256-vnode consistent ring |
       |  - Replicated batch eviction  |   |   - Local synchronous evict   |
       |  - 4,096-entry tombstone ring |   |   - 4,096-entry tombstone ring|
       +---------------+---------------+   +---------------+---------------+
                       |                                   |
                       +-----------------+-----------------+
                                         | (Misses / Fills)
                                         v
                         +-------------------------------+
                         |         PostgreSQL 16         |
                         |   1,000,000 rows (64B k, 512B v)|
                         +-------------------------------+
```

### Core Design Principles
1. **Deterministic Memory Accounting:** Exact 648 bytes per entry (`len(k) + len(v) + 72`). 557,056 bytes reserved upfront on each node for a 4,096-slot circular tombstone ring.
2. **Pluggable Evictions:** Pluggable eviction interface supporting `LRU`, `LFU`, and `W-TinyLFU` (Window LRU + Count-Min Sketch frequency filter + SLRU). Eviction triggers at a 90% soft limit via replicated `EvictBatch` log entries; hard limit (100%) rejections occur strictly on the leader prior to consensus to ensure state-machine determinism.
3. **Safe Tombstone Ring Wrap-around:** When the 4,096-slot tombstone ring overwrites `(oldKey, oldVersion)`, the map entry is only deleted if `map[oldKey] == oldVersion`, preventing double-delete bugs from erasing newer tombstones.
4. **Linearizable Reads (`ReadIndex`):** Reads route through the Raft leader via `ReadIndex`, coalescing concurrent requests across heartbeats to avoid stale reads without disk I/O.
5. **Open-Loop Cache-Aside Client:** Issues 50 ms timed reads, handles asynchronous backfills, retries failed invalidations every 200 ms for up to 5 seconds, and tracks stale reads under two categories: *in-window* (during retried invalidation) vs. *after-ack* (after invalidation succeeded).

---

## 15-Minute Benchmark Results (Kind Cluster)

The benchmark was executed in a 6-node Kubernetes cluster provisioned via `kind` on OrbStack. Both systems ran for 15 minutes at a fixed open-loop rate of 500 req/s with W-TinyLFU eviction, following the evaluation protocol defined in `docs/benchmark_plan.md`:
* **00:00 – 08:00 (480s):** Warm-up phase (cache fills, evictions begin, hit ratio stabilizes).
* **08:00 – 10:00 (120s):** Steady-state baseline measurement ($HR_{base}$ and $Q_{base}$).
* **10:00 (600s):** Forced node kill (`kubectl delete pod <node> --force --grace-period=0`). For System A, the active leader is killed; for System B, a shard is killed.
* **10:00 – 15:00 (300s):** Observation phase tracking recovery and computing cumulative extra queries ($Q_{extra\_300}$).

### Side-by-Side Comparison

| Metric | System A (`raftkv`) | System B (`shardedkv`) | Notes |
| :--- | :---: | :---: | :--- |
| **Cluster Hardware** | 3 nodes (1 Raft group) | 3 nodes (3 shards) | Strictly equal resources |
| **Killed Node** | `raftkv-1` (Leader) | `shardedkv-0` (Shard 0) | Leader crash vs. 1/3 shard crash |
| **Baseline Hit Ratio ($HR_{base}$)** | 31.48% | 25.28% | Steady-state prior to kill |
| **Baseline DB Load ($Q_{base}$)** | 299.1 QPS | 256.7 QPS | Sharding holds 3× distinct keys |
| **Peak DB Failover Load** | **444 QPS** | **306 QPS** | **Sharded baseline is ~31% lower** |
| **Hit Ratio Dip ($HR_{base} - \min HR$)** | 31.48% (100% dip) | 11.68% (partial dip) | Raft pauses reads during election |
| **$Q_{extra}$ (outage to recovery)** | 3,484.9 queries | 547.1 queries | Extra queries dumped on PostgreSQL |
| **$Q_{extra\_300}$ (fixed 300s window)** | 3,477.0 queries | -411.0 queries | Sharded continues filling post-kill |
| **Stale Reads** | **0** | **0** | Zero consistency violations |
| **Lost Committed Writes** | **0** | **100% on dead shard** | Raft replicas survive; shard wiped |

### Analysis: What the Trade-off Means

1. **Why Sharding Had a Lower Peak Database Spike (306 vs. 444 QPS):**
   * When a Raft leader dies, the **entire cluster temporarily pauses linearizable reads** while surviving nodes conduct election (10–15 s). During that window, 100% of incoming reads spill over to PostgreSQL, causing a sharp spike to 444 QPS.
   * When a shard dies in ShardedKV, **the other 2 shards (67% of keys) remain healthy and keep serving hits**. Only 33% of requests spill over to PostgreSQL, keeping peak database load ~31% lower.
2. **Why Raft Replication Matters:**
   * In ShardedKV, Shard 0 has zero redundancy. When its container dies, **all dirty/in-flight writes and warm state are lost forever**. When the replacement pod starts, its memory is empty.
   * In RaftKV, all state is replicated with WAL fsync. Surviving nodes already hold identical data, guaranteeing **zero lost writes** and **zero stale reads** through leader election and failover.

---

## Next Steps

### 1. Full Production Campaign on GKE
Development and initial verification were conducted in `kind`. The full 173-run experimental matrix (testing LRU, LFU, W-TinyLFU across varying election timeout ranges and popularity resets per `docs/benchmark_plan.md` §5) will be run on Google Kubernetes Engine (GKE) across dedicated node pools:
* `pool-cache`: 3 × `e2-standard-4` (100 GB `pd-ssd`, topology spread across zones)
* `pool-db`: 1 × `e2-standard-4` (200 GB `pd-ssd`, PostgreSQL 16)
* `pool-loadgen`: 1 × `e2-standard-8` (100 GB `pd-standard`)

### 2. Multi-Raft Architecture (Combining Both Worlds)
The empirical results prove that neither pure 3-replica Raft (high election spike) nor pure unreplicated sharding (zero write durability) is optimal on its own.

The natural evolution is **Multi-Raft** (similar to CockroachDB and TiKV):
* Partition the keyspace into 3 distinct consensus groups across the cluster nodes.
* Each partition runs as its own independent Raft group with its own quorum.
* **The result:** When a node crashes, only the partition group whose leader was on that node undergoes election (a localized 1/3 blast radius), while all committed writes within that partition remain durable and consistent across surviving replicas.

### 3. Upstream Changes Required in `github.com/jathurchan/raft`
To support a high-performance Multi-Raft architecture cleanly without running redundant resources, the following upstream additions are required in the underlying Raft library:
1. **Multi-Group Transport Multiplexing:** Currently, the transport layer binds to a single consensus instance. Multi-Raft requires multiplexing RPCs (`AppendEntries`, `RequestVote`) for multiple Raft groups over a single gRPC connection with a `group_id` header.
2. **Unified WAL and Storage Engine:** Running separate file handles and fsync loops per Raft group on the same physical disk causes I/O thrashing. A shared append-only log engine that groups multiple raft log flushes into a single `fsync` call is needed.
3. **Shared Event Loop / Election Ticker:** Instead of each Raft group running independent background tickers, a shared tick manager is needed to prevent timer and goroutine exhaustion as shard count grows.

---

## Repository Structure

```
.
├── bench/                  # Automated failure benchmark runner (run_trial.py)
├── cmd/
│   ├── loadgen/            # Open-loop Zipfian benchmark load generator
│   ├── raftkv/             # RaftKV cache service daemon
│   └── shardedkv/          # ShardedKV baseline daemon
├── deploy/
│   └── k8s/                # Base manifests and overlays (kind, gke)
├── docs/                   # Benchmark plan, design doc, and one-pager
├── pkg/
│   ├── cache/              # Core store, 648B accounting, circular tombstone ring
│   ├── client/             # Cache-aside client, stale tracker, consistent routing
│   ├── eviction/           # LRU, LFU, W-TinyLFU, and cold-start wrapper
│   ├── hashring/           # 256-vnode consistent hash ring
│   ├── raftnode/           # Raft applier, read-index coalescer, proposal manager
│   ├── server/             # gRPC and HTTP status/management endpoints
│   └── shardedkv/          # Unreplicated shard engine
└── test/                   # Live 3-node cluster failover e2e test
```

---

## Running Locally (Quickstart)

### Prerequisites
* Go 1.24+
* Docker & `kind`
* `kubectl`

### 1. Run Unit & Cluster Integration Tests
```bash
# Run all unit tests
go test -v ./pkg/...

# Run live 3-node Raft failover integration test with race detector
go test -v -race ./test/...
```

### 2. Run Single Benchmark Trial in Kind
```bash
# Test drive System A (RaftKV) with leader failure
python3 bench/run_trial.py --system a --policy wtinylfu --rate 500 --warmup-sec 60 --baseline-sec 30 --observe-sec 60

# Test drive System B (ShardedKV) with shard failure
python3 bench/run_trial.py --system b --policy wtinylfu --rate 500 --warmup-sec 60 --baseline-sec 30 --observe-sec 60
```
Per-second metrics and trial summary JSONs are saved directly to `results/`.
