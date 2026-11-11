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
       |  - Replicated ACCESS counts   |   |   - 4,096-entry tombstone ring|
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
2. **Pluggable Evictions & Replicated Popularity (v2):** Pluggable eviction interface supporting `LRU`, `LFU`, and `W-TinyLFU` (Window LRU + Count-Min Sketch frequency filter + SLRU). Eviction triggers at a 90% soft limit via replicated `EvictBatch` log entries; hard limit (100%) rejections occur strictly on the leader prior to consensus. In **v2** (`-replicate-access=true`), the leader batches cache hits into a 1 s `ACCESS` log entry so all replicas maintain identical W-TinyLFU frequency sketches and SLRU segments across leader promotions (`docs/improvement_doc.md`).
3. **Safe Tombstone Ring Wrap-around:** When the 4,096-slot tombstone ring overwrites `(oldKey, oldVersion)`, the map entry is only deleted if `map[oldKey] == oldVersion`, preventing double-delete bugs from erasing newer tombstones.
4. **Linearizable Reads (`ReadIndex`):** Reads route through the Raft leader via `ReadIndex`, coalescing concurrent requests across heartbeats to avoid stale reads without disk I/O.
5. **Open-Loop Cache-Aside Client:** Issues 50 ms timed reads, handles asynchronous backfills, retries failed invalidations every 200 ms for up to 5 seconds, and tracks stale reads under two categories: *in-window* (during retried invalidation) vs. *after-ack* (after invalidation succeeded).

---

## Benchmark Results (`kind` Cluster)

Both systems ran in a 6-node `kind` cluster at a fixed open-loop rate of 500 req/s (Zipf $s = 0.9$, 95% reads / 5% writes, W-TinyLFU eviction). Full plots and analysis are in [`benchmark_analysis.ipynb`](benchmark_analysis.ipynb) and [`docs/improvement_doc.md`](docs/improvement_doc.md).

Each trial uses a **20-minute (1200 s) warm-up** so W-TinyLFU frequency sketches and working sets stabilize before measurement:
* **`0..1200s` (1200 s):** Warm-up phase.
* **`1200..1320s` (120 s):** Pre-crash baseline measurement ($Q_{base}$ and $HR_{base}$).
* **`t = 0` (`1320s`):** Forced pod kill (`kubectl delete pod <target> --force --grace-period=0`) — active leader for RaftKV, `shardedkv-0` for ShardedKV.
* **`t = 0..300s` (300 s):** Post-crash observation window measuring cumulative extra PostgreSQL queries above baseline ($Q_{extra\_300} = \sum_{t=0}^{300} (Q(t) - Q_{base})$).

### 3-Way Comparison: RaftKV v1 vs. RaftKV v2 vs. ShardedKV

| Metric | RaftKV v1 (policy reset) | RaftKV v2 (replicated counters) | ShardedKV (1 shard killed) |
| :--- | :---: | :---: | :---: |
| **Cluster Topology** | 3 nodes (1 Raft group) | 3 nodes (1 Raft group) | 3 nodes (3 independent shards) |
| **Eviction State on Promotion** | Reset (`ColdStartWrapper`) | Replicated (`ACCESS` 1 s batch) | N/A (restarted shard is empty) |
| **Killed Target** | `raftkv-2` (leader) | `raftkv-1` (leader) | `shardedkv-0` (1/3 keys) |
| **Baseline PostgreSQL QPS ($Q_{base}$)** | 286/s | 282/s | **241/s** |
| **Baseline Hit Ratio ($HR_{base}$)** | 0.328 | 0.353 | 0.318 |
| **Peak PostgreSQL QPS** | 413/s | 454/s | **291/s** |
| **Lowest `HR10` (`10s` moving avg)** | 0.022 | 0.139 | **0.254** |
| **Extra Queries, Election (`t = 0–12s`)** | 784 | 775 | **205** |
| **Mean Excess QPS (`t = 30–300s`)** | +18.7/s | **+10.5/s** (−44%) | **+8.6/s** |
| **$Q_{extra\_300}$ (`t = 0–300s` total)** | 5,973 | **3,514** (−41%) | **2,871** |
| **Stale Reads (`in-window / after-ack`)** | 2 (total) | **0 / 0** | **0 / 0** |

### Findings

1. **In v1, resetting W-TinyLFU cost more than the election itself (87% of $Q_{extra\_300}$):**
   * The leader election (`t = 0–12s`) blocked `ReadIndex` and added **784** extra PostgreSQL queries.
   * After `t = 12s`, the new leader held every cached key in memory, yet PostgreSQL remained **+18.7 QPS** above baseline for the entire 300 s window (**~5,189** extra queries). Because W-TinyLFU counters were node-local in v1, the new leader reset its policy on promotion (`docs/design_doc.md` §7.6) and evicted hot keys while rebuilding its frequency sketch.
   * Note: $t_{recovery}$ (`0.95 × HR_base`) is not a reliable metric at low hit ratios (~0.32) because the threshold sits inside normal $\pm 0.016$ variance; `HR10` crosses it within 20 s while PostgreSQL load remains elevated. $Q_{extra\_300}$ measures true database cost.
2. **Replicating hit counts in v2 cut RaftKV's failover cost by 41%:**
   * Batching leader cache hits into 1 s `ACCESS` Raft log entries (~6% extra log bytes) keeps every follower's frequency sketch and SLRU segments identical to the leader's.
   * On promotion, the new leader keeps its W-TinyLFU state: post-election excess dropped from **+18.7/s to +10.5/s (−44%)**, and $Q_{extra\_300}$ fell from **5,973 to 3,514**.
   * Election cost (`t = 0–12s`) remained unchanged (**775** vs. **784** extra queries), as it is governed by Raft election timers (`5–9s` outage), not eviction policy.
3. **ShardedKV still wins in both steady state and failover:**
   * **Steady state:** ShardedKV sends **~40 fewer queries/s** to PostgreSQL (`241/s` vs. `282–286/s`) because partitioning across 3 nodes gives 3× the unique cache capacity of a 3-replica Raft group.
   * **Failover:** Killing one shard loses only 1/3 of cached keys with zero election stall, peaking at **291/s** (vs. **413–454/s** in RaftKV) and accumulating **2,871** extra queries over 300 s (vs. **3,514** in RaftKV v2).
4. **Stale reads:**
   * v1 recorded 2 stale reads at `t = +11s` (the first second the new leader served reads, while failed `DELETE` invalidations from the election window were still queued for client retry). v2 and ShardedKV recorded `0 / 0`.

---

## Next Steps

### 1. Experimental Controls & GKE Campaign
* **Same-day A/B & No-Failure Controls:** Run `-replicate-access=false` vs. `true` back-to-back on the fixed harness clock, plus no-failure control trials to separate workload drift from post-crash excess (`docs/improvement_doc.md` §7).
* **GKE Production Campaign:** Execute repeated trials ($n = 20$) across dedicated GKE node pools (`pool-cache`: 3 × `e2-standard-4`, `pool-db`: 1 × `e2-standard-4`, `pool-loadgen`: 1 × `e2-standard-8`) per `docs/benchmark_plan.md`.

### 2. Multi-Raft Architecture (Combining Capacity + Warm Failover)
Full-cluster Raft replication trades away 2/3 of cache capacity and stalls 100% of reads during leader election. **Multi-Raft** (sharding the keyspace across multiple per-shard Raft groups) combines both advantages:
* Partition capacity scales with node count, and a node failure triggers an election on only $1/N$ of the keyspace while surviving followers in that shard preserve both cached keys and replicated W-TinyLFU state.

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
├── benchmark_analysis.ipynb# Failover comparison notebook (RaftKV v1, v2, ShardedKV)
├── cmd/
│   ├── loadgen/            # Open-loop Zipfian benchmark load generator
│   ├── raftkv/             # RaftKV cache service daemon
│   └── shardedkv/          # ShardedKV baseline daemon
├── deploy/
│   └── k8s/                # Base manifests and overlays (kind, gke)
├── docs/                   # Benchmark plan, design doc, improvement doc, and one-pager
├── pkg/
│   ├── cache/              # Core store, 648B accounting, circular tombstone ring
│   ├── client/             # Cache-aside client, stale tracker, consistent routing
│   ├── eviction/           # LRU, LFU, W-TinyLFU, and cold-start wrapper
│   ├── hashring/           # 256-vnode consistent hash ring
│   ├── raftnode/           # Raft applier, read-index coalescer, access replicator
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
# Run System A (RaftKV v2, replicated access counts enabled by default)
python3 bench/run_trial.py --system a --policy wtinylfu --rate 500 --warmup-sec 1200 --baseline-sec 120 --observe-sec 300

# Run System B (ShardedKV)
python3 bench/run_trial.py --system b --policy wtinylfu --rate 500 --warmup-sec 1200 --baseline-sec 120 --observe-sec 300
```
Per-second metrics and trial summary JSONs are saved directly to `results/`.
