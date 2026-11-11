# [Benchmark Plan] raftkv vs. shardedkv Under Node Failure

## 1. Objective

Measure how much extra load PostgreSQL receives when a cache node fails, for a
Raft-replicated cache (System A) and an unreplicated sharded cache (System B),
along with what replication costs when nothing fails. All results are reported,
including those that favour System B.

---

## 2. Setup

### 2.1 Clusters

Final numbers come from GKE only, with cache nodes, PostgreSQL, and the load
generator on separate node pools. `kind` is for development.

| **Pool**       | **Nodes** | **Machine**     | **Disk**             | **Workload**               |
| -------------- | --------- | --------------- | -------------------- | -------------------------- |
| `pool-cache`   | 3         | `e2-standard-4` | 100 GB `pd-ssd` each | Cache pods, one per node   |
| `pool-db`      | 1         | `e2-standard-4` | 200 GB `pd-ssd`      | PostgreSQL 16              |
| `pool-loadgen` | 1         | `e2-standard-8` | 100 GB `pd-standard` | Load generator and harness |

Fsync stays on for the Raft log (`SyncOnAppend: true`) and PostgreSQL.

Final runs use one exact Raft library commit, pinned for the whole campaign. It
must contain every fix in design doc §3. Until those fixes reach `main`, pin it
from the library's `dev` branch.

### 2.2 PostgreSQL

- 1,000,000 rows. Each key is exactly 64 bytes, each value exactly 512 bytes,
  and `version` starts at 1.
- Each run restores the database from a template copy of the seeded data.

```other
max_connections = 300
shared_buffers = 4GB
work_mem = 16MB
checkpoint_completion_target = 0.9
fsync = on
synchronous_commit = on
```

### 2.3 Memory sizing

Each cache node, in both systems:

| **Setting**        | **Value** |
| ------------------ | --------- |
| `max_memory_bytes` | 64 MiB    |
| `GOMEMLIMIT`       | 410 MiB   |
| Pod memory limit   | 512 MiB   |

Each entry is accounted at 648 bytes, and 557,056 bytes per node are reserved
for tombstones.

|                        | **System A**                      | **System B**             |
| ---------------------- | --------------------------------- | ------------------------ |
| Distinct keys at limit | about 102,700                     | about 308,100 (3 shards) |
| Eviction starts at     | 90% soft limit, about 92,300 keys | 100%, about 308,100 keys |
| Share of the 1M keys   | about 9%                          | about 31%                |

Neither system can hold the full key set, so both evict. **System A stores about
one-third of the distinct data of System B and is expected to have a lower
steady-state hit ratio.**

**Model estimate, not a result:** if a cache held exactly the most popular keys
under Zipf 0.9, the hit ratio ceilings would be about 72% for A and 85% for B.
At 4,750 reads/s, that is about 630 more PostgreSQL queries per second for A,
which is why §4 reports full-window load as well as extra requests per failure.

---

## 3. Workload

### 3.1 Load generator

- Open-loop: fixed request rate, independent of response time.
- Implemented as a Go program deployed as a Kubernetes Job.
- Mix: 95% reads and 5% writes, following the client logic in design doc §5.
- Keys: Zipfian, exponent 0.9, over all 1M keys.
- Seeds: `seed_i = 133742 + i` for iteration `i`. Both systems and all policies
  use the same seed for the same iteration, so comparisons are paired while runs
  differ from each other.
- Records per second at the client: hits, misses, errors, timeouts, PostgreSQL
  requests and latency, cache latency, and stale reads.

### 3.2 Calibration (once, before the campaign)

1. Ramp each system in 5-minute steps until more than 1% of requests fail or
   time out. This gives `T_max(A)` and `T_max(B)`.
2. Measure `T_db`, the highest request rate PostgreSQL holds below all
   saturation limits (§5.3), with every read going directly to it.
3. Set the rate used for every run:

`T_target = min(5000, 0.60 × min(T_max(A), T_max(B)), T_db)`

The third term is needed because an election can send nearly all reads to
PostgreSQL. Hold each `T_db` step for longer than the expected election, using
the 3-node model in design doc §7.1 (about 17.8 s at p95 at default timing). The
5.2 s to 10.2 s measured by the library's end-to-end suite came from 5 nodes,
and understates this.

1. Record each shard's share of keys and requests for System B.
2. At the pinned commit, run the library tests and `./e2e/run.sh`, and confirm
   all 25 scenarios pass.
3. Before using GKE, run the leader-kill test on 3 `raftkv` replicas in `kind`
   at benchmark settings. The end-to-end suite used 5 nodes without `raftkv`, so
   it does not cover this setup.

---

## 4. Metrics

Cache behaviour is measured at the client.

| **Metric**                 | **Definition**                                                                                                                                                                                                        |
| -------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Hit ratio, `HR(t)`         | hits / (hits + misses) per second. Every error, timeout, and dead-shard bypass counts as a miss                                                                                                                       |
| `HR10(t)`                  | 10-second trailing moving average of `HR(t)`                                                                                                                                                                          |
| Warm-up end                | Evictions have started, and `HR10` changed by less than 1 point over the last 60 seconds                                                                                                                              |
| Baseline                   | The 120 seconds after warm-up. Gives `HR_base` (mean `HR10`) and `Q_base` (mean PostgreSQL requests per second)                                                                                                       |
| `t_kill`                   | When the harness issues the delete command                                                                                                                                                                            |
| `t_first_error`            | First client cache error or timeout after `t_kill`                                                                                                                                                                    |
| `t_recovery`               | First time after `t_kill` that `HR10 ≥ 0.95 × HR_base` and stays there for 30 seconds                                                                                                                                 |
| Recovery time              | `t_recovery − t_kill`                                                                                                                                                                                                 |
| **Primary: `Q_extra`**     | Sum over each second from `t_kill` to `t_recovery` of `Q(t) − Q_base`, not clamped at zero                                                                                                                            |
| `Q_extra_300`              | The same sum over the fixed 300 seconds after `t_kill`, so slow refill past `t_recovery` is included                                                                                                                  |
| Hit-ratio dip              | `HR_base − min(HR10)` between kill and recovery                                                                                                                                                                       |
| Raw dip                    | `HR_base − min(HR)` over 1-second bins, since `HR10` flattens short outages                                                                                                                                           |
| Peak PostgreSQL QPS        | Highest per-second PostgreSQL requests after `t_kill`                                                                                                                                                                 |
| Peak PostgreSQL p99        | Highest per-second p99 of PostgreSQL latency, measured at the client                                                                                                                                                  |
| Client errors and timeouts | Counts between kill and recovery                                                                                                                                                                                      |
| Write unavailability       | Time from first failed `DELETE` to the last `DELETE` failure before one succeeds                                                                                                                                      |
| Abandoned invalidations    | Deletes that ran out of retries                                                                                                                                                                                       |
| Stale reads                | Counted in the two groups in design doc §5.3                                                                                                                                                                          |
| Pod restart time           | From `t_kill` until the replacement pod is Ready                                                                                                                                                                      |
| Leader election time       | From `t_kill` until `/status`, polled every 50 ms, shows a new leader in a higher term                                                                                                                                |
| Read resume time           | From `t_kill` until the first cache hit after the outage                                                                                                                                                              |
| Steady-state hit ratio     | `HR_base`                                                                                                                                                                                                             |
| Popularity reset effect    | `HR_base − mean HR10` over the 120 seconds after a policy reset (§5.5)                                                                                                                                                |
| Eviction log share         | Eviction bytes / total proposed bytes                                                                                                                                                                                 |
| Noise floor                | `Q_extra_300` computed at the same time offset in a no-failure run                                                                                                                                                    |
| Break-even failure rate    | `3600 × (Q_base(A) − Q_base(B)) / (median Q_extra_300(B) − median Q_extra_300(A))`, in failures per hour. Above this rate, A sends fewer total queries. If the denominator is zero or negative, A does not break even |

---

## 5. Procedure

### 5.1 Test matrix

| **Set**                       | **Calculation**             | **Runs** |
| ----------------------------- | --------------------------- | -------- |
| Failure runs                  | 2 systems × 3 policies × 20 | 120      |
| No-failure controls           | 2 × 3 × 3                   | 18       |
| Election timeout sweep (§5.4) | 4 × 5                       | 20       |
| Popularity reset (§5.5)       | 3 × 5                       | 15       |
| **Total**                     |                             | **173**  |

Run order is randomized to spread time-of-day effects evenly across systems.

### 5.2 Per-run steps

Each step's duration varies by system. Fifteen minutes is the budgeting average.

1. **Reset.** Restore PostgreSQL. Delete cache pods and, for System A, their
   PVCs, then recreate them. Wait until all pods are Ready.
2. **Warm up.** Start load at `T_target` and wait for warm-up to end. If it has
   not ended by 20 minutes, mark the run `INVALID_WARMUP`.
3. **Baseline.** Record 120 seconds.
4. **Kill.** Using `kubectl delete pod <pod> --force --grace-period=0`:
    - System A: find the leader using `/status`, then delete it.
    - System B: delete shard `i mod 3`, so 20 kills split 7/7/6.
5. **Observe.** Kubernetes restarts the pod on its own. Keep running for at
   least 300 seconds and until recovery. If there is no recovery within 15
   minutes, report recovery as censored at that limit.
6. **Save.** Stop load and store per-second data, pod events, snapshot times,
   and a JSON summary.

Control runs skip step 4, and record the same timeline.

### 5.3 PostgreSQL saturation

Sampled every 500 ms. A run is `INVALID_SATURATED` if:

- PostgreSQL host CPU is above 80% for more than 3 seconds, or
- active connections reach 90% of `max_connections`, or
- a `SELECT 1` canary takes more than 100 ms.

Saturated runs are retained and counted per system in Table 8, since saturation
may itself be a result. They are not used in medians. If more than 10% of the
runs in any cell are invalid, lower `T_target` and rerun the **whole** campaign
at the new rate. The rate never changes for individual runs.

### 5.4 Election timeout sweep

System A with W-TinyLFU, 5 runs per value, with a randomization factor of 2.0
throughout.

| **Base ticks** | **Nominal base** | **Code-defined timeout range** |
| -------------- | ---------------- | ------------------------------ |
| 24             | 2.4 s            | 2.6–13.2 s                     |
| 35             | 3.5 s            | 3.7–18.7 s                     |
| 50 (default)   | 5.0 s            | 5.2–26.2 s                     |
| 80             | 8.0 s            | 8.2–41.2 s                     |

Report any elections that occurred without a kill.

### 5.5 Popularity reset runs

System A with no failure, 5 runs per policy. After baseline, call
`/debug/reset-policy` on the leader. This measures the effect of empty
popularity data alone, without an election.

### 5.6 Reporting

- Report the median and worst case for each combination, along with a bootstrap
  95% confidence interval on the median.
- Report System B per shard as well as overall.
- Report the noise floor next to `Q_extra_300`.
- Publish all valid and invalid run counts, and the settings used.
- Publish the Raft library commit used.

---

## 6. Hypotheses

None of these has been tested.

1. **Steady-state hit ratio favours System B**, because it stores about 3 times
   the distinct data.
2. **Baseline PostgreSQL load favours System B**, for the same reason, and this
   cost is continuous rather than only during failures.
3. **Peak PostgreSQL load favours System B.** During an election, nearly all
   System A reads go to PostgreSQL, while System B loses about one shard's
   share.
4. **Stale reads favour System B.** A failed delete on System A leaves the old
   value on surviving replicas until a retry lands.
5. **Write cost favours System B**, which has no fsync or quorum.
6. **Extra PostgreSQL requests per failure probably favour System A**, because
   replicas stay warm. This is uncertain, as election timers are long at default
   settings and the outage covers all keys.
7. **Recovery time probably favours System A**, provided reads resume soon after
   election.
8. **W-TinyLFU gives the highest hit ratio in both systems**, but a new leader
   gives it a temporary drop of [placeholder] points.
9. **Lower election timeouts reduce System A's `Q_extra`** but may cause
   elections without a kill.

---

## 7. Result Templates

All values are placeholders until the runs are complete.

### Table 1: Failure summary (20 kills per row)

| **System** | **Policy** | **`HR_base`** | **Median `Q_extra`** | **Worst `Q_extra`** | **Median `Q_extra_300`** | **Median recovery** | **Worst recovery** | **Median dip** | **Median raw dip** |
| ---------- | ---------- | ------------- | -------------------- | ------------------- | ------------------------ | ------------------- | ------------------ | -------------- | ------------------ |
| A          | LRU        | [placeholder] | [placeholder]        | [placeholder]       | [placeholder]            | [placeholder] s     | [placeholder] s    | [placeholder]  | [placeholder]      |
| A          | LFU        | [placeholder] | [placeholder]        | [placeholder]       | [placeholder]            | [placeholder] s     | [placeholder] s    | [placeholder]  | [placeholder]      |
| A          | W-TinyLFU  | [placeholder] | [placeholder]        | [placeholder]       | [placeholder]            | [placeholder] s     | [placeholder] s    | [placeholder]  | [placeholder]      |
| B          | LRU        | [placeholder] | [placeholder]        | [placeholder]       | [placeholder]            | [placeholder] s     | [placeholder] s    | [placeholder]  | [placeholder]      |
| B          | LFU        | [placeholder] | [placeholder]        | [placeholder]       | [placeholder]            | [placeholder] s     | [placeholder] s    | [placeholder]  | [placeholder]      |
| B          | W-TinyLFU  | [placeholder] | [placeholder]        | [placeholder]       | [placeholder]            | [placeholder] s     | [placeholder] s    | [placeholder]  | [placeholder]      |

### Table 2: Database and client impact (medians)

| **System** | **Policy** | **`Q_base`**  | **Peak QPS**  | **Peak p99**     | **Errors + timeouts** | **Write unavailability** | **Abandoned invalidations** | **Stale in window** | **Stale after ack** |
| ---------- | ---------- | ------------- | ------------- | ---------------- | --------------------- | ------------------------ | --------------------------- | ------------------- | ------------------- |
| A          | LRU        | [placeholder] | [placeholder] | [placeholder] ms | [placeholder]         | [placeholder] s          | [placeholder]               | [placeholder]       | [placeholder]       |
| A          | LFU        | [placeholder] | [placeholder] | [placeholder] ms | [placeholder]         | [placeholder] s          | [placeholder]               | [placeholder]       | [placeholder]       |
| A          | W-TinyLFU  | [placeholder] | [placeholder] | [placeholder] ms | [placeholder]         | [placeholder] s          | [placeholder]               | [placeholder]       | [placeholder]       |
| B          | LRU        | [placeholder] | [placeholder] | [placeholder] ms | [placeholder]         | [placeholder] s          | [placeholder]               | [placeholder]       | [placeholder]       |
| B          | LFU        | [placeholder] | [placeholder] | [placeholder] ms | [placeholder]         | [placeholder] s          | [placeholder]               | [placeholder]       | [placeholder]       |
| B          | W-TinyLFU  | [placeholder] | [placeholder] | [placeholder] ms | [placeholder]         | [placeholder] s          | [placeholder]               | [placeholder]       | [placeholder]       |

### Table 3: Failure timing (median / worst)

| **System** | **Target** | **Pod restart** | **Leader election** | **Read resume** |
| ---------- | ---------- | --------------- | ------------------- | --------------- |
| A          | Leader     | [placeholder] s | [placeholder] s     | [placeholder] s |
| B          | Shard 0    | [placeholder] s | N/A                 | [placeholder] s |
| B          | Shard 1    | [placeholder] s | N/A                 | [placeholder] s |
| B          | Shard 2    | [placeholder] s | N/A                 | [placeholder] s |

### Table 4: Election timeout sweep (System A, W-TinyLFU)

| **Base ticks** | **Election time p50 / p95**       | **Median `Q_extra`** | **Peak QPS**  | **Elections without a kill** |
| -------------- | --------------------------------- | -------------------- | ------------- | ---------------------------- |
| 24             | [placeholder] s / [placeholder] s | [placeholder]        | [placeholder] | [placeholder]                |
| 35             | [placeholder] s / [placeholder] s | [placeholder]        | [placeholder] | [placeholder]                |
| 50             | [placeholder] s / [placeholder] s | [placeholder]        | [placeholder] | [placeholder]                |
| 80             | [placeholder] s / [placeholder] s | [placeholder]        | [placeholder] | [placeholder]                |

### Table 5: No-failure controls

| **System** | **Policy** | **Mean hit ratio** | **Mean PostgreSQL QPS** | **Cache p50 / p99**                 | **Noise floor** |
| ---------- | ---------- | ------------------ | ----------------------- | ----------------------------------- | --------------- |
| A          | LRU        | [placeholder]      | [placeholder]           | [placeholder] ms / [placeholder] ms | [placeholder]   |
| A          | LFU        | [placeholder]      | [placeholder]           | [placeholder] ms / [placeholder] ms | [placeholder]   |
| A          | W-TinyLFU  | [placeholder]      | [placeholder]           | [placeholder] ms / [placeholder] ms | [placeholder]   |
| B          | LRU        | [placeholder]      | [placeholder]           | [placeholder] ms / [placeholder] ms | [placeholder]   |
| B          | LFU        | [placeholder]      | [placeholder]           | [placeholder] ms / [placeholder] ms | [placeholder]   |
| B          | W-TinyLFU  | [placeholder]      | [placeholder]           | [placeholder] ms / [placeholder] ms | [placeholder]   |

### Table 6: Popularity reset and eviction overhead (System A)

| **Policy** | **Popularity reset effect** | **Eviction log share** |
| ---------- | --------------------------- | ---------------------- |
| LRU        | [placeholder] points        | [placeholder]%         |
| LFU        | [placeholder] points        | [placeholder]%         |
| W-TinyLFU  | [placeholder] points        | [placeholder]%         |

### Table 7: Break-even failure rate

| **Policy** | **`Q_base(A) − Q_base(B)`** | **Median `Q_extra_300(B) − Q_extra_300(A)`** | **Break-even failures per hour** |
| ---------- | --------------------------- | -------------------------------------------- | -------------------------------- |
| LRU        | [placeholder]               | [placeholder]                                | [placeholder]                    |
| LFU        | [placeholder]               | [placeholder]                                | [placeholder]                    |
| W-TinyLFU  | [placeholder]               | [placeholder]                                | [placeholder]                    |

### Table 8: Run validity

| **System** | **Policy** | **Valid**     | **`INVALID_SATURATED`** | **`INVALID_WARMUP`** | **Censored recoveries** |
| ---------- | ---------- | ------------- | ----------------------- | -------------------- | ----------------------- |
| A          | LRU        | [placeholder] | [placeholder]           | [placeholder]        | [placeholder]           |
| A          | LFU        | [placeholder] | [placeholder]           | [placeholder]        | [placeholder]           |
| A          | W-TinyLFU  | [placeholder] | [placeholder]           | [placeholder]        | [placeholder]           |
| B          | LRU        | [placeholder] | [placeholder]           | [placeholder]        | [placeholder]           |
| B          | LFU        | [placeholder] | [placeholder]           | [placeholder]        | [placeholder]           |
| B          | W-TinyLFU  | [placeholder] | [placeholder]           | [placeholder]        | [placeholder]           |
