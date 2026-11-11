# [Improvement Doc] raftkv: Replicated Popularity Data Across Leader Changes

| **Item**        | **Value**                                                                 |
| --------------- | ------------------------------------------------------------------------- |
| Status          | Implemented and measured once on `kind` (n = 1)                           |
| Date            | 2026-11-11                                                                |
| Baseline design | `docs/design_doc.md`, kept as the initial version (v1)                    |
| Changes         | §7.6 popularity data, plus stale-read reporting and harness timing fixes  |
| Analysis        | `benchmark_analysis.ipynb`, section "Improvement v2"                      |

This document records what was changed after the first failover benchmark, why,
and what the change achieved. The design doc is not edited. Each section below
names the design doc section it supersedes.

---

## 1. Summary

| **Metric** (300 s after the crash)     | **A, v1** | **A, v2** | **B (same day as v2)** |
| -------------------------------------- | --------- | --------- | ---------------------- |
| `Q_extra_300` (extra PostgreSQL reads) | 5,973     | **3,514** | 2,871                  |
| Mean excess QPS, `t = 30–300 s`        | +18.7     | **+10.5** | +8.6                   |
| Extra queries, `t = 0–12 s` (election) | 784       | 775       | 205                    |

- Replicating hit counts through the Raft log cut System A's failover cost by
  **41%**, and its post-election excess by **44%**.
- The election cost is unchanged, as expected: it depends on Raft timers, not on
  the eviction policy.
- System A still costs more than System B per failure (3,514 vs 2,871), and
  about 40 more PostgreSQL queries per second in steady state.

---

## 2. Problem (measured with v1)

### 2.1 Run

Each system was run once on `kind` with W-TinyLFU, 500 req/s, Zipf 0.9 over 1M
keys, and 95% reads: a 1,200 s warm-up, a 120 s baseline, a crash at `t = 0`,
then 300 s of observation. System A's leader (`raftkv-2`) and System B's
`shardedkv-0` were force-deleted. Raw data:
`results/trial-{a,b}-wtinylfu-20261110-*`.

| **Metric**                     | **System A** | **System B** |
| ------------------------------ | ------------ | ------------ |
| Baseline PostgreSQL QPS        | 286          | 243          |
| Peak PostgreSQL QPS            | 413          | 277          |
| Lowest HR10                    | 0.022        | 0.248        |
| Extra queries, `t = 0–10 s`    | 698          | 179          |
| Mean excess QPS, `t = 30–300s` | +18.7        | +7.4         |
| `Q_extra_300`                  | 5,973        | 2,500        |
| Stale reads                    | 2            | 0            |

### 2.2 Finding: the policy reset costs more than the election

System A's extra load came in two parts:

1. **Election (`t = 0–10 s`):** no leader means no reads, so nearly all traffic
   reaches PostgreSQL. This cost 698 extra queries, about 12% of the total.
2. **After the election (`t = 10–300 s`):** HR10 returns to its threshold within
   20 s, yet PostgreSQL stays about `+18.7/s` above baseline for the rest of the
   window. That adds about 5,275 queries, about 88% of the total.

The second part follows from design doc §7.6. The new leader has every key, but
it resets W-TinyLFU and evicts oldest-first by apply index for up to 30 s. Then
it must rebuild a frequency sketch that took the old leader about 20 minutes to
learn. In the meantime, it evicts keys the old leader knew were hot.

The recovery-time metric hides this. Because `HR_base` is low (about 0.32), the
`0.95 × HR_base` threshold falls within normal noise (±0.016). HR10 can pass it
while PostgreSQL load is still raised. `Q_extra_300` is the reliable measure.

---

## 3. Change: replicated access counts (v2)

Supersedes design doc §7.6. Also changes §2.1 (new log command), §5 step 4 (read
path), §6.2 step 4 (promotion), §7.5 (where policy metadata lives), and §7.7
(log traffic).

### 3.1 Options considered

| **Option**                                      | **Cost**                                          | **Outcome**                                       |
| ----------------------------------------------- | ------------------------------------------------- | ------------------------------------------------- |
| Keep v1 (rebuild on promotion)                  | None                                              | Rejected: about 88% of failover cost (§2.2)       |
| Propose every hit through Raft                  | One log entry and fsync per read                  | Rejected: multiplies log traffic                  |
| Periodically replicate the whole sketch         | About 512 KiB per copy; segments still not shared | Rejected: costly, and the SLRU state would differ |
| **Batch hit counts into `ACCESS` entries, 1 s** | About 6% of log bytes, 1 entry/s (§3.4)           | **Chosen:** all policy state stays identical      |

### 3.2 Design

Reads happen only on the leader, so only the leader sees popularity. In v2 it
shares what it sees through the log:

1. On each cache hit, the leader adds the key to an in-memory access buffer
   (key → hit count) instead of updating its own policy.
2. Every 1 second, or once the buffer holds 1,024 distinct keys, the leader
   proposes one `ACCESS` command carrying the buffered `(key, count)` pairs, and
   clears the buffer. Counts are capped at 15, the sketch's counter maximum.
3. Every replica, including the leader, applies `ACCESS` by calling
   `RecordAccess` on its policy, `count` times per key. This updates both the
   frequency sketch and the window and SLRU positions.
4. On taking leadership, the new leader keeps its policy as it is. There is no
   `Reset` and no oldest-first warm-up.

So the window, the main segments, and the frequency sketch now exist on every
replica, and change only in `Apply` (through `SET`, `DELETE`, `EVICT`, and
`ACCESS`). Only the leader calls `SelectVictims`.

### 3.3 Properties

- **Deterministic:** policy state changes only in `Apply`, in log order, so all
  replicas that applied the same prefix hold the same policy. That includes the
  sketch's periodic halving, which is driven by the number of additions.
- **Bounded loss:** a leader crash loses at most one unflushed buffer, about
  1 second of hits. The sketch samples about 1 million additions between
  halvings, so this loss is negligible.
- **No double counting:** the leader records hits only through `ACCESS`, never
  directly. Its own policy lags its reads by at most one flush interval.
- **Best effort:** an `ACCESS` proposal that fails, for example because
  leadership was lost, is dropped and not retried.
- **Restarted replicas:** a node that restores a snapshot rebuilds its policy
  from keys in apply-index order, then warms up from later `ACCESS` entries.
  Snapshots do not include the sketch yet. That would matter only if the
  restarted node became leader again before it had replayed enough accesses.
- **Idempotent replay:** `ACCESS` advances the store's applied index, so a
  replayed entry is skipped like any other.

The v1 behaviour stays available behind `-replicate-access=false` for
comparison runs.

### 3.4 Extra log traffic

At the benchmark load (500 req/s, 95% reads, hit ratio about 0.32), the leader
sees about 150 hits per second. One `ACCESS` entry per second holds at most 150
pairs of about 70 bytes each, so about 10 KB/s. Misses cause about 300 `SET`
commands per second, at about 590 bytes each, or about 175 KB/s. `ACCESS`
therefore adds about 6% to log bytes and one log entry per second. Not yet
measured.

### 3.5 Implementation

| **File**                                   | **Change**                                                        |
| ------------------------------------------ | ----------------------------------------------------------------- |
| `proto/raftkv/v1/raftkv.proto`             | `AccessItem`, `AccessBatchCommand`, `RaftCommand.access = 5`      |
| `pkg/raftnode/access_replicator.go`        | Buffers hits, flushes `ACCESS` every 1 s or at 1,024 keys         |
| `pkg/raftnode/applier.go`                  | Applies `ACCESS` to the policy on every replica                   |
| `pkg/raftnode/read_index.go`               | Hits go to an `AccessRecorder` (the replicator in v2)             |
| `pkg/cache/store.go`                       | `AdvanceApplied` for commands that change no cache data           |
| `cmd/raftkv/main.go`                       | `-replicate-access` flag (default `true`); no reset on promotion  |
| `deploy/k8s/base/raftkv/statefulset.yaml`  | Passes `-replicate-access=true` explicitly                        |
| `pkg/raftnode/access_replicator_test.go`   | Replicas agree on victims; hot keys survive; replay is idempotent |

---

## 4. Evaluation of v2

### 4.1 Run

Same settings as §2.1 (1,200 s warm-up, 120 s baseline, 300 s observation, seed
133742), System B first, then System A, with `-replicate-access=true`. System A's
leader `raftkv-1` and System B's `shardedkv-0` were force-deleted. Raw data:
`results/trial-b-wtinylfu-20261111-092927.*` and
`results/trial-a-wtinylfu-20261111-095638.*`.

### 4.2 Harness timing defect, and correction

The harness started its kill clock when the load generator's pod became Ready.
In the v2 System A run, the load generator began counting about 10 s later, so
the kill landed at load-generator second ~1310 instead of 1320:

- The outage (0 hits) appears at seconds 1311–1316. In the other three runs, the
  failure starts at 1321–1322.
- The run has 1,615 rows instead of 1,620, because the harness collected the log
  10 s before the load generator finished.

As a result, the summary printed by the harness for that run is invalid
(`Q_extra_300 = 1,059`, recovery 6 s). Its baseline window included the
election outage. The JSON file is kept unchanged as raw output. All v2 numbers
in this document re-align that run at `T_KILL = 1310`, the second before its
outage, matching how the other runs relate to their kill (`T_KILL = 1320`).

**Fix:** `bench/run_trial.py` now starts its clock when the load generator
prints its first stats row, not at pod readiness.

### 4.3 Results

| **Metric**                     | **A, v1** | **A, v2** | **B, 2026-11-10** | **B, 2026-11-11** |
| ------------------------------ | --------- | --------- | ----------------- | ----------------- |
| Baseline PostgreSQL QPS        | 286       | 282       | 243               | 241               |
| Baseline HR10                  | 0.328     | 0.353     | 0.319             | 0.318             |
| Election outage (≈0 hits)      | 9 s       | 5 s       | —                 | —                 |
| Peak PostgreSQL QPS            | 413       | 454       | 277               | 291               |
| Lowest HR10                    | 0.022     | 0.139     | 0.248             | 0.254             |
| Extra queries, `t = 0–12 s`    | 784       | 775       | 229               | 205               |
| Mean excess QPS, `t = 30–300s` | +18.7     | **+10.5** | +7.4              | +8.6              |
| `Q_extra_300`                  | 5,973     | **3,514** | 2,500             | 2,871             |
| Stale reads (window / ack)     | 2 (n/a)   | 0 / 0     | 0                 | 0 / 0             |

### 4.4 Analysis

- **The targeted cost fell.** Post-election excess dropped from `+18.7/s` to
  `+10.5/s`, and `Q_extra_300` from 5,973 to 3,514. The new leader kept the old
  leader's popularity data instead of rebuilding it.
- **The election cost did not change.** About 780 extra queries in both runs.
  The v2 outage was shorter (5 s vs 9 s) but its peak higher (454/s). The
  outage length is random, set by the election timer (design doc §6.1).
- **System A still loses to System B,** both per failure (3,514 vs 2,871 on the
  same day) and in steady state (about +40/s), because all replicas store the
  same keys.
- **Part of the remaining excess may not be caused by the failure.** System B's
  hit ratio after the crash is also about 0.02 below its baseline. Without a
  no-failure control run, drift and failure cost cannot be separated.

---

## 5. Stale reads

### 5.1 v1: most likely in-window

Both of System A's v1 stale reads occurred at `t = +11 s`, the first second the
new leader served reads. System B had none. The code points to the in-window case
(design doc §4.3), which cache-aside allows:

1. During the election, writes (5% of requests, about 25/s) update PostgreSQL,
   but their cache `DELETE` fails, so the client queues it for retry every 200
   ms (design doc §4.2).
2. System A's replicas still hold the older versions, unlike a restarted System
   B shard, which comes back empty.
3. Once the new leader serves reads, a hot key read before its queued `DELETE`
   commits returns the older version. The delete has not been acknowledged yet,
   so the read is in-window.

The after-acknowledgement case would need a tombstone lost from the 4,096-record
table (about 160 s of writes at this rate) or a linearizability defect. The read
path rules out the latter: it confirms leadership by ReadIndex, waits for the
election no-op, then waits for `LastApplied`.

**Reporting fix:** the v1 harness logged only the sum of both categories, so the
category could not be proven. The load generator now prints `StaleWin` and
`StaleAck` columns, and `bench/run_trial.py` records `stale_window` and
`stale_after_ack` in the CSV and `stale_reads_in_window` and
`stale_reads_after_ack` in the JSON.

### 5.2 v2: none observed

Both v2 runs had 0 stale reads in either category, so the v1 category is still
not confirmed. Stale reads in this window depend on which hot keys were written
during the outage, and the v2 outage was shorter.

**Further reduction (not done):** draining the retry queue before reads resume
would close this window. One way is for a new leader to stay `UNAVAILABLE` for
one retry interval after its no-op commits. That trades a little hit ratio for
fewer stale reads.

---

## 6. Hypotheses tracking

Design doc §9 hypotheses, unchanged there, with what the `kind` runs observed.

| **Dimension**                         | **Expected to favour** | **Observed, v1**                               | **Observed, v2**                            |
| ------------------------------------- | ---------------------- | ---------------------------------------------- | ------------------------------------------- |
| Distinct data stored                  | B                      | Not measured directly                          | Not measured directly                       |
| Steady-state hit ratio                | B                      | Not clearly: 0.328 (A) vs 0.319 (B)            | Not clearly: 0.353 (A) vs 0.318 (B)         |
| Baseline PostgreSQL load              | B                      | Confirmed: 286/s (A) vs 243/s (B)              | Confirmed: 282/s (A) vs 241/s (B)           |
| Write cost                            | B                      | Not measured                                   | Not measured                                |
| Peak PostgreSQL load                  | B                      | Confirmed: 413/s (A) vs 277/s (B)              | Confirmed: 454/s (A) vs 291/s (B)           |
| Stale reads                           | B                      | Consistent: 2 (A) vs 0 (B)                     | None on either system                       |
| Extra PostgreSQL requests per failure | Probably A, uncertain  | Refuted: 5,973 (A) vs 2,500 (B)                | Still refuted, gap smaller: 3,514 vs 2,871  |
| Recovery time                         | Probably A             | HR10 recovers in 20 s (A) vs 283 s (B), see §2 | Not reliable at this hit ratio (§2.2)       |

---

## 7. Next steps

1. **Same-day A/B of the flag:** rerun System A with `-replicate-access=false`
   on the fixed harness, to separate the flag's effect from day-to-day noise.
2. **No-failure control** for each system, to measure baseline drift (§4.4).
3. **More trials:** n = 1 per configuration so far; the plan calls for 20 on
   GKE (design doc §8.4).
4. **Measure `ACCESS` log overhead** against the 6% estimate (§3.4).
5. **Include the sketch in snapshots**, so a restarted replica that becomes
   leader is not cold (§3.3).
6. **Optionally close the stale-read window** after an election (§5.2).
7. **Multi-Raft (future, not planned now):** sharding with per-shard
   replication would scale capacity with node count and limit an election to
   1/N of the keyspace.

---

## 8. Reproducing

```bash
# Build linux/arm64 binaries and images
for b in raftkv loadgen shardedkv; do
  CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o "bin/linux/${b}" "./cmd/${b}"
  docker build --target "${b}" -t "${b}:latest" .
done
kind load docker-image raftkv:latest loadgen:latest shardedkv:latest --name raftkv-dev

# Check that every node runs the new binary before trusting a run
docker exec raftkv-dev-worker ctr -n k8s.io run --rm docker.io/library/raftkv:latest chk \
  md5sum /usr/local/bin/raftkv
md5 -q bin/linux/raftkv

kubectl apply -k deploy/k8s/overlays/kind
kubectl delete pod -l app=raftkv --now && kubectl delete pod -l app=shardedkv --now

python3 bench/run_trial.py --system b --policy wtinylfu --rate 500 \
  --warmup-sec 1200 --baseline-sec 120 --observe-sec 300
python3 bench/run_trial.py --system a --policy wtinylfu --rate 500 \
  --warmup-sec 1200 --baseline-sec 120 --observe-sec 300
```

In zsh, write `"${b}:latest"`, not `$b:latest`: zsh reads `:l` as a lowercase
modifier and tags the image `raftkvatest`, so the cluster silently keeps the
old image.
