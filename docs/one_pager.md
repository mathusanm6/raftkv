# [One Pager] Does a Raft Cache Protect PostgreSQL Better During Node Failure?

**Problem.** An unreplicated sharded cache loses a shard's contents when a node dies, sending that traffic to the database until the cache refills. A Raft-replicated cache keeps warm copies, but it stores about one-third of the distinct data for the same memory, and all cache reads can miss during a leader
election. Teams rarely have side-by-side data on which costs the database more.

**Why it matters.** A cache failure can overload the database behind it. The choice trades steady-state capacity against behaviour during failure.

**Proposal.** Build `raftkv`, a 3-replica in-memory cache in Go, and `shardedkv`, a 3-shard baseline with no replication. Run both on Kubernetes in front of PostgreSQL (1M keys) with identical cache-aside clients and memory, then kill nodes and measure the database impact.

*Attribution:* the Raft core comes from the upstream library (`github.com/jathurchan/raft`). This project's work is `raftkv`, the eviction layer, the baseline, the benchmark harness, and fixes contributed upstream (in review, along with a 25-scenario `kind` failure suite that passes).

**Goals**

- Measure database load from node failure, and the steady-state cost of replication.
- Offer LRU, LFU, and W-TinyLFU by configuration through one policy interface, with Raft replicas evicting identical keys.
- Automate every experiment on Kubernetes.
- Publish results even where the baseline wins.
- Fix library problems upstream, so the whole Raft library improves.

**Non-goals.** Multi-Raft, live membership changes, TLS, and durable storage semantics.

**Primary metric.** Total extra PostgreSQL requests from node kill to recovery: the per-second sum of actual requests minus the baseline rate. Recovery is when the 10-second hit ratio returns within 5% of baseline and holds for 30 seconds.

**Top risks**

| **Risk**                                                           | **Mitigation**                                                     |
| ------------------------------------------------------------------ | ------------------------------------------------------------------ |
| PostgreSQL saturates during an election, hiding the real load      | Calibrate for full read fall-through; mark and report invalid runs |
| Failed invalidations leave stale values on Raft replicas           | Versioned, retried deletes; report stale reads                     |
| Eviction floods the Raft log                                       | Batch evictions; report their share of log bytes                   |
| `raftkv` breaks at 3 replicas, where the library was tested with 5 | Run the kill tests on 3 `raftkv` replicas in `kind` before GKE     |
| The library fixes change during review                             | Pin one commit; rerun the end-to-end suite if it changes           |

The end-to-end suite settled whether the Raft library stays correct under failure: 25 of 25 scenarios passed with no lost writes or stale reads. It did not test `raftkv`, PostgreSQL, or eviction, so the risks above remain.

**Decision needed.** Should PreVote stay off for the benchmark? It prevents unnecessary elections when a node rejoins, but adds a round trip to each election. *Recommendation:* keep it off for the main runs and test it in one extra sweep. Library fixes go upstream, because they improve Raft for every user and keep the benchmark measuring Raft rather than a workaround.

**XYZ resume templates** (use only what the results support)

- Quantified the failover trade-off between Raft-replicated and sharded caching, as measured by [X] vs. [Y] median extra PostgreSQL requests per node kill across [N] Kubernetes failure runs, by building `raftkv`, a sharded baseline, and an open-loop benchmark harness on an existing Raft library.
- [Reduced/Increased] extra PostgreSQL requests during node failure by [X]%, as measured against an unreplicated baseline over [N] runs, by replicating cached state across 3 nodes.
- Measured the steady-state cost of replication at [X] hit-ratio points and [Y] added PostgreSQL queries per second, as measured in [N] no-failure runs, by testing both systems under equal memory and identical Zipfian traffic.
- Limited replicated eviction to [X]% of Raft log bytes, as measured across LRU, LFU, and W-TinyLFU runs, by batching leader-selected evictions through the log.

