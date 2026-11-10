#!/usr/bin/env python3
"""
Benchmark Trial Runner for raftkv vs shardedkv under node failure.
Implements the protocol defined in docs/benchmark_plan.md §4 and §5:
  1. Reset: Reset cache cluster and state
  2. Warm-up & Baseline: Stream open-loop load until steady state
  3. Kill: Terminate active leader (System A) or target shard (System B)
  4. Observe: Monitor recovery for at least 300s
  5. Compute: Calculate Q_extra, Q_extra_300, recovery time, dips, stale reads
"""

import argparse
import csv
import json
import os
import subprocess
import sys
import time
import urllib.request

def run_cmd(cmd, check=True):
    res = subprocess.run(cmd, shell=True, text=True, capture_output=True)
    if check and res.returncode != 0:
        raise RuntimeError(f"Command failed ({cmd}): {res.stderr.strip()}")
    return res.stdout.strip()

def get_raft_leader(http_addrs):
    for addr in http_addrs:
        url = f"http://{addr}/status"
        try:
            with urllib.request.urlopen(url, timeout=1.0) as resp:
                if resp.status == 200:
                    data = json.loads(resp.read().decode())
                    if data.get("role") == "leader" and data.get("id"):
                        return data.get("id")
        except Exception:
            pass
    return None

def reset_cluster(system, namespace="default"):
    print(f"[*] Resetting {system} cluster...")
    if system == "a":
        # Delete pods and recreate to clear in-memory state and ensure fresh term
        run_cmd(f"kubectl delete pod -l app=raftkv -n {namespace} --now", check=False)
        time.sleep(2)
        run_cmd(f"kubectl wait --for=condition=ready pod -l app=raftkv -n {namespace} --timeout=60s")
    else:
        run_cmd(f"kubectl delete pod -l app=shardedkv -n {namespace} --now", check=False)
        time.sleep(2)
        run_cmd(f"kubectl wait --for=condition=ready pod -l app=shardedkv -n {namespace} --timeout=60s")
    print(f"[✓] {system} cluster reset complete and ready.")

def execute_trial(args):
    system = args.system.lower()
    policy = args.policy.lower()
    rate = args.rate
    warmup_sec = args.warmup_sec
    baseline_sec = args.baseline_sec
    observe_sec = args.observe_sec
    total_sec = warmup_sec + baseline_sec + observe_sec
    t_kill_sec = warmup_sec + baseline_sec
    namespace = args.namespace

    output_dir = os.path.abspath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    trial_name = f"trial-{system}-{policy}-{timestamp}"
    csv_file = os.path.join(output_dir, f"{trial_name}.csv")
    json_file = os.path.join(output_dir, f"{trial_name}.json")

    print("=" * 80)
    print(f"STARTING BENCHMARK TRIAL: System={system.upper()} Policy={policy.upper()}")
    print(f"Rate: {rate} req/s | Warmup: {warmup_sec}s | Baseline: {baseline_sec}s | Observe: {observe_sec}s")
    print(f"Kill scheduled at t={t_kill_sec}s")
    print("=" * 80)

    # 1. Reset
    if not args.skip_reset:
        reset_cluster(system, namespace=namespace)

    # 2. Configure endpoints
    if system == "a":
        grpc_addrs = ",".join([
            f"raftkv-{i}.raftkv-headless.{namespace}.svc.cluster.local:9000" for i in range(3)
        ])
        http_addrs = ",".join([
            f"raftkv-{i}.raftkv-headless.{namespace}.svc.cluster.local:8080" for i in range(3)
        ])
    else:
        grpc_addrs = ",".join([
            f"shardedkv-{i}.shardedkv-headless.{namespace}.svc.cluster.local:9000" for i in range(3)
        ])
        http_addrs = ",".join([
            f"shardedkv-{i}.shardedkv-headless.{namespace}.svc.cluster.local:8080" for i in range(3)
        ])

    pg_url = f"postgres://postgres:password@postgres.{namespace}.svc.cluster.local:5432/raftkv?sslmode=disable"

    # Clean previous runner pod
    run_cmd(f"kubectl delete pod {trial_name} -n {namespace} --now", check=False)

    # 3. Launch Loadgen Job/Pod
    print(f"[*] Launching benchmark pod {trial_name} in Kubernetes...")
    cmd = (
        f"kubectl run {trial_name} -n {namespace} "
        f"--image={args.image} --image-pull-policy={args.image_pull_policy} --restart=Never -- "
        f"-system={system} "
        f"-grpc-addrs={grpc_addrs} "
        f"-http-addrs={http_addrs} "
        f"-postgres-url=\"{pg_url}\" "
        f"-rate={rate} "
        f"-duration={total_sec}s "
        f"-output-csv=/tmp/results.csv"
    )
    run_cmd(cmd)

    # Wait for pod running
    run_cmd(f"kubectl wait --for=condition=Ready pod/{trial_name} -n {namespace} --timeout=30s")
    print(f"[✓] Benchmark runner {trial_name} is RUNNING.")

    # 4. Monitor until kill time
    start_time = time.time()
    killed = False
    killed_target = ""

    while True:
        elapsed = time.time() - start_time
        if elapsed >= total_sec + 5:
            break

        # Check if it's kill time
        if not killed and elapsed >= t_kill_sec:
            print("\n" + "!" * 80)
            print(f"[*] T_KILL REACHED ({elapsed:.1f}s >= {t_kill_sec}s). Triggering node failure...")

            if system == "a":
                # Find current leader
                leader_pod = ""
                for attempt in range(5):
                    for i in range(3):
                        out = run_cmd(
                            f"kubectl exec raftkv-{i} -n {namespace} -- wget -qO- http://localhost:8080/status",
                            check=False
                        )
                        if out:
                            try:
                                st = json.loads(out)
                                if st.get("role") == "leader":
                                    leader_pod = st.get("id", f"raftkv-{i}")
                                    break
                            except Exception:
                                pass
                    if leader_pod:
                        break
                    time.sleep(0.5)

                if not leader_pod:
                    leader_pod = "raftkv-0"

                killed_target = leader_pod
                print(f"[*] Killing active leader pod: {killed_target}")
                run_cmd(f"kubectl delete pod {killed_target} -n {namespace} --force --grace-period=0")
            else:
                # System B: kill shard i mod 3
                shard_idx = args.iteration % 3
                killed_target = f"shardedkv-{shard_idx}"
                print(f"[*] Killing shard pod: {killed_target}")
                run_cmd(f"kubectl delete pod {killed_target} -n {namespace} --force --grace-period=0")

            killed = True
            print("!" * 80 + "\n")

        time.sleep(1.0)

    # 5. Extract logs and output CSV
    print(f"[*] Fetching benchmark logs from {trial_name}...")
    logs = run_cmd(f"kubectl logs {trial_name} -n {namespace}")
    with open(csv_file.replace(".csv", ".log"), "w") as f:
        f.write(logs)

    # Parse stdout table into structured records
    records = []
    for line in logs.splitlines():
        line = line.strip()
        if not line or line.startswith("Sec") or line.startswith("-") or line.startswith("202"):
            continue
        parts = [p.strip() for p in line.split("|")]
        if len(parts) >= 8:
            try:
                sec = int(parts[0])
                sent = int(parts[1])
                hits = int(parts[2])
                misses = int(parts[3])
                hrt = float(parts[4])
                hr10 = float(parts[5])
                pg_qs = int(parts[6])
                stale = int(parts[7])
                records.append({
                    "sec": sec, "sent": sent, "hits": hits, "misses": misses,
                    "hrt": hrt, "hr10": hr10, "pg_qs": pg_qs, "stale": stale
                })
            except ValueError:
                continue

    # Cleanup pod
    run_cmd(f"kubectl delete pod {trial_name} -n {namespace} --now", check=False)

    if not records:
        print("[!] No records extracted from benchmark run.")
        return None

    # Write records to CSV
    with open(csv_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["sec", "sent", "hits", "misses", "hrt", "hr10", "pg_qs", "stale"])
        writer.writeheader()
        writer.writerows(records)
    print(f"[✓] Saved per-second metrics to {csv_file}")

    # 6. Metric Calculations as per §4 & §5.2
    # Baseline: 120s prior to kill
    baseline_start = max(1, t_kill_sec - baseline_sec)
    baseline_records = [r for r in records if baseline_start <= r["sec"] < t_kill_sec]
    if baseline_records:
        hr_base = sum(r["hr10"] for r in baseline_records) / len(baseline_records)
        q_base = sum(r["pg_qs"] for r in baseline_records) / len(baseline_records)
    else:
        hr_base = 0.0
        q_base = 0.0

    post_kill_records = [r for r in records if r["sec"] >= t_kill_sec]

    # Q_extra_300: 300 seconds after kill
    q_extra_300_records = [r for r in records if t_kill_sec <= r["sec"] < t_kill_sec + 300]
    q_extra_300 = sum(r["pg_qs"] - q_base for r in q_extra_300_records)

    # Recovery: first time after t_kill where hr10 >= 0.95 * hr_base and stays for 30s
    threshold = 0.95 * hr_base
    recovery_sec = None
    for i, r in enumerate(post_kill_records):
        if r["hr10"] >= threshold:
            # Check if next 30s stay above
            window = post_kill_records[i : i + 30]
            if len(window) >= 15 and all(w["hr10"] >= threshold for w in window):
                recovery_sec = r["sec"]
                break

    recovery_time = (recovery_sec - t_kill_sec) if recovery_sec else observe_sec
    q_extra_records = [r for r in records if t_kill_sec <= r["sec"] <= (recovery_sec or (t_kill_sec + observe_sec))]
    q_extra = sum(r["pg_qs"] - q_base for r in q_extra_records)

    min_hr10_post = min(r["hr10"] for r in post_kill_records) if post_kill_records else 0.0
    min_hrt_post = min(r["hrt"] for r in post_kill_records) if post_kill_records else 0.0
    dip_hr10 = max(0.0, hr_base - min_hr10_post)
    dip_raw = max(0.0, hr_base - min_hrt_post)
    peak_pg_qps = max(r["pg_qs"] for r in post_kill_records) if post_kill_records else 0
    total_stale = sum(r["stale"] for r in records)

    metrics = {
        "system": system,
        "policy": policy,
        "rate": rate,
        "killed_target": killed_target,
        "hr_base": round(hr_base, 4),
        "q_base": round(q_base, 2),
        "q_extra": round(q_extra, 2),
        "q_extra_300": round(q_extra_300, 2),
        "recovery_time_sec": recovery_time,
        "dip_hr10": round(dip_hr10, 4),
        "dip_raw": round(dip_raw, 4),
        "peak_pg_qps": peak_pg_qps,
        "total_stale_reads": total_stale,
    }

    with open(json_file, "w") as f:
        json.dump(metrics, f, indent=2)

    print("\n" + "=" * 80)
    print("TRIAL RESULTS SUMMARY (Table 1 Metrics):")
    print(f"  System:                {system.upper()} ({policy})")
    print(f"  Killed Node:           {killed_target}")
    print(f"  Baseline Hit Ratio:    {metrics['hr_base'] * 100:.2f}%")
    print(f"  Baseline Postgres QPS: {metrics['q_base']:.1f} qps")
    print(f"  Q_extra:               {metrics['q_extra']:.1f} queries")
    print(f"  Q_extra_300:           {metrics['q_extra_300']:.1f} queries")
    print(f"  Recovery Time:         {metrics['recovery_time_sec']}s")
    print(f"  Hit Ratio Dip (HR10):  {metrics['dip_hr10'] * 100:.2f}%")
    print(f"  Raw Dip (HR(t)):       {metrics['dip_raw'] * 100:.2f}%")
    print(f"  Peak Postgres QPS:     {metrics['peak_pg_qps']} qps")
    print(f"  Stale Reads:           {metrics['total_stale_reads']}")
    print("=" * 80 + "\n")

    return metrics

def main():
    parser = argparse.ArgumentParser(description="Run failure benchmark trial for raftkv / shardedkv")
    parser.add_argument("--system", choices=["a", "b"], default="a", help="Target system (a=raftkv, b=shardedkv)")
    parser.add_argument("--policy", choices=["lru", "lfu", "wtinylfu"], default="wtinylfu", help="Eviction policy")
    parser.add_argument("--rate", type=int, default=500, help="Target open-loop request rate")
    parser.add_argument("--warmup-sec", type=int, default=15, help="Warm-up phase duration in seconds")
    parser.add_argument("--baseline-sec", type=int, default=15, help="Baseline phase duration in seconds")
    parser.add_argument("--observe-sec", type=int, default=30, help="Observation phase duration after kill")
    parser.add_argument("--iteration", type=int, default=0, help="Trial iteration index")
    parser.add_argument("--namespace", default="default", help="Kubernetes namespace")
    parser.add_argument("--image", default="loadgen:latest", help="Loadgen image")
    parser.add_argument("--image-pull-policy", default="IfNotPresent", help="Loadgen image pull policy")
    parser.add_argument("--skip-reset", action="store_true", help="Skip cluster reset step")
    parser.add_argument("--output-dir", default="results", help="Directory for trial CSV and JSON results")

    args = parser.parse_args()
    execute_trial(args)

if __name__ == "__main__":
    main()
