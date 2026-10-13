#!/usr/bin/env python3
"""Measure real performance of a local cluster. Nothing here is estimated or hard-coded.

Measures: upload/download latency and throughput per file size, raw TCP chunk
store/retrieve time, replication overhead (RF=1 vs RF=3 upload time),
recovery duration after a node crash, and concurrent upload/download behaviour.

    python scripts/benchmark.py                      # default sizes
    python scripts/benchmark.py --sizes 1M,16M --repeat 5 --concurrency 8

Results are printed and written to bench-results/benchmark-<timestamp>.json.
Numbers depend entirely on the machine (disk, CPU, OS); all processes run locally.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import _common  # noqa: F401 - sets sys.path

from app.process_manager import LocalCluster, wait_for
from app.storage.checksum import sha256_bytes
from app.storage.chunking import make_chunk_id
from app.storage.socket_client import NodeClient

ENV = {"DFS_HEARTBEAT_INTERVAL": "0.5", "DFS_HEALTH_CHECK_INTERVAL": "0.5", "DFS_SUSPECT_TIMEOUT": "1.5",
       "DFS_FAILURE_TIMEOUT": "3.0", "DFS_RECOVERY_INTERVAL": "3600", "DFS_LOG_LEVEL": "WARNING",
       "DFS_MAX_UPLOAD_SIZE": str(2 * 1024**3)}


def parse_size(text: str) -> int:
    text = text.strip().upper()
    mult = {"K": 1024, "M": 1024**2, "G": 1024**3}.get(text[-1], 1)
    return int(float(text[:-1] if text[-1] in "KMG" else text) * mult)


def human(n: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB"):
        if n < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}TiB"


def summarize(samples: list[float]) -> dict:
    return {"n": len(samples), "mean_s": statistics.mean(samples), "median_s": statistics.median(samples),
            "min_s": min(samples), "max_s": max(samples)}


def bench_file_ops(cluster: LocalCluster, sizes: list[int], repeat: int) -> list[dict]:
    rows = []
    with cluster.api(timeout=600) as api:
        for size in sizes:
            data = os.urandom(size)
            up, down = [], []
            for _ in range(repeat):
                t0 = time.perf_counter()
                r = api.post("/api/v1/files/upload", files={"file": ("bench.bin", data)})
                up.append(time.perf_counter() - t0)
                r.raise_for_status()
                fid = r.json()["file_id"]
                t0 = time.perf_counter()
                d = api.get(f"/api/v1/files/{fid}/download")
                down.append(time.perf_counter() - t0)
                if d.content != data:
                    raise RuntimeError("downloaded bytes differ")
                api.delete(f"/api/v1/files/{fid}")
            rows.append({"size": size, "upload": summarize(up), "download": summarize(down),
                         "upload_MiBps": size / statistics.median(up) / 1024**2,
                         "download_MiBps": size / statistics.median(down) / 1024**2})
    return rows


def bench_raw_tcp(cluster: LocalCluster, chunk_size: int, count: int) -> dict:
    spec = cluster.nodes["node-1"]
    client = NodeClient("127.0.0.1", spec.port, cluster.secret, node_id="node-1")
    data = os.urandom(chunk_size)
    checksum = sha256_bytes(data)
    store, get = [], []
    for i in range(count):
        cid = make_chunk_id("bench" + "0" * 27, i)
        t0 = time.perf_counter()
        client.store_chunk(cid, data, checksum)
        store.append(time.perf_counter() - t0)
        t0 = time.perf_counter()
        got, _ = client.get_chunk(cid)
        get.append(time.perf_counter() - t0)
        assert got == data
        client.delete_chunk(cid)
    return {"chunk_size": chunk_size, "store": summarize(store), "retrieve": summarize(get)}


def bench_concurrency(cluster: LocalCluster, size: int, workers: int, per_worker: int) -> dict:
    data = os.urandom(size)

    def worker(_: int) -> list[float]:
        lat = []
        with cluster.api(timeout=600) as api:
            for _ in range(per_worker):
                t0 = time.perf_counter()
                r = api.post("/api/v1/files/upload", files={"file": ("c.bin", data)})
                r.raise_for_status()
                d = api.get(f"/api/v1/files/{r.json()['file_id']}/download")
                if d.content != data:
                    raise RuntimeError("concurrent download mismatch")
                lat.append(time.perf_counter() - t0)
        return lat

    t0 = time.perf_counter()
    with ThreadPoolExecutor(workers) as pool:
        lat = [x for xs in pool.map(worker, range(workers)) for x in xs]
    wall = time.perf_counter() - t0
    return {"workers": workers, "ops": len(lat), "file_size": size, "wall_s": wall,
            "round_trip": summarize(lat), "ops_per_s": len(lat) / wall,
            "aggregate_MiBps": 2 * size * len(lat) / wall / 1024**2}


def bench_recovery(base: Path, size: int, chunk_size: int) -> dict:
    env = {**ENV, "DFS_CHUNK_SIZE": str(chunk_size)}
    with LocalCluster(base / "recovery", num_nodes=4, settings_env=env) as cluster, cluster.api(timeout=600) as api:
        r = api.post("/api/v1/files/upload", files={"file": ("r.bin", os.urandom(size))})
        r.raise_for_status()
        victim = max(cluster.nodes, key=lambda n: len(cluster.chunks_on_disk(n)))
        replicas_lost = len(cluster.chunks_on_disk(victim))
        cluster.kill_node(victim)
        t_kill = time.perf_counter()
        cluster.wait_node_health(victim, "DOWN", timeout=30)
        detect = time.perf_counter() - t_kill
        # The DOWN transition wakes the recovery worker; time until every chunk is back at RF=3.
        t0 = time.perf_counter()
        wait_for(lambda: api.get("/api/v1/cluster/replication/status").json()["chunk_counts"]["UNDER_REPLICATED"] == 0,
                 120, interval=0.05, description="recovery")
        repair = time.perf_counter() - t0
        meta = api.get(f"/api/v1/files/{r.json()['file_id']}").json()
        if not all(sum(1 for loc in ch["locations"] if loc["status"] == "HEALTHY" and loc["node_id"] != victim) == 3
                   for ch in meta["chunks"]):
            raise RuntimeError("recovery did not restore 3 healthy replicas on surviving nodes")
        return {"file_size": size, "replicas_recreated": replicas_lost, "detection_s": detect,
                "repair_after_detection_s": repair, "crash_to_full_rf_s": detect + repair,
                "repair_MiBps": replicas_lost * chunk_size / repair / 1024**2 if repair else None}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sizes", default="64K,1M,8M,32M")
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--chunk-size", default="1M")
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--concurrent-size", default="1M")
    parser.add_argument("--recovery-size", default="32M")
    parser.add_argument("--out", default=str(_common.ROOT / "bench-results"))
    args = parser.parse_args()
    sizes = [parse_size(s) for s in args.sizes.split(",")]
    chunk = parse_size(args.chunk_size)
    base = Path(tempfile.mkdtemp(prefix="dfs-bench-"))
    results: dict = {"timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"), "machine": {
        "platform": platform.platform(), "python": sys.version.split()[0], "cpus": os.cpu_count()},
        "chunk_size": chunk, "note": "all processes on one machine over loopback TCP"}

    print(f"Benchmark working dir: {base}")
    for rf in (3, 1):
        env = {**ENV, "DFS_CHUNK_SIZE": str(chunk), "DFS_REPLICATION_FACTOR": str(rf),
               "DFS_WRITE_QUORUM": str(min(2, rf))}
        with LocalCluster(base / f"rf{rf}", num_nodes=3, settings_env=env) as cluster:
            print(f"\n== file operations, RF={rf} ==")
            rows = bench_file_ops(cluster, sizes, args.repeat)
            results[f"file_ops_rf{rf}"] = rows
            for row in rows:
                print(f"  {human(row['size']):>9}: upload {row['upload']['median_s'] * 1000:8.1f} ms "
                      f"({row['upload_MiBps']:7.1f} MiB/s)   download {row['download']['median_s'] * 1000:8.1f} ms "
                      f"({row['download_MiBps']:7.1f} MiB/s)")
            if rf == 3:
                raw = bench_raw_tcp(cluster, chunk, 20)
                results["raw_tcp_chunk"] = raw
                print(f"\n== raw TCP chunk ops ({human(chunk)}) ==\n  store median "
                      f"{raw['store']['median_s'] * 1000:.2f} ms, retrieve median {raw['retrieve']['median_s'] * 1000:.2f} ms")
                conc = bench_concurrency(cluster, parse_size(args.concurrent_size), args.concurrency, 3)
                results["concurrency"] = conc
                print(f"\n== concurrency ({conc['workers']} clients x 3 upload+download of "
                      f"{human(conc['file_size'])}) ==\n  {conc['ops_per_s']:.2f} round-trips/s, median "
                      f"{conc['round_trip']['median_s'] * 1000:.1f} ms, aggregate {conc['aggregate_MiBps']:.1f} MiB/s")

    overhead = []
    for r3, r1 in zip(results["file_ops_rf3"], results["file_ops_rf1"]):
        overhead.append({"size": r3["size"], "rf3_over_rf1_upload_time":
                         r3["upload"]["median_s"] / r1["upload"]["median_s"]})
    results["replication_overhead"] = overhead
    print("\n== replication overhead (median upload time RF=3 / RF=1) ==")
    for o in overhead:
        print(f"  {human(o['size']):>9}: x{o['rf3_over_rf1_upload_time']:.2f}")

    rec = bench_recovery(base, parse_size(args.recovery_size), chunk)
    results["recovery"] = rec
    print(f"\n== recovery after SIGKILL of one of 4 nodes ({human(rec['file_size'])} file) ==\n"
          f"  detection {rec['detection_s']:.2f} s, re-replication {rec['repair_after_detection_s']:.2f} s "
          f"({rec['replicas_recreated']} replicas, {rec['repair_MiBps']:.1f} MiB/s), "
          f"crash -> full RF {rec['crash_to_full_rf_s']:.2f} s")

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"benchmark-{time.strftime('%Y%m%d-%H%M%S')}.json"
    path.write_text(json.dumps(results, indent=2))
    print(f"\nResults written to {path}")
    import shutil

    shutil.rmtree(base, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
