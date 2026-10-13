#!/usr/bin/env python3
"""End-to-end demonstration against a real local cluster.

Starts a coordinator and 4 storage-node processes, then: uploads a file,
shows real chunk placement and on-disk replicas, crashes a node, proves the
file is still downloadable, waits for re-replication onto the spare node,
restarts the crashed node, verifies safe reconciliation, and deletes the file.
Every step inspects real state; any failed check exits with status 1.

    python scripts/demo.py [--keep] [--base-dir PATH] [--size BYTES]
"""

from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import sys
import tempfile
import time
import traceback
from pathlib import Path

import _common  # noqa: F401 - sets sys.path

from app.process_manager import LocalCluster, wait_for

FAST = {
    "DFS_HEARTBEAT_INTERVAL": "0.5", "DFS_HEALTH_CHECK_INTERVAL": "0.5", "DFS_SUSPECT_TIMEOUT": "1.5",
    "DFS_FAILURE_TIMEOUT": "3.0", "DFS_RECOVERY_INTERVAL": "1.0", "DFS_CHUNK_SIZE": str(256 * 1024),
    "DFS_REPLICATION_FACTOR": "3", "DFS_WRITE_QUORUM": "2", "DFS_LOG_LEVEL": "INFO",
}


class DemoFailure(Exception):
    pass


def check(condition: bool, message: str) -> None:
    if not condition:
        raise DemoFailure(message)
    print(f"    [ok] {message}")


def step(n: int, title: str) -> None:
    print(f"\n[{n:02d}] {title}")


def healthy(chunk: dict) -> list[str]:
    return [loc["node_id"] for loc in chunk["locations"] if loc["status"] == "HEALTHY"]


def run(base_dir: Path, size: int) -> None:
    cluster = LocalCluster(base_dir, num_nodes=4, settings_env=FAST)
    try:
        step(1, "Start coordinator + 4 independent storage-node processes")
        cluster.start_coordinator()
        for nid in cluster.nodes:
            cluster.start_node(nid, wait=False)
        print(f"    coordinator pid={cluster.coordinator.pid} url={cluster.url}")
        for nid, spec in cluster.nodes.items():
            print(f"    {nid} pid={cluster.node_procs[nid].pid} tcp=127.0.0.1:{spec.port} data={spec.data_dir}")

        step(2, "Wait for nodes to register and pass TCP health checks")
        cluster.wait_nodes_healthy(list(cluster.nodes), timeout=30)
        check(all(s["health"] == "HEALTHY" for s in cluster.node_states().values()), "all 4 nodes HEALTHY")

        api = cluster.api()
        step(3, "Cluster status (live from the coordinator)")
        status = api.get("/api/v1/cluster/status").json()
        print(f"    nodes={status['nodes']['by_health']} ring={status['nodes']['ring_members']} "
              f"RF={status['settings']['replication_factor']} chunk_size={status['settings']['chunk_size']}")

        step(4, f"Create a {size}-byte random test file")
        src = base_dir / "demo-input.bin"
        src.write_bytes(os.urandom(size))
        original = src.read_bytes()
        original_sha = hashlib.sha256(original).hexdigest()
        print(f"    {src} sha256={original_sha}")

        step(5, "Upload through POST /api/v1/files/upload")
        t0 = time.perf_counter()
        with src.open("rb") as fh:
            r = api.post("/api/v1/files/upload", files={"file": ("demo-input.bin", fh)})
        check(r.status_code == 201, f"upload returned 201 ({time.perf_counter() - t0:.3f}s)")
        meta = r.json()
        fid = meta["file_id"]

        step(6, "File metadata")
        print(f"    id={fid} size={meta['size']} chunks={meta['chunk_count']} state={meta['state']}")
        check(meta["checksum"] == original_sha, "coordinator checksum matches local SHA-256")
        check(meta["state"] == "COMMITTED", "file is COMMITTED")

        step(7, "Chunk placement chosen by consistent hashing")
        for ch in meta["chunks"]:
            print(f"    chunk {ch['index']:>2} {ch['chunk_id'][:12]}… -> {', '.join(healthy(ch))}")

        step(8, "Verify replica files physically exist on node disks with correct bytes")
        offset = 0
        for ch in meta["chunks"]:
            expected = original[offset:offset + ch["size"]]
            offset += ch["size"]
            for nid in healthy(ch):
                path = cluster.chunk_path(nid, ch["chunk_id"])
                check(path.exists() and path.read_bytes() == expected,
                      f"chunk {ch['index']} bytes on {nid} ({path.relative_to(base_dir)})")

        step(9, "Healthy replica counts")
        counts = [len(healthy(ch)) for ch in meta["chunks"]]
        print(f"    per chunk: {counts}")
        check(all(c == 3 for c in counts), "every chunk has 3 healthy replicas on distinct nodes")

        step(10, "Download through GET /api/v1/files/{id}/download")
        d = api.get(f"/api/v1/files/{fid}/download")
        check(d.status_code == 200, "download returned 200")

        step(11, "Compare downloaded bytes and checksum with the original")
        check(d.content == original, "downloaded bytes are identical")
        check(hashlib.sha256(d.content).hexdigest() == original_sha, "downloaded SHA-256 matches")

        victim = max(cluster.nodes, key=lambda n: len(cluster.chunks_on_disk(n)))
        affected = cluster.chunks_on_disk(victim)
        step(12, f"Crash {victim} with SIGKILL (no drain); it holds {len(affected)} replica(s)")
        cluster.kill_node(victim)
        t_crash = time.perf_counter()
        check(not cluster.node_alive(victim), f"{victim} process is gone")

        step(13, "Wait for failure detection")
        t0 = time.perf_counter()
        cluster.wait_node_health(victim, "DOWN", timeout=20)
        check(True, f"{victim} marked DOWN after {time.perf_counter() - t0:.2f}s")

        step(14, "File remains downloadable from surviving replicas")
        d = api.get(f"/api/v1/files/{fid}/download")
        check(d.status_code == 200 and d.content == original, "download after failure is byte-identical")
        print(f"    served by: {d.headers['x-chunks-served-by']}")
        check(victim not in d.headers["x-chunks-served-by"], f"no chunk served by {victim}")

        step(15, "Wait for the background recovery worker to re-replicate")
        def restored():
            m = api.get(f"/api/v1/files/{fid}").json()
            return m if all(len(healthy(ch)) == 3 for ch in m["chunks"]) else None

        after = wait_for(restored, 60, interval=0.5, description="replication factor restored")
        check(True, f"replication factor 3 restored {time.perf_counter() - t_crash:.2f}s after the crash "
                    f"(without restarting {victim})")

        step(16, "Confirm replacement replica files on healthy nodes")
        offset = 0
        for ch in after["chunks"]:
            expected = original[offset:offset + ch["size"]]
            offset += ch["size"]
            locs = healthy(ch)
            check(victim not in locs, f"chunk {ch['index']} -> {', '.join(locs)}")
            for nid in locs:
                if cluster.chunk_path(nid, ch["chunk_id"]).read_bytes() != expected:
                    raise DemoFailure(f"chunk {ch['index']} on {nid} has wrong bytes")
        rs = api.get("/api/v1/cluster/replication/status").json()
        check(rs["chunk_counts"]["UNDER_REPLICATED"] == 0, f"replication status {rs['chunk_counts']}")

        step(17, f"Restart {victim} (same port and data directory)")
        cluster.start_node(victim)
        cluster.wait_nodes_healthy([victim], timeout=30)
        log_text = (base_dir / "logs" / f"{victim}.log").read_text(errors="replace")
        ready_lines = [ln for ln in log_text.splitlines() if " ready (data_dir=" in ln]
        recovered = int(ready_lines[-1].rsplit("chunks=", 1)[1].rstrip(")")) if ready_lines else -1
        check(recovered == len(affected),
              f"{victim} rebuilt its inventory from disk after the crash ({recovered} chunks)")

        step(18, "Verify the returning node is reconciled safely")
        wait_for(lambda: not cluster.node_states()[victim]["needs_reconcile"], 30, description="reconcile")
        final = api.get(f"/api/v1/files/{fid}").json()
        for ch in final["chunks"]:
            nodes = [loc["node_id"] for loc in ch["locations"]]
            if len(nodes) != len(set(nodes)) or len(healthy(ch)) != 3:
                raise DemoFailure(f"chunk {ch['index']} replicas inconsistent: {ch['locations']}")
        check(True, "each chunk still has exactly 3 healthy, distinct replicas")
        print(f"    stale copies removed from {victim}: {len(affected - cluster.chunks_on_disk(victim))}")
        check(api.get(f"/api/v1/files/{fid}/download").content == original, "file still byte-identical")

        step(19, "Delete the file")
        r = api.delete(f"/api/v1/files/{fid}").json()
        print(f"    state={r['state']} deleted_replicas={r['deleted_replicas']} pending={len(r['pending_cleanup'])}")

        step(20, "Verify metadata and physical chunks are gone (or cleanup is reported)")
        chunk_ids = {ch["chunk_id"] for ch in final["chunks"]}
        if r["state"] == "DELETED":
            check(api.get(f"/api/v1/files/{fid}").status_code == 404, "metadata removed (404)")
            leftovers = {n: chunk_ids & cluster.chunks_on_disk(n) for n in cluster.nodes}
            check(not any(leftovers.values()), "no chunk files remain on any node")
        else:
            check(bool(r["pending_cleanup"]), "outstanding cleanup is explicitly reported")
        api.close()
        print("\nDEMO PASSED")
    finally:
        cluster.shutdown()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base-dir", default=None, help="working dir (default: a new temp dir)")
    parser.add_argument("--size", type=int, default=3 * 1024 * 1024 + 4321)
    parser.add_argument("--keep", action="store_true", help="keep the working directory afterwards")
    args = parser.parse_args()
    base = Path(args.base_dir) if args.base_dir else Path(tempfile.mkdtemp(prefix="dfs-demo-"))
    base.mkdir(parents=True, exist_ok=True)
    try:
        run(base.resolve(), args.size)
        return 0
    except (DemoFailure, TimeoutError) as exc:
        print(f"\nDEMO FAILED: {exc}\nLogs: {base / 'logs'}", file=sys.stderr)
        return 1
    except Exception:  # noqa: BLE001
        traceback.print_exc()
        print(f"\nDEMO FAILED (unexpected error). Logs: {base / 'logs'}", file=sys.stderr)
        return 1
    finally:
        if not args.keep and not args.base_dir:
            shutil.rmtree(base, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
