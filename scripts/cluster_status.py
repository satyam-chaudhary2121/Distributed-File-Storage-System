#!/usr/bin/env python3
"""Print node health, replication health and storage usage from the coordinator.

    python scripts/cluster_status.py [--url http://127.0.0.1:8000]
"""

from __future__ import annotations

import argparse
import json

import _common  # noqa: F401 - sets sys.path
import httpx


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=None)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    url = args.url
    if url is None:
        state = _common.load_state() if _common.STATE_FILE.exists() else {}
        url = state.get("coordinator", {}).get("url", "http://127.0.0.1:8000")
    with httpx.Client(base_url=url, timeout=10) as api:
        status = api.get("/api/v1/cluster/status").json()
        nodes = api.get("/api/v1/nodes").json()
        storage = api.get("/api/v1/cluster/storage").json()
    if args.json:
        print(json.dumps({"status": status, "nodes": nodes, "storage": storage}, indent=2))
        return 0
    print(f"Coordinator {url}")
    print(f"{'node':<10} {'health':<8} {'admin':<9} {'replicas':>8} {'used bytes':>12} {'last seen':>10}")
    for n in nodes:
        seen = f"{n['seconds_since_seen']:.1f}s" if n["seconds_since_seen"] is not None else "-"
        print(f"{n['node_id']:<10} {n['health']:<8} {n['admin_state']:<9} {n['replica_count']:>8} "
              f"{n['used_bytes']:>12} {seen:>10}")
    r = status["replication"]
    print(f"\nFiles by state: {status['files']}")
    print(f"Chunks: {r['chunk_counts']}  (desired RF {r['desired_replication_factor']}, "
          f"max distinct copies possible now {r['max_distinct_copies_possible']})")
    print(f"Logical bytes: {storage['logical_bytes_committed']}  physical (healthy replicas): "
          f"{storage['physical_bytes_in_healthy_replicas']}")
    print(f"Recovery: {status['recovery']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
