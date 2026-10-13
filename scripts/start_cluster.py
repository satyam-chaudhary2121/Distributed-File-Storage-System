#!/usr/bin/env python3
"""Start a coordinator and N storage nodes as detached background processes.

    python scripts/start_cluster.py --nodes 3
    python scripts/start_cluster.py --nodes 4 --coordinator-port 8000 --node-base-port 9001

State (PIDs, ports, data dirs, secret) is written to run/cluster.json so that
scripts/stop_cluster.py, scripts/node_ctl.py and scripts/cluster_status.py can find it.
Logs go to <base-dir>/logs/. Existing data in <base-dir> is reused, never deleted.
"""

from __future__ import annotations

import argparse
import os
import secrets
import sys

from _common import ROOT, STATE_FILE, save_state

from app.process_manager import LocalCluster, pid_alive


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--nodes", type=int, default=3)
    parser.add_argument("--base-dir", default=str(ROOT / "data"))
    parser.add_argument("--coordinator-port", type=int, default=8000)
    parser.add_argument("--node-base-port", type=int, default=9001)
    args = parser.parse_args()

    if STATE_FILE.exists():
        import json

        old = json.loads(STATE_FILE.read_text())
        pid = old.get("coordinator", {}).get("pid")
        if pid and pid_alive(pid):
            sys.exit(f"A cluster appears to be running already (coordinator pid {pid}). "
                     "Run scripts/stop_cluster.py first.")

    secret = os.environ.get("DFS_CLUSTER_SECRET")
    if not secret:
        secret = secrets.token_hex(32)
        print("DFS_CLUSTER_SECRET not set: generated a random secret (stored in run/cluster.json).")
    cluster = LocalCluster(
        args.base_dir, num_nodes=args.nodes, settings_env={"DFS_CLUSTER_SECRET": secret},
        coordinator_port=args.coordinator_port,
        node_ports=[args.node_base_port + i for i in range(args.nodes)], detach=True,
    )
    try:
        cluster.start(wait_healthy=True, timeout=60)
    except Exception as exc:  # noqa: BLE001
        print(f"Cluster failed to start: {exc}\nLogs: {cluster.base_dir / 'logs'}", file=sys.stderr)
        cluster.shutdown()
        return 1
    state = cluster.state()
    state["secret"] = secret
    save_state(state)
    print(f"Coordinator: {cluster.url}  (Swagger UI: {cluster.url}/docs)")
    for nid, spec in cluster.nodes.items():
        print(f"  {nid}: tcp://127.0.0.1:{spec.port}  data={spec.data_dir}  pid={cluster.node_procs[nid].pid}")
    print(f"Logs: {cluster.base_dir / 'logs'}\nStop with: python scripts/stop_cluster.py")
    # Detach: leave processes running; release our handles without terminating them.
    for proc in [*cluster.node_procs.values(), cluster.coordinator]:
        if proc is not None:
            proc._close_log()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
