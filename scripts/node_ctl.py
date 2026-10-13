#!/usr/bin/env python3
"""Kill, stop, start or add individual storage nodes of a running cluster (failure simulation).

    python scripts/node_ctl.py kill node-2      # abrupt crash (SIGKILL / TerminateProcess)
    python scripts/node_ctl.py stop node-2      # graceful stop
    python scripts/node_ctl.py start node-2     # restart with the same port and data dir
    python scripts/node_ctl.py add node-4 --port 9004
"""

from __future__ import annotations

import argparse
import os
import signal
import sys
from pathlib import Path

from _common import load_state, save_state

from app.process_manager import IS_WINDOWS, LocalCluster, NodeSpec, pid_alive, terminate_pid


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("action", choices=["kill", "stop", "start", "add"])
    parser.add_argument("node_id")
    parser.add_argument("--port", type=int, default=None, help="port for 'add'")
    args = parser.parse_args()
    state = load_state()
    nodes = state["nodes"]

    if args.action in ("kill", "stop"):
        info = nodes.get(args.node_id) or sys.exit(f"unknown node {args.node_id}")
        pid = info.get("pid")
        if not pid or not pid_alive(pid):
            print(f"{args.node_id} is not running")
            return 0
        if args.action == "kill":
            os.kill(pid, signal.SIGTERM if IS_WINDOWS else signal.SIGKILL)
        else:
            terminate_pid(pid)
        print(f"{args.action}ed {args.node_id} (pid {pid})")
        return 0

    cluster = LocalCluster(state["base_dir"], num_nodes=0, settings_env={"DFS_CLUSTER_SECRET": state["secret"]},
                           coordinator_port=state["coordinator"]["port"], detach=True)
    if args.action == "add":
        if args.node_id in nodes:
            sys.exit(f"{args.node_id} already exists; use 'start'")
        from app.process_manager import free_port

        port = args.port or free_port()
        nodes[args.node_id] = {"pid": None, "port": port, "data_dir": str(Path(state["base_dir"]) / "nodes" / args.node_id)}
    info = nodes.get(args.node_id) or sys.exit(f"unknown node {args.node_id}")
    if info.get("pid") and pid_alive(info["pid"]):
        print(f"{args.node_id} is already running (pid {info['pid']})")
        return 0
    cluster.nodes[args.node_id] = NodeSpec(args.node_id, info["port"], Path(info["data_dir"]))
    cluster.start_node(args.node_id)
    info["pid"] = cluster.node_procs[args.node_id].pid
    cluster.node_procs[args.node_id]._close_log()
    save_state(state)
    print(f"started {args.node_id} on port {info['port']} (pid {info['pid']})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
