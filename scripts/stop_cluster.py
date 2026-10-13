#!/usr/bin/env python3
"""Stop the cluster started by scripts/start_cluster.py (data is kept).

    python scripts/stop_cluster.py
"""

from __future__ import annotations

from _common import STATE_FILE, load_state

from app.process_manager import pid_alive, terminate_pid


def main() -> int:
    state = load_state()
    for nid, info in state.get("nodes", {}).items():
        pid = info.get("pid")
        if pid and pid_alive(pid):
            terminate_pid(pid)
            print(f"stopped {nid} (pid {pid})")
    pid = state.get("coordinator", {}).get("pid")
    if pid and pid_alive(pid):
        terminate_pid(pid)
        print(f"stopped coordinator (pid {pid})")
    STATE_FILE.unlink(missing_ok=True)
    print("cluster stopped; data directories were left intact")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
