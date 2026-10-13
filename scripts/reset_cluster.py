#!/usr/bin/env python3
"""DESTRUCTIVE: delete the coordinator database and all node data under a base dir.

Normal start-up never deletes anything; this script is the only way to wipe state.

    python scripts/reset_cluster.py --yes [--base-dir data]
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

from _common import ROOT, STATE_FILE

from app.process_manager import pid_alive


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-dir", default=str(ROOT / "data"))
    parser.add_argument("--yes", action="store_true", help="confirm deletion")
    args = parser.parse_args()
    base = Path(args.base_dir).resolve()
    if not args.yes:
        sys.exit(f"Refusing to delete {base} without --yes")
    if STATE_FILE.exists():
        import json

        pid = json.loads(STATE_FILE.read_text()).get("coordinator", {}).get("pid")
        if pid and pid_alive(pid):
            sys.exit("Stop the running cluster first (scripts/stop_cluster.py)")
    if base.exists():
        shutil.rmtree(base)
        print(f"deleted {base}")
    else:
        print(f"nothing to delete at {base}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
