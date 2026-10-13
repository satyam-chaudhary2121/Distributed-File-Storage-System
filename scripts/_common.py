"""Helpers shared by the scripts (makes the project importable without installation)."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

STATE_FILE = ROOT / "run" / "cluster.json"


def load_state() -> dict:
    if not STATE_FILE.exists():
        sys.exit(f"No running cluster recorded at {STATE_FILE}. Start one with scripts/start_cluster.py")
    return json.loads(STATE_FILE.read_text(encoding="utf-8"))


def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")
    try:
        os.chmod(STATE_FILE, 0o600)  # contains the cluster secret
    except OSError:
        pass
