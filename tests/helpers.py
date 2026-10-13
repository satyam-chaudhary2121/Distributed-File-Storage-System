"""Shared helpers for tests that run real coordinator/node processes."""

from __future__ import annotations

import hashlib
import os
from typing import Any

import httpx

# Short timeouts so failure detection happens in seconds during tests.
FAST_ENV = {
    "DFS_HEARTBEAT_INTERVAL": "0.5",
    "DFS_HEALTH_CHECK_INTERVAL": "0.3",
    "DFS_SUSPECT_TIMEOUT": "1.0",
    "DFS_FAILURE_TIMEOUT": "2.0",
    "DFS_RECOVERY_INTERVAL": "1.0",
    "DFS_RECOVERY_BACKOFF_BASE": "0.2",
    "DFS_RECOVERY_BACKOFF_MAX": "2.0",
    "DFS_CHUNK_SIZE": "65536",
    "DFS_CONNECT_TIMEOUT": "1.0",
    "DFS_IO_TIMEOUT": "10.0",
    "DFS_CLIENT_RETRIES": "1",
    "DFS_ORPHAN_GRACE_SECONDS": "0",
    "DFS_LOG_LEVEL": "INFO",
}

# For tests that must act *before* the coordinator notices a dead node,
# and that must not be disturbed by the background worker.
SLOW_DETECTION_ENV = {
    **FAST_ENV,
    "DFS_SUSPECT_TIMEOUT": "60",
    "DFS_FAILURE_TIMEOUT": "120",
    "DFS_RECOVERY_INTERVAL": "3600",
}


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def random_bytes(n: int) -> bytes:
    return os.urandom(n)


def upload(api: httpx.Client, data: bytes, name: str = "test.bin") -> dict[str, Any]:
    resp = api.post("/api/v1/files/upload", files={"file": (name, data, "application/octet-stream")})
    assert resp.status_code == 201, resp.text
    return resp.json()


def file_meta(api: httpx.Client, file_id: str) -> dict[str, Any]:
    resp = api.get(f"/api/v1/files/{file_id}")
    assert resp.status_code == 200, resp.text
    return resp.json()


def healthy_locations(chunk: dict[str, Any]) -> list[str]:
    return [loc["node_id"] for loc in chunk["locations"] if loc["status"] == "HEALTHY"]


def download(api: httpx.Client, file_id: str) -> httpx.Response:
    return api.get(f"/api/v1/files/{file_id}/download")


def trigger_recovery(api: httpx.Client, scrub: bool = False, force: bool = True) -> dict[str, Any]:
    resp = api.post("/api/v1/recovery/trigger", json={"scrub": scrub, "force": force})
    assert resp.status_code == 200, resp.text
    return resp.json()
