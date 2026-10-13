"""On-disk chunk storage for a single storage node.

Layout under ``data_dir``::

    node_identity.json          node_id + storage_uuid (identity of this disk)
    chunks/<id[:2]>/<id>.chunk  raw chunk bytes
    chunks/<id[:2]>/<id>.meta   {"checksum", "size", "stored_at"} sidecar
    tmp/                        in-flight writes (cleaned on start)

Writes are atomic: data goes to ``tmp/`` and is fsync'd, then ``os.replace``'d
into place. The in-memory inventory is rebuilt from disk on start.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
import threading
import time
import uuid
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.storage.checksum import is_sha256_hex, sha256_bytes, sha256_file

CHUNK_ID_RE = re.compile(r"^[A-Za-z0-9_-]{8,128}$")
_LOCK_STRIPES = 256


class StoreError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


@dataclass
class _Entry:
    size: int
    checksum: str
    mtime: float


def _fsync_dir(path: Path) -> None:
    if os.name == "nt":  # directories cannot be fsync'd on Windows
        return
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


class ChunkStore:
    def __init__(self, data_dir: str | Path, node_id: str, capacity_bytes: int) -> None:
        self.root = Path(data_dir).resolve()
        self.node_id = node_id
        self.capacity_bytes = capacity_bytes
        self.chunks_dir = self.root / "chunks"
        self.tmp_dir = self.root / "tmp"
        self.chunks_dir.mkdir(parents=True, exist_ok=True)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)
        self.storage_uuid = self._load_identity()
        self._locks = [threading.Lock() for _ in range(_LOCK_STRIPES)]
        self._index_lock = threading.Lock()
        self._index: dict[str, _Entry] = {}
        self.rebuild_inventory()

    # ------------------------------------------------------------ identity
    def _load_identity(self) -> str:
        path = self.root / "node_identity.json"
        if path.exists():
            data = json.loads(path.read_text(encoding="utf-8"))
            if data.get("node_id") != self.node_id:
                raise StoreError(
                    "IDENTITY_MISMATCH",
                    f"data dir {self.root} belongs to node {data.get('node_id')!r}, not {self.node_id!r}",
                )
            return str(data["storage_uuid"])
        storage_uuid = uuid.uuid4().hex
        path.write_text(
            json.dumps({"node_id": self.node_id, "storage_uuid": storage_uuid, "created_at": time.time()}),
            encoding="utf-8",
        )
        return storage_uuid

    # --------------------------------------------------------------- paths
    def _lock(self, chunk_id: str) -> threading.Lock:
        return self._locks[zlib.crc32(chunk_id.encode()) % _LOCK_STRIPES]

    def _paths(self, chunk_id: str) -> tuple[Path, Path]:
        if not isinstance(chunk_id, str) or not CHUNK_ID_RE.match(chunk_id):
            raise StoreError("BAD_REQUEST", "invalid chunk_id")
        directory = (self.chunks_dir / chunk_id[:2]).resolve()
        data_path = (directory / f"{chunk_id}.chunk").resolve()
        # Defence in depth: the regex already forbids separators and dots.
        if not data_path.is_relative_to(self.chunks_dir):
            raise StoreError("BAD_REQUEST", "chunk path escapes storage directory")
        return data_path, data_path.with_suffix(".meta")

    # ----------------------------------------------------------- inventory
    def rebuild_inventory(self) -> None:
        for leftover in self.tmp_dir.iterdir():
            try:
                leftover.unlink()
            except OSError:
                pass
        index: dict[str, _Entry] = {}
        for data_path in self.chunks_dir.glob("*/*.chunk"):
            chunk_id = data_path.stem
            if not CHUNK_ID_RE.match(chunk_id):
                continue
            meta_path = data_path.with_suffix(".meta")
            stat = data_path.stat()
            entry: _Entry | None = None
            if meta_path.exists():
                try:
                    meta = json.loads(meta_path.read_text(encoding="utf-8"))
                    if int(meta["size"]) == stat.st_size and is_sha256_hex(meta["checksum"]):
                        entry = _Entry(stat.st_size, meta["checksum"], stat.st_mtime)
                except (ValueError, KeyError, OSError):
                    entry = None
            if entry is None:
                # Sidecar missing or stale: record what is actually on disk.
                entry = _Entry(stat.st_size, sha256_file(data_path), stat.st_mtime)
                self._write_meta(meta_path, entry)
            index[chunk_id] = entry
        with self._index_lock:
            self._index = index

    def _write_meta(self, meta_path: Path, entry: _Entry) -> None:
        payload = json.dumps({"checksum": entry.checksum, "size": entry.size, "stored_at": entry.mtime})
        fd, tmp = tempfile.mkstemp(dir=self.tmp_dir, suffix=".meta")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(payload)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, meta_path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    @property
    def used_bytes(self) -> int:
        with self._index_lock:
            return sum(e.size for e in self._index.values())

    @property
    def chunk_count(self) -> int:
        with self._index_lock:
            return len(self._index)

    # ---------------------------------------------------------- operations
    def store(self, chunk_id: str, data: bytes, checksum: str) -> dict[str, Any]:
        if not is_sha256_hex(checksum):
            raise StoreError("BAD_REQUEST", "checksum must be a lowercase SHA-256 hex digest")
        data_path, meta_path = self._paths(chunk_id)
        actual = sha256_bytes(data)
        if actual != checksum:
            raise StoreError("CHECKSUM_MISMATCH", f"payload checksum {actual} != declared {checksum}")
        with self._lock(chunk_id):
            with self._index_lock:
                existing = self._index.get(chunk_id)
            if existing is not None and existing.checksum == checksum and data_path.exists():
                if sha256_file(data_path) == checksum:
                    return {"stored": False, "already_present": True, "checksum": checksum, "size": len(data)}
            existing_size = existing.size if existing else 0
            if self.capacity_bytes and self.used_bytes - existing_size + len(data) > self.capacity_bytes:
                raise StoreError("STORAGE_FULL", f"capacity {self.capacity_bytes} bytes would be exceeded")
            data_path.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=self.tmp_dir, suffix=".chunk")
            try:
                with os.fdopen(fd, "wb") as fh:
                    fh.write(data)
                    fh.flush()
                    os.fsync(fh.fileno())
                os.replace(tmp, data_path)
            except BaseException:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
                raise
            entry = _Entry(len(data), checksum, time.time())
            self._write_meta(meta_path, entry)
            _fsync_dir(data_path.parent)
            with self._index_lock:
                self._index[chunk_id] = entry
        return {"stored": True, "already_present": False, "checksum": checksum, "size": len(data)}

    def get(self, chunk_id: str) -> tuple[bytes, str]:
        data_path, _ = self._paths(chunk_id)
        with self._lock(chunk_id):
            with self._index_lock:
                entry = self._index.get(chunk_id)
            if entry is None or not data_path.exists():
                raise StoreError("NOT_FOUND", f"chunk {chunk_id} not stored on this node")
            return data_path.read_bytes(), entry.checksum

    def delete(self, chunk_id: str) -> bool:
        data_path, meta_path = self._paths(chunk_id)
        with self._lock(chunk_id):
            existed = data_path.exists()
            for p in (data_path, meta_path):
                try:
                    p.unlink()
                except FileNotFoundError:
                    pass
            with self._index_lock:
                self._index.pop(chunk_id, None)
            try:
                data_path.parent.rmdir()  # only succeeds when empty
            except OSError:
                pass
        return existed

    def verify(self, chunk_id: str, expected: str | None = None) -> dict[str, Any]:
        data_path, _ = self._paths(chunk_id)
        with self._lock(chunk_id):
            with self._index_lock:
                entry = self._index.get(chunk_id)
            if entry is None or not data_path.exists():
                return {"exists": False, "valid": False, "actual_checksum": None, "recorded_checksum": None}
            actual = sha256_file(data_path)
        valid = actual == entry.checksum and (expected is None or actual == expected)
        return {
            "exists": True,
            "valid": valid,
            "actual_checksum": actual,
            "recorded_checksum": entry.checksum,
            "size": data_path.stat().st_size,
        }

    def list(self) -> list[dict[str, Any]]:
        with self._index_lock:
            return [
                {"chunk_id": cid, "size": e.size, "checksum": e.checksum, "mtime": e.mtime}
                for cid, e in sorted(self._index.items())
            ]

    def status(self) -> dict[str, Any]:
        used = self.used_bytes
        return {
            "chunk_count": self.chunk_count,
            "used_bytes": used,
            "capacity_bytes": self.capacity_bytes,
            "free_bytes": max(0, self.capacity_bytes - used) if self.capacity_bytes else None,
            "storage_uuid": self.storage_uuid,
        }
