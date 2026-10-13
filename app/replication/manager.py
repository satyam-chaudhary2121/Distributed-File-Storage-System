"""Copying real chunk bytes between storage nodes.

A copy is only reported successful after the destination has (a) accepted the
bytes, which it checks against the declared SHA-256 before writing, and (b)
answered an independent VERIFY_CHUNK with the expected checksum.
"""

from __future__ import annotations

import logging
import threading
import zlib
from contextlib import contextmanager
from typing import Iterator

from app.metadata.models import ChunkRecord, NodeRecord
from app.storage.checksum import sha256_bytes
from app.storage.node_manager import NodeManager
from app.storage.protocol import ErrorCode
from app.storage.socket_client import NodeError, NodeUnavailable

log = logging.getLogger("dfs.replication")
_TRANSPORT_ERRORS = ("CONNECT_FAILED", "CONNECT_TIMEOUT", "TIMEOUT", "CONNECTION_LOST")


class CopyError(Exception):
    def __init__(self, kind: str, message: str) -> None:
        super().__init__(f"{kind}: {message}")
        self.kind = kind  # SOURCE_CORRUPTED | SOURCE_MISSING | SOURCE_UNAVAILABLE | DESTINATION_FAILED
        self.message = message


class ChunkLocks:
    """Striped per-chunk locks so recovery, rebalance and drain never write the same chunk at once."""

    def __init__(self, stripes: int = 1024) -> None:
        self._locks = [threading.Lock() for _ in range(stripes)]

    @contextmanager
    def hold(self, chunk_id: str) -> Iterator[None]:
        lock = self._locks[zlib.crc32(chunk_id.encode()) % len(self._locks)]
        with lock:
            yield


class ReplicationManager:
    def __init__(self, node_manager: NodeManager) -> None:
        self.nodes = node_manager

    def copy_chunk(self, chunk: ChunkRecord, source: NodeRecord, dest: NodeRecord) -> str:
        """Copy ``chunk`` from ``source`` to ``dest``. Returns the transfer mode used."""
        mode = "node-to-node"
        try:
            self.nodes.client(source).replicate_chunk(
                chunk.chunk_id, chunk.checksum, dest.host, dest.port, dest.node_id
            )
        except NodeUnavailable as exc:
            raise CopyError("SOURCE_UNAVAILABLE", str(exc)) from exc
        except NodeError as exc:
            if exc.code == ErrorCode.CHECKSUM_MISMATCH.value:
                raise CopyError("SOURCE_CORRUPTED", exc.message) from exc
            if exc.code == ErrorCode.NOT_FOUND.value:
                raise CopyError("SOURCE_MISSING", exc.message) from exc
            if exc.code != ErrorCode.REPLICATION_TARGET_FAILED.value:
                raise CopyError("SOURCE_UNAVAILABLE", str(exc)) from exc
            if not exc.message.startswith(_TRANSPORT_ERRORS):
                # The destination answered and refused (e.g. STORAGE_FULL): relaying cannot help.
                raise CopyError("DESTINATION_FAILED", exc.message) from exc
            # The source could not reach the destination directly. Retry once,
            # relaying the bytes through the coordinator.
            log.info("direct transfer %s -> %s failed (%s); relaying", source.node_id, dest.node_id, exc.message)
            mode = "relay"
            self._relay(chunk, source, dest)
        self._verify_destination(chunk, dest)
        return mode

    def _relay(self, chunk: ChunkRecord, source: NodeRecord, dest: NodeRecord) -> None:
        try:
            data, _ = self.nodes.client(source).get_chunk(chunk.chunk_id)
        except NodeUnavailable as exc:
            raise CopyError("SOURCE_UNAVAILABLE", str(exc)) from exc
        except NodeError as exc:
            raise CopyError("SOURCE_MISSING" if exc.code == "NOT_FOUND" else "SOURCE_UNAVAILABLE", str(exc)) from exc
        if sha256_bytes(data) != chunk.checksum:
            raise CopyError("SOURCE_CORRUPTED", f"source {source.node_id} returned bytes with wrong checksum")
        try:
            self.nodes.client(dest).store_chunk(chunk.chunk_id, data, chunk.checksum)
        except NodeError as exc:
            raise CopyError("DESTINATION_FAILED", str(exc)) from exc

    def _verify_destination(self, chunk: ChunkRecord, dest: NodeRecord) -> None:
        try:
            result = self.nodes.client(dest).verify_chunk(chunk.chunk_id, chunk.checksum)
        except NodeError as exc:
            raise CopyError("DESTINATION_FAILED", f"verification failed: {exc}") from exc
        if not result.get("valid"):
            raise CopyError("DESTINATION_FAILED", f"destination copy invalid: {result}")
