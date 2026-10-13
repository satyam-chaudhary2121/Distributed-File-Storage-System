"""Download workflow.

Chunks are fetched over TCP in index order and each is checked against its
recorded SHA-256 before being written to a private temp file. A replica that
fails (unreachable, missing, wrong bytes) is skipped in favour of the next,
and bad replicas are marked so the recovery worker repairs them. Only when
the whole reconstructed file matches the original file checksum is it
streamed to the client, so a client never receives truncated or corrupted
content: it receives either the exact file or an error.
"""

from __future__ import annotations

import hashlib
import logging
import os
import tempfile
from pathlib import Path

from app.cluster.placement import PlacementService
from app.core.config import Settings
from app.core.exceptions import ChunkUnavailable, Conflict, DataLoss, IntegrityFailure, NotFound
from app.metadata.models import (
    AdminState,
    ChunkRecord,
    FileRecord,
    FileState,
    NodeHealth,
    NodeRecord,
    ReplicaStatus,
)
from app.metadata.repository import MetadataRepository
from app.storage.checksum import sha256_bytes
from app.storage.node_manager import NodeManager
from app.storage.protocol import ErrorCode
from app.storage.socket_client import NodeError, NodeUnavailable

log = logging.getLogger("dfs.download")

_HEALTH_ORDER = {NodeHealth.HEALTHY.value: 0, NodeHealth.SUSPECT.value: 1, NodeHealth.DOWN.value: 2}


class DownloadService:
    def __init__(self, settings: Settings, repo: MetadataRepository, placement: PlacementService,
                 nodes: NodeManager) -> None:
        self.settings = settings
        self.repo = repo
        self.placement = placement
        self.nodes = nodes
        self.temp_dir = settings.resolve_path(settings.temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    def ordered_replica_nodes(self, chunk: ChunkRecord, nodes: dict[str, NodeRecord]) -> list[NodeRecord]:
        """HEALTHY replicas, ordered by node health, then ring preference."""
        preference = self.placement.preference_list(chunk.chunk_id, list(nodes.values()))
        rank = {nid: i for i, nid in enumerate(preference)}
        candidates = [
            nodes[r.node_id]
            for r in self.repo.replicas_for_chunk(chunk.chunk_id)
            if r.status == ReplicaStatus.HEALTHY.value
            and r.node_id in nodes and nodes[r.node_id].admin_state != AdminState.REMOVED.value
        ]
        return sorted(candidates, key=lambda n: (_HEALTH_ORDER.get(n.health, 3), rank.get(n.node_id, 10**6)))

    def fetch_chunk(self, chunk: ChunkRecord, nodes: dict[str, NodeRecord]) -> tuple[bytes, str]:
        """Return (bytes, node_id) of the first replica whose content verifies."""
        errors: dict[str, str] = {}
        bad = 0
        candidates = self.ordered_replica_nodes(chunk, nodes)
        for node in candidates:
            try:
                data, _ = self.nodes.client(node, retries=0).get_chunk(chunk.chunk_id)
            except NodeUnavailable as exc:
                errors[node.node_id] = f"{exc.code}: {exc.message}"
                continue
            except NodeError as exc:
                errors[node.node_id] = f"{exc.code}: {exc.message}"
                if exc.code == ErrorCode.NOT_FOUND.value:
                    self.repo.set_replica_status(chunk.chunk_id, node.node_id, ReplicaStatus.MISSING)
                    bad += 1
                continue
            if len(data) != chunk.size or sha256_bytes(data) != chunk.checksum:
                log.warning("checksum mismatch for chunk %s on %s; marking CORRUPTED",
                            chunk.chunk_id[:12], node.node_id)
                self.repo.set_replica_status(chunk.chunk_id, node.node_id, ReplicaStatus.CORRUPTED)
                errors[node.node_id] = "CHECKSUM_MISMATCH"
                bad += 1
                continue
            return data, node.node_id
        details = {"chunk_id": chunk.chunk_id, "chunk_index": chunk.chunk_index, "errors": errors}
        if bad == len(candidates):
            # Every replica we know of is corrupted or missing (or none exist).
            raise DataLoss(f"no valid replica exists for chunk {chunk.chunk_index}", details=details)
        raise ChunkUnavailable(f"no reachable valid replica for chunk {chunk.chunk_index}", details=details)

    def prepare(self, file_id: str) -> tuple[FileRecord, Path, dict[int, str]]:
        """Reconstruct a committed file into a temp file. Returns (record, path, chunk->node used)."""
        record = self.repo.get_file(file_id)
        if record is None or record.state == FileState.DELETED.value:
            raise NotFound(f"File {file_id} not found")
        if record.state != FileState.COMMITTED.value:
            raise Conflict(f"File {file_id} is {record.state} and cannot be downloaded",
                           details={"state": record.state})
        chunks = self.repo.get_chunks(file_id)
        if [c.chunk_index for c in chunks] != list(range(record.chunk_count)):
            raise IntegrityFailure(f"chunk metadata for {file_id} is incomplete")
        nodes = self.repo.nodes_by_id(include_removed=False)
        fd, tmp_name = tempfile.mkstemp(dir=self.temp_dir, prefix="download-")
        path = Path(tmp_name)
        served: dict[int, str] = {}
        try:
            h = hashlib.sha256()
            with os.fdopen(fd, "wb") as out:
                for chunk in chunks:
                    data, node_id = self.fetch_chunk(chunk, nodes)
                    out.write(data)
                    h.update(data)
                    served[chunk.chunk_index] = node_id
            if h.hexdigest() != record.checksum:
                raise IntegrityFailure(f"reconstructed file {file_id} does not match its checksum")
        except BaseException:
            path.unlink(missing_ok=True)
            raise
        return record, path, served
