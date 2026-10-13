"""Upload workflow: PENDING -> UPLOADING -> COMMITTED (or FAILED).

Atomicity
---------
A multi-node upload cannot be made atomic across independent disks, so the
system makes it *atomic from the reader's point of view*:

* A file is visible/downloadable only once it is COMMITTED. COMMITTED is set
  in a single SQLite transaction after every chunk has at least
  ``write_quorum`` replicas whose bytes the receiving node has verified.
* Before each replica write a PENDING replica row is recorded, so if the
  upload fails (or the coordinator crashes) every possibly-written replica is
  known. On failure the file becomes FAILED and those replicas are queued in
  ``pending_deletions`` for cleanup; on coordinator start-up, uploads found in
  PENDING/UPLOADING are aborted the same way.
* A committed chunk with fewer than ``replication_factor`` replicas is
  reported as under-replicated and repaired by the recovery worker.
"""

from __future__ import annotations

import hashlib
import logging
import os
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import BinaryIO

from app.cluster.placement import PlacementService
from app.core.config import Settings
from app.core.exceptions import InsufficientNodes, PayloadTooLarge, UploadFailed
from app.core.validation import sanitize_filename
from app.metadata.models import FileRecord, FileState, NodeRecord, ReplicaStatus
from app.metadata.repository import MetadataRepository
from app.services.deletion_service import DeletionService
from app.storage.chunking import Chunk, iter_chunks
from app.storage.node_manager import NodeManager
from app.storage.socket_client import NodeError

log = logging.getLogger("dfs.upload")
_COPY_BLOCK = 1024 * 1024


class UploadService:
    def __init__(self, settings: Settings, repo: MetadataRepository, placement: PlacementService,
                 nodes: NodeManager, deletion: DeletionService, executor: ThreadPoolExecutor) -> None:
        self.settings = settings
        self.repo = repo
        self.placement = placement
        self.nodes = nodes
        self.deletion = deletion
        self.executor = executor
        self.temp_dir = settings.resolve_path(settings.temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------- staging
    def _stage(self, source: BinaryIO) -> tuple[Path, int, str]:
        """Copy the upload to a private temp file, hashing as we go (bounded memory)."""
        h = hashlib.sha256()
        size = 0
        fd, tmp_name = tempfile.mkstemp(dir=self.temp_dir, prefix="upload-", suffix=".part")
        try:
            with os.fdopen(fd, "wb") as out:
                while True:
                    block = source.read(_COPY_BLOCK)
                    if not block:
                        break
                    size += len(block)
                    if self.settings.max_upload_size and size > self.settings.max_upload_size:
                        raise PayloadTooLarge(
                            f"file exceeds max_upload_size of {self.settings.max_upload_size} bytes"
                        )
                    h.update(block)
                    out.write(block)
        except BaseException:
            Path(tmp_name).unlink(missing_ok=True)
            raise
        return Path(tmp_name), size, h.hexdigest()

    # -------------------------------------------------------------- upload
    def upload(self, filename: str | None, source: BinaryIO) -> FileRecord:
        safe_name = sanitize_filename(filename)
        staged, size, checksum = self._stage(source)
        try:
            return self._upload_staged(safe_name, staged, size, checksum)
        finally:
            staged.unlink(missing_ok=True)

    def _upload_staged(self, filename: str, staged: Path, size: int, checksum: str) -> FileRecord:
        s = self.settings
        quorum = min(s.write_quorum, s.replication_factor)
        file_id = uuid.uuid4().hex
        self.repo.create_file(file_id=file_id, filename=filename, size=size, checksum=checksum,
                              chunk_size=s.chunk_size, replication_factor=s.replication_factor)
        op_id = self.repo.start_operation("UPLOAD", file_id, {"filename": filename, "size": size})
        stats = {"chunks": 0, "replicas_written": 0, "under_replicated_chunks": 0, "write_fallbacks": 0}
        try:
            self.repo.transition_file(file_id, FileState.UPLOADING)
            nodes = self.repo.list_nodes()
            writable = [n for n in nodes if n.is_writable]
            if size > 0 and len(writable) < quorum:
                raise InsufficientNodes(
                    f"{len(writable)} healthy active node(s) available; write quorum is {quorum}",
                    details={"healthy_nodes": len(writable), "write_quorum": quorum},
                )
            count = 0
            with staged.open("rb") as fh:
                for chunk in iter_chunks(fh, s.chunk_size, file_id):
                    self.repo.add_chunk(chunk_id=chunk.chunk_id, file_id=file_id, chunk_index=chunk.index,
                                        size=chunk.size, checksum=chunk.checksum)
                    written, fallbacks = self._place_chunk(chunk, nodes, quorum)
                    stats["replicas_written"] += written
                    stats["write_fallbacks"] += fallbacks
                    if written < s.replication_factor:
                        stats["under_replicated_chunks"] += 1
                    count += 1
            stats["chunks"] = count
            record = self.repo.transition_file(file_id, FileState.COMMITTED, extra={"chunk_count": count})
            self.repo.finish_operation(op_id, "SUCCEEDED", stats)
            log.info("committed %s (%s, %d bytes, %d chunks)", file_id, filename, size, count)
            return record
        except Exception as exc:
            message = str(exc) if str(exc) else type(exc).__name__
            log.warning("upload %s failed: %s", file_id, message)
            try:
                self.repo.abort_upload(file_id, message[:500])
                self.deletion.process_pending(file_id=file_id, ignore_backoff=True)
            except Exception:  # noqa: BLE001 - cleanup must not mask the original error
                log.exception("cleanup after failed upload %s did not complete", file_id)
            self.repo.finish_operation(op_id, "FAILED", stats, error=message[:500])
            if isinstance(exc, (InsufficientNodes, PayloadTooLarge, UploadFailed)):
                exc.details.setdefault("file_id", file_id)
                raise
            raise UploadFailed(message, details={"file_id": file_id}) from exc

    def _store_one(self, node: NodeRecord, chunk: Chunk) -> NodeRecord:
        result = self.nodes.client(node).store_chunk(chunk.chunk_id, chunk.data, chunk.checksum)
        if result.get("checksum") != chunk.checksum:
            raise NodeError("CHECKSUM_MISMATCH", "node acknowledged a different checksum", node.node_id)
        return node

    def _place_chunk(self, chunk: Chunk, nodes: list[NodeRecord], quorum: int) -> tuple[int, int]:
        """Write ``chunk`` to up to R nodes in ring order; returns (verified copies, fallbacks used)."""
        replication_factor = self.settings.replication_factor
        by_id = {n.node_id: n for n in nodes}
        candidates = [by_id[nid] for nid in self.placement.write_candidates(chunk.chunk_id, nodes)]
        targets, spares = candidates[:replication_factor], candidates[replication_factor:]
        for node in targets:
            self.repo.upsert_replica(chunk.chunk_id, node.node_id, ReplicaStatus.PENDING)
        futures = {self.executor.submit(self._store_one, n, chunk): n for n in targets}
        successes = 0
        failures: list[tuple[NodeRecord, str]] = []
        for fut in as_completed(futures):
            node = futures[fut]
            try:
                fut.result()
                self.repo.record_verified_replica(chunk.chunk_id, node.node_id)
                successes += 1
            except NodeError as exc:
                failures.append((node, f"{exc.code}: {exc.message}"))
                # The write may or may not have landed: schedule cleanup of that location.
                self.repo.remove_replica_and_schedule_delete(chunk.chunk_id, node.node_id, "upload_write_failed")
        fallbacks = 0
        for node in spares:
            if successes >= replication_factor or not failures:
                break
            fallbacks += 1
            self.repo.upsert_replica(chunk.chunk_id, node.node_id, ReplicaStatus.PENDING)
            try:
                self._store_one(node, chunk)
                self.repo.record_verified_replica(chunk.chunk_id, node.node_id)
                successes += 1
            except NodeError as exc:
                failures.append((node, f"{exc.code}: {exc.message}"))
                self.repo.remove_replica_and_schedule_delete(chunk.chunk_id, node.node_id, "upload_write_failed")
        if failures:
            log.warning("chunk %s: %d write failure(s): %s", chunk.chunk_id[:12], len(failures),
                        "; ".join(f"{n.node_id}={e}" for n, e in failures))
        if successes < quorum:
            raise UploadFailed(
                f"chunk {chunk.index} stored on {successes} node(s); write quorum is {quorum}",
                details={"chunk_index": chunk.index,
                         "errors": {n.node_id: e for n, e in failures}},
            )
        return successes, fallbacks

    # ------------------------------------------------------------- startup
    def abort_interrupted_uploads(self) -> list[str]:
        """Called at coordinator start: uploads left mid-flight by a crash are failed and cleaned up."""
        aborted = []
        for f in self.repo.files_in_states([FileState.PENDING, FileState.UPLOADING]):
            self.repo.abort_upload(f.file_id, "coordinator restarted during upload")
            aborted.append(f.file_id)
        if aborted:
            log.warning("aborted %d interrupted upload(s): %s", len(aborted), aborted)
        for leftover in self.temp_dir.glob("*.part"):
            leftover.unlink(missing_ok=True)
        for leftover in self.temp_dir.glob("download-*"):
            leftover.unlink(missing_ok=True)
        return aborted
