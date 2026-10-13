"""Deletion lifecycle: COMMITTED/FAILED -> DELETING -> DELETED.

``DELETE /files/{id}`` atomically moves the file to DELETING (it can no longer
be downloaded), turns every replica row into a ``pending_deletions`` entry and
removes the chunk metadata. Physical deletes are then attempted; anything that
fails (e.g. node offline) stays queued with exponential backoff and is retried
by the recovery worker. The file only becomes DELETED when no physical
replica remains outstanding, so the API never claims a deletion that did not
happen.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from app.core.config import Settings
from app.core.exceptions import Conflict, NotFound
from app.core.retry import backoff_delay
from app.metadata.models import AdminState, FileState
from app.metadata.repository import MetadataRepository
from app.storage.node_manager import NodeManager
from app.storage.socket_client import NodeError

log = logging.getLogger("dfs.delete")


class DeletionService:
    def __init__(self, settings: Settings, repo: MetadataRepository, nodes: NodeManager) -> None:
        self.settings = settings
        self.repo = repo
        self.nodes = nodes

    def delete_file(self, file_id: str) -> dict[str, Any]:
        record = self.repo.get_file(file_id)
        if record is None or record.state == FileState.DELETED.value:
            raise NotFound(f"File {file_id} not found")
        if record.state in (FileState.PENDING.value, FileState.UPLOADING.value):
            raise Conflict(f"File {file_id} is still uploading", details={"state": record.state})
        op_id = self.repo.start_operation("DELETE", file_id)
        if record.state in (FileState.COMMITTED.value, FileState.FAILED.value):
            self.repo.begin_file_deletion(file_id)
        stats = self.process_pending(file_id=file_id, ignore_backoff=True)
        remaining = self.repo.pending_deletions_for_file(file_id)
        state = self.repo.get_file(file_id).state  # type: ignore[union-attr]
        result = {
            "file_id": file_id,
            "state": state,
            "deleted_replicas": stats["deleted"],
            "pending_cleanup": [
                {"chunk_id": p.chunk_id, "node_id": p.node_id, "attempts": p.attempts, "last_error": p.last_error}
                for p in remaining
            ],
        }
        self.repo.finish_operation(op_id, "SUCCEEDED" if not remaining else "PARTIAL",
                                   {"deleted": stats["deleted"], "pending": len(remaining)})
        return result

    def process_pending(self, *, file_id: str | None = None, ignore_backoff: bool = False) -> dict[str, int]:
        """Attempt every due physical deletion. Safe to call repeatedly (DELETE_CHUNK is idempotent)."""
        stats = {"deleted": 0, "failed": 0, "dropped": 0}
        nodes = self.repo.nodes_by_id(include_removed=True)
        for entry in self.repo.due_pending_deletions(file_id=file_id, ignore_backoff=ignore_backoff):
            node = nodes.get(entry.node_id)
            if node is None or node.admin_state == AdminState.REMOVED.value:
                # A removed node's disk is outside the cluster; nothing can be reached.
                self.repo.remove_pending_deletion(entry.chunk_id, entry.node_id)
                stats["dropped"] += 1
                continue
            if self.repo.get_chunk(entry.chunk_id) is not None and any(
                r.node_id == entry.node_id for r in self.repo.replicas_for_chunk(entry.chunk_id)
            ):
                # The location became a live replica again (e.g. re-adopted); never delete it.
                self.repo.remove_pending_deletion(entry.chunk_id, entry.node_id)
                continue
            try:
                self.nodes.client(node, retries=0).delete_chunk(entry.chunk_id)
            except NodeError as exc:
                attempts = entry.attempts + 1
                delay = backoff_delay(attempts, self.settings.recovery_backoff_base, self.settings.recovery_backoff_max)
                self.repo.record_pending_deletion_failure(
                    entry.chunk_id, entry.node_id, f"{exc.code}: {exc.message}", time.time() + delay
                )
                stats["failed"] += 1
                continue
            self.repo.remove_pending_deletion(entry.chunk_id, entry.node_id)
            stats["deleted"] += 1
        finalized = self.repo.finalize_deleted_files()
        if finalized:
            log.info("deletion completed for %d file(s)", len(finalized))
        return stats
