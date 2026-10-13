"""Read-side views of file metadata with live replica health."""

from __future__ import annotations

from typing import Any

from app.cluster.placement import PlacementService
from app.core.exceptions import NotFound
from app.metadata.models import AdminState, FileRecord, FileState, NodeHealth, NodeRecord, ReplicaRecord
from app.metadata.repository import MetadataRepository
from app.replication.policy import assess_chunk


def effective_replica_status(replica: ReplicaRecord, node: NodeRecord | None) -> str:
    if node is None or node.admin_state == AdminState.REMOVED.value:
        return "REMOVED"
    if replica.status == "HEALTHY" and node.health == NodeHealth.DOWN.value:
        return "UNAVAILABLE"
    return replica.status


class FileService:
    def __init__(self, repo: MetadataRepository, placement: PlacementService) -> None:
        self.repo = repo
        self.placement = placement

    @staticmethod
    def summary(record: FileRecord) -> dict[str, Any]:
        return {
            "file_id": record.file_id,
            "filename": record.filename,
            "size": record.size,
            "checksum": record.checksum,
            "chunk_count": record.chunk_count,
            "chunk_size": record.chunk_size,
            "state": record.state,
            "replication_factor": record.replication_factor,
            "created_at": record.created_at,
            "updated_at": record.updated_at,
            "committed_at": record.committed_at,
        }

    def describe(self, file_id: str) -> dict[str, Any]:
        record = self.repo.get_file(file_id)
        if record is None or record.state == FileState.DELETED.value:
            raise NotFound(f"File {file_id} not found")
        nodes_list = self.repo.list_nodes(include_removed=True)
        nodes = {n.node_id: n for n in nodes_list}
        replicas = self.repo.replicas_for_file(file_id)
        chunks_out = []
        healthy_counts = []
        status_counts: dict[str, int] = {}
        for chunk in self.repo.get_chunks(file_id):
            reps = replicas.get(chunk.chunk_id, [])
            health = assess_chunk(chunk.chunk_id, record.replication_factor, reps, nodes)
            preference = self.placement.preference_list(chunk.chunk_id, [n for n in nodes_list])
            rank = {nid: i for i, nid in enumerate(preference)}
            ordered = sorted(reps, key=lambda r: rank.get(r.node_id, 10**6))
            locations = []
            for r in ordered:
                node = nodes.get(r.node_id)
                locations.append({
                    "node_id": r.node_id,
                    "status": effective_replica_status(r, node),
                    "node_health": node.health if node else None,
                    "node_admin_state": node.admin_state if node else None,
                    "verified_at": r.verified_at,
                })
            healthy_counts.append(len(health.counted))
            status_counts[health.status.value] = status_counts.get(health.status.value, 0) + 1
            chunks_out.append({
                "index": chunk.chunk_index,
                "chunk_id": chunk.chunk_id,
                "size": chunk.size,
                "checksum": chunk.checksum,
                "status": health.status.value,
                "healthy_replicas": len(health.counted),
                "locations": locations,
            })
        out = self.summary(record)
        out["error"] = record.error
        out["replication"] = {
            "desired": record.replication_factor,
            "min_healthy_replicas": min(healthy_counts) if healthy_counts else None,
            "chunk_status_counts": status_counts,
        }
        out["chunks"] = chunks_out
        if record.state == FileState.DELETING.value:
            out["pending_cleanup"] = [
                {"chunk_id": p.chunk_id, "node_id": p.node_id, "attempts": p.attempts, "last_error": p.last_error}
                for p in self.repo.pending_deletions_for_file(file_id)
            ]
        return out

    def list_files(self, limit: int, offset: int, name: str | None) -> dict[str, Any]:
        records, total = self.repo.list_files(limit=limit, offset=offset, name_contains=name)
        return {"items": [self.summary(r) for r in records], "total": total, "limit": limit, "offset": offset}
