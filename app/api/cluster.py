from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from app.api.deps import get_coordinator
from app.metadata.models import AdminState, NodeHealth
from app.runtime import Coordinator

router = APIRouter(prefix="/api/v1/cluster", tags=["cluster"])


@router.get("/status")
def cluster_status(coord: Coordinator = Depends(get_coordinator)) -> dict:
    nodes = coord.repo.list_nodes()
    health = {h.value: 0 for h in NodeHealth}
    admin = {a.value: 0 for a in AdminState if a != AdminState.REMOVED}
    for n in nodes:
        health[n.health] += 1
        admin[n.admin_state] += 1
    replication = coord.recovery.replication_status(limit=0)
    replication.pop("problem_chunks", None)
    ring = coord.placement.ring(nodes)
    return {
        "nodes": {"total": len(nodes), "by_health": health, "by_admin_state": admin,
                  "ring_members": sorted(ring.nodes)},
        "files": coord.repo.count_files_by_state(),
        "replication": replication,
        "settings": {
            "replication_factor": coord.settings.replication_factor,
            "write_quorum": coord.settings.write_quorum,
            "chunk_size": coord.settings.chunk_size,
            "virtual_nodes": coord.placement.vnodes,
            "suspect_timeout": coord.settings.suspect_timeout,
            "failure_timeout": coord.settings.failure_timeout,
        },
        "recovery": {"worker_running": coord.recovery.running, "cycles": coord.recovery.cycles,
                     "pending_deletions": coord.repo.count_pending_deletions(),
                     "repair_tasks": coord.repo.count_repair_tasks_by_state()},
    }


@router.get("/storage")
def cluster_storage(coord: Coordinator = Depends(get_coordinator)) -> dict:
    nodes = coord.repo.list_nodes()
    per_node = []
    for n in nodes:
        per_node.append({
            "node_id": n.node_id, "health": n.health, "admin_state": n.admin_state,
            "used_bytes": n.used_bytes, "capacity_bytes": n.capacity_bytes,
            "utilization": round(n.used_bytes / n.capacity_bytes, 6) if n.capacity_bytes else None,
            "chunk_count": n.chunk_count, "replica_count": coord.repo.count_replicas_on_node(n.node_id),
        })
    logical = coord.repo.committed_logical_bytes()
    physical = coord.repo.live_physical_bytes()
    return {
        "nodes": per_node,
        "total_used_bytes": sum(n.used_bytes for n in nodes),
        "total_capacity_bytes": sum(n.capacity_bytes for n in nodes),
        "logical_bytes_committed": logical,
        "physical_bytes_in_healthy_replicas": physical,
        "storage_amplification": round(physical / logical, 4) if logical else None,
    }


@router.get("/replication/status")
def replication_status(limit: int = Query(100, ge=0, le=10000), coord: Coordinator = Depends(get_coordinator)) -> dict:
    return coord.recovery.replication_status(limit=limit)


@router.post("/rebalance")
def rebalance(dry_run: bool = Query(False), coord: Coordinator = Depends(get_coordinator)) -> dict:
    """Move chunk data so placement matches the current hash ring (or, with dry_run, show the plan)."""
    if dry_run:
        plan = coord.rebalancer.plan()
        return {"dry_run": True, "chunks_to_change": len(plan), "plan": plan[:500]}
    return coord.rebalancer.rebalance()
