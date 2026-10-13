from __future__ import annotations

import time

from fastapi import APIRouter, Depends, Path, Query, Response

from app.api.deps import get_coordinator, require_cluster_token
from app.core.validation import NODE_ID_PATTERN
from app.metadata.models import NodeRecord
from app.runtime import Coordinator
from app.schemas.nodes import HeartbeatRequest, NodeInfo, RegisterNodeRequest

router = APIRouter(prefix="/api/v1/nodes", tags=["nodes"])
NodeId = Path(..., pattern=NODE_ID_PATTERN)


def node_info(coord: Coordinator, node: NodeRecord) -> dict:
    return {
        "node_id": node.node_id, "host": node.host, "port": node.port, "data_dir": node.data_dir,
        "health": node.health, "admin_state": node.admin_state, "last_seen": node.last_seen,
        "last_heartbeat": node.last_heartbeat,
        "seconds_since_seen": round(time.time() - node.last_seen, 3) if node.last_seen else None,
        "registered_at": node.registered_at, "capacity_bytes": node.capacity_bytes,
        "used_bytes": node.used_bytes, "chunk_count": node.chunk_count,
        "replica_count": coord.repo.count_replicas_on_node(node.node_id),
        "needs_reconcile": bool(node.needs_reconcile), "last_reconciled_at": node.last_reconciled_at,
    }


@router.post("/register", response_model=NodeInfo, dependencies=[Depends(require_cluster_token)])
def register_node(body: RegisterNodeRequest, response: Response,
                  coord: Coordinator = Depends(get_coordinator)) -> dict:
    """Called by storage nodes on start-up (requires X-Cluster-Token). Idempotent."""
    record, created = coord.membership.register(**body.model_dump())
    response.status_code = 201 if created else 200
    return node_info(coord, record)


@router.post("/{node_id}/heartbeat", response_model=NodeInfo, dependencies=[Depends(require_cluster_token)])
def heartbeat(body: HeartbeatRequest, node_id: str = NodeId, coord: Coordinator = Depends(get_coordinator)) -> dict:
    record = coord.membership.heartbeat(node_id, **body.model_dump())
    return node_info(coord, record)


@router.get("", response_model=list[NodeInfo])
def list_nodes(include_removed: bool = Query(False), coord: Coordinator = Depends(get_coordinator)) -> list[dict]:
    return [node_info(coord, n) for n in coord.repo.list_nodes(include_removed=include_removed)]


@router.get("/{node_id}", response_model=NodeInfo)
def get_node(node_id: str = NodeId, coord: Coordinator = Depends(get_coordinator)) -> dict:
    return node_info(coord, coord.membership.get(node_id))


@router.post("/{node_id}/drain")
def drain_node(node_id: str = NodeId, coord: Coordinator = Depends(get_coordinator)) -> dict:
    """Stop placing data on the node and move its replicas elsewhere. Re-callable to check progress."""
    return coord.membership.drain(node_id)


@router.delete("/{node_id}")
def remove_node(node_id: str = NodeId, force: bool = Query(False),
                coord: Coordinator = Depends(get_coordinator)) -> dict:
    """Remove a DRAINED node. ``force=true`` skips draining but never drops a chunk's last valid copy."""
    return coord.membership.remove(node_id, force=force)
