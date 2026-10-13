"""Node membership: registration, heartbeats, draining and removal."""

from __future__ import annotations

import logging
import time
from typing import Any

from app.core.exceptions import Conflict, Gone, NotFound, ValidationFailed
from app.metadata.models import AdminState, NodeHealth, NodeRecord
from app.metadata.repository import MetadataRepository
from app.replication.policy import assess_chunk
from app.replication.recovery import RecoveryManager
from app.storage.node_manager import NodeManager
from app.storage.socket_client import NodeClient, NodeError

log = logging.getLogger("dfs.membership")


class MembershipService:
    def __init__(self, repo: MetadataRepository, nodes: NodeManager, recovery: RecoveryManager) -> None:
        self.repo = repo
        self.nodes = nodes
        self.recovery = recovery

    def register(self, *, node_id: str, host: str, port: int, data_dir: str | None, storage_uuid: str,
                 capacity_bytes: int, used_bytes: int, chunk_count: int) -> tuple[NodeRecord, bool]:
        existing = self.repo.get_node(node_id)
        if existing is not None and existing.admin_state == AdminState.REMOVED.value:
            raise Gone(f"node {node_id} was removed from the cluster; start it with a new node id to rejoin")
        clash = self.repo.find_node_by_address(host, port, node_id)
        if clash is not None and clash.health != NodeHealth.DOWN.value:
            raise Conflict(f"address {host}:{port} is already registered to live node {clash.node_id}")
        if existing is not None and existing.storage_uuid and existing.storage_uuid != storage_uuid:
            if existing.health == NodeHealth.HEALTHY.value and (existing.host, existing.port) != (host, port):
                raise Conflict(f"node id {node_id} is already in use by a live node at "
                               f"{existing.host}:{existing.port}")
            log.warning("node %s registered with a different storage identity (disk replaced?); "
                        "its replicas will be re-verified", node_id)
        # Identity check over the data path: the TCP endpoint must answer as this node.
        probe = NodeClient(host, port, self.nodes.settings.cluster_secret, node_id=node_id,
                           connect_timeout=self.nodes.settings.connect_timeout,
                           io_timeout=self.nodes.settings.connect_timeout)
        try:
            pong = probe.ping()
        except NodeError as exc:
            raise ValidationFailed(f"node TCP endpoint {host}:{port} failed identity check: {exc}") from exc
        if pong.get("storage_uuid") != storage_uuid:
            raise ValidationFailed("storage_uuid reported over TCP does not match registration")
        record, created = self.repo.register_node(
            node_id=node_id, host=host, port=port, data_dir=data_dir, storage_uuid=storage_uuid,
            capacity_bytes=capacity_bytes, used_bytes=used_bytes, chunk_count=chunk_count,
        )
        log.info("node %s %s at %s:%s", node_id, "registered" if created else "re-registered", host, port)
        self.recovery.wake()
        return record, created

    def heartbeat(self, node_id: str, *, storage_uuid: str, used_bytes: int, chunk_count: int,
                  capacity_bytes: int) -> NodeRecord:
        node = self.repo.get_node(node_id)
        if node is None:
            raise NotFound(f"node {node_id} is not registered")
        if node.admin_state == AdminState.REMOVED.value:
            raise Gone(f"node {node_id} was removed")
        if node.storage_uuid != storage_uuid:
            raise Conflict("storage identity changed; re-register")
        self.repo.record_heartbeat(node_id, used_bytes, chunk_count, capacity_bytes, time.time())
        return self.repo.get_node(node_id)  # type: ignore[return-value]

    def get(self, node_id: str) -> NodeRecord:
        node = self.repo.get_node(node_id)
        if node is None:
            raise NotFound(f"node {node_id} not found")
        return node

    def drain(self, node_id: str) -> dict[str, Any]:
        node = self.get(node_id)
        if node.admin_state == AdminState.REMOVED.value:
            raise Gone(f"node {node_id} was removed")
        if node.admin_state == AdminState.ACTIVE.value:
            self.repo.set_admin_state(node_id, AdminState.DRAINING)
            log.info("node %s set to DRAINING", node_id)
        report = self.recovery.run_cycle(trigger=f"drain:{node_id}")
        node = self.get(node_id)
        return {
            "node_id": node_id,
            "admin_state": node.admin_state,
            "remaining_replicas": self.repo.count_replicas_on_node(node_id),
            "drained": node.admin_state == AdminState.DRAINED.value,
            "cycle": {"repair": report["repair"], "drain": report["drain"]},
        }

    def remove(self, node_id: str, *, force: bool = False) -> dict[str, Any]:
        """Remove a node from the cluster.

        Normal path: the node must be DRAINED (or hold no replicas).
        Force policy: allowed while replicas remain *only if* every chunk on
        the node still has at least one other valid copy on a reachable node;
        the chunks become under-replicated and recovery repairs them. Force
        never removes the last valid copy of any chunk.
        """
        node = self.get(node_id)
        if node.admin_state == AdminState.REMOVED.value:
            return {"node_id": node_id, "admin_state": node.admin_state, "removed": True}
        remaining = self.repo.replicas_on_node(node_id)
        if remaining and node.admin_state != AdminState.DRAINED.value:
            if not force:
                raise Conflict(f"node {node_id} still holds {len(remaining)} replica(s); drain it first "
                               "or use force=true", details={"remaining_replicas": len(remaining)})
            nodes = self.repo.nodes_by_id()
            blocking = []
            for r in remaining:
                found = self.repo.get_chunk_file_state(r.chunk_id)
                if found is None:
                    continue
                chunk, _ = found
                rf = self.repo.get_file(chunk.file_id).replication_factor  # type: ignore[union-attr]
                health = assess_chunk(chunk.chunk_id, rf, self.repo.replicas_for_chunk(chunk.chunk_id), nodes)
                others = [n for n in health.live if n != node_id]
                if not others:
                    blocking.append(chunk.chunk_id)
            if blocking:
                raise Conflict(f"refusing to remove {node_id}: it holds the only valid copy of "
                               f"{len(blocking)} chunk(s)", details={"chunks": blocking[:50]})
        for r in remaining:
            self.repo.remove_replica_and_schedule_delete(r.chunk_id, node_id, "node_removed")
        # Physical deletes are attempted now; the entries are dropped once the node is REMOVED.
        if node.is_reachable:
            self.recovery.deletion.process_pending(ignore_backoff=True)
        self.repo.set_admin_state(node_id, AdminState.REMOVED)
        log.warning("node %s REMOVED (forced=%s, replicas dropped=%d)", node_id, force, len(remaining))
        self.recovery.wake()
        return {"node_id": node_id, "admin_state": AdminState.REMOVED.value, "removed": True,
                "forced": bool(remaining) and force, "replicas_dropped": len(remaining)}
