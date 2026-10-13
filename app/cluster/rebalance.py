"""Rebalancing: move real chunk data to match the current hash ring.

For each chunk of a committed file:

1. ``desired`` = first R ACTIVE nodes on the chunk's ring preference list.
2. Copy (and verify) the chunk onto each HEALTHY desired node that lacks a
   valid copy.
3. Only once every reachable desired node holds a verified copy, delete
   copies on nodes outside ``desired`` - and never if that would leave fewer
   than R valid copies (so the last valid replica is never deleted).

Ring changes (membership) and data movement are separate steps: adding a node
changes ``desired`` immediately but nothing is deleted until the new copies
are verified, so an interrupted rebalance leaves extra copies, never fewer.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from app.cluster.placement import PlacementService
from app.metadata.models import NodeHealth
from app.metadata.repository import MetadataRepository
from app.replication.manager import ChunkLocks, CopyError, ReplicationManager
from app.replication.policy import assess_chunk
from app.replication.recovery import RecoveryManager
from app.storage.node_manager import NodeManager
from app.storage.socket_client import NodeError

log = logging.getLogger("dfs.rebalance")


class Rebalancer:
    def __init__(self, repo: MetadataRepository, nodes: NodeManager, placement: PlacementService,
                 replication: ReplicationManager, recovery: RecoveryManager, locks: ChunkLocks) -> None:
        self.repo = repo
        self.nodes = nodes
        self.placement = placement
        self.replication = replication
        self.recovery = recovery
        self.locks = locks

    def plan(self) -> list[dict[str, Any]]:
        """Chunks whose current valid copies differ from their desired placement."""
        nodes_list = self.repo.list_nodes()
        nodes = {n.node_id: n for n in nodes_list}
        out = []
        for chunk, rf, replicas in self.repo.committed_chunks_with_replicas():
            desired = self.placement.desired_nodes(chunk.chunk_id, rf, nodes_list)
            health = assess_chunk(chunk.chunk_id, rf, replicas, nodes)
            current = set(health.live)
            if set(desired) != current:
                out.append({"chunk_id": chunk.chunk_id, "desired": desired, "current": sorted(current),
                            "to_add": [n for n in desired if n not in current],
                            "to_remove": sorted(current - set(desired))})
        return out

    def rebalance(self) -> dict[str, Any]:
        with self.recovery.run_lock:
            started = time.time()
            op = self.repo.start_operation("REBALANCE")
            report: dict[str, Any] = {"chunks_examined": 0, "already_balanced": 0, "replicas_added": 0,
                                      "replicas_removed": 0, "failed": [], "pending": []}
            for chunk, rf, _ in self.repo.committed_chunks_with_replicas():
                report["chunks_examined"] += 1
                with self.locks.hold(chunk.chunk_id):
                    self._rebalance_chunk(chunk, rf, report)
            self.recovery.deletion.process_pending(ignore_backoff=True)
            report["duration_seconds"] = round(time.time() - started, 4)
            report["complete"] = not report["failed"] and not report["pending"]
            state = "SUCCEEDED" if report["complete"] else "PARTIAL"
            self.repo.finish_operation(op, state, {k: (v[:50] if isinstance(v, list) else v)
                                                   for k, v in report.items()})
            log.info("rebalance: +%d -%d replicas, %d failed, %d pending", report["replicas_added"],
                     report["replicas_removed"], len(report["failed"]), len(report["pending"]))
            return report

    def _rebalance_chunk(self, chunk: Any, rf: int, report: dict[str, Any]) -> None:
        nodes_list = self.repo.list_nodes()
        nodes = {n.node_id: n for n in nodes_list}
        desired = self.placement.desired_nodes(chunk.chunk_id, rf, nodes_list)
        health = assess_chunk(chunk.chunk_id, rf, self.repo.replicas_for_chunk(chunk.chunk_id), nodes)
        current = set(health.live)
        if set(desired) == current:
            report["already_balanced"] += 1
            return
        sources = sorted(current, key=lambda n: nodes[n].health != NodeHealth.HEALTHY.value)
        for dest_id in desired:
            if dest_id in current:
                continue
            if nodes[dest_id].health != NodeHealth.HEALTHY.value:
                report["pending"].append({"chunk_id": chunk.chunk_id, "reason": f"{dest_id} not healthy"})
                continue
            for src_id in sources:
                try:
                    self.replication.copy_chunk(chunk, nodes[src_id], nodes[dest_id])
                except CopyError as exc:
                    report["failed"].append({"chunk_id": chunk.chunk_id, "from": src_id, "to": dest_id,
                                             "error": exc.message[:200]})
                    continue
                if self.repo.record_verified_replica(chunk.chunk_id, dest_id):
                    report["replicas_added"] += 1
                break
        health = assess_chunk(chunk.chunk_id, rf, self.repo.replicas_for_chunk(chunk.chunk_id),
                              self.repo.nodes_by_id())
        current = set(health.live)
        reachable_desired = [d for d in desired if nodes[d].health != NodeHealth.DOWN.value]
        if not all(d in current for d in reachable_desired):
            return  # keep extra copies until every desired location is verified
        for extra in sorted(current - set(desired)):
            if len(current) - 1 < rf:
                break  # never drop below R valid copies
            try:
                self.nodes.client(nodes[extra]).delete_chunk(chunk.chunk_id)
                self.repo.delete_replica(chunk.chunk_id, extra)
            except NodeError:
                self.repo.remove_replica_and_schedule_delete(chunk.chunk_id, extra, "rebalance")
            current.discard(extra)
            report["replicas_removed"] += 1
