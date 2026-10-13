"""Background recovery: cleanup, reconciliation, scrubbing, repair and draining.

One *cycle* (``run_cycle``) performs, in order:

1. **Pending deletions** - retry physical deletes that previously failed.
2. **Reconciliation** - for every reachable node flagged ``needs_reconcile``
   (newly registered, returned from DOWN, coordinator restart), compare the
   node's LIST_CHUNKS inventory with metadata (see ``reconcile_node``).
3. **Scrub** (optional) - VERIFY_CHUNK every replica on reachable nodes to find
   silent corruption or missing files.
4. **Repair** - for every chunk of every COMMITTED file, compute its health
   (``policy.assess_chunk``); copy real bytes from a verified source to
   eligible nodes until ``replication_factor`` copies exist on ACTIVE nodes,
   or report why that is impossible (BLOCKED / UNAVAILABLE / UNRECOVERABLE).
   Failed repairs are retried with bounded exponential backoff.
5. **Drain** - drop replicas from DRAINING nodes once enough copies exist
   elsewhere; a node with nothing left becomes DRAINED (removable).

Cycles are serialised by a lock (the background loop, ``POST
/recovery/trigger``, drain and rebalance never interleave) and per-chunk locks
guard individual copies. Every step is idempotent.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

from app.cluster.placement import PlacementService
from app.core.config import Settings
from app.core.retry import backoff_delay
from app.metadata.models import (
    AdminState,
    ChunkRecord,
    ChunkStatus,
    FileState,
    NodeHealth,
    NodeRecord,
    RepairState,
    ReplicaStatus,
)
from app.metadata.repository import MetadataRepository
from app.replication.manager import ChunkLocks, CopyError, ReplicationManager
from app.replication.policy import assess_chunk, drain_target, plan_repair
from app.services.deletion_service import DeletionService
from app.storage.node_manager import NodeManager
from app.storage.socket_client import NodeError, NodeUnavailable

log = logging.getLogger("dfs.recovery")


class RecoveryManager:
    def __init__(self, settings: Settings, repo: MetadataRepository, nodes: NodeManager,
                 placement: PlacementService, replication: ReplicationManager, deletion: DeletionService,
                 locks: ChunkLocks) -> None:
        self.settings = settings
        self.repo = repo
        self.nodes = nodes
        self.placement = placement
        self.replication = replication
        self.deletion = deletion
        self.locks = locks
        self.run_lock = threading.RLock()
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None
        self.last_cycle: dict[str, Any] | None = None
        self.cycles = 0

    # ------------------------------------------------------------ lifecycle
    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="recovery-worker", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(timeout=30)

    def wake(self, *_: Any) -> None:
        self._wake.set()

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def _loop(self) -> None:
        while not self._stop.is_set():
            self._wake.wait(self.settings.recovery_interval)
            self._wake.clear()
            if self._stop.is_set():
                return
            try:
                self.run_cycle(trigger="background")
            except Exception:  # noqa: BLE001 - the worker must survive bad cycles
                log.exception("recovery cycle failed")

    # ---------------------------------------------------------------- cycle
    def run_cycle(self, *, scrub: bool = False, force: bool = False, trigger: str = "manual") -> dict[str, Any]:
        with self.run_lock:
            started = time.time()
            report: dict[str, Any] = {"trigger": trigger, "started_at": started}
            report["deletions"] = self.deletion.process_pending(ignore_backoff=force)
            report["reconciled"] = self._reconcile_flagged_nodes()
            report["scrub"] = self.scrub() if scrub else None
            report["repair"] = self.repair_all(force=force)
            report["drain"] = self.progress_drains()
            report["duration_seconds"] = round(time.time() - started, 4)
            self.last_cycle = report
            self.cycles += 1
            activity = (report["repair"]["replicas_created"] or report["repair"]["failed"]
                        or report["drain"]["replicas_dropped"] or report["reconciled"]
                        or report["deletions"]["deleted"] or trigger != "background")
            if activity:
                state = "SUCCEEDED" if not report["repair"]["failed"] else "PARTIAL"
                op = self.repo.start_operation("RECOVERY", trigger)
                self.repo.finish_operation(op, state, _compact(report))
            return report

    # -------------------------------------------------------- reconciliation
    def _reconcile_flagged_nodes(self) -> list[dict[str, Any]]:
        results = []
        for node in self.repo.list_nodes():
            if node.needs_reconcile and node.health != NodeHealth.DOWN.value:
                result = self.reconcile_node(node)
                if result is not None:
                    results.append(result)
        return results

    def reconcile_node(self, node: NodeRecord) -> dict[str, Any] | None:
        """Align a node's physical inventory with authoritative metadata.

        * Replica rows whose bytes are gone -> MISSING; present -> re-verified
          (HEALTHY or CORRUPTED).
        * Chunks on disk with no replica row:
            - chunk unknown to metadata (failed/deleted upload) -> deleted once
              older than ``orphan_grace_seconds``;
            - chunk known, bytes valid, chunk under-replicated -> adopted as a
              replica (it may be the only surviving copy: never deleted);
            - chunk known, bytes valid, already fully replicated -> surplus
              copy deleted (others already hold >= R valid copies);
            - chunk known, bytes invalid -> deleted only if a valid copy
              exists elsewhere.
        """
        client = self.nodes.client(node)
        try:
            inventory = {c["chunk_id"]: c for c in client.list_chunks()}
        except NodeError as exc:
            log.info("cannot reconcile %s yet: %s", node.node_id, exc)
            return None
        stats = {"node_id": node.node_id, "on_disk": len(inventory), "verified": 0, "marked_missing": 0,
                 "marked_corrupted": 0, "adopted": 0, "surplus_deleted": 0, "orphans_deleted": 0,
                 "kept_for_safety": 0}
        nodes = self.repo.nodes_by_id()
        try:
            known = {r.chunk_id: r for r in self.repo.replicas_on_node(node.node_id)}
            for chunk_id, replica in known.items():
                chunk = self.repo.get_chunk(chunk_id)
                if chunk is None:
                    continue
                with self.locks.hold(chunk_id):
                    if chunk_id not in inventory:
                        if replica.status != ReplicaStatus.PENDING.value:
                            self.repo.set_replica_status(chunk_id, node.node_id, ReplicaStatus.MISSING)
                            stats["marked_missing"] += 1
                        continue
                    if self._verify_on(node, chunk):
                        stats["verified"] += 1
                        if self._trim_surplus(node, chunk, client):
                            stats["surplus_deleted"] += 1
                    else:
                        stats["marked_corrupted"] += 1
            now = time.time()
            for chunk_id, item in inventory.items():
                if chunk_id in known or self.repo.pending_deletion_exists(chunk_id, node.node_id):
                    continue
                found = self.repo.get_chunk_file_state(chunk_id)
                if found is None:
                    if now - float(item.get("mtime", now)) >= self.settings.orphan_grace_seconds:
                        client.delete_chunk(chunk_id)
                        stats["orphans_deleted"] += 1
                    continue
                chunk, file_state = found
                if file_state != FileState.COMMITTED.value:
                    continue  # upload in flight; the uploader owns this chunk
                with self.locks.hold(chunk_id):
                    self._reconcile_unknown_copy(node, chunk, client, nodes, stats)
        except NodeUnavailable as exc:
            log.warning("reconciliation of %s interrupted: %s", node.node_id, exc)
            return None
        self.repo.mark_reconciled(node.node_id)
        if any(v for k, v in stats.items() if k not in ("node_id", "on_disk", "verified")):
            log.info("reconciled %s: %s", node.node_id, stats)
        return stats

    def _trim_surplus(self, node: NodeRecord, chunk: ChunkRecord, client: Any) -> bool:
        """Drop this node's copy if the chunk already has more than R valid copies elsewhere.

        Happens when a node returns after its replicas were re-created on other
        nodes: keeping it would leave the chunk over-replicated.
        """
        found = self.repo.get_chunk_file_state(chunk.chunk_id)
        if found is None or found[1] != FileState.COMMITTED.value:
            return False
        rf = self.repo.get_file(chunk.file_id).replication_factor  # type: ignore[union-attr]
        health = assess_chunk(chunk.chunk_id, rf, self.repo.replicas_for_chunk(chunk.chunk_id),
                              self.repo.nodes_by_id())
        others = [n for n in health.counted if n != node.node_id]
        if len(others) < rf:
            return False
        client.delete_chunk(chunk.chunk_id)
        self.repo.delete_replica(chunk.chunk_id, node.node_id)
        return True

    def _reconcile_unknown_copy(self, node: NodeRecord, chunk: ChunkRecord, client: Any,
                                nodes: dict[str, NodeRecord], stats: dict[str, Any]) -> None:
        result = client.verify_chunk(chunk.chunk_id, chunk.checksum)
        rf = self.repo.get_file(chunk.file_id).replication_factor  # type: ignore[union-attr]
        health = assess_chunk(chunk.chunk_id, rf, self.repo.replicas_for_chunk(chunk.chunk_id), nodes)
        if result.get("valid"):
            if len(health.counted) < rf or not health.live:
                self.repo.record_verified_replica(chunk.chunk_id, node.node_id)
                stats["adopted"] += 1
            else:
                client.delete_chunk(chunk.chunk_id)
                stats["surplus_deleted"] += 1
        elif health.live:
            client.delete_chunk(chunk.chunk_id)
            stats["surplus_deleted"] += 1
        else:
            stats["kept_for_safety"] += 1

    def _verify_on(self, node: NodeRecord, chunk: ChunkRecord) -> bool:
        if not self.settings.reconcile_verify_data:
            self.repo.set_replica_status(chunk.chunk_id, node.node_id, ReplicaStatus.HEALTHY, time.time())
            return True
        result = self.nodes.client(node).verify_chunk(chunk.chunk_id, chunk.checksum)
        if not result.get("exists"):
            self.repo.set_replica_status(chunk.chunk_id, node.node_id, ReplicaStatus.MISSING)
            return False
        if result.get("valid"):
            self.repo.set_replica_status(chunk.chunk_id, node.node_id, ReplicaStatus.HEALTHY, time.time())
            return True
        log.warning("replica of %s on %s is corrupted", chunk.chunk_id[:12], node.node_id)
        self.repo.set_replica_status(chunk.chunk_id, node.node_id, ReplicaStatus.CORRUPTED)
        return False

    # ----------------------------------------------------------------- scrub
    def scrub(self) -> dict[str, int]:
        """Verify every replica on every reachable node against its expected checksum."""
        stats = {"checked": 0, "corrupted": 0, "missing": 0, "unreachable": 0}
        nodes = self.repo.nodes_by_id()
        for chunk, _rf, replicas in self.repo.committed_chunks_with_replicas():
            for r in replicas:
                node = nodes.get(r.node_id)
                if node is None or not node.is_reachable or r.status == ReplicaStatus.PENDING.value:
                    continue
                with self.locks.hold(chunk.chunk_id):
                    try:
                        ok = self._verify_on(node, chunk)
                    except NodeError:
                        stats["unreachable"] += 1
                        continue
                stats["checked"] += 1
                if not ok:
                    current = {x.node_id: x.status for x in self.repo.replicas_for_chunk(chunk.chunk_id)}
                    if current.get(node.node_id) == ReplicaStatus.MISSING.value:
                        stats["missing"] += 1
                    else:
                        stats["corrupted"] += 1
        return stats

    # ---------------------------------------------------------------- repair
    def repair_all(self, *, force: bool = False) -> dict[str, Any]:
        stats: dict[str, Any] = {"chunks_examined": 0, "healthy": 0, "under_replicated": 0, "unavailable": 0,
                                 "lost": 0, "replicas_created": 0, "failed": 0, "blocked": 0, "deferred": 0,
                                 "fully_restored": 0}
        now = time.time()
        for chunk, rf, _ in self.repo.committed_chunks_with_replicas():
            stats["chunks_examined"] += 1
            nodes_list = self.repo.list_nodes(include_removed=True)
            nodes = {n.node_id: n for n in nodes_list}
            health = assess_chunk(chunk.chunk_id, rf, self.repo.replicas_for_chunk(chunk.chunk_id), nodes)
            status = health.status
            task = self.repo.get_repair_task(chunk.chunk_id)
            if status == ChunkStatus.OK:
                stats["healthy"] += 1
                if task is not None:
                    self.repo.clear_repair_task(chunk.chunk_id)
                continue
            if status == ChunkStatus.UNAVAILABLE:
                stats["unavailable"] += 1
                self.repo.upsert_repair_task(chunk.chunk_id, RepairState.UNAVAILABLE.value, 0, 0,
                                             f"all valid copies on DOWN node(s): {health.unavailable}")
                continue
            if status == ChunkStatus.LOST:
                stats["lost"] += 1
                if task is None or task.state != RepairState.UNRECOVERABLE.value:
                    log.error("chunk %s (file %s, index %d) is UNRECOVERABLE: corrupted=%s missing=%s",
                              chunk.chunk_id[:12], chunk.file_id, chunk.chunk_index,
                              health.corrupted, health.missing)
                self.repo.upsert_repair_task(chunk.chunk_id, RepairState.UNRECOVERABLE.value, 0, 0,
                                             "no valid replica exists on any node")
                continue
            stats["under_replicated"] += 1
            if task is not None and not force:
                if task.state == RepairState.FAILED.value or task.next_attempt_at > now:
                    stats["deferred"] += 1
                    continue
            outcome = self.repair_chunk(chunk, rf, force=force)
            stats["replicas_created"] += outcome["created"]
            if outcome["restored"]:
                stats["fully_restored"] += 1
            if outcome["blocked"]:
                stats["blocked"] += 1
            if outcome["errors"]:
                stats["failed"] += 1
        return stats

    def repair_chunk(self, chunk: ChunkRecord, rf: int, *, force: bool = False) -> dict[str, Any]:
        outcome: dict[str, Any] = {"created": 0, "restored": False, "blocked": False, "errors": []}
        with self.locks.hold(chunk.chunk_id):
            nodes_list = self.repo.list_nodes(include_removed=True)
            nodes = {n.node_id: n for n in nodes_list}
            health = assess_chunk(chunk.chunk_id, rf, self.repo.replicas_for_chunk(chunk.chunk_id), nodes)
            plan = plan_repair(health, self.placement.preference_list(chunk.chunk_id, nodes_list), nodes)
            needed = plan.needed
            sources = list(plan.sources)
            for dest_id in plan.destinations:
                if needed <= 0:
                    break
                if not sources:
                    outcome["errors"].append("no usable source replica left")
                    break
                dest = nodes[dest_id]
                copied = False
                for src_id in list(sources):
                    try:
                        mode = self.replication.copy_chunk(chunk, nodes[src_id], dest)
                    except CopyError as exc:
                        if exc.kind in ("SOURCE_CORRUPTED", "SOURCE_MISSING"):
                            status = (ReplicaStatus.CORRUPTED if exc.kind == "SOURCE_CORRUPTED"
                                      else ReplicaStatus.MISSING)
                            self.repo.set_replica_status(chunk.chunk_id, src_id, status)
                            log.warning("source %s for chunk %s is %s", src_id, chunk.chunk_id[:12], status.value)
                            sources.remove(src_id)
                            continue
                        if exc.kind == "SOURCE_UNAVAILABLE":
                            outcome["errors"].append(f"source {src_id}: {exc.message}")
                            sources.remove(src_id)
                            continue
                        outcome["errors"].append(f"destination {dest_id}: {exc.message}")
                        break  # destination problem: try the next destination
                    if self.repo.record_verified_replica(chunk.chunk_id, dest_id):
                        log.info("re-replicated chunk %s %s -> %s (%s)", chunk.chunk_id[:12], src_id, dest_id, mode)
                        outcome["created"] += 1
                        needed -= 1
                    copied = True
                    break
                if not copied and not sources:
                    break
            final = assess_chunk(chunk.chunk_id, rf, self.repo.replicas_for_chunk(chunk.chunk_id),
                                 self.repo.nodes_by_id())
            if final.status == ChunkStatus.OK:
                outcome["restored"] = True
                self.repo.clear_repair_task(chunk.chunk_id)
                return outcome
            task = self.repo.get_repair_task(chunk.chunk_id)
            if not outcome["errors"] and needed > 0:
                # Nothing failed; there simply are not enough eligible nodes.
                outcome["blocked"] = True
                self.repo.upsert_repair_task(
                    chunk.chunk_id, RepairState.BLOCKED.value, 0, 0,
                    f"{len(final.counted)}/{rf} copies; no further eligible node "
                    f"(max achievable now: {len(final.counted)})",
                )
                return outcome
            # Attempts accumulate across RETRYING cycles; a forced retry of a FAILED task starts afresh.
            attempts = (task.attempts if task and task.state == RepairState.RETRYING.value else 0) + 1
            state = RepairState.FAILED if attempts >= self.settings.recovery_max_attempts else RepairState.RETRYING
            delay = backoff_delay(attempts, self.settings.recovery_backoff_base, self.settings.recovery_backoff_max)
            self.repo.upsert_repair_task(chunk.chunk_id, state.value, attempts, time.time() + delay,
                                         "; ".join(outcome["errors"]) or "under-replicated")
        return outcome

    # ----------------------------------------------------------------- drain
    def progress_drains(self) -> dict[str, Any]:
        stats: dict[str, Any] = {"nodes": [], "replicas_dropped": 0, "waiting": 0}
        for node in self.repo.list_nodes():
            if node.admin_state != AdminState.DRAINING.value:
                continue
            dropped, waiting = self._drain_node(node)
            stats["replicas_dropped"] += dropped
            stats["waiting"] += waiting
            remaining = self.repo.count_replicas_on_node(node.node_id)
            if remaining == 0:
                self.repo.set_admin_state(node.node_id, AdminState.DRAINED)
                log.info("node %s is DRAINED and can be removed", node.node_id)
            stats["nodes"].append({"node_id": node.node_id, "remaining_replicas": remaining,
                                   "dropped": dropped, "drained": remaining == 0})
        return stats

    def _drain_node(self, node: NodeRecord) -> tuple[int, int]:
        dropped = waiting = 0
        for replica in self.repo.replicas_on_node(node.node_id):
            found = self.repo.get_chunk_file_state(replica.chunk_id)
            if found is None:
                continue
            chunk, file_state = found
            if file_state != FileState.COMMITTED.value:
                waiting += 1
                continue
            with self.locks.hold(chunk.chunk_id):
                nodes = self.repo.nodes_by_id()
                rf = self.repo.get_file(chunk.file_id).replication_factor  # type: ignore[union-attr]
                if replica.status != ReplicaStatus.HEALTHY.value:
                    self.repo.remove_replica_and_schedule_delete(chunk.chunk_id, node.node_id, "drained")
                    dropped += 1
                    continue
                health = assess_chunk(chunk.chunk_id, rf, self.repo.replicas_for_chunk(chunk.chunk_id), nodes)
                active_healthy = sum(1 for n in nodes.values() if n.is_writable)
                if len(health.counted) < drain_target(rf, active_healthy):
                    # Not yet safe: copy at least one more replica off this node first.
                    plan = plan_repair(health, self.placement.preference_list(chunk.chunk_id), nodes)
                    if plan.destinations and node.is_reachable:
                        try:
                            self.replication.copy_chunk(chunk, node, nodes[plan.destinations[0]])
                            self.repo.record_verified_replica(chunk.chunk_id, plan.destinations[0])
                        except CopyError as exc:
                            log.warning("drain copy of %s failed: %s", chunk.chunk_id[:12], exc)
                    health = assess_chunk(chunk.chunk_id, rf, self.repo.replicas_for_chunk(chunk.chunk_id), nodes)
                    if len(health.counted) < drain_target(rf, active_healthy):
                        waiting += 1
                        continue
                self.repo.remove_replica_and_schedule_delete(chunk.chunk_id, node.node_id, "drained")
                dropped += 1
        if dropped:
            self.deletion.process_pending(ignore_backoff=True)
        return dropped, waiting

    # ---------------------------------------------------------------- status
    def replication_status(self, limit: int = 100) -> dict[str, Any]:
        nodes = self.repo.nodes_by_id()
        counts = {s.value: 0 for s in ChunkStatus}
        problems = []
        for chunk, rf, replicas in self.repo.committed_chunks_with_replicas():
            health = assess_chunk(chunk.chunk_id, rf, replicas, nodes)
            counts[health.status.value] += 1
            if health.status != ChunkStatus.OK and len(problems) < limit:
                problems.append({
                    "chunk_id": chunk.chunk_id, "file_id": chunk.file_id, "chunk_index": chunk.chunk_index,
                    "status": health.status.value, "desired": rf, "healthy_replicas": len(health.counted),
                    "healthy_on": health.counted, "leaving": health.leaving, "unavailable_on": health.unavailable,
                    "corrupted_on": health.corrupted, "missing_on": health.missing,
                })
        eligible = sum(1 for n in nodes.values() if n.is_writable)
        rf = self.settings.replication_factor
        return {
            "desired_replication_factor": rf,
            "eligible_nodes": eligible,
            "max_distinct_copies_possible": min(rf, eligible),
            "replication_factor_achievable": eligible >= rf,
            "chunk_counts": counts,
            "total_chunks": sum(counts.values()),
            "problem_chunks": problems,
        }

    def status(self) -> dict[str, Any]:
        return {
            "worker_running": self.running,
            "cycles_completed": self.cycles,
            "interval_seconds": self.settings.recovery_interval,
            "last_cycle": self.last_cycle,
            "repair_task_counts": self.repo.count_repair_tasks_by_state(),
            "repair_tasks": [t.__dict__ for t in self.repo.list_repair_tasks(100)],
            "pending_deletions": self.repo.count_pending_deletions(),
            "pending_deletion_samples": [p.__dict__ for p in self.repo.list_pending_deletions(20)],
            "recent_operations": self.repo.list_operations("RECOVERY", 5),
        }


def _compact(report: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in report.items() if k not in ("started_at",)}
