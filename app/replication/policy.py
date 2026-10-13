"""Pure replica-health and repair-planning logic (no I/O; unit-tested directly)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Mapping, Sequence

from app.metadata.models import AdminState, ChunkStatus, NodeHealth, NodeRecord, ReplicaRecord, ReplicaStatus


@dataclass
class ChunkHealth:
    chunk_id: str
    desired: int
    counted: list[str] = field(default_factory=list)  # valid, reachable, on ACTIVE nodes
    leaving: list[str] = field(default_factory=list)  # valid, reachable, on DRAINING/DRAINED nodes
    unavailable: list[str] = field(default_factory=list)  # marked HEALTHY but node is DOWN
    corrupted: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)
    pending: list[str] = field(default_factory=list)

    @property
    def live(self) -> list[str]:
        return self.counted + self.leaving

    @property
    def status(self) -> ChunkStatus:
        if self.live:
            return ChunkStatus.OK if len(self.counted) >= self.desired else ChunkStatus.UNDER_REPLICATED
        if self.unavailable:
            return ChunkStatus.UNAVAILABLE
        return ChunkStatus.LOST

    @property
    def missing_replicas(self) -> int:
        return max(0, self.desired - len(self.counted))


def assess_chunk(chunk_id: str, desired: int, replicas: Iterable[ReplicaRecord],
                 nodes: Mapping[str, NodeRecord]) -> ChunkHealth:
    """Classify each replica of a chunk using metadata + current node health."""
    h = ChunkHealth(chunk_id, desired)
    for r in replicas:
        node = nodes.get(r.node_id)
        if node is None or node.admin_state == AdminState.REMOVED.value:
            continue
        if r.status == ReplicaStatus.CORRUPTED.value:
            h.corrupted.append(r.node_id)
        elif r.status == ReplicaStatus.MISSING.value:
            h.missing.append(r.node_id)
        elif r.status == ReplicaStatus.PENDING.value:
            h.pending.append(r.node_id)
        elif node.health == NodeHealth.DOWN.value:
            h.unavailable.append(r.node_id)
        elif node.admin_state == AdminState.ACTIVE.value:
            h.counted.append(r.node_id)
        else:
            h.leaving.append(r.node_id)
    return h


@dataclass
class RepairPlan:
    sources: list[str]
    destinations: list[str]  # ordered candidates; first ``needed`` are preferred
    needed: int
    max_achievable: int

    @property
    def blocked(self) -> bool:
        return self.needed > 0 and not self.destinations


def plan_repair(health: ChunkHealth, preference: Sequence[str], nodes: Mapping[str, NodeRecord]) -> RepairPlan:
    """Choose sources and destinations to restore ``desired`` counted replicas.

    * Sources: reachable valid copies; HEALTHY nodes before SUSPECT ones,
      ACTIVE before leaving nodes.
    * Destinations: ring-preference order, ACTIVE + HEALTHY, not already
      holding a valid copy (a node holding a CORRUPTED/MISSING copy is
      eligible: the repair overwrites it). Never a node holding an
      UNAVAILABLE copy (it may come back; avoids duplicate logical replicas).
    """

    def source_rank(nid: str) -> tuple[int, int]:
        node = nodes[nid]
        return (0 if node.health == NodeHealth.HEALTHY.value else 1,
                0 if node.admin_state == AdminState.ACTIVE.value else 1)

    sources = sorted(health.live, key=source_rank)
    holding = set(health.counted) | set(health.leaving) | set(health.unavailable)
    eligible = [
        nid for nid in preference
        if nid in nodes and nid not in holding
        and nodes[nid].admin_state == AdminState.ACTIVE.value
        and nodes[nid].health == NodeHealth.HEALTHY.value
    ]
    needed = health.missing_replicas
    return RepairPlan(sources, eligible, needed, len(health.counted) + len(eligible))


def drain_target(desired: int, active_healthy_nodes: int) -> int:
    """Copies required on ACTIVE nodes before a draining replica may be dropped."""
    return max(1, min(desired, active_healthy_nodes))
