"""Replica placement on top of the consistent hash ring.

Ring membership = nodes whose admin_state is ACTIVE (health does not change
membership; that would reshuffle placement on every blip). Health is applied
when *choosing* targets: writes go to the first ``R`` HEALTHY members of a
chunk's preference list, falling back further along the list on failure.
"""

from __future__ import annotations

import threading

from app.cluster.consistent_hashing import ConsistentHashRing
from app.metadata.models import AdminState, NodeHealth, NodeRecord
from app.metadata.repository import MetadataRepository


class PlacementService:
    def __init__(self, repo: MetadataRepository, vnodes: int) -> None:
        self.repo = repo
        self.vnodes = vnodes
        self._lock = threading.Lock()
        self._cached_members: frozenset[str] | None = None
        self._ring = ConsistentHashRing(vnodes=vnodes)

    def ring(self, nodes: list[NodeRecord] | None = None) -> ConsistentHashRing:
        nodes = nodes if nodes is not None else self.repo.list_nodes()
        members = frozenset(n.node_id for n in nodes if n.admin_state == AdminState.ACTIVE.value)
        with self._lock:
            if members != self._cached_members:
                self._ring = ConsistentHashRing(sorted(members), vnodes=self.vnodes)
                self._cached_members = members
            return self._ring

    def preference_list(self, chunk_id: str, nodes: list[NodeRecord] | None = None) -> list[str]:
        return self.ring(nodes).preference_list(chunk_id)

    def write_candidates(self, chunk_id: str, nodes: list[NodeRecord]) -> list[str]:
        """Ring-ordered nodes that may receive new data right now."""
        by_id = {n.node_id: n for n in nodes}
        return [
            nid for nid in self.preference_list(chunk_id, nodes)
            if by_id[nid].health == NodeHealth.HEALTHY.value and by_id[nid].admin_state == AdminState.ACTIVE.value
        ]

    def desired_nodes(self, chunk_id: str, replicas: int, nodes: list[NodeRecord] | None = None) -> list[str]:
        return self.ring(nodes).get_nodes(chunk_id, replicas)
