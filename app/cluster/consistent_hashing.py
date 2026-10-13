"""Consistent hash ring with virtual nodes.

* Hash function: first 8 bytes of SHA-256, as an unsigned 64-bit integer.
  Deterministic across processes and platforms (unlike Python's ``hash``).
* Each physical node owns ``vnodes`` points: ``H(f"{node_id}#{i}")``.
* A key maps to the first point clockwise from ``H(key)``. Its replica
  *preference list* continues clockwise, skipping points belonging to physical
  nodes already chosen, so replicas always land on distinct physical nodes.
* Adding/removing a node only changes ownership of the arcs adjacent to its
  points, so roughly ``1/N`` of keys move instead of nearly all of them (as
  with ``hash(key) % N``).
"""

from __future__ import annotations

import bisect
import hashlib
from dataclasses import dataclass
from typing import Iterable, Sequence


def ring_hash(key: str) -> int:
    return int.from_bytes(hashlib.sha256(key.encode("utf-8")).digest()[:8], "big")


class ConsistentHashRing:
    def __init__(self, nodes: Iterable[str] = (), vnodes: int = 64) -> None:
        if vnodes < 1:
            raise ValueError("vnodes must be >= 1")
        self.vnodes = vnodes
        self._nodes: set[str] = set()
        self._points: list[tuple[int, str]] = []
        self._hashes: list[int] = []
        for node in nodes:
            self.add_node(node)

    @property
    def nodes(self) -> frozenset[str]:
        return frozenset(self._nodes)

    def __len__(self) -> int:
        return len(self._nodes)

    def __contains__(self, node_id: object) -> bool:
        return node_id in self._nodes

    def _rebuild_index(self) -> None:
        self._points.sort()
        self._hashes = [h for h, _ in self._points]

    def add_node(self, node_id: str) -> None:
        if node_id in self._nodes:
            return
        self._nodes.add(node_id)
        self._points.extend((ring_hash(f"{node_id}#{i}"), node_id) for i in range(self.vnodes))
        self._rebuild_index()

    def remove_node(self, node_id: str) -> None:
        if node_id not in self._nodes:
            return
        self._nodes.discard(node_id)
        self._points = [p for p in self._points if p[1] != node_id]
        self._rebuild_index()

    def preference_list(self, key: str, n: int | None = None, exclude: Iterable[str] = ()) -> list[str]:
        """Distinct physical nodes in clockwise order from ``key``'s position."""
        if not self._points:
            return []
        excluded = set(exclude)
        limit = len(self._nodes - excluded) if n is None else min(n, len(self._nodes - excluded))
        result: list[str] = []
        seen: set[str] = set()
        start = bisect.bisect_right(self._hashes, ring_hash(key))
        total = len(self._points)
        for offset in range(total):
            node = self._points[(start + offset) % total][1]
            if node in seen or node in excluded:
                continue
            seen.add(node)
            result.append(node)
            if len(result) >= limit:
                break
        return result

    def get_nodes(self, key: str, n: int) -> list[str]:
        return self.preference_list(key, n)

    def primary(self, key: str) -> str | None:
        nodes = self.preference_list(key, 1)
        return nodes[0] if nodes else None

    def ownership(self) -> dict[str, float]:
        """Fraction of the 64-bit hash space owned (as primary) by each node."""
        if not self._points:
            return {}
        space = 2**64
        share: dict[str, int] = {n: 0 for n in self._nodes}
        for i, (h, node) in enumerate(self._points):
            prev = self._points[i - 1][0] if i else self._points[-1][0] - space
            share[node] += h - prev
        return {n: s / space for n, s in share.items()}


@dataclass(frozen=True)
class PlacementChange:
    key: str
    before: tuple[str, ...]
    after: tuple[str, ...]

    @property
    def added(self) -> set[str]:
        return set(self.after) - set(self.before)

    @property
    def removed(self) -> set[str]:
        return set(self.before) - set(self.after)


def placement_changes(
    before: ConsistentHashRing, after: ConsistentHashRing, keys: Sequence[str], replicas: int
) -> list[PlacementChange]:
    """Keys whose replica *set* differs between two ring memberships."""
    changes = []
    for key in keys:
        b = tuple(before.get_nodes(key, replicas))
        a = tuple(after.get_nodes(key, replicas))
        if set(a) != set(b):
            changes.append(PlacementChange(key, b, a))
    return changes
