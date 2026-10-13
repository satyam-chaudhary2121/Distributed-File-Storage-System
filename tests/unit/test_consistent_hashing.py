from collections import Counter

import pytest

from app.cluster.consistent_hashing import ConsistentHashRing, placement_changes, ring_hash

KEYS = [f"chunk-{i}" for i in range(10000)]


def test_hash_is_deterministic():
    assert ring_hash("abc") == ring_hash("abc")
    assert ring_hash("abc") != ring_hash("abd")
    assert 0 <= ring_hash("x") < 2**64


def test_identical_inputs_give_identical_placement():
    a = ConsistentHashRing(["n1", "n2", "n3", "n4"], vnodes=64)
    b = ConsistentHashRing(["n4", "n3", "n2", "n1"], vnodes=64)  # insertion order irrelevant
    for key in KEYS[:500]:
        assert a.get_nodes(key, 3) == b.get_nodes(key, 3)


def test_keys_are_distributed_across_nodes():
    ring = ConsistentHashRing([f"n{i}" for i in range(4)], vnodes=128)
    counts = Counter(ring.primary(k) for k in KEYS)
    assert set(counts) == {"n0", "n1", "n2", "n3"}
    for n in counts.values():
        assert 0.15 < n / len(KEYS) < 0.35
    assert abs(sum(ring.ownership().values()) - 1.0) < 1e-9


def test_virtual_nodes_improve_balance():
    def spread(vnodes: int) -> float:
        ring = ConsistentHashRing([f"n{i}" for i in range(5)], vnodes=vnodes)
        counts = Counter(ring.primary(k) for k in KEYS)
        return max(counts.values()) / min(counts.values())

    assert spread(256) < spread(1)


def test_replicas_on_distinct_physical_nodes():
    ring = ConsistentHashRing(["a", "b", "c", "d", "e"], vnodes=64)
    for key in KEYS[:2000]:
        nodes = ring.get_nodes(key, 3)
        assert len(nodes) == 3 and len(set(nodes)) == 3


def test_replica_count_capped_by_physical_nodes():
    ring = ConsistentHashRing(["a", "b"], vnodes=32)
    assert sorted(ring.get_nodes("k", 3)) == ["a", "b"]  # never pretends to have 3
    assert ConsistentHashRing(vnodes=8).get_nodes("k", 3) == []


def test_preference_list_excludes_and_extends():
    ring = ConsistentHashRing(["a", "b", "c", "d"], vnodes=16)
    full = ring.preference_list("key")
    assert sorted(full) == ["a", "b", "c", "d"]
    assert ring.preference_list("key", exclude=[full[0]]) == full[1:]
    assert ring.get_nodes("key", 2) == full[:2]


def test_adding_a_node_moves_only_a_fraction_of_keys():
    before = ConsistentHashRing(["n1", "n2", "n3", "n4"], vnodes=128)
    after = ConsistentHashRing(["n1", "n2", "n3", "n4", "n5"], vnodes=128)
    moved = [k for k in KEYS if before.primary(k) != after.primary(k)]
    # Ideal is 1/5 = 20%; modulo hashing would move ~80%.
    assert 0.10 < len(moved) / len(KEYS) < 0.30
    # Every key that moved now belongs to the new node.
    assert all(after.primary(k) == "n5" for k in moved)

    modulo_moved = sum(1 for k in KEYS if ring_hash(k) % 4 != ring_hash(k) % 5)
    assert modulo_moved > 3 * len(moved)


def test_removing_a_node_only_affects_its_keys():
    before = ConsistentHashRing(["n1", "n2", "n3", "n4"], vnodes=64)
    after = ConsistentHashRing(["n1", "n2", "n3"], vnodes=64)
    changes = placement_changes(before, after, KEYS[:3000], 3)
    assert changes, "removing a node must require placement work"
    for change in changes:
        assert "n4" in change.before
        assert change.removed == {"n4"}
        assert len(change.added) == 1  # exactly one replacement replica needed
    unaffected = [k for k in KEYS[:3000] if "n4" not in before.get_nodes(k, 3)]
    assert all(set(before.get_nodes(k, 3)) == set(after.get_nodes(k, 3)) for k in unaffected)


def test_add_remove_round_trip():
    ring = ConsistentHashRing(["a", "b", "c"], vnodes=32)
    snapshot = [ring.get_nodes(k, 2) for k in KEYS[:300]]
    ring.add_node("d")
    ring.remove_node("d")
    assert [ring.get_nodes(k, 2) for k in KEYS[:300]] == snapshot
    ring.add_node("a")  # idempotent
    assert len(ring) == 3 and "a" in ring


def test_invalid_vnodes():
    with pytest.raises(ValueError):
        ConsistentHashRing(vnodes=0)
