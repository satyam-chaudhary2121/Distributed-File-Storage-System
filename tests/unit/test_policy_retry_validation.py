import pytest

from app.core.config import load_settings
from app.core.retry import backoff_delay, retry_call
from app.core.validation import is_valid_node_id, sanitize_filename
from app.metadata.models import ChunkStatus, NodeHealth, NodeRecord, ReplicaRecord, classify_health
from app.replication.policy import assess_chunk, drain_target, plan_repair


def node(nid, health="HEALTHY", admin="ACTIVE"):
    return NodeRecord(nid, "127.0.0.1", 1, None, None, health, admin, 0, 0, 0, 0, 0, 0, 0, 0, None)


def rep(nid, status="HEALTHY"):
    return ReplicaRecord("c", nid, status, None, 0, 0)


# ------------------------------------------------------------ classification
def test_failure_classification():
    assert classify_health(0.5, 1, 3) == NodeHealth.HEALTHY
    assert classify_health(2.0, 1, 3) == NodeHealth.SUSPECT
    assert classify_health(3.5, 1, 3) == NodeHealth.DOWN


def test_assess_chunk_distinguishes_replica_states():
    nodes = {"a": node("a"), "b": node("b", "DOWN"), "c": node("c"), "d": node("d", admin="DRAINING"),
             "e": node("e"), "f": node("f", admin="REMOVED")}
    h = assess_chunk("c", 3, [rep("a"), rep("b"), rep("c", "CORRUPTED"), rep("d"), rep("e", "MISSING"),
                              rep("f")], nodes)
    assert h.counted == ["a"] and h.unavailable == ["b"] and h.corrupted == ["c"]
    assert h.leaving == ["d"] and h.missing == ["e"]
    assert h.status == ChunkStatus.UNDER_REPLICATED and h.missing_replicas == 2


def test_suspect_nodes_still_count():
    h = assess_chunk("c", 2, [rep("a"), rep("b")], {"a": node("a"), "b": node("b", "SUSPECT")})
    assert h.status == ChunkStatus.OK


def test_unavailable_vs_lost():
    nodes = {"a": node("a", "DOWN"), "b": node("b")}
    assert assess_chunk("c", 2, [rep("a"), rep("b", "CORRUPTED")], nodes).status == ChunkStatus.UNAVAILABLE
    assert assess_chunk("c", 2, [rep("b", "CORRUPTED")], nodes).status == ChunkStatus.LOST
    assert assess_chunk("c", 2, [], nodes).status == ChunkStatus.LOST


def test_plan_repair_chooses_valid_destinations():
    nodes = {n: node(n) for n in "abcde"}
    nodes["d"] = node("d", "DOWN")
    nodes["e"] = node("e", "SUSPECT")
    h = assess_chunk("c", 3, [rep("a"), rep("b", "CORRUPTED")], nodes)
    plan = plan_repair(h, ["e", "d", "c", "b", "a"], nodes)
    assert plan.sources == ["a"]
    assert plan.destinations == ["c", "b"]  # ring order; down/suspect excluded; corrupted holder eligible
    assert plan.needed == 2 and not plan.blocked


def test_plan_repair_never_targets_node_with_unavailable_copy_and_reports_limits():
    nodes = {"a": node("a"), "b": node("b", "DOWN"), "c": node("c")}
    h = assess_chunk("c", 3, [rep("a"), rep("b"), rep("c")], nodes)
    plan = plan_repair(h, ["a", "b", "c"], nodes)
    assert plan.needed == 1 and plan.destinations == [] and plan.blocked
    assert plan.max_achievable == 2  # only two distinct physical copies possible now


def test_drain_target():
    assert drain_target(3, 5) == 3
    assert drain_target(3, 2) == 2
    assert drain_target(3, 0) == 1


# --------------------------------------------------------------------- retry
def test_backoff_delay():
    assert [backoff_delay(i, 1, 10) for i in range(0, 6)] == [0, 1, 2, 4, 8, 10]


def test_retry_call_bounded():
    calls, sleeps = [], []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise OSError("transient")
        return "ok"

    assert retry_call(flaky, attempts=5, base_delay=0.1, max_delay=1, retry_on=(OSError,), sleep=sleeps.append) == "ok"
    assert len(calls) == 3 and sleeps == [0.1, 0.2]

    with pytest.raises(OSError):
        retry_call(lambda: (_ for _ in ()).throw(OSError("x")), attempts=2, base_delay=0, max_delay=0,
                   retry_on=(OSError,), sleep=lambda s: None)
    with pytest.raises(ValueError):  # non-retryable errors propagate immediately
        retry_call(lambda: (_ for _ in ()).throw(ValueError()), attempts=5, base_delay=0, max_delay=0,
                   retry_on=(OSError,))


# ---------------------------------------------------------------- validation
@pytest.mark.parametrize("raw,expected", [
    ("report.pdf", "report.pdf"),
    ("../../etc/passwd", "passwd"),
    ("C:\\Users\\me\\secret.txt", "secret.txt"),
    ("a\x00b\nc.txt", "abc.txt"),
    ("..", "unnamed"),
    ("", "unnamed"),
    (None, "unnamed"),
    ('evil"; rm -rf.sh', "evil__ rm -rf.sh"),
])
def test_sanitize_filename(raw, expected):
    assert sanitize_filename(raw) == expected


def test_sanitize_filename_length():
    assert len(sanitize_filename("a" * 400 + ".txt")) == 255
    assert sanitize_filename("a" * 400 + ".txt").endswith(".txt")


def test_node_id_validation():
    assert is_valid_node_id("node-1") and not is_valid_node_id("../x") and not is_valid_node_id("")


def test_settings_validation_and_env(monkeypatch):
    monkeypatch.setenv("DFS_REPLICATION_FACTOR", "2")
    assert load_settings(write_quorum=1).replication_factor == 2
    with pytest.raises(ValueError):
        load_settings(replication_factor=2, write_quorum=3)
    with pytest.raises(ValueError):
        load_settings(chunk_size=1024 * 1024, max_payload_size=2048)
