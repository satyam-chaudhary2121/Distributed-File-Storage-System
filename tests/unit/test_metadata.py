import sqlite3

import pytest

from app.core.exceptions import InvalidStateTransition
from app.metadata.database import Database
from app.metadata.models import FileState, NodeHealth, ReplicaStatus, can_transition
from app.metadata.repository import MetadataRepository


@pytest.fixture
def repo(tmp_path):
    db = Database(tmp_path / "meta.db")
    db.initialize()
    yield MetadataRepository(db)
    db.close_all()


def add_node(repo, node_id):
    repo.register_node(node_id=node_id, host="127.0.0.1", port=9000 + int(node_id[-1]), data_dir="/x",
                       storage_uuid="0" * 32, capacity_bytes=100, used_bytes=0, chunk_count=0)


def make_file(repo, fid="f" * 32, chunks=2, nodes=("n1", "n2")):
    repo.create_file(file_id=fid, filename="a", size=10, checksum="0" * 64, chunk_size=5, replication_factor=2)
    repo.transition_file(fid, FileState.UPLOADING)
    for i in range(chunks):
        repo.add_chunk(chunk_id=f"{fid}-c{i}", file_id=fid, chunk_index=i, size=5, checksum="1" * 64)
        for n in nodes:
            repo.upsert_replica(f"{fid}-c{i}", n, ReplicaStatus.HEALTHY, 1.0)
    return fid


def test_state_machine_rules():
    assert can_transition(FileState.PENDING, FileState.UPLOADING)
    assert can_transition(FileState.UPLOADING, FileState.COMMITTED)
    assert can_transition(FileState.COMMITTED, FileState.DELETING)
    assert not can_transition(FileState.PENDING, FileState.COMMITTED)
    assert not can_transition(FileState.DELETED, FileState.COMMITTED)
    assert not can_transition(FileState.DELETING, FileState.COMMITTED)


def test_invalid_transition_raises(repo):
    repo.create_file(file_id="a" * 32, filename="x", size=0, checksum=None, chunk_size=1, replication_factor=1)
    with pytest.raises(InvalidStateTransition):
        repo.transition_file("a" * 32, FileState.COMMITTED)
    assert repo.get_file("a" * 32).state == "PENDING"


def test_constraints_enforced(repo):
    add_node(repo, "n1")
    fid = make_file(repo, nodes=("n1",))
    with pytest.raises(sqlite3.IntegrityError):  # duplicate chunk index
        repo.add_chunk(chunk_id="other", file_id=fid, chunk_index=0, size=1, checksum="2" * 64)
    with pytest.raises(sqlite3.IntegrityError):  # replica on unknown node (FK)
        repo.upsert_replica(f"{fid}-c0", "ghost", ReplicaStatus.HEALTHY)
    with pytest.raises(sqlite3.IntegrityError):  # bad state value (CHECK)
        repo.db.execute("UPDATE files SET state='WEIRD' WHERE file_id=?", (fid,))


def test_transaction_rolls_back(repo):
    with pytest.raises(RuntimeError):
        with repo.db.transaction() as conn:
            conn.execute("INSERT INTO cluster_meta(key, value) VALUES ('k', 'v')")
            raise RuntimeError("boom")
    assert repo.get_meta("k") is None


def test_abort_upload_schedules_cleanup(repo):
    add_node(repo, "n1"), add_node(repo, "n2")
    fid = make_file(repo)
    repo.abort_upload(fid, "test failure")
    assert repo.get_file(fid).state == "FAILED"
    assert repo.get_chunks(fid) == []
    pending = repo.pending_deletions_for_file(fid)
    assert {(p.chunk_id, p.node_id) for p in pending} == {
        (f"{fid}-c{i}", n) for i in range(2) for n in ("n1", "n2")}


def test_deletion_lifecycle(repo):
    add_node(repo, "n1"), add_node(repo, "n2")
    fid = make_file(repo)
    repo.transition_file(fid, FileState.COMMITTED)
    repo.begin_file_deletion(fid)
    assert repo.get_file(fid).state == "DELETING"
    assert repo.finalize_deleted_files() == []  # cleanup still outstanding
    for p in repo.pending_deletions_for_file(fid):
        repo.remove_pending_deletion(p.chunk_id, p.node_id)
    assert repo.finalize_deleted_files() == [fid]
    assert repo.get_file(fid).state == "DELETED"


def test_record_verified_replica_after_concurrent_delete(repo):
    add_node(repo, "n1"), add_node(repo, "n2")
    fid = make_file(repo, nodes=("n1",))
    repo.transition_file(fid, FileState.COMMITTED)
    repo.begin_file_deletion(fid)
    # A recovery copy that finishes after the delete must not resurrect metadata.
    assert repo.record_verified_replica(f"{fid}-c0", "n2") is False
    assert repo.pending_deletion_exists(f"{fid}-c0", "n2")


def test_node_health_transition_flags_reconcile(repo):
    add_node(repo, "n1")
    repo.mark_reconciled("n1")
    repo.set_node_health("n1", NodeHealth.DOWN)
    assert repo.get_node("n1").needs_reconcile == 0
    assert repo.set_node_health("n1", NodeHealth.HEALTHY) == "DOWN"
    assert repo.get_node("n1").needs_reconcile == 1


def test_initialize_is_idempotent_and_non_destructive(tmp_path):
    db = Database(tmp_path / "m.db")
    db.initialize()
    repo = MetadataRepository(db)
    repo.set_meta("marker", "1")
    db.initialize()
    assert repo.get_meta("marker") == "1"
    db.close_all()
    db2 = Database(tmp_path / "m.db")
    db2.initialize()
    assert MetadataRepository(db2).get_meta("marker") == "1"
    db2.close_all()


def test_list_files_only_committed_with_pagination_and_filter(repo):
    for i in range(5):
        fid = f"{i:032x}"
        repo.create_file(file_id=fid, filename=f"report_{i}.txt" if i % 2 else f"img{i}.png", size=0,
                         checksum=None, chunk_size=1, replication_factor=1)
        repo.transition_file(fid, FileState.UPLOADING)
        if i != 4:
            repo.transition_file(fid, FileState.COMMITTED)
    items, total = repo.list_files(limit=2, offset=0)
    assert total == 4 and len(items) == 2
    items, total = repo.list_files(name_contains="report")
    assert total == 2
    items, total = repo.list_files(name_contains="%")  # LIKE wildcards are escaped
    assert total == 0
