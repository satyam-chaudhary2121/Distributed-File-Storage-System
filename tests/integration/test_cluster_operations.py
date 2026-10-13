"""Integration tests: coordinator + storage nodes as separate OS processes."""

import os
import sqlite3

import pytest

from app.process_manager import wait_for
from app.storage.socket_client import NodeClient
from tests.helpers import download, file_meta, healthy_locations, sha256, trigger_recovery, upload

pytestmark = pytest.mark.integration


def test_nodes_are_independent_processes_speaking_tcp(cluster_factory):
    c = cluster_factory(3)
    pids = {c.node_procs[n].pid for n in c.nodes} | {c.coordinator.pid}
    assert len(pids) == 4 and os.getpid() not in pids
    for nid, spec in c.nodes.items():
        pong = NodeClient("127.0.0.1", spec.port, c.secret, node_id=nid).ping()
        assert pong["node_id"] == nid


def test_upload_places_real_bytes_on_distinct_nodes_and_downloads_exactly(cluster_factory):
    c = cluster_factory(3)
    data = os.urandom(65536 * 4 + 123)  # 5 chunks with a short tail
    with c.api() as api:
        meta = upload(api, data, "multi.bin")
        assert meta["chunk_count"] == 5 and meta["checksum"] == sha256(data)
        offset = 0
        for chunk in meta["chunks"]:
            expected = data[offset:offset + chunk["size"]]
            offset += chunk["size"]
            locations = healthy_locations(chunk)
            assert len(locations) == 3 and len(set(locations)) == 3
            for nid in locations:
                path = c.chunk_path(nid, chunk["chunk_id"])
                assert path.read_bytes() == expected, f"wrong bytes on {nid}"
        r = download(api, meta["file_id"])
        assert r.status_code == 200 and r.content == data


def test_binary_and_empty_files(cluster_factory):
    c = cluster_factory(3)
    with c.api() as api:
        binary = bytes(range(256)) * 600
        for payload in (binary, b"", b"\x00", os.urandom(65536)):
            meta = upload(api, payload)
            assert download(api, meta["file_id"]).content == payload


def test_delete_removes_metadata_and_physical_chunks(cluster_factory):
    c = cluster_factory(3)
    with c.api() as api:
        meta = upload(api, os.urandom(200_000))
        assert all(c.chunks_on_disk(n) for n in c.nodes)
        r = api.delete(f"/api/v1/files/{meta['file_id']}").json()
        assert r["state"] == "DELETED" and r["deleted_replicas"] == 3 * meta["chunk_count"]
        assert all(not c.chunks_on_disk(n) for n in c.nodes)
        assert api.get(f"/api/v1/files/{meta['file_id']}").status_code == 404


def test_coordinator_restart_preserves_metadata(cluster_factory):
    c = cluster_factory(3)
    data = os.urandom(150_000)
    with c.api() as api:
        meta = upload(api, data, "persist.bin")
    c.stop_coordinator()
    db = sqlite3.connect(c.base_dir / "coordinator" / "metadata.db")
    try:
        assert db.execute("SELECT state FROM files WHERE file_id = ?", (meta["file_id"],)).fetchone()[0] == "COMMITTED"
        assert db.execute("SELECT COUNT(*) FROM replicas").fetchone()[0] == 3 * meta["chunk_count"]
    finally:
        db.close()
    c.start_coordinator()
    c.wait_nodes_healthy(list(c.nodes))
    with c.api() as api:
        after = file_meta(api, meta["file_id"])
        assert after["state"] == "COMMITTED" and after["checksum"] == meta["checksum"]
        assert [ch["chunk_id"] for ch in after["chunks"]] == [ch["chunk_id"] for ch in meta["chunks"]]
        assert download(api, meta["file_id"]).content == data
        assert api.get("/api/v1/files").json()["total"] == 1


def test_coordinator_crash_mid_operation_aborts_inflight_uploads(cluster_factory):
    """Simulate a crash that left an upload UPLOADING: restart must fail it and clean its chunks."""
    c = cluster_factory(3)
    with c.api() as api:
        meta = upload(api, os.urandom(100_000))
    c.kill_coordinator()
    db = sqlite3.connect(c.base_dir / "coordinator" / "metadata.db")
    try:
        db.execute("UPDATE files SET state = 'UPLOADING' WHERE file_id = ?", (meta["file_id"],))
        db.commit()
    finally:
        db.close()
    c.start_coordinator()
    with c.api() as api:
        assert file_meta(api, meta["file_id"])["state"] == "FAILED"
        wait_for(lambda: all(not c.chunks_on_disk(n) for n in c.nodes), 15, description="orphan cleanup")


def test_storage_node_restart_keeps_chunk_data(cluster_factory):
    c = cluster_factory(3)
    data = os.urandom(120_000)
    with c.api() as api:
        meta = upload(api, data)
        before = c.chunks_on_disk("node-2")
        c.stop_node("node-2")
        c.start_node("node-2")
        c.wait_nodes_healthy(["node-2"])
        assert c.chunks_on_disk("node-2") == before
        listed = {x["chunk_id"] for x in NodeClient("127.0.0.1", c.nodes["node-2"].port, c.secret).list_chunks()}
        assert listed == before
        wait_for(lambda: not c.node_states()["node-2"]["needs_reconcile"], 15)
        assert all(len(healthy_locations(ch)) == 3 for ch in file_meta(api, meta["file_id"])["chunks"])
        assert download(api, meta["file_id"]).content == data


def test_adding_node_and_rebalancing_moves_real_data(cluster_factory):
    c = cluster_factory(3)
    files = []
    with c.api() as api:
        for _ in range(4):
            data = os.urandom(65536 * 5)
            files.append((upload(api, data)["file_id"], data))
        total_chunks = sum(file_meta(api, f)["chunk_count"] for f, _ in files)
        c.add_node("node-4")
        c.wait_nodes_healthy(["node-4"])
        plan = api.post("/api/v1/cluster/rebalance", params={"dry_run": True}).json()
        # Consistent hashing: only some chunks change placement (with 4 nodes and R=3, ~3/4 of
        # chunks gain node-4 in their replica set; modulo hashing would reshuffle all of them).
        assert 0 < plan["chunks_to_change"] < total_chunks
        assert all(p["to_add"] == ["node-4"] and len(p["to_remove"]) == 1 for p in plan["plan"])
        report = api.post("/api/v1/cluster/rebalance").json()
        assert report["complete"] is True
        assert report["replicas_added"] == plan["chunks_to_change"] == report["replicas_removed"]
        on_new = c.chunks_on_disk("node-4")
        assert len(on_new) == plan["chunks_to_change"]
        assert api.post("/api/v1/cluster/rebalance", params={"dry_run": True}).json()["chunks_to_change"] == 0
        for fid, data in files:
            meta = file_meta(api, fid)
            for ch in meta["chunks"]:
                locs = healthy_locations(ch)
                assert len(locs) == 3
                for nid in locs:  # metadata matches disk exactly
                    assert ch["chunk_id"] in c.chunks_on_disk(nid)
            assert download(api, fid).content == data
        disk_total = sum(len(c.chunks_on_disk(n)) for n in c.nodes)
        assert disk_total == 3 * total_chunks  # moved, not duplicated


def test_graceful_drain_moves_data_before_removal(cluster_factory):
    c = cluster_factory(4)
    with c.api() as api:
        data = os.urandom(65536 * 6)
        fid = upload(api, data)["file_id"]
        held = c.chunks_on_disk("node-1")
        assert held
        r = api.delete("/api/v1/nodes/node-1")
        assert r.status_code == 409  # still holds data
        drain = api.post("/api/v1/nodes/node-1/drain").json()
        assert drain["drained"] is True and drain["remaining_replicas"] == 0
        assert api.get("/api/v1/nodes/node-1").json()["admin_state"] == "DRAINED"
        assert not c.chunks_on_disk("node-1")
        for ch in file_meta(api, fid)["chunks"]:
            locs = healthy_locations(ch)
            assert len(locs) == 3 and "node-1" not in locs
            for nid in locs:
                assert ch["chunk_id"] in c.chunks_on_disk(nid)
        assert api.delete("/api/v1/nodes/node-1").json()["admin_state"] == "REMOVED"
        assert download(api, fid).content == data
        # New data never lands on the removed node.
        upload(api, os.urandom(100_000))
        assert not c.chunks_on_disk("node-1")
        trigger_recovery(api)
        assert api.get("/api/v1/cluster/replication/status").json()["chunk_counts"]["OK"] > 0
