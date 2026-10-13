"""Node crashes, detection and re-replication, using real killed processes (no mocks)."""

import os
import sqlite3

import pytest

from app.process_manager import wait_for
from tests.helpers import SLOW_DETECTION_ENV, download, file_meta, healthy_locations, trigger_recovery, upload

pytestmark = pytest.mark.failure


def replicas_in_db(cluster, file_id):
    db = sqlite3.connect(cluster.base_dir / "coordinator" / "metadata.db")
    try:
        rows = db.execute(
            "SELECT r.chunk_id, r.node_id, r.status FROM replicas r JOIN chunks c ON c.chunk_id = r.chunk_id "
            "WHERE c.file_id = ?", (file_id,)).fetchall()
    finally:
        db.close()
    return rows


def test_crash_detection_recovery_and_safe_return(cluster_factory):
    """The full scenario: upload, kill -9 a node, detect, download, re-replicate, restart, reconcile."""
    c = cluster_factory(4)
    data = os.urandom(65536 * 8 + 17)
    with c.api() as api:
        # 1. upload with RF=3
        meta = upload(api, data)
        fid = meta["file_id"]
        assert all(len(healthy_locations(ch)) == 3 for ch in meta["chunks"])
        victim = max(c.nodes, key=lambda n: len(c.chunks_on_disk(n)))
        victim_chunks = c.chunks_on_disk(victim)
        assert victim_chunks

        # 2-4. terminate the process abruptly and wait for failure detection
        c.kill_node(victim)
        c.wait_node_health(victim, "DOWN", timeout=15)
        assert api.get(f"/api/v1/nodes/{victim}").json()["health"] == "DOWN"

        # 5. downloads use surviving replicas
        r = download(api, fid)
        assert r.status_code == 200 and r.content == data
        assert victim not in r.headers["x-chunks-served-by"]

        # 6-8. recovery creates real replicas elsewhere and records them persistently
        def restored():
            trigger_recovery(api)
            m = file_meta(api, fid)
            return all(len(healthy_locations(ch)) == 3 for ch in m["chunks"]) and m

        m = wait_for(restored, 30, interval=0.5, description="re-replication")
        for ch in m["chunks"]:
            locs = healthy_locations(ch)
            assert victim not in locs and len(set(locs)) == 3
            for nid in locs:
                assert ch["chunk_id"] in c.chunks_on_disk(nid)
                assert c.chunk_path(nid, ch["chunk_id"]).stat().st_size == ch["size"]
        db_rows = replicas_in_db(c, fid)
        healthy_db = {(cid, nid) for cid, nid, st in db_rows if st == "HEALTHY" and nid != victim}
        assert len(healthy_db) == 3 * m["chunk_count"]
        status = api.get("/api/v1/cluster/replication/status").json()
        assert status["chunk_counts"]["UNDER_REPLICATED"] == 0

        # 9-10. restart the failed node: no duplicate replicas, metadata stays consistent
        c.start_node(victim)
        c.wait_nodes_healthy([victim])
        wait_for(lambda: not c.node_states()[victim]["needs_reconcile"], 20, description="reconcile")
        final = file_meta(api, fid)
        for ch in final["chunks"]:
            locs = [loc["node_id"] for loc in ch["locations"]]
            assert len(locs) == len(set(locs)) == 3, locs
            assert all(loc["status"] == "HEALTHY" for loc in ch["locations"])
        # Surplus stale copies on the returning node were removed, not double-counted.
        total_on_disk = sum(1 for n in c.nodes for cid in c.chunks_on_disk(n)
                            if cid in {ch["chunk_id"] for ch in final["chunks"]})
        assert total_on_disk == 3 * final["chunk_count"]
        assert download(api, fid).content == data


def test_three_nodes_one_failure_reports_under_replication_until_capacity_returns(cluster_factory):
    c = cluster_factory(3)
    data = os.urandom(65536 * 3)
    with c.api() as api:
        fid = upload(api, data)["file_id"]
        c.kill_node("node-2")
        c.wait_node_health("node-2", "DOWN", timeout=15)
        report = trigger_recovery(api)
        assert report["repair"]["blocked"] == report["repair"]["under_replicated"] > 0
        assert report["repair"]["replicas_created"] == 0
        rs = api.get("/api/v1/cluster/replication/status").json()
        assert rs["max_distinct_copies_possible"] == 2 and rs["replication_factor_achievable"] is False
        assert rs["chunk_counts"]["UNDER_REPLICATED"] == file_meta(api, fid)["chunk_count"]
        assert all(p["healthy_replicas"] == 2 for p in rs["problem_chunks"])
        tasks = api.get("/api/v1/recovery/status").json()["repair_task_counts"]
        assert tasks.get("BLOCKED", 0) > 0
        assert download(api, fid).content == data

        # A new eligible physical node lets recovery restore RF=3.
        c.add_node("node-4")
        c.wait_nodes_healthy(["node-4"])

        def ok():
            trigger_recovery(api)
            return api.get("/api/v1/cluster/replication/status").json()["chunk_counts"]["UNDER_REPLICATED"] == 0

        wait_for(ok, 30, interval=0.5, description="RF restored on node-4")
        meta = file_meta(api, fid)
        assert all("node-4" in healthy_locations(ch) for ch in meta["chunks"])
        assert all(ch["chunk_id"] in c.chunks_on_disk("node-4") for ch in meta["chunks"])


def test_multiple_node_failures(cluster_factory):
    c = cluster_factory(5)
    data = os.urandom(65536 * 6)
    with c.api() as api:
        fid = upload(api, data)["file_id"]
        for n in ("node-1", "node-2"):
            c.kill_node(n)
        for n in ("node-1", "node-2"):
            c.wait_node_health(n, "DOWN", timeout=15)
        assert download(api, fid).content == data

        def ok():
            trigger_recovery(api)
            m = file_meta(api, fid)
            return all(len(healthy_locations(ch)) == 3 for ch in m["chunks"])

        wait_for(ok, 30, interval=0.5, description="recovery after double failure")
        for ch in file_meta(api, fid)["chunks"]:
            assert set(healthy_locations(ch)) == {"node-3", "node-4", "node-5"}


def test_primary_unavailable_secondary_serves(cluster_factory):
    """Before the coordinator even notices, a dead primary is skipped in favour of another replica."""
    c = cluster_factory(3, env=SLOW_DETECTION_ENV)
    data = os.urandom(65536 * 2)
    with c.api() as api:
        meta = upload(api, data)
        primary = meta["chunks"][0]["locations"][0]["node_id"]
        c.kill_node(primary)
        assert c.node_states()[primary]["health"] == "HEALTHY"  # not yet detected
        r = download(api, meta["file_id"])
        assert r.status_code == 200 and r.content == data
        assert f"0:{primary}" not in r.headers["x-chunks-served-by"]


def test_upload_survives_node_crash_not_yet_detected(cluster_factory):
    """A node that died moments ago is still believed healthy; writes fall back along the ring."""
    c = cluster_factory(4, env=SLOW_DETECTION_ENV)
    with c.api() as api:
        c.kill_node("node-3")
        data = os.urandom(65536 * 6)
        meta = upload(api, data)
        assert meta["state"] == "COMMITTED"
        for ch in meta["chunks"]:
            locs = healthy_locations(ch)
            assert len(locs) == 3 and "node-3" not in locs
        assert download(api, meta["file_id"]).content == data


def test_upload_fails_honestly_when_quorum_unreachable_and_cleans_up(cluster_factory):
    c = cluster_factory(3, env={**SLOW_DETECTION_ENV, "DFS_WRITE_QUORUM": "3"})
    with c.api() as api:
        c.kill_node("node-1")
        r = api.post("/api/v1/files/upload", files={"file": ("x.bin", os.urandom(200_000))})
        assert r.status_code == 503
        err = r.json()["error"]
        assert err["code"] == "UPLOAD_FAILED"
        fid = err["details"]["file_id"]
        meta = file_meta(api, fid)
        assert meta["state"] == "FAILED" and meta["chunks"] == []
        assert api.get("/api/v1/files").json()["total"] == 0
        assert api.get(f"/api/v1/files/{fid}/download").status_code == 409
        # Chunks written to the live nodes before the failure were cleaned up.
        assert not c.chunks_on_disk("node-2") and not c.chunks_on_disk("node-3")
        # Writes to the dead node are queued, not falsely claimed as deleted.
        pending = api.get("/api/v1/recovery/status").json()["pending_deletions"]
        assert pending >= 1
