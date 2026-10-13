"""Corruption, replication failures, partial deletes and unrecoverable data loss (real processes)."""

import os

import pytest

from app.process_manager import wait_for
from tests.helpers import SLOW_DETECTION_ENV, download, file_meta, healthy_locations, trigger_recovery, upload

pytestmark = pytest.mark.failure


def corrupt(cluster, node_id, chunk_id):
    path = cluster.chunk_path(node_id, chunk_id)
    raw = bytearray(path.read_bytes())
    raw[len(raw) // 2] ^= 0xFF
    path.write_bytes(bytes(raw))


def test_corrupted_replica_detected_bypassed_and_repaired(cluster_factory):
    c = cluster_factory(3, env={"DFS_RECOVERY_INTERVAL": "3600"})
    data = os.urandom(65536 * 3)
    with c.api() as api:
        meta = upload(api, data)
        chunk = meta["chunks"][1]
        first = chunk["locations"][0]["node_id"]  # the replica a download tries first
        corrupt(c, first, chunk["chunk_id"])

        r = download(api, meta["file_id"])
        assert r.status_code == 200 and r.content == data  # served from another replica
        assert f"1:{first}" not in r.headers["x-chunks-served-by"]
        statuses = {loc["node_id"]: loc["status"] for loc in file_meta(api, meta["file_id"])["chunks"][1]["locations"]}
        assert statuses[first] == "CORRUPTED"

        report = trigger_recovery(api)
        assert report["repair"]["replicas_created"] == 1
        after = file_meta(api, meta["file_id"])["chunks"][1]
        assert all(loc["status"] == "HEALTHY" for loc in after["locations"])
        assert c.chunk_path(first, chunk["chunk_id"]).read_bytes() == data[65536:131072]


def test_scrub_finds_silent_corruption_and_missing_files(cluster_factory):
    c = cluster_factory(3, env={"DFS_RECOVERY_INTERVAL": "3600"})
    data = os.urandom(65536 * 2)
    with c.api() as api:
        meta = upload(api, data)
        c0, c1 = meta["chunks"]
        corrupt(c, "node-1", c0["chunk_id"])
        c.chunk_path("node-2", c1["chunk_id"]).unlink()
        report = trigger_recovery(api, scrub=True)
        assert report["scrub"]["corrupted"] == 1 and report["scrub"]["missing"] == 1
        assert report["repair"]["replicas_created"] == 2
        assert trigger_recovery(api, scrub=True)["scrub"]["corrupted"] == 0
        for ch in file_meta(api, meta["file_id"])["chunks"]:
            assert len(healthy_locations(ch)) == 3


def test_destination_failure_during_replication_is_recorded_and_retried(cluster_factory):
    """node-4 is real but full: copies to it fail with STORAGE_FULL and recovery reports it."""
    c = cluster_factory(3, env={"DFS_RECOVERY_INTERVAL": "3600", "DFS_RECOVERY_MAX_ATTEMPTS": "2"})
    with c.api() as api:
        meta = upload(api, os.urandom(65536 * 2))
        c.add_node("node-4", capacity_bytes=1000)
        c.wait_nodes_healthy(["node-4"])
        c.kill_node("node-1")
        c.wait_node_health("node-1", "DOWN", timeout=15)
        report = trigger_recovery(api)
        assert report["repair"]["failed"] == report["repair"]["under_replicated"] == meta["chunk_count"]
        assert report["repair"]["replicas_created"] == 0
        tasks = api.get("/api/v1/recovery/status").json()["repair_tasks"]
        assert tasks and all(t["state"] in ("RETRYING", "FAILED") for t in tasks)
        assert any("STORAGE_FULL" in (t["last_error"] or "") for t in tasks)
        assert not c.chunks_on_disk("node-4")  # nothing was falsely recorded
        rs = api.get("/api/v1/cluster/replication/status").json()
        assert rs["chunk_counts"]["UNDER_REPLICATED"] == meta["chunk_count"]
        # Bounded retries: after max attempts (2 here) the task stops auto-retrying.
        # (The background worker may also run a cycle when node-1 goes DOWN, so poll.)
        def all_failed():
            states = {t["state"] for t in api.get("/api/v1/recovery/status").json()["repair_tasks"]}
            if states == {"FAILED"}:
                return True
            trigger_recovery(api, force=False)
            return False

        wait_for(all_failed, 20, interval=0.3, description="repair tasks to exhaust retries")
        assert all(t["attempts"] <= 2 for t in api.get("/api/v1/recovery/status").json()["repair_tasks"])
        assert trigger_recovery(api, force=False)["repair"]["deferred"] == meta["chunk_count"]


def test_source_unavailable_during_recovery_falls_back_to_other_source(cluster_factory):
    """The first-choice source died but is not yet marked DOWN; recovery uses the next source."""
    c = cluster_factory(4, env=SLOW_DETECTION_ENV)
    data = os.urandom(65536)
    with c.api() as api:
        meta = upload(api, data)
        chunk = meta["chunks"][0]
        holders = sorted(healthy_locations(chunk))  # sources are tried in node-id order
        dead_source, damaged = holders[0], holders[2]
        c.kill_node(dead_source)
        c.chunk_path(damaged, chunk["chunk_id"]).unlink()
        report = trigger_recovery(api, scrub=True)
        assert report["scrub"]["missing"] == 1
        assert report["repair"]["replicas_created"] >= 1
        locs = healthy_locations(file_meta(api, meta["file_id"])["chunks"][0])
        assert holders[1] in locs and len(locs) == 3
        restored_on = [n for n in locs if n != dead_source]
        for nid in restored_on:
            assert c.chunk_path(nid, chunk["chunk_id"]).read_bytes() == data


def test_partial_deletion_is_reported_then_reconciled(cluster_factory):
    c = cluster_factory(3, env={"DFS_RECOVERY_INTERVAL": "0.5"})
    data = os.urandom(65536 * 2)
    with c.api() as api:
        meta = upload(api, data)
        fid = meta["file_id"]
        c.kill_node("node-3")
        c.wait_node_health("node-3", "DOWN", timeout=15)
        r = api.delete(f"/api/v1/files/{fid}").json()
        assert r["state"] == "DELETING"
        assert {p["node_id"] for p in r["pending_cleanup"]} == {"node-3"}
        assert len(r["pending_cleanup"]) == meta["chunk_count"]
        assert download(api, fid).status_code == 409  # not downloadable once deletion starts
        assert api.get("/api/v1/files").json()["total"] == 0
        assert file_meta(api, fid)["pending_cleanup"]
        assert not c.chunks_on_disk("node-1") and not c.chunks_on_disk("node-2")
        assert c.chunks_on_disk("node-3")  # still there: honestly reported as pending

        c.start_node("node-3")
        c.wait_nodes_healthy(["node-3"])

        def cleaned():
            trigger_recovery(api)
            return api.get(f"/api/v1/files/{fid}").status_code == 404

        wait_for(cleaned, 30, interval=0.5, description="deferred deletion")
        assert not c.chunks_on_disk("node-3")


def test_loss_of_every_replica_is_reported_unrecoverable(cluster_factory):
    c = cluster_factory(3, env={"DFS_RECOVERY_INTERVAL": "3600"})
    data = os.urandom(65536 * 2)
    with c.api() as api:
        meta = upload(api, data)
        chunk = meta["chunks"][0]
        for nid in healthy_locations(chunk):
            corrupt(c, nid, chunk["chunk_id"])
        r = download(api, meta["file_id"])
        assert r.status_code == 500 and r.json()["error"]["code"] == "DATA_LOSS"
        report = trigger_recovery(api, scrub=True)
        assert report["repair"]["lost"] == 1 and report["repair"]["replicas_created"] == 0
        rs = api.get("/api/v1/cluster/replication/status").json()
        assert rs["chunk_counts"]["LOST"] == 1
        tasks = api.get("/api/v1/recovery/status").json()["repair_tasks"]
        assert [t["state"] for t in tasks if t["chunk_id"] == chunk["chunk_id"]] == ["UNRECOVERABLE"]
        # Corrupted bytes were never "repaired" from nothing: still corrupted on disk.
        for nid in ("node-1", "node-2", "node-3"):
            p = c.chunk_path(nid, chunk["chunk_id"])
            assert p.exists() and p.read_bytes() != data[:65536]
        # The other chunk is unaffected.
        assert file_meta(api, meta["file_id"])["chunks"][1]["status"] == "OK"


def test_all_holders_down_is_unavailable_not_lost(cluster_factory):
    c = cluster_factory(3)
    with c.api() as api:
        meta = upload(api, os.urandom(1000))
        for n in c.nodes:
            c.kill_node(n)
        for n in c.nodes:
            c.wait_node_health(n, "DOWN", timeout=15)
        r = download(api, meta["file_id"])
        assert r.status_code == 503 and r.json()["error"]["code"] == "CHUNK_UNAVAILABLE"
        trigger_recovery(api)
        rs = api.get("/api/v1/cluster/replication/status").json()
        assert rs["chunk_counts"]["UNAVAILABLE"] == 1 and rs["chunk_counts"]["LOST"] == 0
        for n in c.nodes:
            c.start_node(n)
        c.wait_nodes_healthy(list(c.nodes))
        assert download(api, meta["file_id"]).status_code == 200
