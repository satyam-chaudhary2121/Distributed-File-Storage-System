"""REST API tests: real FastAPI app (TestClient) + real TCP node servers running in-process threads.

Multi-process behaviour (crashes, restarts) is covered by tests/integration and
tests/failure_scenarios; these tests focus on request validation and API contracts.
"""

import os
import time

import pytest
from fastapi.testclient import TestClient

from app.core.config import load_settings
from app.main import create_app
from app.storage.node_server import StorageNodeServer
from app.storage.node_store import ChunkStore
from tests.helpers import sha256

SECRET = "api-test-secret-123"


@pytest.fixture
def settings(tmp_path):
    return load_settings(
        database_path=str(tmp_path / "coord" / "m.db"), temp_dir=str(tmp_path / "coord" / "tmp"),
        cluster_secret=SECRET, chunk_size=1000, replication_factor=3, write_quorum=2,
        max_upload_size=200_000, health_check_interval=0.2, suspect_timeout=5, failure_timeout=10,
        recovery_interval=0.5, connect_timeout=1, client_retries=0,
    )


@pytest.fixture
def api(settings):
    with TestClient(create_app(settings)) as client:
        yield client


@pytest.fixture
def nodes(tmp_path, api):
    servers = []
    for i in range(1, 4):
        store = ChunkStore(tmp_path / f"node-{i}", f"node-{i}", 10_000_000)
        srv = StorageNodeServer(node_id=f"node-{i}", host="127.0.0.1", port=0, store=store, secret=SECRET,
                                max_payload=1_000_000)
        srv.start()
        servers.append(srv)
        body = {"node_id": f"node-{i}", "host": "127.0.0.1", "port": srv.port, "data_dir": str(store.root),
                "storage_uuid": store.storage_uuid, "capacity_bytes": 10_000_000}
        r = api.post("/api/v1/nodes/register", json=body, headers={"X-Cluster-Token": SECRET})
        assert r.status_code == 201, r.text
    yield servers
    for s in servers:
        s.stop()


def test_health_and_openapi(api):
    assert api.get("/health").json()["status"] == "ok"
    paths = api.get("/openapi.json").json()["paths"]
    for p in ["/api/v1/files/upload", "/api/v1/files", "/api/v1/files/{file_id}", "/api/v1/files/{file_id}/download",
              "/api/v1/nodes/register", "/api/v1/nodes", "/api/v1/nodes/{node_id}", "/api/v1/nodes/{node_id}/drain",
              "/api/v1/cluster/status", "/api/v1/cluster/storage", "/api/v1/cluster/rebalance",
              "/api/v1/cluster/replication/status", "/api/v1/recovery/status", "/api/v1/recovery/trigger"]:
        assert p in paths
    assert "delete" in paths["/api/v1/nodes/{node_id}"]
    assert "delete" in paths["/api/v1/files/{file_id}"]


def test_upload_without_nodes_fails_cleanly(api):
    r = api.post("/api/v1/files/upload", files={"file": ("a.txt", b"hello")})
    assert r.status_code == 503 and r.json()["error"]["code"] == "INSUFFICIENT_NODES"
    fid = r.json()["error"]["details"]["file_id"]
    assert api.get(f"/api/v1/files/{fid}").json()["state"] == "FAILED"
    assert api.get("/api/v1/files").json()["total"] == 0


def test_validation_errors(api):
    assert api.get("/api/v1/files/not-a-valid-id").status_code == 422
    assert api.get("/api/v1/files/" + "0" * 32).status_code == 404
    assert api.get("/api/v1/files/" + "0" * 32 + "/download").status_code == 404
    assert api.delete("/api/v1/files/" + "0" * 32).status_code == 404
    assert api.get("/api/v1/nodes/missing-node").status_code == 404
    assert api.get("/api/v1/files?limit=0").status_code == 422
    assert api.post("/api/v1/files/upload").status_code == 422


def test_internal_endpoints_require_cluster_token(api):
    body = {"node_id": "n1", "host": "127.0.0.1", "port": 9999, "storage_uuid": "0" * 32, "capacity_bytes": 1}
    assert api.post("/api/v1/nodes/register", json=body).status_code == 401
    assert api.post("/api/v1/nodes/register", json=body, headers={"X-Cluster-Token": "nope"}).status_code == 401
    r = api.post("/api/v1/nodes/register", json=body, headers={"X-Cluster-Token": SECRET})
    assert r.status_code == 400  # token ok, but nothing answers on that TCP port -> identity check fails
    bad = {**body, "node_id": "../evil"}
    assert api.post("/api/v1/nodes/register", json=bad, headers={"X-Cluster-Token": SECRET}).status_code == 422


def test_request_size_limit(api, nodes):
    r = api.post("/api/v1/files/upload", files={"file": ("big.bin", os.urandom(300_000))})
    assert r.status_code == 413


def test_full_file_lifecycle(api, nodes):
    data = os.urandom(5_500)
    r = api.post("/api/v1/files/upload", files={"file": ("../../secret report.bin", data)})
    assert r.status_code == 201, r.text
    meta = r.json()
    assert meta["filename"] == "secret report.bin"
    assert meta["state"] == "COMMITTED" and meta["chunk_count"] == 6 and meta["size"] == 5500
    assert meta["checksum"] == sha256(data)
    for chunk in meta["chunks"]:
        nodes_for_chunk = [loc["node_id"] for loc in chunk["locations"]]
        assert len(set(nodes_for_chunk)) == 3 and chunk["status"] == "OK"

    d = api.get(f"/api/v1/files/{meta['file_id']}/download")
    assert d.status_code == 200 and d.content == data
    assert d.headers["x-checksum-sha256"] == sha256(data)
    assert "secret%20report.bin" in d.headers["content-disposition"]

    listing = api.get("/api/v1/files", params={"name": "secret"}).json()
    assert listing["total"] == 1 and listing["items"][0]["file_id"] == meta["file_id"]

    storage = api.get("/api/v1/cluster/storage").json()
    assert storage["logical_bytes_committed"] == 5500
    assert storage["physical_bytes_in_healthy_replicas"] == 3 * 5500

    r = api.delete(f"/api/v1/files/{meta['file_id']}")
    assert r.json()["state"] == "DELETED" and r.json()["pending_cleanup"] == []
    assert api.get(f"/api/v1/files/{meta['file_id']}").status_code == 404
    assert api.get(f"/api/v1/files/{meta['file_id']}/download").status_code == 404
    assert all(s.store.chunk_count == 0 for s in nodes)


def test_empty_file(api, nodes):
    r = api.post("/api/v1/files/upload", files={"file": ("empty.txt", b"")})
    assert r.status_code == 201 and r.json()["chunk_count"] == 0
    d = api.get(f"/api/v1/files/{r.json()['file_id']}/download")
    assert d.status_code == 200 and d.content == b""


def test_pagination(api, nodes):
    ids = [api.post("/api/v1/files/upload", files={"file": (f"f{i}.txt", b"x" * i)}).json()["file_id"]
           for i in range(1, 6)]
    page1 = api.get("/api/v1/files", params={"limit": 2, "offset": 0}).json()
    page3 = api.get("/api/v1/files", params={"limit": 2, "offset": 4}).json()
    assert page1["total"] == 5 and len(page1["items"]) == 2 and len(page3["items"]) == 1
    seen = {i["file_id"] for p in range(3) for i in api.get("/api/v1/files", params={"limit": 2, "offset": 2 * p}).json()["items"]}
    assert seen == set(ids)


def test_cluster_and_recovery_endpoints(api, nodes):
    status = api.get("/api/v1/cluster/status").json()
    assert status["nodes"]["by_health"]["HEALTHY"] == 3
    assert sorted(status["nodes"]["ring_members"]) == ["node-1", "node-2", "node-3"]
    rs = api.get("/api/v1/cluster/replication/status").json()
    assert rs["max_distinct_copies_possible"] == 3 and rs["replication_factor_achievable"]
    report = api.post("/api/v1/recovery/trigger", json={"scrub": True}).json()
    assert report["scrub"] is not None and "repair" in report
    assert api.get("/api/v1/recovery/status").json()["worker_running"] is True
    assert api.post("/api/v1/cluster/rebalance", params={"dry_run": True}).json()["dry_run"] is True


def test_repeated_registration_is_idempotent(api, nodes):
    srv = nodes[0]
    body = {"node_id": "node-1", "host": "127.0.0.1", "port": srv.port, "storage_uuid": srv.store.storage_uuid,
            "capacity_bytes": 10_000_000}
    for _ in range(3):
        r = api.post("/api/v1/nodes/register", json=body, headers={"X-Cluster-Token": SECRET})
        assert r.status_code == 200
    assert len(api.get("/api/v1/nodes").json()) == 3
    # Another node id claiming the same live address is rejected.
    clash = {**body, "node_id": "impostor"}
    r = api.post("/api/v1/nodes/register", json=clash, headers={"X-Cluster-Token": SECRET})
    assert r.status_code == 409


def test_drain_and_remove_node(api, nodes):
    data = os.urandom(3000)
    fid = api.post("/api/v1/files/upload", files={"file": ("d.bin", data)}).json()["file_id"]
    # Removing a node that still holds data must be refused.
    r = api.delete("/api/v1/nodes/node-3")
    assert r.status_code == 409
    drain = api.post("/api/v1/nodes/node-3/drain").json()
    # Only two other nodes: chunks keep 2 copies, and the draining node's copies are dropped.
    assert drain["drained"] is True and drain["remaining_replicas"] == 0
    assert nodes[2].store.chunk_count == 0
    assert api.delete("/api/v1/nodes/node-3").json()["admin_state"] == "REMOVED"
    meta = api.get(f"/api/v1/files/{fid}").json()
    assert all(c["healthy_replicas"] == 2 and c["status"] == "UNDER_REPLICATED" for c in meta["chunks"])
    assert api.get(f"/api/v1/files/{fid}/download").content == data
    rs = api.get("/api/v1/cluster/replication/status").json()
    assert rs["max_distinct_copies_possible"] == 2 and not rs["replication_factor_achievable"]
    time.sleep(0.1)
