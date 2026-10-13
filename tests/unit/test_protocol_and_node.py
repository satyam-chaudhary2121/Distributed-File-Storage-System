"""Framing protocol and storage-node server over real loopback TCP sockets."""

import json
import os
import socket
import struct
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.storage.checksum import sha256_bytes
from app.storage.chunking import make_chunk_id
from app.storage.node_server import StorageNodeServer
from app.storage.node_store import ChunkStore
from app.storage.protocol import (
    MAGIC,
    PREFIX,
    Command,
    ConnectionClosed,
    FrameTooLarge,
    ProtocolError,
    recv_frame,
    send_frame,
)
from app.storage.socket_client import NodeClient, NodeError, NodeUnavailable

SECRET = "unit-test-secret"


# ------------------------------------------------------------------ framing
def test_frame_round_trip_with_binary_payload():
    a, b = socket.socketpair()
    payload = bytes(range(256)) * 1000
    with a, b:
        t = threading.Thread(target=send_frame, args=(a, {"cmd": "X", "n": 1}, payload))
        t.start()
        header, data = recv_frame(b, len(payload))
        t.join()
    assert header == {"cmd": "X", "n": 1}
    assert data == payload


def test_partial_writes_are_reassembled():
    a, b = socket.socketpair()
    header = json.dumps({"cmd": "PING"}).encode()
    payload = os.urandom(5000)
    raw = PREFIX.pack(MAGIC, len(header), len(payload)) + header + payload

    def trickle():
        for i in range(0, len(raw), 7):
            a.sendall(raw[i:i + 7])
            if i % 700 == 0:
                time.sleep(0.001)

    with a, b:
        t = threading.Thread(target=trickle)
        t.start()
        h, p = recv_frame(b, 10_000)
        t.join()
    assert h == {"cmd": "PING"} and p == payload


def test_multiple_frames_on_one_connection():
    a, b = socket.socketpair()
    with a, b:
        for i in range(5):
            send_frame(a, {"i": i}, bytes([i]) * i)
        for i in range(5):
            h, p = recv_frame(b, 100)
            assert h == {"i": i} and p == bytes([i]) * i


def test_bad_magic_non_json_and_non_object_headers():
    for raw in (
        b"XXXX" + struct.pack("!IQ", 2, 0) + b"{}",
        MAGIC + struct.pack("!IQ", 3, 0) + b"{x}",
        MAGIC + struct.pack("!IQ", 2, 0) + b"[]",
        MAGIC + struct.pack("!IQ", 0, 0),
    ):
        a, b = socket.socketpair()
        with a, b:
            a.sendall(raw)
            with pytest.raises(ProtocolError):
                recv_frame(b, 100)


def test_payload_limit_enforced_before_reading():
    a, b = socket.socketpair()
    with a, b:
        a.sendall(PREFIX.pack(MAGIC, 2, 10**9) + b"{}")
        with pytest.raises(FrameTooLarge):
            recv_frame(b, 1024)


def test_clean_eof_vs_truncated_frame():
    a, b = socket.socketpair()
    a.close()
    with b:
        assert recv_frame(b, 100, allow_clean_eof=True) is None
    a, b = socket.socketpair()
    with b:
        a.sendall(PREFIX.pack(MAGIC, 2, 100) + b"{}" + b"only-part")
        a.close()
        with pytest.raises(ConnectionClosed):
            recv_frame(b, 1000)


# ------------------------------------------------------------- node server
@pytest.fixture
def node(tmp_path):
    store = ChunkStore(tmp_path / "n1", "n1", capacity_bytes=10_000_000)
    server = StorageNodeServer(node_id="n1", host="127.0.0.1", port=0, store=store, secret=SECRET,
                               max_payload=4_000_000, io_timeout=5, idle_timeout=5, max_connections=32)
    server.start()
    yield server
    server.stop()


def client(server, secret=SECRET, **kw):
    return NodeClient("127.0.0.1", server.port, secret, node_id=server.node_id, **kw)


def cid(i=0):
    return make_chunk_id("f" * 32, i)


def test_store_get_verify_list_delete(node):
    c = client(node)
    data = os.urandom(100_000)
    r = c.store_chunk(cid(), data, sha256_bytes(data))
    assert r["stored"] is True
    assert c.store_chunk(cid(), data, sha256_bytes(data))["already_present"] is True  # idempotent
    got, meta = c.get_chunk(cid())
    assert got == data and meta["checksum"] == sha256_bytes(data)
    v = c.verify_chunk(cid(), sha256_bytes(data))
    assert v["exists"] and v["valid"]
    assert not c.verify_chunk(cid(), "0" * 64)["valid"]
    assert [x["chunk_id"] for x in c.list_chunks()] == [cid()]
    assert c.status()["chunk_count"] == 1
    assert c.ping()["node_id"] == "n1"
    assert c.delete_chunk(cid()) is True
    assert c.delete_chunk(cid()) is False  # idempotent
    with pytest.raises(NodeError) as e:
        c.get_chunk(cid())
    assert e.value.code == "NOT_FOUND"


def test_checksum_mismatch_rejected_and_nothing_written(node):
    c = client(node)
    with pytest.raises(NodeError) as e:
        c.store_chunk(cid(), b"data", sha256_bytes(b"other"))
    assert e.value.code == "CHECKSUM_MISMATCH"
    assert c.list_chunks() == []


def test_authentication_required(node):
    with pytest.raises(NodeError) as e:
        client(node, secret="wrong-secret-value").ping()
    assert e.value.code == "AUTH_FAILED"


def test_identity_mismatch_detected(node):
    with pytest.raises(NodeError) as e:
        NodeClient("127.0.0.1", node.port, SECRET, node_id="someone-else").ping()
    assert e.value.code == "IDENTITY_MISMATCH"


def test_unknown_command_and_malformed_arguments(node):
    sock = socket.create_connection(("127.0.0.1", node.port))
    with sock:
        send_frame(sock, {"cmd": "FORMAT_DISK", "token": SECRET})
        h, _ = recv_frame(sock, 1000)
        assert h["error"]["code"] == "UNKNOWN_COMMAND"
        send_frame(sock, {"cmd": "GET_CHUNK", "token": SECRET})  # missing chunk_id
        h, _ = recv_frame(sock, 1000)
        assert h["error"]["code"] == "BAD_REQUEST"
        # Same connection keeps working after errors.
        send_frame(sock, {"cmd": "PING", "token": SECRET})
        h, _ = recv_frame(sock, 1000)
        assert h["ok"] is True


@pytest.mark.parametrize("bad_id", ["../../etc/passwd", "..", "a/b/c/d/e/f/g/h", "abc", "x" * 200, "ab\\..\\cd1234"])
def test_path_traversal_and_invalid_ids_rejected(node, bad_id, tmp_path):
    c = client(node)
    with pytest.raises(NodeError) as e:
        c.store_chunk(bad_id, b"x", sha256_bytes(b"x"))
    assert e.value.code == "BAD_REQUEST"
    assert not any(p.name == "passwd" for p in tmp_path.rglob("*"))


def test_oversized_payload_rejected(node):
    c = client(node, max_payload=10_000_000)
    data = os.urandom(4_000_001)
    with pytest.raises(NodeError) as e:
        c.store_chunk(cid(), data, sha256_bytes(data))
    assert e.value.code in ("PAYLOAD_TOO_LARGE", "CONNECTION_LOST")
    assert client(node).ping()["node_id"] == "n1"  # server still healthy


def test_storage_full(tmp_path):
    store = ChunkStore(tmp_path / "small", "small", capacity_bytes=1000)
    server = StorageNodeServer(node_id="small", host="127.0.0.1", port=0, store=store, secret=SECRET,
                               max_payload=100_000)
    server.start()
    try:
        c = client(server)
        c.store_chunk(cid(0), b"a" * 800, sha256_bytes(b"a" * 800))
        with pytest.raises(NodeError) as e:
            c.store_chunk(cid(1), b"b" * 300, sha256_bytes(b"b" * 300))
        assert e.value.code == "STORAGE_FULL"
    finally:
        server.stop()


def test_interrupted_connection_does_not_break_server(node):
    sock = socket.create_connection(("127.0.0.1", node.port))
    sock.sendall(PREFIX.pack(MAGIC, 40, 1000) + b'{"cmd":')  # truncated frame
    sock.close()
    sock = socket.create_connection(("127.0.0.1", node.port))
    sock.sendall(b"garbage-that-is-not-a-frame!!")
    sock.close()
    time.sleep(0.1)
    assert client(node).ping()["node_id"] == "n1"


def test_concurrent_clients(node):
    def work(i):
        c = client(node)
        data = os.urandom(20_000 + i)
        c.store_chunk(cid(i), data, sha256_bytes(data))
        got, _ = c.get_chunk(cid(i))
        return got == data

    with ThreadPoolExecutor(16) as pool:
        assert all(pool.map(work, range(64)))
    assert len(client(node).list_chunks()) == 64


def test_node_to_node_replication(node, tmp_path):
    store2 = ChunkStore(tmp_path / "n2", "n2", capacity_bytes=10_000_000)
    other = StorageNodeServer(node_id="n2", host="127.0.0.1", port=0, store=store2, secret=SECRET,
                              max_payload=4_000_000)
    other.start()
    try:
        data = os.urandom(50_000)
        client(node).store_chunk(cid(), data, sha256_bytes(data))
        client(node).replicate_chunk(cid(), sha256_bytes(data), "127.0.0.1", other.port, "n2")
        got, _ = client(other).get_chunk(cid())
        assert got == data
    finally:
        other.stop()


def test_connection_refused_and_timeout():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    with pytest.raises(NodeUnavailable) as e:
        NodeClient("127.0.0.1", port, SECRET, connect_timeout=0.5).ping()
    assert e.value.code in ("CONNECT_FAILED", "CONNECT_TIMEOUT")

    silent = socket.socket()
    silent.bind(("127.0.0.1", 0))
    silent.listen(5)  # accepts at the TCP level but never answers
    try:
        start = time.monotonic()
        with pytest.raises(NodeUnavailable) as e:
            NodeClient("127.0.0.1", silent.getsockname()[1], SECRET, io_timeout=0.5).ping()
        assert e.value.code == "TIMEOUT"
        assert time.monotonic() - start < 3
    finally:
        silent.close()


def test_client_requests_carry_command_names():
    assert {c.value for c in Command} >= {"PING", "STORE_CHUNK", "GET_CHUNK", "DELETE_CHUNK", "VERIFY_CHUNK",
                                          "LIST_CHUNKS", "GET_NODE_STATUS", "REPLICATE_CHUNK"}
