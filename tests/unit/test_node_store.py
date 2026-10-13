import os

import pytest

from app.storage.checksum import sha256_bytes
from app.storage.chunking import make_chunk_id
from app.storage.node_store import ChunkStore, StoreError


def cid(i=0):
    return make_chunk_id("a" * 32, i)


def test_data_and_inventory_survive_restart(tmp_path):
    s1 = ChunkStore(tmp_path, "n1", 0)
    blobs = {cid(i): os.urandom(1000 + i) for i in range(10)}
    for k, v in blobs.items():
        s1.store(k, v, sha256_bytes(v))
    uuid1 = s1.storage_uuid
    s2 = ChunkStore(tmp_path, "n1", 0)  # simulates a process restart
    assert s2.storage_uuid == uuid1
    assert {c["chunk_id"] for c in s2.list()} == set(blobs)
    for k, v in blobs.items():
        assert s2.get(k)[0] == v


def test_identity_protects_data_dir(tmp_path):
    ChunkStore(tmp_path, "n1", 0)
    with pytest.raises(StoreError) as e:
        ChunkStore(tmp_path, "n2", 0)
    assert e.value.code == "IDENTITY_MISMATCH"


def test_verify_detects_on_disk_corruption(tmp_path):
    s = ChunkStore(tmp_path, "n1", 0)
    data = os.urandom(4096)
    s.store(cid(), data, sha256_bytes(data))
    path = tmp_path / "chunks" / cid()[:2] / f"{cid()}.chunk"
    raw = bytearray(path.read_bytes())
    raw[100] ^= 0xFF
    path.write_bytes(bytes(raw))
    v = s.verify(cid(), sha256_bytes(data))
    assert v["exists"] and not v["valid"]
    # Storing the good bytes again repairs the corrupted copy.
    assert s.store(cid(), data, sha256_bytes(data))["stored"] is True
    assert s.verify(cid(), sha256_bytes(data))["valid"]


def test_missing_sidecar_is_rebuilt_and_tmp_cleaned(tmp_path):
    s = ChunkStore(tmp_path, "n1", 0)
    data = b"xyz" * 100
    s.store(cid(), data, sha256_bytes(data))
    (tmp_path / "chunks" / cid()[:2] / f"{cid()}.meta").unlink()
    (tmp_path / "tmp" / "abandoned.chunk").write_bytes(b"partial write")
    s2 = ChunkStore(tmp_path, "n1", 0)
    assert s2.list()[0]["checksum"] == sha256_bytes(data)
    assert list((tmp_path / "tmp").iterdir()) == []


def test_used_bytes_and_capacity(tmp_path):
    s = ChunkStore(tmp_path, "n1", 2500)
    s.store(cid(0), b"a" * 1000, sha256_bytes(b"a" * 1000))
    s.store(cid(1), b"b" * 1000, sha256_bytes(b"b" * 1000))
    assert s.used_bytes == 2000 and s.chunk_count == 2
    with pytest.raises(StoreError):
        s.store(cid(2), b"c" * 1000, sha256_bytes(b"c" * 1000))
    s.delete(cid(0))
    s.store(cid(2), b"c" * 1000, sha256_bytes(b"c" * 1000))
    assert s.status()["free_bytes"] == 500
