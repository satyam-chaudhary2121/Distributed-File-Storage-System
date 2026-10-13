import hashlib
import io
import os

import pytest

from app.storage.checksum import is_sha256_hex, sha256_bytes, sha256_file, sha256_stream
from app.storage.chunking import (
    ReconstructionError,
    expected_chunk_count,
    iter_chunks,
    make_chunk_id,
    reconstruct,
)


class TrickleStream(io.RawIOBase):
    """Returns at most 3 bytes per read, like a slow socket/pipe."""

    def __init__(self, data: bytes) -> None:
        self._buf = io.BytesIO(data)

    def readable(self) -> bool:
        return True

    def read(self, n: int = -1) -> bytes:
        return self._buf.read(min(3, n if n > 0 else 3))


def test_sha256_matches_hashlib(tmp_path):
    data = os.urandom(5000)
    assert sha256_bytes(data) == hashlib.sha256(data).hexdigest()
    assert sha256_stream(io.BytesIO(data)) == hashlib.sha256(data).hexdigest()
    p = tmp_path / "f"
    p.write_bytes(data)
    assert sha256_file(p) == hashlib.sha256(data).hexdigest()
    assert is_sha256_hex(sha256_bytes(b""))
    assert not is_sha256_hex("ABC") and not is_sha256_hex(None)


@pytest.mark.parametrize("size,chunk,expected", [
    (0, 10, 0), (1, 10, 1), (9, 10, 1), (10, 10, 1), (11, 10, 2), (100, 10, 10), (101, 10, 11),
])
def test_chunk_boundaries(size, chunk, expected):
    data = os.urandom(size)
    chunks = list(iter_chunks(io.BytesIO(data), chunk, "f" * 32))
    assert len(chunks) == expected == expected_chunk_count(size, chunk)
    assert [c.index for c in chunks] == list(range(expected))
    assert all(c.size == chunk for c in chunks[:-1])
    if chunks:
        assert chunks[-1].size == size - chunk * (expected - 1)
    assert b"".join(c.data for c in chunks) == data
    for c in chunks:
        assert c.checksum == hashlib.sha256(c.data).hexdigest()


def test_empty_file_has_no_chunks():
    assert list(iter_chunks(io.BytesIO(b""), 1024, "a" * 32)) == []


def test_short_reads_do_not_change_chunking():
    data = os.urandom(1000)
    chunks = list(iter_chunks(TrickleStream(data), 128, "b" * 32))
    assert [c.size for c in chunks] == [128] * 7 + [104]
    assert b"".join(c.data for c in chunks) == data


def test_chunk_ids_are_deterministic_and_unique():
    assert make_chunk_id("x" * 32, 0) == make_chunk_id("x" * 32, 0)
    ids = {make_chunk_id("x" * 32, i) for i in range(1000)}
    assert len(ids) == 1000
    assert make_chunk_id("x" * 32, 0) != make_chunk_id("y" * 32, 0)
    with pytest.raises(ValueError):
        make_chunk_id("x", -1)


def test_binary_integrity_all_byte_values():
    data = bytes(range(256)) * 50 + b"\x00\r\n\x1a\xff"
    chunks = list(iter_chunks(io.BytesIO(data), 777, "c" * 32))
    store = {c.index: c.data for c in chunks}
    out = io.BytesIO()
    digest = reconstruct([(c.index, c.checksum) for c in chunks], store.__getitem__, out,
                         hashlib.sha256(data).hexdigest())
    assert out.getvalue() == data and digest == hashlib.sha256(data).hexdigest()


def test_reconstruct_orders_chunks_and_detects_corruption():
    data = os.urandom(1000)
    chunks = list(iter_chunks(io.BytesIO(data), 100, "d" * 32))
    meta = [(c.index, c.checksum) for c in reversed(chunks)]  # given out of order
    store = {c.index: c.data for c in chunks}
    out = io.BytesIO()
    reconstruct(meta, store.__getitem__, out, hashlib.sha256(data).hexdigest())
    assert out.getvalue() == data

    store[3] = b"X" + store[3][1:]
    with pytest.raises(ReconstructionError, match="chunk 3"):
        reconstruct(meta, store.__getitem__, io.BytesIO())


def test_reconstruct_rejects_gaps_and_wrong_file_checksum():
    with pytest.raises(ReconstructionError, match="contiguous"):
        reconstruct([(0, "a"), (2, "b")], lambda i: b"", io.BytesIO())
    data = b"hello"
    chunks = list(iter_chunks(io.BytesIO(data), 2, "e" * 32))
    store = {c.index: c.data for c in chunks}
    with pytest.raises(ReconstructionError, match="file checksum"):
        reconstruct([(c.index, c.checksum) for c in chunks], store.__getitem__, io.BytesIO(), "0" * 64)
