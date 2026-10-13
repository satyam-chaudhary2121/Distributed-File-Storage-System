"""Splitting files into chunks and reassembling them.

Chunk identity: ``chunk_id = sha256(f"{file_id}:{index}")``. It is unique and
deterministic within a file's identity scheme, but it says nothing about the
chunk's *content*: content is always verified against the separately stored
SHA-256 ``checksum``.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import BinaryIO, Callable, Iterator, Sequence

from app.storage.checksum import sha256_bytes


@dataclass(frozen=True)
class Chunk:
    chunk_id: str
    index: int
    data: bytes
    checksum: str

    @property
    def size(self) -> int:
        return len(self.data)


def make_chunk_id(file_id: str, index: int) -> str:
    if index < 0:
        raise ValueError("chunk index must be non-negative")
    return hashlib.sha256(f"{file_id}:{index}".encode("utf-8")).hexdigest()


def _read_exact(stream: BinaryIO, size: int) -> bytes:
    """Read up to ``size`` bytes, looping because streams may return short reads."""
    parts: list[bytes] = []
    remaining = size
    while remaining > 0:
        block = stream.read(remaining)
        if not block:
            break
        parts.append(block)
        remaining -= len(block)
    return b"".join(parts)


def iter_chunks(stream: BinaryIO, chunk_size: int, file_id: str) -> Iterator[Chunk]:
    """Yield chunks one at a time; only one chunk is held in memory."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    index = 0
    while True:
        data = _read_exact(stream, chunk_size)
        if not data:
            return
        yield Chunk(make_chunk_id(file_id, index), index, data, sha256_bytes(data))
        index += 1


def expected_chunk_count(file_size: int, chunk_size: int) -> int:
    if file_size < 0 or chunk_size <= 0:
        raise ValueError("invalid sizes")
    return (file_size + chunk_size - 1) // chunk_size


class ReconstructionError(Exception):
    pass


def reconstruct(
    chunk_meta: Sequence[tuple[int, str]],
    fetch: Callable[[int], bytes],
    out: BinaryIO,
    expected_file_checksum: str | None = None,
) -> str:
    """Write chunks to ``out`` in index order, verifying every checksum.

    ``chunk_meta`` is a sequence of (index, checksum). Indices must be the
    contiguous range 0..n-1. Returns the SHA-256 of the reconstructed bytes.
    """
    ordered = sorted(chunk_meta, key=lambda c: c[0])
    if [i for i, _ in ordered] != list(range(len(ordered))):
        raise ReconstructionError("chunk indices are not contiguous from 0")
    h = hashlib.sha256()
    for index, checksum in ordered:
        data = fetch(index)
        if sha256_bytes(data) != checksum:
            raise ReconstructionError(f"checksum mismatch for chunk {index}")
        out.write(data)
        h.update(data)
    digest = h.hexdigest()
    if expected_file_checksum is not None and digest != expected_file_checksum:
        raise ReconstructionError("reconstructed file checksum mismatch")
    return digest
