"""SHA-256 helpers."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import BinaryIO

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_READ_SIZE = 1024 * 1024


def sha256_bytes(data: bytes | bytearray | memoryview) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_stream(stream: BinaryIO, read_size: int = _READ_SIZE) -> str:
    h = hashlib.sha256()
    while True:
        block = stream.read(read_size)
        if not block:
            break
        h.update(block)
    return h.hexdigest()


def sha256_file(path: str | Path) -> str:
    with open(path, "rb") as fh:
        return sha256_stream(fh)


def is_sha256_hex(value: object) -> bool:
    return isinstance(value, str) and bool(SHA256_RE.match(value))
