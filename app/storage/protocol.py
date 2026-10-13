"""Binary-safe, length-prefixed TCP protocol between coordinator and nodes.

Frame layout (network byte order)::

    +---------+----------------+-----------------+-------------+-----------------+
    | magic   | header_len     | payload_len     | header      | payload         |
    | 4 bytes | uint32         | uint64          | UTF-8 JSON  | raw bytes       |
    | "DFS1"  |                |                 | header_len  | payload_len     |
    +---------+----------------+-----------------+-------------+-----------------+

* The header is a JSON object. Requests carry ``cmd``, ``req_id`` and ``token``
  plus command arguments; responses carry ``ok``, ``req_id`` and either
  result fields or ``error: {code, message}``.
* Chunk bytes travel in the payload, never inside JSON/Base64.
* Reads loop until exactly N bytes arrive (``recv_exact``); writes use
  ``sendall``. A connection may carry many request/response pairs.
"""

from __future__ import annotations

import json
import socket
import struct
from enum import Enum
from typing import Any

MAGIC = b"DFS1"
PREFIX = struct.Struct("!4sIQ")
MAX_HEADER_SIZE = 64 * 1024


class Command(str, Enum):
    PING = "PING"
    STORE_CHUNK = "STORE_CHUNK"
    GET_CHUNK = "GET_CHUNK"
    DELETE_CHUNK = "DELETE_CHUNK"
    VERIFY_CHUNK = "VERIFY_CHUNK"
    LIST_CHUNKS = "LIST_CHUNKS"
    GET_NODE_STATUS = "GET_NODE_STATUS"
    REPLICATE_CHUNK = "REPLICATE_CHUNK"


class ErrorCode(str, Enum):
    BAD_REQUEST = "BAD_REQUEST"
    AUTH_FAILED = "AUTH_FAILED"
    UNKNOWN_COMMAND = "UNKNOWN_COMMAND"
    NOT_FOUND = "NOT_FOUND"
    CHECKSUM_MISMATCH = "CHECKSUM_MISMATCH"
    PAYLOAD_TOO_LARGE = "PAYLOAD_TOO_LARGE"
    STORAGE_FULL = "STORAGE_FULL"
    REPLICATION_TARGET_FAILED = "REPLICATION_TARGET_FAILED"
    BUSY = "BUSY"
    INTERNAL = "INTERNAL"


class ProtocolError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


class ConnectionClosed(ProtocolError):
    def __init__(self, message: str = "connection closed by peer") -> None:
        super().__init__("CONNECTION_CLOSED", message)


class FrameTooLarge(ProtocolError):
    def __init__(self, message: str) -> None:
        super().__init__(ErrorCode.PAYLOAD_TOO_LARGE.value, message)


def recv_exact(sock: socket.socket, n: int, *, allow_clean_eof: bool = False) -> bytes | None:
    """Receive exactly ``n`` bytes, handling partial reads.

    Returns ``None`` only when ``allow_clean_eof`` is set and the peer closed
    the connection before sending any byte of this read.
    """
    buf = bytearray(n)
    view = memoryview(buf)
    received = 0
    while received < n:
        count = sock.recv_into(view[received:], n - received)
        if count == 0:
            if received == 0 and allow_clean_eof:
                return None
            raise ConnectionClosed(f"peer closed after {received}/{n} bytes")
        received += count
    return bytes(buf)


def encode_header(header: dict[str, Any]) -> bytes:
    data = json.dumps(header, separators=(",", ":"), sort_keys=True).encode("utf-8")
    if len(data) > MAX_HEADER_SIZE:
        raise FrameTooLarge(f"header of {len(data)} bytes exceeds {MAX_HEADER_SIZE}")
    return data


def send_frame(sock: socket.socket, header: dict[str, Any], payload: bytes | bytearray | memoryview = b"") -> None:
    header_bytes = encode_header(header)
    sock.sendall(PREFIX.pack(MAGIC, len(header_bytes), len(payload)) + header_bytes)
    if len(payload):
        sock.sendall(payload)


def recv_frame(
    sock: socket.socket, max_payload: int, *, allow_clean_eof: bool = False
) -> tuple[dict[str, Any], bytes] | None:
    prefix = recv_exact(sock, PREFIX.size, allow_clean_eof=allow_clean_eof)
    if prefix is None:
        return None
    magic, header_len, payload_len = PREFIX.unpack(prefix)
    if magic != MAGIC:
        raise ProtocolError(ErrorCode.BAD_REQUEST.value, "invalid frame magic")
    if header_len == 0 or header_len > MAX_HEADER_SIZE:
        raise ProtocolError(ErrorCode.BAD_REQUEST.value, f"invalid header length {header_len}")
    if payload_len > max_payload:
        raise FrameTooLarge(f"payload of {payload_len} bytes exceeds limit {max_payload}")
    header_bytes = recv_exact(sock, header_len)
    assert header_bytes is not None
    try:
        header = json.loads(header_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError(ErrorCode.BAD_REQUEST.value, f"header is not valid JSON: {exc}") from exc
    if not isinstance(header, dict):
        raise ProtocolError(ErrorCode.BAD_REQUEST.value, "header must be a JSON object")
    payload = recv_exact(sock, payload_len) if payload_len else b""
    assert payload is not None
    return header, payload


def error_header(code: str, message: str, req_id: Any = None) -> dict[str, Any]:
    return {"ok": False, "req_id": req_id, "error": {"code": code, "message": message}}


def ok_header(req_id: Any = None, **fields: Any) -> dict[str, Any]:
    return {"ok": True, "req_id": req_id, **fields}
