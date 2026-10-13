"""TCP client for talking to storage nodes."""

from __future__ import annotations

import json
import socket
import time
import uuid
from typing import Any

from app.core.retry import backoff_delay
from app.storage.protocol import Command, ProtocolError, recv_frame, send_frame


class NodeError(Exception):
    """A node answered with an error, or the exchange failed."""

    def __init__(self, code: str, message: str, node: str = "") -> None:
        super().__init__(f"[{node}] {code}: {message}")
        self.code = code
        self.message = message
        self.node = node


class NodeUnavailable(NodeError):
    """The node could not be reached or the connection broke mid-request."""


class NodeClient:
    def __init__(
        self,
        host: str,
        port: int,
        secret: str,
        *,
        node_id: str | None = None,
        connect_timeout: float = 3.0,
        io_timeout: float = 30.0,
        max_payload: int = 64 * 1024 * 1024,
        retries: int = 0,
        retry_base_delay: float = 0.1,
    ) -> None:
        self.host = host
        self.port = port
        self.secret = secret
        self.node_id = node_id
        self.connect_timeout = connect_timeout
        self.io_timeout = io_timeout
        self.max_payload = max_payload
        self.retries = retries
        self.retry_base_delay = retry_base_delay

    @property
    def label(self) -> str:
        return self.node_id or f"{self.host}:{self.port}"

    def _once(self, cmd: Command, payload: bytes, fields: dict[str, Any]) -> tuple[dict[str, Any], bytes]:
        req_id = uuid.uuid4().hex[:12]
        header = {"cmd": cmd.value, "req_id": req_id, "token": self.secret, **fields}
        try:
            sock = socket.create_connection((self.host, self.port), timeout=self.connect_timeout)
        except socket.timeout as exc:
            raise NodeUnavailable("CONNECT_TIMEOUT", str(exc), self.label) from exc
        except OSError as exc:
            raise NodeUnavailable("CONNECT_FAILED", str(exc), self.label) from exc
        try:
            sock.settimeout(self.io_timeout)
            send_frame(sock, header, payload)
            frame = recv_frame(sock, self.max_payload)
        except socket.timeout as exc:
            raise NodeUnavailable("TIMEOUT", f"no response within {self.io_timeout}s", self.label) from exc
        except ProtocolError as exc:
            if exc.code == "CONNECTION_CLOSED":
                raise NodeUnavailable("CONNECTION_LOST", exc.message, self.label) from exc
            raise NodeError("PROTOCOL_ERROR", exc.message, self.label) from exc
        except OSError as exc:
            raise NodeUnavailable("CONNECTION_LOST", str(exc), self.label) from exc
        finally:
            try:
                sock.close()
            except OSError:
                pass
        assert frame is not None
        resp, resp_payload = frame
        if resp.get("req_id") not in (None, req_id):
            raise NodeError("PROTOCOL_ERROR", "response req_id mismatch", self.label)
        if not resp.get("ok"):
            err = resp.get("error") or {}
            raise NodeError(str(err.get("code", "UNKNOWN")), str(err.get("message", "")), self.label)
        return resp, resp_payload

    def request(self, cmd: Command, payload: bytes = b"", **fields: Any) -> tuple[dict[str, Any], bytes]:
        """Send one request. Every command is idempotent, so transport failures are retried."""
        attempt = 0
        while True:
            try:
                return self._once(cmd, payload, fields)
            except NodeUnavailable:
                attempt += 1
                if attempt > self.retries:
                    raise
                time.sleep(backoff_delay(attempt, self.retry_base_delay, 2.0))

    # ----------------------------------------------------------- commands
    def ping(self) -> dict[str, Any]:
        resp, _ = self.request(Command.PING)
        if self.node_id and resp.get("node_id") != self.node_id:
            raise NodeError(
                "IDENTITY_MISMATCH",
                f"expected node {self.node_id}, got {resp.get('node_id')}",
                self.label,
            )
        return resp

    def store_chunk(self, chunk_id: str, data: bytes, checksum: str) -> dict[str, Any]:
        resp, _ = self.request(Command.STORE_CHUNK, data, chunk_id=chunk_id, checksum=checksum)
        return resp

    def get_chunk(self, chunk_id: str) -> tuple[bytes, dict[str, Any]]:
        resp, payload = self.request(Command.GET_CHUNK, chunk_id=chunk_id)
        return payload, resp

    def delete_chunk(self, chunk_id: str) -> bool:
        resp, _ = self.request(Command.DELETE_CHUNK, chunk_id=chunk_id)
        return bool(resp.get("deleted"))

    def verify_chunk(self, chunk_id: str, expected_checksum: str | None = None) -> dict[str, Any]:
        fields: dict[str, Any] = {"chunk_id": chunk_id}
        if expected_checksum:
            fields["expected_checksum"] = expected_checksum
        resp, _ = self.request(Command.VERIFY_CHUNK, **fields)
        return resp

    def list_chunks(self) -> list[dict[str, Any]]:
        _, payload = self.request(Command.LIST_CHUNKS)
        return json.loads(payload.decode("utf-8")) if payload else []

    def status(self) -> dict[str, Any]:
        resp, _ = self.request(Command.GET_NODE_STATUS)
        return resp

    def replicate_chunk(self, chunk_id: str, checksum: str, target_host: str, target_port: int,
                        target_node_id: str | None = None) -> dict[str, Any]:
        resp, _ = self.request(
            Command.REPLICATE_CHUNK,
            chunk_id=chunk_id,
            checksum=checksum,
            target_host=target_host,
            target_port=target_port,
            target_node_id=target_node_id,
        )
        return resp
