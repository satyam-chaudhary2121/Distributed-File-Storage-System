"""Storage node process: TCP server + registration/heartbeat with the coordinator.

Run as an independent OS process::

    python -m app.storage.node_server --node-id node-1 --port 9001 --data-dir data/nodes/node-1
"""

from __future__ import annotations

import argparse
import hmac
import json
import logging
import signal
import socket
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Callable

from app.core.config import Settings, load_settings
from app.core.logging_config import setup_logging
from app.core.retry import backoff_delay
from app.core.validation import is_valid_node_id
from app.storage.checksum import is_sha256_hex, sha256_bytes
from app.storage.node_store import ChunkStore, StoreError
from app.storage.protocol import (
    Command,
    ErrorCode,
    FrameTooLarge,
    ProtocolError,
    error_header,
    ok_header,
    recv_frame,
    send_frame,
)
from app.storage.socket_client import NodeClient, NodeError

log = logging.getLogger("dfs.node")

Handler = Callable[[dict[str, Any], bytes], tuple[dict[str, Any], bytes]]


class StorageNodeServer:
    def __init__(
        self,
        *,
        node_id: str,
        host: str,
        port: int,
        store: ChunkStore,
        secret: str,
        max_payload: int,
        io_timeout: float = 30.0,
        connect_timeout: float = 3.0,
        idle_timeout: float = 60.0,
        max_connections: int = 64,
    ) -> None:
        self.node_id = node_id
        self.host = host
        self.port = port
        self.store = store
        self._secret = secret.encode("utf-8")
        self._secret_str = secret
        self.max_payload = max_payload
        self.io_timeout = io_timeout
        self.connect_timeout = connect_timeout
        self.idle_timeout = idle_timeout
        self._slots = threading.BoundedSemaphore(max_connections)
        self._stop = threading.Event()
        self._sock: socket.socket | None = None
        self._accept_thread: threading.Thread | None = None
        self._conn_threads: set[threading.Thread] = set()
        self._conn_lock = threading.Lock()
        self.started_at = time.time()
        self._handlers: dict[str, Handler] = {
            Command.PING.value: self._h_ping,
            Command.STORE_CHUNK.value: self._h_store,
            Command.GET_CHUNK.value: self._h_get,
            Command.DELETE_CHUNK.value: self._h_delete,
            Command.VERIFY_CHUNK.value: self._h_verify,
            Command.LIST_CHUNKS.value: self._h_list,
            Command.GET_NODE_STATUS.value: self._h_status,
            Command.REPLICATE_CHUNK.value: self._h_replicate,
        }

    # ----------------------------------------------------------- lifecycle
    def start(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):  # Windows
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        else:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.host, self.port))
        sock.listen(128)
        sock.settimeout(0.5)
        self.port = sock.getsockname()[1]
        self._sock = sock
        self._accept_thread = threading.Thread(target=self._accept_loop, name="accept", daemon=True)
        self._accept_thread.start()
        log.info("node %s listening on %s:%s", self.node_id, self.host, self.port)

    def stop(self) -> None:
        self._stop.set()
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
        if self._accept_thread is not None:
            self._accept_thread.join(timeout=2)
        with self._conn_lock:
            threads = list(self._conn_threads)
        for t in threads:
            t.join(timeout=2)

    def _accept_loop(self) -> None:
        assert self._sock is not None
        while not self._stop.is_set():
            try:
                conn, addr = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                if self._stop.is_set():
                    return
                log.exception("accept failed")
                continue
            if not self._slots.acquire(blocking=False):
                self._reject_busy(conn)
                continue
            t = threading.Thread(target=self._serve_connection, args=(conn, addr), daemon=True)
            with self._conn_lock:
                self._conn_threads.add(t)
            t.start()

    def _reject_busy(self, conn: socket.socket) -> None:
        try:
            conn.settimeout(1.0)
            send_frame(conn, error_header(ErrorCode.BUSY.value, "too many concurrent connections"))
        except OSError:
            pass
        finally:
            conn.close()

    def _serve_connection(self, conn: socket.socket, addr: Any) -> None:
        try:
            conn.settimeout(self.idle_timeout)
            while not self._stop.is_set():
                try:
                    frame = recv_frame(conn, self.max_payload, allow_clean_eof=True)
                except socket.timeout:
                    return
                except FrameTooLarge as exc:
                    # The oversized payload is still in the stream: answer and close.
                    self._safe_send(conn, error_header(exc.code, exc.message))
                    return
                except ProtocolError as exc:
                    if exc.code != "CONNECTION_CLOSED":
                        log.warning("protocol error from %s: %s", addr, exc)
                        self._safe_send(conn, error_header(exc.code, exc.message))
                    return
                except OSError:
                    return
                if frame is None:
                    return
                header, payload = frame
                resp_header, resp_payload, keep_open = self.dispatch(header, payload)
                conn.settimeout(self.io_timeout)
                if not self._safe_send(conn, resp_header, resp_payload):
                    return
                if not keep_open:
                    return
                conn.settimeout(self.idle_timeout)
        finally:
            try:
                conn.close()
            except OSError:
                pass
            self._slots.release()
            with self._conn_lock:
                self._conn_threads.discard(threading.current_thread())

    @staticmethod
    def _safe_send(conn: socket.socket, header: dict[str, Any], payload: bytes = b"") -> bool:
        try:
            send_frame(conn, header, payload)
            return True
        except OSError:
            return False

    # ------------------------------------------------------------ dispatch
    def dispatch(self, header: dict[str, Any], payload: bytes) -> tuple[dict[str, Any], bytes, bool]:
        req_id = header.get("req_id")
        token = header.get("token")
        if not isinstance(token, str) or not hmac.compare_digest(token.encode("utf-8"), self._secret):
            return error_header(ErrorCode.AUTH_FAILED.value, "invalid cluster token", req_id), b"", False
        cmd = header.get("cmd")
        handler = self._handlers.get(cmd) if isinstance(cmd, str) else None
        if handler is None:
            return error_header(ErrorCode.UNKNOWN_COMMAND.value, f"unsupported command {cmd!r}", req_id), b"", True
        try:
            result, out_payload = handler(header, payload)
        except StoreError as exc:
            return error_header(exc.code, exc.message, req_id), b"", True
        except _BadRequest as exc:
            return error_header(ErrorCode.BAD_REQUEST.value, str(exc), req_id), b"", True
        except Exception as exc:  # noqa: BLE001 - report, never crash the node
            log.exception("unhandled error in %s", cmd)
            return error_header(ErrorCode.INTERNAL.value, type(exc).__name__, req_id), b"", True
        return ok_header(req_id, **result), out_payload, True

    # ------------------------------------------------------------ handlers
    @staticmethod
    def _chunk_id(header: dict[str, Any]) -> str:
        chunk_id = header.get("chunk_id")
        if not isinstance(chunk_id, str):
            raise _BadRequest("chunk_id is required")
        return chunk_id

    def _h_ping(self, header: dict[str, Any], payload: bytes) -> tuple[dict[str, Any], bytes]:
        return {
            "node_id": self.node_id,
            "storage_uuid": self.store.storage_uuid,
            "time": time.time(),
            "used_bytes": self.store.used_bytes,
            "chunk_count": self.store.chunk_count,
        }, b""

    def _h_store(self, header: dict[str, Any], payload: bytes) -> tuple[dict[str, Any], bytes]:
        checksum = header.get("checksum")
        if not is_sha256_hex(checksum):
            raise _BadRequest("checksum must be a SHA-256 hex digest")
        return self.store.store(self._chunk_id(header), payload, checksum), b""

    def _h_get(self, header: dict[str, Any], payload: bytes) -> tuple[dict[str, Any], bytes]:
        data, checksum = self.store.get(self._chunk_id(header))
        return {"checksum": checksum, "size": len(data)}, data

    def _h_delete(self, header: dict[str, Any], payload: bytes) -> tuple[dict[str, Any], bytes]:
        return {"deleted": self.store.delete(self._chunk_id(header))}, b""

    def _h_verify(self, header: dict[str, Any], payload: bytes) -> tuple[dict[str, Any], bytes]:
        expected = header.get("expected_checksum")
        if expected is not None and not is_sha256_hex(expected):
            raise _BadRequest("expected_checksum must be a SHA-256 hex digest")
        return self.store.verify(self._chunk_id(header), expected), b""

    def _h_list(self, header: dict[str, Any], payload: bytes) -> tuple[dict[str, Any], bytes]:
        items = self.store.list()
        return {"count": len(items)}, json.dumps(items).encode("utf-8")

    def _h_status(self, header: dict[str, Any], payload: bytes) -> tuple[dict[str, Any], bytes]:
        return {"node_id": self.node_id, "uptime": time.time() - self.started_at, **self.store.status()}, b""

    def _h_replicate(self, header: dict[str, Any], payload: bytes) -> tuple[dict[str, Any], bytes]:
        """Push a local chunk to another node (node-to-node transfer)."""
        chunk_id = self._chunk_id(header)
        checksum = header.get("checksum")
        target_host = header.get("target_host")
        target_port = header.get("target_port")
        if not is_sha256_hex(checksum):
            raise _BadRequest("checksum must be a SHA-256 hex digest")
        if not isinstance(target_host, str) or not isinstance(target_port, int) or not 0 < target_port < 65536:
            raise _BadRequest("target_host/target_port are required")
        data, _ = self.store.get(chunk_id)
        actual = sha256_bytes(data)
        if actual != checksum:
            raise StoreError(ErrorCode.CHECKSUM_MISMATCH.value, "local source copy is corrupted")
        client = NodeClient(
            target_host, target_port, self._secret_str,
            node_id=header.get("target_node_id"),
            connect_timeout=self.connect_timeout, io_timeout=self.io_timeout, max_payload=self.max_payload,
        )
        try:
            result = client.store_chunk(chunk_id, data, checksum)
        except NodeError as exc:
            raise StoreError(ErrorCode.REPLICATION_TARGET_FAILED.value, f"{exc.code}: {exc.message}") from exc
        return {"replicated": True, "target_result": result.get("stored"), "size": len(data)}, b""


class _BadRequest(Exception):
    pass


class CoordinatorLink:
    """Registers the node with the coordinator and sends periodic heartbeats over HTTP."""

    def __init__(self, server: StorageNodeServer, coordinator_url: str, settings: Settings,
                 advertise_host: str) -> None:
        self.server = server
        self.url = coordinator_url.rstrip("/")
        self.settings = settings
        self.advertise_host = advertise_host
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="heartbeat", daemon=True)
        self.registered = False

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=3)

    def _post(self, path: str, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        req = urllib.request.Request(
            self.url + path,
            data=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json", "X-Cluster-Token": self.settings.cluster_secret},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:  # noqa: S310 - configured URL
                return resp.status, json.loads(resp.read() or b"{}")
        except urllib.error.HTTPError as exc:
            try:
                body_json = json.loads(exc.read() or b"{}")
            except ValueError:
                body_json = {}
            return exc.code, body_json

    def _registration_body(self) -> dict[str, Any]:
        st = self.server.store
        return {
            "node_id": self.server.node_id,
            "host": self.advertise_host,
            "port": self.server.port,
            "data_dir": str(st.root),
            "storage_uuid": st.storage_uuid,
            "capacity_bytes": st.capacity_bytes,
            "used_bytes": st.used_bytes,
            "chunk_count": st.chunk_count,
        }

    def _run(self) -> None:
        failures = 0
        while not self._stop.is_set():
            try:
                if not self.registered:
                    status, body = self._post("/api/v1/nodes/register", self._registration_body())
                    if status in (200, 201):
                        self.registered = True
                        failures = 0
                        log.info("registered with coordinator at %s", self.url)
                    else:
                        failures += 1
                        log.warning("registration rejected (%s): %s", status, body.get("error", body))
                else:
                    st = self.server.store
                    status, body = self._post(
                        f"/api/v1/nodes/{self.server.node_id}/heartbeat",
                        {"storage_uuid": st.storage_uuid, "used_bytes": st.used_bytes,
                         "chunk_count": st.chunk_count, "capacity_bytes": st.capacity_bytes},
                    )
                    if status == 200:
                        failures = 0
                    elif status in (404, 409):
                        log.warning("coordinator does not recognise this node (%s); re-registering", status)
                        self.registered = False
                    elif status == 410:
                        log.warning("node has been removed from the cluster; heartbeats paused")
                        failures += 1
                    else:
                        failures += 1
            except (urllib.error.URLError, OSError, ValueError) as exc:
                failures += 1
                if failures in (1, 5) or failures % 30 == 0:
                    log.warning("coordinator unreachable: %s", exc)
                self.registered = self.registered and failures < 3
            delay = self.settings.heartbeat_interval
            if failures:
                delay = max(delay, backoff_delay(failures, self.settings.heartbeat_interval, 10.0))
            self._stop.wait(delay)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Distributed file storage node")
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--host", default=None, help="bind address (default: settings.node_host)")
    parser.add_argument("--advertise-host", default=None, help="address the coordinator should use")
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--coordinator-url", default=None)
    parser.add_argument("--capacity-bytes", type=int, default=None)
    parser.add_argument("--config", default=None)
    args = parser.parse_args(argv)

    settings = load_settings(args.config)
    setup_logging(settings.log_level, component=args.node_id)
    if not is_valid_node_id(args.node_id):
        parser.error("node-id must match [A-Za-z0-9_.-]{1,64}")
    host = args.host or settings.node_host
    capacity = args.capacity_bytes if args.capacity_bytes is not None else settings.node_capacity_bytes
    store = ChunkStore(args.data_dir, args.node_id, capacity)
    server = StorageNodeServer(
        node_id=args.node_id, host=host, port=args.port, store=store, secret=settings.cluster_secret,
        max_payload=settings.max_payload_size, io_timeout=settings.io_timeout,
        connect_timeout=settings.connect_timeout, idle_timeout=settings.node_idle_timeout,
        max_connections=settings.node_max_connections,
    )
    server.start()
    advertise = args.advertise_host or (host if host not in ("0.0.0.0", "") else "127.0.0.1")
    link = CoordinatorLink(server, args.coordinator_url or settings.coordinator_url, settings, advertise)
    link.start()

    stop = threading.Event()

    def _handle(signum: int, frame: Any) -> None:
        stop.set()

    signal.signal(signal.SIGINT, _handle)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle)
    if hasattr(signal, "SIGBREAK"):  # Windows Ctrl+Break
        signal.signal(signal.SIGBREAK, _handle)
    log.info("node %s ready (data_dir=%s, chunks=%d)", args.node_id, store.root, store.chunk_count)
    while not stop.is_set():
        stop.wait(0.5)
    log.info("node %s shutting down", args.node_id)
    link.stop()
    server.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
