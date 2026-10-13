"""Cross-platform launcher for a local cluster of real OS processes.

Used by ``scripts/start_cluster.py``, the demo, the benchmark and the
integration/failure tests. Every node and the coordinator is a separate
``python -m ...`` subprocess with its own port and data directory, so nodes
can be killed and restarted independently.
"""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, TypeVar

import httpx

from app.core.config import PROJECT_ROOT

T = TypeVar("T")
IS_WINDOWS = os.name == "nt"


def free_port(host: str = "127.0.0.1") -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, 0))
        return int(s.getsockname()[1])


def wait_for(predicate: Callable[[], T], timeout: float, interval: float = 0.2,
             description: str = "condition") -> T:
    """Poll ``predicate`` until it returns a truthy value or ``timeout`` elapses."""
    deadline = time.monotonic() + timeout
    last_exc: BaseException | None = None
    while True:
        try:
            result = predicate()
            if result:
                return result
        except Exception as exc:  # noqa: BLE001 - keep polling, report at the end
            last_exc = exc
        if time.monotonic() >= deadline:
            raise TimeoutError(f"timed out after {timeout:.1f}s waiting for {description}"
                               + (f" (last error: {last_exc})" if last_exc else ""))
        time.sleep(interval)


@dataclass
class ManagedProcess:
    name: str
    args: list[str]
    env: dict[str, str]
    log_path: Path
    popen: subprocess.Popen | None = None
    _log_handle: Any = None

    def start(self, detach: bool = False) -> None:
        if self.alive:
            return
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._log_handle = open(self.log_path, "ab")  # noqa: SIM115 - closed in _close_log
        kwargs: dict[str, Any] = {}
        if IS_WINDOWS:
            flags = subprocess.CREATE_NEW_PROCESS_GROUP  # type: ignore[attr-defined]
            if detach:
                flags |= subprocess.DETACHED_PROCESS  # type: ignore[attr-defined]
            kwargs["creationflags"] = flags
        elif detach:
            kwargs["start_new_session"] = True
        self.popen = subprocess.Popen(
            self.args, cwd=str(PROJECT_ROOT), env=self.env, stdout=self._log_handle,
            stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, **kwargs,
        )

    @property
    def pid(self) -> int | None:
        return self.popen.pid if self.popen else None

    @property
    def alive(self) -> bool:
        return self.popen is not None and self.popen.poll() is None

    def kill(self) -> None:
        """Abrupt termination (SIGKILL / TerminateProcess): simulates a crash."""
        if self.popen is not None and self.alive:
            self.popen.kill()
            self.popen.wait(timeout=10)
        self._close_log()

    def stop(self, timeout: float = 10.0) -> None:
        """Graceful termination, escalating to kill."""
        if self.popen is not None and self.alive:
            try:
                if IS_WINDOWS:
                    self.popen.send_signal(signal.CTRL_BREAK_EVENT)  # type: ignore[attr-defined]
                else:
                    self.popen.terminate()
                self.popen.wait(timeout=timeout)
            except (subprocess.TimeoutExpired, OSError, ValueError):
                self.popen.kill()
                self.popen.wait(timeout=10)
        self._close_log()

    def _close_log(self) -> None:
        if self._log_handle is not None:
            try:
                self._log_handle.close()
            except OSError:
                pass
            self._log_handle = None


@dataclass
class NodeSpec:
    node_id: str
    port: int
    data_dir: Path
    capacity_bytes: int | None = None


@dataclass
class LocalCluster:
    base_dir: Path
    num_nodes: int = 3
    settings_env: dict[str, str] = field(default_factory=dict)
    coordinator_port: int | None = None
    node_ports: list[int] | None = None
    detach: bool = False
    python: str = sys.executable

    def __post_init__(self) -> None:
        self.base_dir = Path(self.base_dir).resolve()
        self.coordinator_port = self.coordinator_port or free_port()
        self.nodes: dict[str, NodeSpec] = {}
        self.node_procs: dict[str, ManagedProcess] = {}
        self.coordinator: ManagedProcess | None = None
        self.secret = self.settings_env.get("DFS_CLUSTER_SECRET") or os.environ.get(
            "DFS_CLUSTER_SECRET", "local-cluster-secret-" + os.urandom(8).hex())
        ports = list(self.node_ports or [])
        for i in range(1, self.num_nodes + 1):
            port = ports[i - 1] if i - 1 < len(ports) else free_port()
            self.nodes[f"node-{i}"] = NodeSpec(f"node-{i}", port, self.base_dir / "nodes" / f"node-{i}")

    # ------------------------------------------------------------ settings
    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.coordinator_port}"

    def _env(self) -> dict[str, str]:
        env = dict(os.environ)
        env.update({
            "PYTHONPATH": str(PROJECT_ROOT) + os.pathsep + env.get("PYTHONPATH", ""),
            "PYTHONUNBUFFERED": "1",
            "DFS_CLUSTER_SECRET": self.secret,
            "DFS_COORDINATOR_HOST": "127.0.0.1",
            "DFS_COORDINATOR_PORT": str(self.coordinator_port),
            "DFS_DATABASE_PATH": str(self.base_dir / "coordinator" / "metadata.db"),
            "DFS_TEMP_DIR": str(self.base_dir / "coordinator" / "tmp"),
        })
        env.update(self.settings_env)
        env["DFS_CLUSTER_SECRET"] = self.secret
        return env

    # -------------------------------------------------------- coordinator
    def start_coordinator(self, wait: bool = True) -> None:
        self.coordinator = ManagedProcess(
            "coordinator", [self.python, "-m", "app.main", "--port", str(self.coordinator_port),
                            "--host", "127.0.0.1"],
            self._env(), self.base_dir / "logs" / "coordinator.log",
        )
        self.coordinator.start(detach=self.detach)
        if wait:
            wait_for(self._coordinator_up, 30, description="coordinator /health")

    def _coordinator_up(self) -> bool:
        if self.coordinator is not None and not self.coordinator.alive:
            raise RuntimeError(f"coordinator exited; see {self.coordinator.log_path}")
        return httpx.get(self.url + "/health", timeout=1).status_code == 200

    def stop_coordinator(self) -> None:
        if self.coordinator:
            self.coordinator.stop()

    def kill_coordinator(self) -> None:
        if self.coordinator:
            self.coordinator.kill()

    # --------------------------------------------------------------- nodes
    def add_node(self, node_id: str | None = None, capacity_bytes: int | None = None,
                 start: bool = True) -> NodeSpec:
        node_id = node_id or f"node-{len(self.nodes) + 1}"
        spec = NodeSpec(node_id, free_port(), self.base_dir / "nodes" / node_id, capacity_bytes)
        self.nodes[node_id] = spec
        if start:
            self.start_node(node_id)
        return spec

    def start_node(self, node_id: str, wait: bool = True) -> None:
        spec = self.nodes[node_id]
        args = [self.python, "-m", "app.storage.node_server", "--node-id", node_id, "--port", str(spec.port),
                "--host", "127.0.0.1", "--data-dir", str(spec.data_dir), "--coordinator-url", self.url]
        if spec.capacity_bytes is not None:
            args += ["--capacity-bytes", str(spec.capacity_bytes)]
        proc = ManagedProcess(node_id, args, self._env(), self.base_dir / "logs" / f"{node_id}.log")
        self.node_procs[node_id] = proc
        proc.start(detach=self.detach)
        if wait:
            self.wait_node_listening(node_id)

    def wait_node_listening(self, node_id: str, timeout: float = 20.0) -> None:
        """Block until the node process answers an authenticated PING over TCP."""
        from app.storage.socket_client import NodeClient

        spec, proc = self.nodes[node_id], self.node_procs[node_id]

        def up() -> bool:
            if not proc.alive:
                raise RuntimeError(f"{node_id} exited; see {proc.log_path}")
            NodeClient("127.0.0.1", spec.port, self.secret, node_id=node_id, connect_timeout=0.5,
                       io_timeout=2).ping()
            return True

        wait_for(up, timeout, interval=0.1, description=f"{node_id} TCP listener")

    def kill_node(self, node_id: str) -> None:
        self.node_procs[node_id].kill()

    def stop_node(self, node_id: str) -> None:
        self.node_procs[node_id].stop()

    def node_alive(self, node_id: str) -> bool:
        proc = self.node_procs.get(node_id)
        return proc is not None and proc.alive

    # ------------------------------------------------------------- startup
    def start(self, wait_healthy: bool = True, timeout: float = 30.0) -> "LocalCluster":
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.start_coordinator()
        for node_id in list(self.nodes):
            self.start_node(node_id, wait=False)
        if wait_healthy:
            self.wait_nodes_healthy(list(self.nodes), timeout=timeout)
        return self

    def api(self, timeout: float = 120.0) -> httpx.Client:
        return httpx.Client(base_url=self.url, timeout=timeout)

    def node_states(self) -> dict[str, dict[str, Any]]:
        resp = httpx.get(self.url + "/api/v1/nodes", timeout=5)
        resp.raise_for_status()
        return {n["node_id"]: n for n in resp.json()}

    def wait_nodes_healthy(self, node_ids: list[str], timeout: float = 30.0) -> None:
        def ok() -> bool:
            for nid in node_ids:
                if nid in self.node_procs and not self.node_procs[nid].alive:
                    raise RuntimeError(f"{nid} exited; see {self.node_procs[nid].log_path}")
            states = self.node_states()
            return all(nid in states and states[nid]["health"] == "HEALTHY" for nid in node_ids)

        wait_for(ok, timeout, description=f"nodes {node_ids} HEALTHY")

    def wait_node_health(self, node_id: str, health: str, timeout: float = 30.0) -> None:
        wait_for(lambda: self.node_states()[node_id]["health"] == health, timeout,
                 description=f"{node_id} {health}")

    # ---------------------------------------------------------------- disk
    def chunk_path(self, node_id: str, chunk_id: str) -> Path:
        return self.nodes[node_id].data_dir / "chunks" / chunk_id[:2] / f"{chunk_id}.chunk"

    def chunks_on_disk(self, node_id: str) -> set[str]:
        root = self.nodes[node_id].data_dir / "chunks"
        return {p.stem for p in root.glob("*/*.chunk")} if root.exists() else set()

    # ------------------------------------------------------------ shutdown
    def shutdown(self) -> None:
        for proc in self.node_procs.values():
            proc.stop(timeout=5)
        if self.coordinator:
            self.coordinator.stop(timeout=10)

    def __enter__(self) -> "LocalCluster":
        return self.start()

    def __exit__(self, *exc: Any) -> None:
        self.shutdown()

    # -------------------------------------------------- state file (scripts)
    def state(self) -> dict[str, Any]:
        return {
            "base_dir": str(self.base_dir),
            "coordinator": {"pid": self.coordinator.pid if self.coordinator else None,
                            "port": self.coordinator_port, "url": self.url},
            "nodes": {nid: {"pid": self.node_procs[nid].pid if nid in self.node_procs else None,
                            "port": spec.port, "data_dir": str(spec.data_dir)}
                      for nid, spec in self.nodes.items()},
        }

    def write_state(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.state(), indent=2), encoding="utf-8")


def terminate_pid(pid: int, timeout: float = 10.0) -> bool:
    """Terminate a process started by another invocation (stop_cluster.py)."""
    try:
        if IS_WINDOWS:
            os.kill(pid, signal.SIGTERM)  # TerminateProcess on Windows
        else:
            os.kill(pid, signal.SIGTERM)
    except (ProcessLookupError, OSError):
        return False
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not pid_alive(pid):
            return True
        time.sleep(0.2)
    if not IS_WINDOWS:
        try:
            os.kill(pid, signal.SIGKILL)
        except (ProcessLookupError, OSError):
            pass
    return True


def pid_alive(pid: int) -> bool:
    if IS_WINDOWS:
        import ctypes

        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
        handle = kernel32.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
        if not handle:
            return False
        code = ctypes.c_ulong()
        kernel32.GetExitCodeProcess(handle, ctypes.byref(code))
        kernel32.CloseHandle(handle)
        return code.value == 259  # STILL_ACTIVE
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    try:  # reap if it is our zombie child
        waited, _ = os.waitpid(pid, os.WNOHANG)
        return waited == 0
    except ChildProcessError:
        return True
