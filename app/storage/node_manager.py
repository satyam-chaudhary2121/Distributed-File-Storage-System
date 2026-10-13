"""Coordinator-side view of storage nodes: TCP clients and the health monitor.

Health model
------------
* The coordinator actively PINGs every non-removed node over TCP every
  ``health_check_interval``. A successful, identity-checked PING updates
  ``last_seen``. HTTP heartbeats from nodes refresh usage statistics and let
  a node re-register, but only TCP reachability counts as liveness, because
  TCP is the path data actually takes.
* Health is a function of ``now - last_seen``:
  ``<= suspect_timeout`` HEALTHY, ``<= failure_timeout`` SUSPECT, else DOWN.
  A single slow request never marks a node DOWN; it takes sustained silence.
* DOWN means *unavailable*, not *data lost*: replicas on a DOWN node are
  reported UNAVAILABLE and are re-verified when the node returns.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Callable

from app.core.config import Settings
from app.metadata.models import AdminState, NodeHealth, NodeRecord, classify_health
from app.metadata.repository import MetadataRepository
from app.storage.socket_client import NodeClient, NodeError

log = logging.getLogger("dfs.health")


class NodeManager:
    def __init__(self, repo: MetadataRepository, settings: Settings) -> None:
        self.repo = repo
        self.settings = settings
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._pool = ThreadPoolExecutor(max_workers=16, thread_name_prefix="probe")
        self.on_node_down: Callable[[NodeRecord], None] | None = None
        self.on_node_up: Callable[[NodeRecord], None] | None = None

    # ------------------------------------------------------------- clients
    def client(self, node: NodeRecord, *, retries: int | None = None, io_timeout: float | None = None) -> NodeClient:
        s = self.settings
        return NodeClient(
            node.host, node.port, s.cluster_secret, node_id=node.node_id,
            connect_timeout=s.connect_timeout, io_timeout=io_timeout or s.io_timeout,
            max_payload=s.max_payload_size, retries=s.client_retries if retries is None else retries,
        )

    def probe_client(self, node: NodeRecord) -> NodeClient:
        timeout = min(self.settings.connect_timeout, max(0.5, self.settings.suspect_timeout / 2))
        return NodeClient(
            node.host, node.port, self.settings.cluster_secret, node_id=node.node_id,
            connect_timeout=timeout, io_timeout=timeout, max_payload=self.settings.max_payload_size,
        )

    # ------------------------------------------------------- health monitor
    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="health-monitor", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        self._pool.shutdown(wait=False, cancel_futures=True)

    def _loop(self) -> None:
        while not self._stop.wait(self.settings.health_check_interval):
            try:
                self.check_all()
            except Exception:  # noqa: BLE001 - monitor must keep running
                log.exception("health check cycle failed")

    def probe(self, node: NodeRecord) -> bool:
        try:
            resp = self.probe_client(node).ping()
        except NodeError as exc:
            log.debug("probe of %s failed: %s", node.node_id, exc)
            return False
        self.repo.record_probe_success(
            node.node_id, int(resp.get("used_bytes", 0)), int(resp.get("chunk_count", 0))
        )
        return True

    def check_all(self) -> dict[str, str]:
        nodes = [n for n in self.repo.list_nodes() if n.admin_state != AdminState.REMOVED.value]
        if not self._stop.is_set():
            list(self._pool.map(self.probe, nodes))
        return self.evaluate_health()

    def evaluate_health(self, now: float | None = None) -> dict[str, str]:
        now = now or time.time()
        result: dict[str, str] = {}
        for node in self.repo.list_nodes():
            reference = node.last_seen or node.registered_at
            new = classify_health(now - reference, self.settings.suspect_timeout, self.settings.failure_timeout)
            result[node.node_id] = new.value
            if new.value == node.health:
                continue
            previous = self.repo.set_node_health(node.node_id, new, now)
            log.warning("node %s health %s -> %s", node.node_id, previous, new.value)
            updated = self.repo.get_node(node.node_id)
            if updated is None:
                continue
            if new == NodeHealth.DOWN:
                affected = self.repo.count_replicas_on_node(node.node_id)
                log.warning("node %s is DOWN; %d replica(s) now unavailable, recovery scheduled",
                            node.node_id, affected)
                if self.on_node_down:
                    self.on_node_down(updated)
            elif previous == NodeHealth.DOWN.value and self.on_node_up:
                self.on_node_up(updated)
        return result
