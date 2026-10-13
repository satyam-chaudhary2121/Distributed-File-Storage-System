"""Wires the coordinator's components together and owns their lifecycle."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor

from app.cluster.membership import MembershipService
from app.cluster.placement import PlacementService
from app.cluster.rebalance import Rebalancer
from app.core.config import Settings
from app.metadata.database import Database
from app.metadata.repository import MetadataRepository
from app.replication.manager import ChunkLocks, ReplicationManager
from app.replication.recovery import RecoveryManager
from app.services.deletion_service import DeletionService
from app.services.download_service import DownloadService
from app.services.file_service import FileService
from app.services.upload_service import UploadService
from app.storage.node_manager import NodeManager

log = logging.getLogger("dfs.coordinator")


class Coordinator:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.db = Database(settings.resolve_path(settings.database_path))
        self.repo = MetadataRepository(self.db)
        self.executor = ThreadPoolExecutor(max_workers=32, thread_name_prefix="replica-io")
        self.node_manager = NodeManager(self.repo, settings)
        self.placement = PlacementService(self.repo, settings.virtual_nodes)
        self.locks = ChunkLocks()
        self.replication = ReplicationManager(self.node_manager)
        self.deletion = DeletionService(settings, self.repo, self.node_manager)
        self.upload = UploadService(settings, self.repo, self.placement, self.node_manager, self.deletion,
                                    self.executor)
        self.download = DownloadService(settings, self.repo, self.placement, self.node_manager)
        self.files = FileService(self.repo, self.placement)
        self.recovery = RecoveryManager(settings, self.repo, self.node_manager, self.placement,
                                        self.replication, self.deletion, self.locks)
        self.membership = MembershipService(self.repo, self.node_manager, self.recovery)
        self.rebalancer = Rebalancer(self.repo, self.node_manager, self.placement, self.replication,
                                     self.recovery, self.locks)
        self.node_manager.on_node_down = self.recovery.wake
        self.node_manager.on_node_up = self.recovery.wake
        self._started = False

    def _pin_ring_settings(self) -> None:
        """Keep the hash ring stable across restarts even if config changes."""
        stored = self.repo.get_meta("virtual_nodes")
        if stored is None:
            self.repo.set_meta("virtual_nodes", self.settings.virtual_nodes)
        elif int(stored) != self.settings.virtual_nodes:
            log.warning("virtual_nodes in config (%s) differs from the value this cluster was created with "
                        "(%s); keeping %s so placement does not change unexpectedly",
                        self.settings.virtual_nodes, stored, stored)
            self.placement.vnodes = int(stored)

    def start(self) -> None:
        if self._started:
            return
        self.db.initialize()
        self._pin_ring_settings()
        self.upload.abort_interrupted_uploads()
        # Anything may have changed on node disks while the coordinator was down.
        self.repo.mark_all_nodes_for_reconcile()
        self.node_manager.start()
        self.recovery.start()
        self._started = True
        log.info("coordinator started (db=%s, R=%d, quorum=%d, chunk_size=%d, vnodes=%d)",
                 self.db.path, self.settings.replication_factor, self.settings.write_quorum,
                 self.settings.chunk_size, self.placement.vnodes)

    def stop(self) -> None:
        if not self._started:
            return
        self.recovery.stop()
        self.node_manager.stop()
        self.executor.shutdown(wait=True, cancel_futures=True)
        self.db.close_all()
        self._started = False
        log.info("coordinator stopped")
