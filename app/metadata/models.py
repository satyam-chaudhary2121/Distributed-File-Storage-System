"""Metadata enums, state machines and record types."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, fields
from enum import Enum
from typing import Any, TypeVar


class FileState(str, Enum):
    PENDING = "PENDING"
    UPLOADING = "UPLOADING"
    COMMITTED = "COMMITTED"
    FAILED = "FAILED"
    DELETING = "DELETING"
    DELETED = "DELETED"


FILE_TRANSITIONS: dict[FileState, frozenset[FileState]] = {
    FileState.PENDING: frozenset({FileState.UPLOADING, FileState.FAILED}),
    FileState.UPLOADING: frozenset({FileState.COMMITTED, FileState.FAILED}),
    FileState.COMMITTED: frozenset({FileState.DELETING}),
    FileState.FAILED: frozenset({FileState.DELETING}),
    FileState.DELETING: frozenset({FileState.DELETED}),
    FileState.DELETED: frozenset(),
}


def can_transition(current: FileState, new: FileState) -> bool:
    return new in FILE_TRANSITIONS[FileState(current)]


class NodeHealth(str, Enum):
    HEALTHY = "HEALTHY"
    SUSPECT = "SUSPECT"  # missed recent probes; still counted as holding data
    DOWN = "DOWN"  # unreachable for longer than failure_timeout


class AdminState(str, Enum):
    ACTIVE = "ACTIVE"  # member of the hash ring, receives new data
    DRAINING = "DRAINING"  # leaving: no new data, replicas being moved off
    DRAINED = "DRAINED"  # holds no required data; can be removed
    REMOVED = "REMOVED"  # no longer part of the cluster


class ReplicaStatus(str, Enum):
    PENDING = "PENDING"  # write in progress or unverified
    HEALTHY = "HEALTHY"  # bytes stored and checksum verified
    CORRUPTED = "CORRUPTED"  # bytes present but checksum mismatch
    MISSING = "MISSING"  # node is reachable but no longer has the chunk


class ChunkStatus(str, Enum):
    OK = "OK"
    UNDER_REPLICATED = "UNDER_REPLICATED"
    UNAVAILABLE = "UNAVAILABLE"  # only copies are on DOWN nodes; may come back
    LOST = "LOST"  # no valid copy exists anywhere


class RepairState(str, Enum):
    RETRYING = "RETRYING"
    FAILED = "FAILED"  # retries exhausted; needs manual trigger with force
    BLOCKED = "BLOCKED"  # not enough eligible nodes to place more replicas
    UNAVAILABLE = "UNAVAILABLE"
    UNRECOVERABLE = "UNRECOVERABLE"


def classify_health(age_seconds: float, suspect_timeout: float, failure_timeout: float) -> NodeHealth:
    """Map time since the last successful probe to a health state."""
    if age_seconds <= suspect_timeout:
        return NodeHealth.HEALTHY
    if age_seconds <= failure_timeout:
        return NodeHealth.SUSPECT
    return NodeHealth.DOWN


R = TypeVar("R")


def _from_row(cls: type[R], row: sqlite3.Row | dict[str, Any]) -> R:
    keys = row.keys()
    return cls(**{f.name: row[f.name] for f in fields(cls) if f.name in keys})  # type: ignore[arg-type]


@dataclass(frozen=True)
class NodeRecord:
    node_id: str
    host: str
    port: int
    data_dir: str | None
    storage_uuid: str | None
    health: str
    admin_state: str
    last_seen: float | None
    last_heartbeat: float | None
    registered_at: float
    health_changed_at: float | None
    capacity_bytes: int
    used_bytes: int
    chunk_count: int
    needs_reconcile: int
    last_reconciled_at: float | None

    @classmethod
    def from_row(cls, row: Any) -> "NodeRecord":
        return _from_row(cls, row)

    @property
    def is_member(self) -> bool:
        return self.admin_state == AdminState.ACTIVE.value

    @property
    def is_reachable(self) -> bool:
        return self.admin_state != AdminState.REMOVED.value and self.health != NodeHealth.DOWN.value

    @property
    def is_writable(self) -> bool:
        return self.admin_state == AdminState.ACTIVE.value and self.health == NodeHealth.HEALTHY.value


@dataclass(frozen=True)
class FileRecord:
    file_id: str
    filename: str
    size: int
    checksum: str | None
    chunk_size: int
    chunk_count: int
    replication_factor: int
    state: str
    error: str | None
    created_at: float
    updated_at: float
    committed_at: float | None
    deleted_at: float | None

    @classmethod
    def from_row(cls, row: Any) -> "FileRecord":
        return _from_row(cls, row)


@dataclass(frozen=True)
class ChunkRecord:
    chunk_id: str
    file_id: str
    chunk_index: int
    size: int
    checksum: str
    created_at: float

    @classmethod
    def from_row(cls, row: Any) -> "ChunkRecord":
        return _from_row(cls, row)


@dataclass(frozen=True)
class ReplicaRecord:
    chunk_id: str
    node_id: str
    status: str
    verified_at: float | None
    created_at: float
    updated_at: float

    @classmethod
    def from_row(cls, row: Any) -> "ReplicaRecord":
        return _from_row(cls, row)


@dataclass(frozen=True)
class PendingDeletion:
    chunk_id: str
    node_id: str
    file_id: str | None
    reason: str
    attempts: int
    next_attempt_at: float
    last_error: str | None
    created_at: float

    @classmethod
    def from_row(cls, row: Any) -> "PendingDeletion":
        return _from_row(cls, row)


@dataclass(frozen=True)
class RepairTask:
    chunk_id: str
    state: str
    attempts: int
    next_attempt_at: float
    last_error: str | None
    updated_at: float

    @classmethod
    def from_row(cls, row: Any) -> "RepairTask":
        return _from_row(cls, row)
