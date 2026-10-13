from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel


class ReplicaLocation(BaseModel):
    node_id: str
    status: str
    node_health: Optional[str] = None
    node_admin_state: Optional[str] = None
    verified_at: Optional[float] = None


class ChunkInfo(BaseModel):
    index: int
    chunk_id: str
    size: int
    checksum: str
    status: str
    healthy_replicas: int
    locations: list[ReplicaLocation]


class ReplicationSummary(BaseModel):
    desired: int
    min_healthy_replicas: Optional[int] = None
    chunk_status_counts: dict[str, int]


class FileSummary(BaseModel):
    file_id: str
    filename: str
    size: int
    checksum: Optional[str] = None
    chunk_count: int
    chunk_size: int
    state: str
    replication_factor: int
    created_at: float
    updated_at: float
    committed_at: Optional[float] = None


class PendingCleanup(BaseModel):
    chunk_id: str
    node_id: str
    attempts: int
    last_error: Optional[str] = None


class FileDetail(FileSummary):
    error: Optional[str] = None
    replication: ReplicationSummary
    chunks: list[ChunkInfo]
    pending_cleanup: Optional[list[PendingCleanup]] = None


class FileList(BaseModel):
    items: list[FileSummary]
    total: int
    limit: int
    offset: int


class DeleteResult(BaseModel):
    file_id: str
    state: str
    deleted_replicas: int
    pending_cleanup: list[PendingCleanup]


class ErrorBody(BaseModel):
    code: str
    message: str
    details: dict[str, Any] = {}


class ErrorResponse(BaseModel):
    error: ErrorBody
