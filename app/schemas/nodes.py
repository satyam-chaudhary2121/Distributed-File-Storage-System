from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from app.core.validation import NODE_ID_PATTERN


class RegisterNodeRequest(BaseModel):
    node_id: str = Field(pattern=NODE_ID_PATTERN)
    host: str = Field(min_length=1, max_length=255)
    port: int = Field(ge=1, le=65535)
    data_dir: Optional[str] = Field(default=None, max_length=4096)
    storage_uuid: str = Field(pattern=r"^[0-9a-f]{32}$")
    capacity_bytes: int = Field(ge=0)
    used_bytes: int = Field(default=0, ge=0)
    chunk_count: int = Field(default=0, ge=0)


class HeartbeatRequest(BaseModel):
    storage_uuid: str = Field(pattern=r"^[0-9a-f]{32}$")
    used_bytes: int = Field(ge=0)
    chunk_count: int = Field(ge=0)
    capacity_bytes: int = Field(ge=0)


class NodeInfo(BaseModel):
    node_id: str
    host: str
    port: int
    data_dir: Optional[str] = None
    health: str
    admin_state: str
    last_seen: Optional[float] = None
    last_heartbeat: Optional[float] = None
    seconds_since_seen: Optional[float] = None
    registered_at: float
    capacity_bytes: int
    used_bytes: int
    chunk_count: int
    replica_count: int
    needs_reconcile: bool
    last_reconciled_at: Optional[float] = None
