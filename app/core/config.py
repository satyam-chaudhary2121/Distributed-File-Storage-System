"""Configuration loading.

Precedence (lowest to highest):
  1. Defaults defined on :class:`Settings`.
  2. A YAML file (``DFS_CONFIG`` env var, or ``./config.yaml`` when present).
  3. Environment variables named ``DFS_<FIELD_NAME_UPPERCASE>``.
  4. Explicit overrides passed to :func:`load_settings` (used by CLIs and tests).
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, model_validator

MiB = 1024 * 1024
GiB = 1024 * MiB

ENV_PREFIX = "DFS_"
PROJECT_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseModel):
    """All tunables for the coordinator and storage nodes."""

    model_config = {"extra": "ignore"}

    # --- coordinator -------------------------------------------------------
    coordinator_host: str = "127.0.0.1"
    coordinator_port: int = Field(8000, ge=1, le=65535)
    database_path: str = "data/coordinator/metadata.db"
    temp_dir: str = "data/coordinator/tmp"

    # Shared secret used to authenticate coordinator <-> node traffic.
    cluster_secret: str = Field("dev-insecure-change-me", min_length=8)

    # --- data layout --------------------------------------------------------
    chunk_size: int = Field(1 * MiB, ge=1, le=256 * MiB)
    replication_factor: int = Field(3, ge=1, le=16)
    # Minimum number of verified replicas per chunk required to COMMIT an upload.
    write_quorum: int = Field(2, ge=1, le=16)
    max_upload_size: int = Field(1 * GiB, ge=0)
    max_payload_size: int = Field(64 * MiB, ge=1024)
    virtual_nodes: int = Field(64, ge=1, le=4096)

    # --- health -------------------------------------------------------------
    heartbeat_interval: float = Field(2.0, gt=0)
    health_check_interval: float = Field(2.0, gt=0)
    suspect_timeout: float = Field(6.0, gt=0)
    failure_timeout: float = Field(15.0, gt=0)

    # --- recovery -----------------------------------------------------------
    recovery_interval: float = Field(5.0, gt=0)
    recovery_max_attempts: int = Field(5, ge=1)
    recovery_backoff_base: float = Field(1.0, ge=0)
    recovery_backoff_max: float = Field(60.0, ge=0)
    orphan_grace_seconds: float = Field(300.0, ge=0)
    reconcile_verify_data: bool = True

    # --- network ------------------------------------------------------------
    connect_timeout: float = Field(3.0, gt=0)
    io_timeout: float = Field(30.0, gt=0)
    client_retries: int = Field(2, ge=0, le=10)
    node_max_connections: int = Field(64, ge=1)
    node_idle_timeout: float = Field(60.0, gt=0)

    # --- storage node defaults ----------------------------------------------
    node_host: str = "127.0.0.1"
    node_capacity_bytes: int = Field(10 * GiB, ge=0)

    log_level: str = "INFO"

    @model_validator(mode="after")
    def _check_consistency(self) -> "Settings":
        if self.chunk_size > self.max_payload_size:
            raise ValueError("chunk_size must not exceed max_payload_size")
        if self.write_quorum > self.replication_factor:
            raise ValueError("write_quorum must not exceed replication_factor")
        if self.failure_timeout <= self.suspect_timeout:
            raise ValueError("failure_timeout must be greater than suspect_timeout")
        return self

    @property
    def coordinator_url(self) -> str:
        return f"http://{self.coordinator_host}:{self.coordinator_port}"

    def resolve_path(self, value: str) -> Path:
        """Resolve a configured path relative to the project root."""
        p = Path(value)
        return p if p.is_absolute() else (PROJECT_ROOT / p)


def _read_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Config file {path} must contain a mapping")
    # Allow nested sections for readability: they are flattened.
    flat: dict[str, Any] = {}
    for key, value in data.items():
        if isinstance(value, dict):
            flat.update(value)
        else:
            flat[key] = value
    return flat


def _env_values() -> dict[str, Any]:
    values: dict[str, Any] = {}
    for name in Settings.model_fields:
        env_name = ENV_PREFIX + name.upper()
        if env_name in os.environ:
            values[name] = os.environ[env_name]
    return values


def load_settings(config_path: str | Path | None = None, **overrides: Any) -> Settings:
    values: dict[str, Any] = {}
    path = config_path or os.environ.get(ENV_PREFIX + "CONFIG")
    if path:
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Config file not found: {p}")
        values.update(_read_yaml(p))
    else:
        default = PROJECT_ROOT / "config.yaml"
        if default.exists():
            values.update(_read_yaml(default))
    values.update(_env_values())
    values.update({k: v for k, v in overrides.items() if v is not None})
    return Settings(**values)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return load_settings()
