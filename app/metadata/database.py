"""SQLite connection management and schema initialisation.

The coordinator's SQLite database is the authoritative source of logical
cluster state (files, chunks, replica locations, node membership).  Storage
nodes are authoritative only for the bytes they physically hold; their
inventories are reconciled against this database.

``initialize()`` is non-destructive: it only creates missing tables/indexes.
Destructive reset lives in ``scripts/reset_cluster.py``.
"""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Sequence

SCHEMA_VERSION = 1

SCHEMA = """
CREATE TABLE IF NOT EXISTS cluster_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS nodes (
    node_id            TEXT PRIMARY KEY,
    host               TEXT NOT NULL,
    port               INTEGER NOT NULL CHECK (port BETWEEN 1 AND 65535),
    data_dir           TEXT,
    storage_uuid       TEXT,
    health             TEXT NOT NULL DEFAULT 'SUSPECT'
                       CHECK (health IN ('HEALTHY','SUSPECT','DOWN')),
    admin_state        TEXT NOT NULL DEFAULT 'ACTIVE'
                       CHECK (admin_state IN ('ACTIVE','DRAINING','DRAINED','REMOVED')),
    last_seen          REAL,
    last_heartbeat     REAL,
    registered_at      REAL NOT NULL,
    health_changed_at  REAL,
    capacity_bytes     INTEGER NOT NULL DEFAULT 0 CHECK (capacity_bytes >= 0),
    used_bytes         INTEGER NOT NULL DEFAULT 0 CHECK (used_bytes >= 0),
    chunk_count        INTEGER NOT NULL DEFAULT 0 CHECK (chunk_count >= 0),
    needs_reconcile    INTEGER NOT NULL DEFAULT 1 CHECK (needs_reconcile IN (0,1)),
    last_reconciled_at REAL
);

CREATE TABLE IF NOT EXISTS files (
    file_id            TEXT PRIMARY KEY,
    filename           TEXT NOT NULL,
    size               INTEGER NOT NULL CHECK (size >= 0),
    checksum           TEXT,
    chunk_size         INTEGER NOT NULL CHECK (chunk_size > 0),
    chunk_count        INTEGER NOT NULL DEFAULT 0 CHECK (chunk_count >= 0),
    replication_factor INTEGER NOT NULL CHECK (replication_factor >= 1),
    state              TEXT NOT NULL
                       CHECK (state IN ('PENDING','UPLOADING','COMMITTED','FAILED','DELETING','DELETED')),
    error              TEXT,
    created_at         REAL NOT NULL,
    updated_at         REAL NOT NULL,
    committed_at       REAL,
    deleted_at         REAL
);
CREATE INDEX IF NOT EXISTS idx_files_state_created ON files(state, created_at);

CREATE TABLE IF NOT EXISTS chunks (
    chunk_id    TEXT PRIMARY KEY,
    file_id     TEXT NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL CHECK (chunk_index >= 0),
    size        INTEGER NOT NULL CHECK (size >= 0),
    checksum    TEXT NOT NULL,
    created_at  REAL NOT NULL,
    UNIQUE (file_id, chunk_index)
);

CREATE TABLE IF NOT EXISTS replicas (
    chunk_id    TEXT NOT NULL REFERENCES chunks(chunk_id) ON DELETE CASCADE,
    node_id     TEXT NOT NULL REFERENCES nodes(node_id),
    status      TEXT NOT NULL CHECK (status IN ('PENDING','HEALTHY','CORRUPTED','MISSING')),
    verified_at REAL,
    created_at  REAL NOT NULL,
    updated_at  REAL NOT NULL,
    PRIMARY KEY (chunk_id, node_id)
);
CREATE INDEX IF NOT EXISTS idx_replicas_node ON replicas(node_id, status);

CREATE TABLE IF NOT EXISTS pending_deletions (
    chunk_id        TEXT NOT NULL,
    node_id         TEXT NOT NULL REFERENCES nodes(node_id),
    file_id         TEXT REFERENCES files(file_id) ON DELETE SET NULL,
    reason          TEXT NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    next_attempt_at REAL NOT NULL DEFAULT 0,
    last_error      TEXT,
    created_at      REAL NOT NULL,
    PRIMARY KEY (chunk_id, node_id)
);
CREATE INDEX IF NOT EXISTS idx_pending_del_file ON pending_deletions(file_id);
CREATE INDEX IF NOT EXISTS idx_pending_del_due ON pending_deletions(next_attempt_at);

CREATE TABLE IF NOT EXISTS repair_tasks (
    chunk_id        TEXT PRIMARY KEY REFERENCES chunks(chunk_id) ON DELETE CASCADE,
    state           TEXT NOT NULL
                    CHECK (state IN ('RETRYING','FAILED','BLOCKED','UNAVAILABLE','UNRECOVERABLE')),
    attempts        INTEGER NOT NULL DEFAULT 0,
    next_attempt_at REAL NOT NULL DEFAULT 0,
    last_error      TEXT,
    updated_at      REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS operations (
    op_id      TEXT PRIMARY KEY,
    op_type    TEXT NOT NULL
               CHECK (op_type IN ('UPLOAD','DELETE','RECOVERY','REBALANCE','DRAIN','RECONCILE')),
    target     TEXT,
    state      TEXT NOT NULL CHECK (state IN ('RUNNING','SUCCEEDED','FAILED','PARTIAL')),
    details    TEXT,
    error      TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_operations_type ON operations(op_type, created_at);
"""


class Database:
    """Thread-local SQLite connections with explicit transactions."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._local = threading.local()
        self._connections: list[sqlite3.Connection] = []
        self._lock = threading.Lock()

    def connect(self) -> sqlite3.Connection:
        conn: sqlite3.Connection | None = getattr(self._local, "conn", None)
        if conn is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(
                self.path, timeout=30.0, isolation_level=None, check_same_thread=False
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.execute("PRAGMA busy_timeout = 30000")
            self._local.conn = conn
            with self._lock:
                self._connections.append(conn)
        return conn

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """BEGIN IMMEDIATE transaction; nested use joins the outer transaction."""
        conn = self.connect()
        if conn.in_transaction:
            yield conn
            return
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")

    def execute(self, sql: str, params: Sequence[Any] = ()) -> sqlite3.Cursor:
        return self.connect().execute(sql, params)

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[sqlite3.Row]:
        return self.connect().execute(sql, params).fetchall()

    def query_one(self, sql: str, params: Sequence[Any] = ()) -> sqlite3.Row | None:
        return self.connect().execute(sql, params).fetchone()

    def initialize(self) -> None:
        conn = self.connect()
        conn.executescript(SCHEMA)
        row = conn.execute("SELECT value FROM cluster_meta WHERE key='schema_version'").fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO cluster_meta(key, value) VALUES ('schema_version', ?)", (str(SCHEMA_VERSION),)
            )
        elif int(row["value"]) != SCHEMA_VERSION:
            raise RuntimeError(
                f"Database schema version {row['value']} does not match expected {SCHEMA_VERSION}"
            )

    def close_all(self) -> None:
        with self._lock:
            for conn in self._connections:
                try:
                    conn.close()
                except sqlite3.Error:
                    pass
            self._connections.clear()
        self._local = threading.local()
