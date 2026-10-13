"""All SQL access for coordinator metadata lives here."""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from collections import defaultdict
from typing import Any, Iterable

from app.core.exceptions import InvalidStateTransition, NotFound
from app.metadata.database import Database
from app.metadata.models import (
    AdminState,
    ChunkRecord,
    FileRecord,
    FileState,
    NodeHealth,
    NodeRecord,
    PendingDeletion,
    RepairTask,
    ReplicaRecord,
    ReplicaStatus,
    can_transition,
)


def _now() -> float:
    return time.time()


class MetadataRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    # ------------------------------------------------------------------ meta
    def get_meta(self, key: str) -> str | None:
        row = self.db.query_one("SELECT value FROM cluster_meta WHERE key = ?", (key,))
        return row["value"] if row else None

    def set_meta(self, key: str, value: Any) -> None:
        self.db.execute(
            "INSERT INTO cluster_meta(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, str(value)),
        )

    # ----------------------------------------------------------------- nodes
    def get_node(self, node_id: str) -> NodeRecord | None:
        row = self.db.query_one("SELECT * FROM nodes WHERE node_id = ?", (node_id,))
        return NodeRecord.from_row(row) if row else None

    def list_nodes(self, include_removed: bool = False) -> list[NodeRecord]:
        sql = "SELECT * FROM nodes"
        if not include_removed:
            sql += " WHERE admin_state != 'REMOVED'"
        sql += " ORDER BY node_id"
        return [NodeRecord.from_row(r) for r in self.db.query(sql)]

    def nodes_by_id(self, include_removed: bool = True) -> dict[str, NodeRecord]:
        return {n.node_id: n for n in self.list_nodes(include_removed=include_removed)}

    def find_node_by_address(self, host: str, port: int, exclude_node_id: str) -> NodeRecord | None:
        row = self.db.query_one(
            "SELECT * FROM nodes WHERE host = ? AND port = ? AND node_id != ? AND admin_state != 'REMOVED'",
            (host, port, exclude_node_id),
        )
        return NodeRecord.from_row(row) if row else None

    def register_node(
        self,
        *,
        node_id: str,
        host: str,
        port: int,
        data_dir: str | None,
        storage_uuid: str | None,
        capacity_bytes: int,
        used_bytes: int,
        chunk_count: int,
        ts: float | None = None,
    ) -> tuple[NodeRecord, bool]:
        """Insert or refresh a node. Returns (record, created)."""
        ts = ts or _now()
        with self.db.transaction() as conn:
            existing = conn.execute("SELECT * FROM nodes WHERE node_id = ?", (node_id,)).fetchone()
            if existing is None:
                conn.execute(
                    """INSERT INTO nodes(node_id, host, port, data_dir, storage_uuid, health, admin_state,
                           last_seen, last_heartbeat, registered_at, health_changed_at, capacity_bytes,
                           used_bytes, chunk_count, needs_reconcile)
                       VALUES (?, ?, ?, ?, ?, 'HEALTHY', 'ACTIVE', ?, ?, ?, ?, ?, ?, ?, 1)""",
                    (node_id, host, port, data_dir, storage_uuid, ts, ts, ts, ts,
                     capacity_bytes, used_bytes, chunk_count),
                )
                created = True
            else:
                health_changed = existing["health"] != NodeHealth.HEALTHY.value
                conn.execute(
                    """UPDATE nodes SET host = ?, port = ?, data_dir = ?, storage_uuid = ?,
                           health = 'HEALTHY', last_seen = ?, last_heartbeat = ?,
                           health_changed_at = CASE WHEN ? THEN ? ELSE health_changed_at END,
                           capacity_bytes = ?, used_bytes = ?, chunk_count = ?, needs_reconcile = 1
                       WHERE node_id = ?""",
                    (host, port, data_dir, storage_uuid, ts, ts, int(health_changed), ts,
                     capacity_bytes, used_bytes, chunk_count, node_id),
                )
                created = False
            row = conn.execute("SELECT * FROM nodes WHERE node_id = ?", (node_id,)).fetchone()
        return NodeRecord.from_row(row), created

    def record_heartbeat(self, node_id: str, used_bytes: int, chunk_count: int, capacity_bytes: int,
                         ts: float | None = None) -> None:
        self.db.execute(
            "UPDATE nodes SET last_heartbeat = ?, used_bytes = ?, chunk_count = ?, capacity_bytes = ? "
            "WHERE node_id = ?",
            (ts or _now(), used_bytes, chunk_count, capacity_bytes, node_id),
        )

    def record_probe_success(self, node_id: str, used_bytes: int, chunk_count: int,
                             ts: float | None = None) -> None:
        self.db.execute(
            "UPDATE nodes SET last_seen = ?, used_bytes = ?, chunk_count = ? WHERE node_id = ?",
            (ts or _now(), used_bytes, chunk_count, node_id),
        )

    def set_node_health(self, node_id: str, health: NodeHealth, ts: float | None = None) -> str | None:
        """Set health; returns the previous value. Recovery from DOWN schedules a reconcile."""
        ts = ts or _now()
        with self.db.transaction() as conn:
            row = conn.execute("SELECT health FROM nodes WHERE node_id = ?", (node_id,)).fetchone()
            if row is None:
                return None
            previous = row["health"]
            if previous == health.value:
                return previous
            needs = 1 if (previous == NodeHealth.DOWN.value and health != NodeHealth.DOWN) else None
            conn.execute(
                "UPDATE nodes SET health = ?, health_changed_at = ?, "
                "needs_reconcile = COALESCE(?, needs_reconcile) WHERE node_id = ?",
                (health.value, ts, needs, node_id),
            )
            return previous

    def set_admin_state(self, node_id: str, state: AdminState) -> None:
        self.db.execute("UPDATE nodes SET admin_state = ? WHERE node_id = ?", (state.value, node_id))

    def set_needs_reconcile(self, node_id: str, flag: bool = True) -> None:
        self.db.execute("UPDATE nodes SET needs_reconcile = ? WHERE node_id = ?", (int(flag), node_id))

    def mark_all_nodes_for_reconcile(self) -> None:
        self.db.execute("UPDATE nodes SET needs_reconcile = 1 WHERE admin_state != 'REMOVED'")

    def mark_reconciled(self, node_id: str, ts: float | None = None) -> None:
        self.db.execute(
            "UPDATE nodes SET needs_reconcile = 0, last_reconciled_at = ? WHERE node_id = ?",
            (ts or _now(), node_id),
        )

    # ----------------------------------------------------------------- files
    def create_file(self, *, file_id: str, filename: str, size: int, checksum: str | None,
                    chunk_size: int, replication_factor: int) -> FileRecord:
        ts = _now()
        self.db.execute(
            """INSERT INTO files(file_id, filename, size, checksum, chunk_size, chunk_count,
                   replication_factor, state, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, 0, ?, 'PENDING', ?, ?)""",
            (file_id, filename, size, checksum, chunk_size, replication_factor, ts, ts),
        )
        record = self.get_file(file_id)
        assert record is not None
        return record

    def get_file(self, file_id: str) -> FileRecord | None:
        row = self.db.query_one("SELECT * FROM files WHERE file_id = ?", (file_id,))
        return FileRecord.from_row(row) if row else None

    def _transition(self, conn: sqlite3.Connection, file_id: str, new: FileState,
                    error: str | None = None, extra: dict[str, Any] | None = None) -> FileRecord:
        row = conn.execute("SELECT * FROM files WHERE file_id = ?", (file_id,)).fetchone()
        if row is None:
            raise NotFound(f"File {file_id} not found")
        current = FileState(row["state"])
        if not can_transition(current, new):
            raise InvalidStateTransition(
                f"File {file_id} cannot move from {current.value} to {new.value}",
                details={"file_id": file_id, "state": current.value},
            )
        ts = _now()
        sets = {"state": new.value, "updated_at": ts}
        if error is not None:
            sets["error"] = error
        if new == FileState.COMMITTED:
            sets["committed_at"] = ts
        if new == FileState.DELETED:
            sets["deleted_at"] = ts
        if extra:
            sets.update(extra)
        assignments = ", ".join(f"{k} = ?" for k in sets)
        conn.execute(f"UPDATE files SET {assignments} WHERE file_id = ?", (*sets.values(), file_id))
        return FileRecord.from_row(conn.execute("SELECT * FROM files WHERE file_id = ?", (file_id,)).fetchone())

    def transition_file(self, file_id: str, new: FileState, *, error: str | None = None,
                        extra: dict[str, Any] | None = None) -> FileRecord:
        with self.db.transaction() as conn:
            return self._transition(conn, file_id, new, error, extra)

    def list_files(self, *, state: FileState = FileState.COMMITTED, limit: int = 50, offset: int = 0,
                   name_contains: str | None = None) -> tuple[list[FileRecord], int]:
        where = "state = ?"
        params: list[Any] = [state.value]
        if name_contains:
            where += " AND filename LIKE ? ESCAPE '\\'"
            escaped = name_contains.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            params.append(f"%{escaped}%")
        total = self.db.query_one(f"SELECT COUNT(*) AS n FROM files WHERE {where}", params)["n"]
        rows = self.db.query(
            f"SELECT * FROM files WHERE {where} ORDER BY created_at DESC, file_id LIMIT ? OFFSET ?",
            (*params, limit, offset),
        )
        return [FileRecord.from_row(r) for r in rows], total

    def files_in_states(self, states: Iterable[FileState]) -> list[FileRecord]:
        values = [s.value for s in states]
        marks = ",".join("?" for _ in values)
        rows = self.db.query(f"SELECT * FROM files WHERE state IN ({marks}) ORDER BY created_at", values)
        return [FileRecord.from_row(r) for r in rows]

    def count_files_by_state(self) -> dict[str, int]:
        rows = self.db.query("SELECT state, COUNT(*) AS n FROM files GROUP BY state")
        return {r["state"]: r["n"] for r in rows}

    def committed_logical_bytes(self) -> int:
        row = self.db.query_one("SELECT COALESCE(SUM(size), 0) AS n FROM files WHERE state = 'COMMITTED'")
        return int(row["n"])

    # ---------------------------------------------------------------- chunks
    def add_chunk(self, *, chunk_id: str, file_id: str, chunk_index: int, size: int, checksum: str) -> ChunkRecord:
        ts = _now()
        self.db.execute(
            "INSERT INTO chunks(chunk_id, file_id, chunk_index, size, checksum, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (chunk_id, file_id, chunk_index, size, checksum, ts),
        )
        return ChunkRecord(chunk_id, file_id, chunk_index, size, checksum, ts)

    def get_chunks(self, file_id: str) -> list[ChunkRecord]:
        rows = self.db.query("SELECT * FROM chunks WHERE file_id = ? ORDER BY chunk_index", (file_id,))
        return [ChunkRecord.from_row(r) for r in rows]

    def get_chunk(self, chunk_id: str) -> ChunkRecord | None:
        row = self.db.query_one("SELECT * FROM chunks WHERE chunk_id = ?", (chunk_id,))
        return ChunkRecord.from_row(row) if row else None

    def get_chunk_file_state(self, chunk_id: str) -> tuple[ChunkRecord, str] | None:
        row = self.db.query_one(
            "SELECT c.*, f.state AS file_state, f.replication_factor AS rf FROM chunks c "
            "JOIN files f ON f.file_id = c.file_id WHERE c.chunk_id = ?",
            (chunk_id,),
        )
        if row is None:
            return None
        return ChunkRecord.from_row(row), row["file_state"]

    def committed_chunks_with_replicas(self) -> list[tuple[ChunkRecord, int, list[ReplicaRecord]]]:
        """Every chunk of every COMMITTED file with its desired RF and replica rows."""
        chunk_rows = self.db.query(
            "SELECT c.*, f.replication_factor AS rf FROM chunks c JOIN files f ON f.file_id = c.file_id "
            "WHERE f.state = 'COMMITTED' ORDER BY c.file_id, c.chunk_index"
        )
        replica_rows = self.db.query(
            "SELECT r.* FROM replicas r JOIN chunks c ON c.chunk_id = r.chunk_id "
            "JOIN files f ON f.file_id = c.file_id WHERE f.state = 'COMMITTED'"
        )
        by_chunk: dict[str, list[ReplicaRecord]] = defaultdict(list)
        for r in replica_rows:
            by_chunk[r["chunk_id"]].append(ReplicaRecord.from_row(r))
        return [(ChunkRecord.from_row(c), int(c["rf"]), by_chunk.get(c["chunk_id"], [])) for c in chunk_rows]

    # -------------------------------------------------------------- replicas
    def upsert_replica(self, chunk_id: str, node_id: str, status: ReplicaStatus,
                       verified_at: float | None = None) -> None:
        ts = _now()
        self.db.execute(
            """INSERT INTO replicas(chunk_id, node_id, status, verified_at, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(chunk_id, node_id) DO UPDATE SET status = excluded.status,
                   verified_at = COALESCE(excluded.verified_at, replicas.verified_at),
                   updated_at = excluded.updated_at""",
            (chunk_id, node_id, status.value, verified_at, ts, ts),
        )

    def record_verified_replica(self, chunk_id: str, node_id: str, ts: float | None = None) -> bool:
        """Mark a replica HEALTHY if its chunk still exists.

        If the chunk was deleted concurrently (file deletion raced with a
        recovery copy), schedule the freshly written bytes for deletion instead.
        """
        ts = ts or _now()
        with self.db.transaction() as conn:
            exists = conn.execute("SELECT 1 FROM chunks WHERE chunk_id = ?", (chunk_id,)).fetchone()
            if exists is None:
                self._add_pending_deletion(conn, chunk_id, node_id, "orphan_after_race", None)
                return False
            conn.execute(
                """INSERT INTO replicas(chunk_id, node_id, status, verified_at, created_at, updated_at)
                   VALUES (?, ?, 'HEALTHY', ?, ?, ?)
                   ON CONFLICT(chunk_id, node_id) DO UPDATE SET status = 'HEALTHY',
                       verified_at = excluded.verified_at, updated_at = excluded.updated_at""",
                (chunk_id, node_id, ts, ts, ts),
            )
            conn.execute("DELETE FROM pending_deletions WHERE chunk_id = ? AND node_id = ?", (chunk_id, node_id))
            return True

    def set_replica_status(self, chunk_id: str, node_id: str, status: ReplicaStatus,
                           verified_at: float | None = None) -> None:
        self.db.execute(
            "UPDATE replicas SET status = ?, updated_at = ?, verified_at = COALESCE(?, verified_at) "
            "WHERE chunk_id = ? AND node_id = ?",
            (status.value, _now(), verified_at, chunk_id, node_id),
        )

    def delete_replica(self, chunk_id: str, node_id: str) -> None:
        self.db.execute("DELETE FROM replicas WHERE chunk_id = ? AND node_id = ?", (chunk_id, node_id))

    def remove_replica_and_schedule_delete(self, chunk_id: str, node_id: str, reason: str) -> None:
        with self.db.transaction() as conn:
            row = conn.execute("SELECT file_id FROM chunks WHERE chunk_id = ?", (chunk_id,)).fetchone()
            conn.execute("DELETE FROM replicas WHERE chunk_id = ? AND node_id = ?", (chunk_id, node_id))
            self._add_pending_deletion(conn, chunk_id, node_id, reason, row["file_id"] if row else None)

    def replicas_for_chunk(self, chunk_id: str) -> list[ReplicaRecord]:
        rows = self.db.query("SELECT * FROM replicas WHERE chunk_id = ? ORDER BY node_id", (chunk_id,))
        return [ReplicaRecord.from_row(r) for r in rows]

    def replicas_for_file(self, file_id: str) -> dict[str, list[ReplicaRecord]]:
        rows = self.db.query(
            "SELECT r.* FROM replicas r JOIN chunks c ON c.chunk_id = r.chunk_id WHERE c.file_id = ? "
            "ORDER BY r.node_id",
            (file_id,),
        )
        result: dict[str, list[ReplicaRecord]] = defaultdict(list)
        for r in rows:
            result[r["chunk_id"]].append(ReplicaRecord.from_row(r))
        return result

    def replicas_on_node(self, node_id: str) -> list[ReplicaRecord]:
        rows = self.db.query("SELECT * FROM replicas WHERE node_id = ? ORDER BY chunk_id", (node_id,))
        return [ReplicaRecord.from_row(r) for r in rows]

    def count_replicas_on_node(self, node_id: str) -> int:
        return int(self.db.query_one("SELECT COUNT(*) AS n FROM replicas WHERE node_id = ?", (node_id,))["n"])

    def live_physical_bytes(self) -> int:
        row = self.db.query_one(
            "SELECT COALESCE(SUM(c.size), 0) AS n FROM replicas r JOIN chunks c ON c.chunk_id = r.chunk_id "
            "JOIN nodes n ON n.node_id = r.node_id "
            "WHERE r.status = 'HEALTHY' AND n.health != 'DOWN' AND n.admin_state != 'REMOVED'"
        )
        return int(row["n"])

    # ------------------------------------------------ upload abort / deletion
    def abort_upload(self, file_id: str, error: str) -> FileRecord:
        """FAILED + schedule physical cleanup of every replica written so far."""
        with self.db.transaction() as conn:
            record = self._transition(conn, file_id, FileState.FAILED, error=error)
            self._schedule_file_replicas_for_deletion(conn, file_id, "upload_failed")
            conn.execute("DELETE FROM chunks WHERE file_id = ?", (file_id,))
            return record

    def begin_file_deletion(self, file_id: str) -> FileRecord:
        with self.db.transaction() as conn:
            record = self._transition(conn, file_id, FileState.DELETING)
            self._schedule_file_replicas_for_deletion(conn, file_id, "file_deleted")
            conn.execute("DELETE FROM chunks WHERE file_id = ?", (file_id,))
            return record

    def _schedule_file_replicas_for_deletion(self, conn: sqlite3.Connection, file_id: str, reason: str) -> None:
        conn.execute(
            """INSERT OR IGNORE INTO pending_deletions(chunk_id, node_id, file_id, reason, attempts,
                   next_attempt_at, created_at)
               SELECT r.chunk_id, r.node_id, c.file_id, ?, 0, 0, ?
               FROM replicas r JOIN chunks c ON c.chunk_id = r.chunk_id WHERE c.file_id = ?""",
            (reason, _now(), file_id),
        )

    # ----------------------------------------------------- pending deletions
    def _add_pending_deletion(self, conn: sqlite3.Connection, chunk_id: str, node_id: str, reason: str,
                              file_id: str | None) -> None:
        conn.execute(
            """INSERT OR IGNORE INTO pending_deletions(chunk_id, node_id, file_id, reason, attempts,
                   next_attempt_at, created_at) VALUES (?, ?, ?, ?, 0, 0, ?)""",
            (chunk_id, node_id, file_id, reason, _now()),
        )

    def add_pending_deletion(self, chunk_id: str, node_id: str, reason: str, file_id: str | None = None) -> None:
        with self.db.transaction() as conn:
            self._add_pending_deletion(conn, chunk_id, node_id, reason, file_id)

    def due_pending_deletions(self, *, now: float | None = None, file_id: str | None = None,
                              ignore_backoff: bool = False, limit: int = 5000) -> list[PendingDeletion]:
        clauses, params = [], []
        if not ignore_backoff:
            clauses.append("next_attempt_at <= ?")
            params.append(now or _now())
        if file_id is not None:
            clauses.append("file_id = ?")
            params.append(file_id)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        rows = self.db.query(
            f"SELECT * FROM pending_deletions {where} ORDER BY created_at LIMIT ?", (*params, limit)
        )
        return [PendingDeletion.from_row(r) for r in rows]

    def remove_pending_deletion(self, chunk_id: str, node_id: str) -> None:
        self.db.execute("DELETE FROM pending_deletions WHERE chunk_id = ? AND node_id = ?", (chunk_id, node_id))

    def record_pending_deletion_failure(self, chunk_id: str, node_id: str, error: str,
                                        next_attempt_at: float) -> None:
        self.db.execute(
            "UPDATE pending_deletions SET attempts = attempts + 1, last_error = ?, next_attempt_at = ? "
            "WHERE chunk_id = ? AND node_id = ?",
            (error[:500], next_attempt_at, chunk_id, node_id),
        )

    def pending_deletions_for_file(self, file_id: str) -> list[PendingDeletion]:
        rows = self.db.query("SELECT * FROM pending_deletions WHERE file_id = ? ORDER BY chunk_id", (file_id,))
        return [PendingDeletion.from_row(r) for r in rows]

    def pending_deletion_exists(self, chunk_id: str, node_id: str) -> bool:
        return self.db.query_one(
            "SELECT 1 FROM pending_deletions WHERE chunk_id = ? AND node_id = ?", (chunk_id, node_id)
        ) is not None

    def count_pending_deletions(self) -> int:
        return int(self.db.query_one("SELECT COUNT(*) AS n FROM pending_deletions")["n"])

    def list_pending_deletions(self, limit: int = 100) -> list[PendingDeletion]:
        rows = self.db.query("SELECT * FROM pending_deletions ORDER BY created_at LIMIT ?", (limit,))
        return [PendingDeletion.from_row(r) for r in rows]

    def finalize_deleted_files(self) -> list[str]:
        """Move DELETING files with no outstanding physical cleanup to DELETED."""
        rows = self.db.query(
            "SELECT f.file_id FROM files f WHERE f.state = 'DELETING' AND NOT EXISTS "
            "(SELECT 1 FROM pending_deletions p WHERE p.file_id = f.file_id)"
        )
        done = []
        for r in rows:
            try:
                self.transition_file(r["file_id"], FileState.DELETED)
                done.append(r["file_id"])
            except InvalidStateTransition:
                continue
        return done

    # ---------------------------------------------------------- repair tasks
    def get_repair_task(self, chunk_id: str) -> RepairTask | None:
        row = self.db.query_one("SELECT * FROM repair_tasks WHERE chunk_id = ?", (chunk_id,))
        return RepairTask.from_row(row) if row else None

    def upsert_repair_task(self, chunk_id: str, state: str, attempts: int, next_attempt_at: float,
                           last_error: str | None) -> None:
        self.db.execute(
            """INSERT INTO repair_tasks(chunk_id, state, attempts, next_attempt_at, last_error, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(chunk_id) DO UPDATE SET state = excluded.state, attempts = excluded.attempts,
                   next_attempt_at = excluded.next_attempt_at, last_error = excluded.last_error,
                   updated_at = excluded.updated_at""",
            (chunk_id, state, attempts, next_attempt_at, (last_error or "")[:500] or None, _now()),
        )

    def clear_repair_task(self, chunk_id: str) -> None:
        self.db.execute("DELETE FROM repair_tasks WHERE chunk_id = ?", (chunk_id,))

    def list_repair_tasks(self, limit: int = 200) -> list[RepairTask]:
        rows = self.db.query("SELECT * FROM repair_tasks ORDER BY updated_at DESC LIMIT ?", (limit,))
        return [RepairTask.from_row(r) for r in rows]

    def count_repair_tasks_by_state(self) -> dict[str, int]:
        rows = self.db.query("SELECT state, COUNT(*) AS n FROM repair_tasks GROUP BY state")
        return {r["state"]: r["n"] for r in rows}

    # ------------------------------------------------------------ operations
    def start_operation(self, op_type: str, target: str | None = None,
                        details: dict[str, Any] | None = None) -> str:
        op_id = uuid.uuid4().hex
        ts = _now()
        self.db.execute(
            "INSERT INTO operations(op_id, op_type, target, state, details, created_at, updated_at) "
            "VALUES (?, ?, ?, 'RUNNING', ?, ?, ?)",
            (op_id, op_type, target, json.dumps(details or {}), ts, ts),
        )
        return op_id

    def finish_operation(self, op_id: str, state: str, details: dict[str, Any] | None = None,
                         error: str | None = None) -> None:
        self.db.execute(
            "UPDATE operations SET state = ?, details = COALESCE(?, details), error = ?, updated_at = ? "
            "WHERE op_id = ?",
            (state, json.dumps(details) if details is not None else None, error, _now(), op_id),
        )

    def list_operations(self, op_type: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        if op_type:
            rows = self.db.query(
                "SELECT * FROM operations WHERE op_type = ? ORDER BY created_at DESC LIMIT ?", (op_type, limit)
            )
        else:
            rows = self.db.query("SELECT * FROM operations ORDER BY created_at DESC LIMIT ?", (limit,))
        out = []
        for r in rows:
            item = dict(r)
            item["details"] = json.loads(item["details"]) if item["details"] else {}
            out.append(item)
        return out
