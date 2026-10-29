from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path


class Status(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    DONE = "done"
    ERROR = "error"
    FAILED_PERMANENTLY = "failed_permanently"


RETRYABLE_STATUSES = (Status.ERROR, Status.FAILED_PERMANENTLY, Status.DONE)

# Each entry upgrades the schema by one version (tracked in PRAGMA user_version). Never edit a released entry.
MIGRATIONS: tuple[str, ...] = (
    """
    CREATE TABLE files (
        content_hash   TEXT PRIMARY KEY,
        size           INTEGER NOT NULL,
        last_seen_path TEXT NOT NULL,
        first_seen_at  TEXT NOT NULL,
        last_seen_at   TEXT NOT NULL,
        status         TEXT NOT NULL CHECK (status IN
                           ('pending', 'processing', 'done', 'error', 'failed_permanently')),
        attempts       INTEGER NOT NULL DEFAULT 0,
        last_error     TEXT,
        job_name       TEXT,
        started_at     TEXT,
        finished_at    TEXT
    );
    CREATE INDEX files_status ON files (status);
    CREATE INDEX files_job_name ON files (job_name);

    CREATE TABLE file_paths (
        content_hash  TEXT NOT NULL REFERENCES files (content_hash),
        path          TEXT NOT NULL,
        first_seen_at TEXT NOT NULL,
        last_seen_at  TEXT NOT NULL,
        PRIMARY KEY (content_hash, path)
    );

    -- Last scan result per path: drives the stability check and caches hashes across scans and restarts.
    CREATE TABLE observations (
        path         TEXT PRIMARY KEY,
        size         INTEGER NOT NULL,
        mtime_ns     INTEGER NOT NULL,
        stable_count INTEGER NOT NULL,
        content_hash TEXT
    );

    CREATE TABLE meta (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );
    """,
)


def utcnow() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


@dataclass(frozen=True)
class FileRecord:
    content_hash: str
    size: int
    last_seen_path: str
    first_seen_at: str
    last_seen_at: str
    status: Status
    attempts: int
    last_error: str | None
    job_name: str | None
    started_at: str | None
    finished_at: str | None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> FileRecord:
        values = dict(row)
        values["status"] = Status(values["status"])
        return cls(**values)


@dataclass(frozen=True)
class PathRecord:
    path: str
    first_seen_at: str
    last_seen_at: str


@dataclass(frozen=True)
class Observation:
    path: str
    size: int
    mtime_ns: int
    stable_count: int
    content_hash: str | None


class Sighting(StrEnum):
    NEW = "new"
    KNOWN_PATH = "known_path"
    NEW_PATH = "new_path"


class StateStore:
    """SQLite-backed state. Every public mutating method runs in its own ``BEGIN IMMEDIATE`` transaction."""

    def __init__(self, path: Path, *, create: bool = True) -> None:
        if not create and not path.exists():
            raise FileNotFoundError(f"state database not found: {path}")
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._conn = sqlite3.connect(path, isolation_level=None, timeout=30)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._migrate()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> StateStore:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            yield self._conn
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise
        self._conn.execute("COMMIT")

    @property
    def schema_version(self) -> int:
        return self._conn.execute("PRAGMA user_version").fetchone()[0]

    def _migrate(self) -> None:
        with self.transaction() as conn:
            version = conn.execute("PRAGMA user_version").fetchone()[0]
            for number, script in enumerate(MIGRATIONS[version:], start=version + 1):
                for statement in script.split(";"):
                    if statement.strip():
                        conn.execute(statement)
                conn.execute(f"PRAGMA user_version = {number}")

    # Observations ---------------------------------------------------------------------------------------------

    def observations(self) -> dict[str, Observation]:
        rows = self._conn.execute("SELECT * FROM observations").fetchall()
        return {row["path"]: Observation(**dict(row)) for row in rows}

    def replace_observations(self, observations: Iterable[Observation]) -> None:
        with self.transaction() as conn:
            conn.execute("DELETE FROM observations")
            conn.executemany(
                "INSERT INTO observations (path, size, mtime_ns, stable_count, content_hash) VALUES (?, ?, ?, ?, ?)",
                [(o.path, o.size, o.mtime_ns, o.stable_count, o.content_hash) for o in observations],
            )

    # Files ----------------------------------------------------------------------------------------------------

    def register_sighting(self, content_hash: str, size: int, path: str, *, now: str | None = None) -> Sighting:
        """Record that a stable file with ``content_hash`` is present at ``path``."""
        now = now or utcnow()
        with self.transaction() as conn:
            known = conn.execute("SELECT 1 FROM files WHERE content_hash = ?", (content_hash,)).fetchone()
            if known is None:
                conn.execute(
                    "INSERT INTO files (content_hash, size, last_seen_path, first_seen_at, last_seen_at, status)"
                    " VALUES (?, ?, ?, ?, ?, ?)",
                    (content_hash, size, path, now, now, Status.PENDING),
                )
                result = Sighting.NEW
            else:
                seen_here = conn.execute(
                    "SELECT 1 FROM file_paths WHERE content_hash = ? AND path = ?", (content_hash, path)
                ).fetchone()
                result = Sighting.KNOWN_PATH if seen_here else Sighting.NEW_PATH
            conn.execute(
                "INSERT INTO file_paths (content_hash, path, first_seen_at, last_seen_at) VALUES (?, ?, ?, ?)"
                " ON CONFLICT (content_hash, path) DO UPDATE SET last_seen_at = excluded.last_seen_at",
                (content_hash, path, now, now),
            )
        return result

    def touch(self, content_hash: str, path: str, *, now: str | None = None) -> None:
        """Set the path a file is currently found at (used to launch it) and its last-seen time."""
        with self.transaction() as conn:
            conn.execute(
                "UPDATE files SET last_seen_path = ?, last_seen_at = ? WHERE content_hash = ?",
                (path, now or utcnow(), content_hash),
            )

    def get(self, content_hash: str) -> FileRecord | None:
        row = self._conn.execute("SELECT * FROM files WHERE content_hash = ?", (content_hash,)).fetchone()
        return FileRecord.from_row(row) if row else None

    def find_by_prefix(self, prefix: str) -> list[FileRecord]:
        escaped = prefix.lower().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        rows = self._conn.execute(
            "SELECT * FROM files WHERE content_hash LIKE ? ESCAPE '\\' ORDER BY content_hash", (escaped + "%",)
        ).fetchall()
        return [FileRecord.from_row(row) for row in rows]

    def paths(self, content_hash: str) -> list[PathRecord]:
        rows = self._conn.execute(
            "SELECT path, first_seen_at, last_seen_at FROM file_paths WHERE content_hash = ? ORDER BY first_seen_at",
            (content_hash,),
        ).fetchall()
        return [PathRecord(**dict(row)) for row in rows]

    def list_files(self, status: Status | None = None) -> list[FileRecord]:
        if status is None:
            rows = self._conn.execute("SELECT * FROM files ORDER BY first_seen_at, content_hash").fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM files WHERE status = ? ORDER BY first_seen_at, content_hash", (status,)
            ).fetchall()
        return [FileRecord.from_row(row) for row in rows]

    def count_by_status(self) -> dict[Status, int]:
        counts = {status: 0 for status in Status}
        for row in self._conn.execute("SELECT status, COUNT(*) AS n FROM files GROUP BY status"):
            counts[Status(row["status"])] = row["n"]
        return counts

    def active_job_names(self) -> set[str]:
        rows = self._conn.execute(
            "SELECT DISTINCT job_name FROM files WHERE status = ? AND job_name IS NOT NULL", (Status.PROCESSING,)
        ).fetchall()
        return {row["job_name"] for row in rows}

    def processing(self) -> list[FileRecord]:
        return self.list_files(Status.PROCESSING)

    def launchable(self, max_attempts: int) -> list[FileRecord]:
        """Files waiting for a Job: new ones first, then retries, oldest first."""
        rows = self._conn.execute(
            "SELECT * FROM files WHERE status = ? OR (status = ? AND attempts < ?)"
            " ORDER BY CASE status WHEN ? THEN 0 ELSE 1 END, first_seen_at, content_hash",
            (Status.PENDING, Status.ERROR, max_attempts, Status.PENDING),
        ).fetchall()
        return [FileRecord.from_row(row) for row in rows]

    # State transitions ----------------------------------------------------------------------------------------

    def claim(self, content_hashes: Iterable[str], job_name: str, *, now: str | None = None) -> None:
        """Mark files as ``processing`` under ``job_name``; committed before the Job is created."""
        hashes = list(content_hashes)
        with self.transaction() as conn:
            for content_hash in hashes:
                updated = conn.execute(
                    "UPDATE files SET status = ?, job_name = ?, started_at = ?, finished_at = NULL"
                    " WHERE content_hash = ? AND status IN (?, ?)",
                    (Status.PROCESSING, job_name, now or utcnow(), content_hash, Status.PENDING, Status.ERROR),
                ).rowcount
                if updated != 1:
                    raise ValueError(f"file {content_hash} is not launchable")

    def mark_done(self, job_name: str, *, now: str | None = None) -> int:
        with self.transaction() as conn:
            return conn.execute(
                "UPDATE files SET status = ?, finished_at = ?, last_error = NULL WHERE job_name = ? AND status = ?",
                (Status.DONE, now or utcnow(), job_name, Status.PROCESSING),
            ).rowcount

    def mark_failed(self, job_name: str, reason: str, max_attempts: int, *, now: str | None = None) -> int:
        with self.transaction() as conn:
            return conn.execute(
                "UPDATE files SET attempts = attempts + 1, last_error = ?, finished_at = ?,"
                " status = CASE WHEN attempts + 1 >= ? THEN ? ELSE ? END"
                " WHERE job_name = ? AND status = ?",
                (
                    reason,
                    now or utcnow(),
                    max_attempts,
                    Status.FAILED_PERMANENTLY,
                    Status.ERROR,
                    job_name,
                    Status.PROCESSING,
                ),
            ).rowcount

    def release(self, job_name: str, reason: str) -> int:
        """Return files of a Job that never ran (or vanished) to ``pending`` without counting an attempt."""
        with self.transaction() as conn:
            return conn.execute(
                "UPDATE files SET status = ?, last_error = ?, job_name = NULL, started_at = NULL"
                " WHERE job_name = ? AND status = ?",
                (Status.PENDING, reason, job_name, Status.PROCESSING),
            ).rowcount

    def reset(self, content_hashes: Iterable[str]) -> int:
        """Operator retry: back to ``pending`` with a fresh attempt budget."""
        retryable = tuple(RETRYABLE_STATUSES)
        placeholders = ",".join("?" * len(retryable))
        total = 0
        with self.transaction() as conn:
            for content_hash in content_hashes:
                total += conn.execute(
                    "UPDATE files SET status = ?, attempts = 0, last_error = NULL, job_name = NULL,"
                    f" started_at = NULL, finished_at = NULL WHERE content_hash = ? AND status IN ({placeholders})",
                    (Status.PENDING, content_hash, *retryable),
                ).rowcount
        return total

    # Meta -----------------------------------------------------------------------------------------------------

    def set_meta(self, key: str, value: str) -> None:
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                (key, value),
            )

    def get_meta(self, key: str) -> str | None:
        row = self._conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else None
