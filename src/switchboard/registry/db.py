"""SQLite connection handling: WAL, short IMMEDIATE transactions, schema creation."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from importlib import resources
from pathlib import Path
from typing import Iterator

SCHEMA_VERSION = 4

# Upgrade steps from version N to N+1 (applied in order to existing databases).
UPGRADES = {
    1: ["ALTER TABLE tasks ADD COLUMN cancel_requested INTEGER NOT NULL DEFAULT 0"],
    2: ["ALTER TABLE experiments ADD COLUMN policy TEXT NOT NULL DEFAULT 'manual'",
        "CREATE TABLE IF NOT EXISTS experiment_backends (experiment_id INTEGER NOT NULL REFERENCES experiments(id), "
        "backend TEXT NOT NULL, ok INTEGER NOT NULL, detail TEXT, checked_at REAL NOT NULL, "
        "PRIMARY KEY (experiment_id, backend))"],
    3: ["ALTER TABLE tasks ADD COLUMN route_note TEXT"],
}


def connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, isolation_level=None, timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=10000")
    _migrate(conn)
    return conn


@contextmanager
def tx(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """One short write transaction. Never hold one across a network or subprocess call."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


def _migrate(conn: sqlite3.Connection) -> None:
    has_meta = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta'").fetchone()
    version = None
    if has_meta:
        row = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
        version = int(row["value"]) if row else None
    if version == SCHEMA_VERSION:
        return
    if version is not None and version > SCHEMA_VERSION:
        raise RuntimeError(f"registry schema v{version} is newer than this switchboard (v{SCHEMA_VERSION})")
    if version is None:
        schema = resources.files("switchboard.registry").joinpath("schema.sql").read_text()
        conn.executescript(schema)
    else:
        conn.execute("BEGIN IMMEDIATE")
        for step in range(version, SCHEMA_VERSION):
            for statement in UPGRADES[step]:
                conn.execute(statement)
        conn.execute("COMMIT")
    conn.execute(
        "INSERT INTO meta(key, value) VALUES('schema_version', ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (str(SCHEMA_VERSION),),
    )
