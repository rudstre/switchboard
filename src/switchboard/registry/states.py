"""Task and attempt state machines. Every transition is validated and written to the event log."""

from __future__ import annotations

import sqlite3
import time

# Task states (as in the brief). STAGED is reserved for post-MVP staging.
PENDING, STAGED, SUBMITTED, RUNNING = "PENDING", "STAGED", "SUBMITTED", "RUNNING"
SUCCEEDED, FAILED, CANCELLED = "SUCCEEDED", "FAILED", "CANCELLED"
# Attempt-only states.
SUBMITTING, UNKNOWN, ABANDONED, SUPERSEDED = "SUBMITTING", "UNKNOWN", "ABANDONED", "SUPERSEDED"

ACTIVE_ATTEMPT_STATES = (SUBMITTING, SUBMITTED, RUNNING, UNKNOWN)
FINAL_ATTEMPT_STATES = (SUCCEEDED, FAILED, CANCELLED, SUPERSEDED)

TASK_TRANSITIONS: dict[str, set[str]] = {
    PENDING: {SUBMITTED, CANCELLED, SUCCEEDED},  # SUCCEEDED: a late result from an abandoned attempt
    SUBMITTED: {RUNNING, PENDING, SUCCEEDED, FAILED, CANCELLED},
    RUNNING: {PENDING, SUCCEEDED, FAILED, CANCELLED},
    FAILED: {PENDING, SUCCEEDED},
    CANCELLED: {PENDING, SUCCEEDED},
    SUCCEEDED: set(),
}

ATTEMPT_TRANSITIONS: dict[str, set[str]] = {
    SUBMITTING: {SUBMITTED, RUNNING, UNKNOWN, ABANDONED, CANCELLED, SUCCEEDED, FAILED, SUPERSEDED},
    SUBMITTED: {RUNNING, UNKNOWN, SUCCEEDED, FAILED, CANCELLED, SUPERSEDED},
    RUNNING: {UNKNOWN, SUCCEEDED, FAILED, CANCELLED, SUPERSEDED},
    UNKNOWN: {SUBMITTED, RUNNING, SUCCEEDED, FAILED, CANCELLED, ABANDONED, SUPERSEDED},
    # Late evidence that an abandoned attempt did run after all.
    ABANDONED: {SUCCEEDED, FAILED, CANCELLED, SUPERSEDED},
    SUCCEEDED: set(),
    FAILED: set(),
    CANCELLED: set(),
    SUPERSEDED: set(),
}

LAUNCH_TRANSITIONS: dict[str, set[str]] = {
    SUBMITTING: {"ACCEPTED", ABANDONED},
    "ACCEPTED": set(),
    ABANDONED: {"ACCEPTED"},
}

_TABLES = {
    "task": ("tasks", TASK_TRANSITIONS),
    "attempt": ("attempts", ATTEMPT_TRANSITIONS),
    "launch": ("launches", LAUNCH_TRANSITIONS),
}


class IllegalTransition(RuntimeError):
    pass


def transition(
    conn: sqlite3.Connection,
    entity: str,
    entity_id: str | int,
    to_state: str,
    why: str,
    actor: str,
    **fields: object,
) -> str:
    """Move one row to ``to_state`` (plus extra column updates). Returns the previous state.

    A transition to the current state only applies ``fields`` and logs nothing.
    Must be called inside a transaction.
    """
    table, allowed = _TABLES[entity]
    row = conn.execute(f"SELECT state FROM {table} WHERE id=?", (entity_id,)).fetchone()
    if row is None:
        raise KeyError(f"{entity} {entity_id} not found")
    current = row["state"]
    if current != to_state and to_state not in allowed[current]:
        raise IllegalTransition(f"{entity} {entity_id}: {current} -> {to_state} is not allowed ({why})")
    now = time.time()
    columns = {"state": to_state, **fields}
    if entity in ("task", "launch"):
        columns["updated_at"] = now
    assignments = ", ".join(f"{col}=?" for col in columns)
    conn.execute(f"UPDATE {table} SET {assignments} WHERE id=?", (*columns.values(), entity_id))
    if current != to_state:
        log_event(conn, entity, entity_id, current, to_state, why, actor, ts=now)
    return current


def log_event(
    conn: sqlite3.Connection,
    entity: str,
    entity_id: str | int,
    from_state: str | None,
    to_state: str | None,
    reason: str,
    actor: str,
    ts: float | None = None,
) -> None:
    conn.execute(
        "INSERT INTO events(ts, entity, entity_id, from_state, to_state, reason, actor) VALUES (?,?,?,?,?,?,?)",
        (ts or time.time(), entity, str(entity_id), from_state, to_state, reason, actor),
    )
