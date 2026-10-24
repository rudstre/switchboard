import pytest

from switchboard import policy
from switchboard.config import Budget
from switchboard.registry import db
from switchboard.registry.states import IllegalTransition, transition


@pytest.fixture
def conn(tmp_path):
    c = db.connect(tmp_path / "s.db")
    with db.tx(c):
        c.execute("INSERT INTO experiments(name, run_no, spec_text, spec_json, spec_dir, spec_hash, identity_json, "
                  "created_at) VALUES ('e', 1, '', '{}', '.', 'h', '{}', 0)")
        c.execute("INSERT INTO tasks(experiment_id, idx, key, params_json, cpus, mem_mb, state, updated_at) "
                  "VALUES (1, 0, 'k', '{}', 1, 1, 'PENDING', 0)")
    return c


def test_legal_transition_is_logged(conn):
    with db.tx(conn):
        prev = transition(conn, "task", 1, "SUBMITTED", "admitted", "test")
    assert prev == "PENDING"
    ev = conn.execute("SELECT * FROM events WHERE entity='task'").fetchall()
    assert [(e["from_state"], e["to_state"], e["reason"]) for e in ev] == [("PENDING", "SUBMITTED", "admitted")]


def test_illegal_transition_raises_and_rolls_back(conn):
    with pytest.raises(IllegalTransition):
        with db.tx(conn):
            transition(conn, "task", 1, "RUNNING", "skip ahead", "test")
    assert conn.execute("SELECT state FROM tasks").fetchone()[0] == "PENDING"


def test_succeeded_task_is_terminal(conn):
    with db.tx(conn):
        transition(conn, "task", 1, "SUCCEEDED", "late result", "test")
    with pytest.raises(IllegalTransition):
        with db.tx(conn):
            transition(conn, "task", 1, "PENDING", "retry", "test")


def test_schema_allows_one_active_attempt_per_task(conn):
    import sqlite3
    with db.tx(conn):
        conn.execute("INSERT INTO launches(id, backend, state, created_at, updated_at) VALUES ('l', 'local', "
                     "'SUBMITTING', 0, 0)")
        conn.execute("INSERT INTO attempts(id, task_id, launch_id, backend, platform, state, attempt_dir, created_at) "
                     "VALUES ('a1', 1, 'l', 'local', 'p', 'RUNNING', '/x', 0)")
    with pytest.raises(sqlite3.IntegrityError):
        with db.tx(conn):
            conn.execute("INSERT INTO attempts(id, task_id, launch_id, backend, platform, state, attempt_dir, "
                         "created_at) VALUES ('a2', 1, 'l', 'local', 'p', 'SUBMITTING', '/x', 0)")


BUDGET = Budget(cpus=8, mem_gb=8, max_admits_per_tick=10)


def C(i, cpus, gb):
    return policy.Candidate(i, cpus, int(gb * 1024))


def test_admission_respects_cpu_and_memory_sums():
    r = policy.admit_local([C(1, 4, 2), C(2, 4, 2), C(3, 1, 1)], 0, 0, BUDGET, "away", [])
    assert r.admit == [1, 2]
    assert "budget full" in r.notes[3]


def test_admission_counts_already_reserved_and_backfills():
    r = policy.admit_local([C(1, 6, 1), C(2, 2, 1)], reserved_cpus=4, reserved_mem_mb=1024, budget=BUDGET,
                           mode="away", gates=[])
    assert r.admit == [2]
    r = policy.admit_local([C(1, 1, 7.5)], 0, 1024, BUDGET, "away", [])
    assert r.admit == []


def test_gates_and_oversized_tasks():
    r = policy.admit_local([C(1, 1, 1)], 0, 0, BUDGET, "away", ["memory pressure WARN"])
    assert r.admit == [] and "memory pressure" in r.notes[1]
    r = policy.admit_local([C(1, 16, 1), C(2, 1, 1)], 0, 0, BUDGET, "away", [])
    assert r.admit == [2] and "larger than the whole" in r.notes[1]


def test_admission_ramp():
    ramp = Budget(cpus=8, mem_gb=8, max_admits_per_tick=1)
    r = policy.admit_local([C(1, 1, 1), C(2, 1, 1)], 0, 0, ramp, "focus", [])
    assert r.admit == [1] and "ramp" in r.notes[2]


def test_default_budgets_follow_the_benchmark():
    from switchboard.config import DEFAULT_BUDGETS
    assert DEFAULT_BUDGETS["focus"].cpus == 4 and DEFAULT_BUDGETS["away"].cpus == 12
    assert all(b.launch_prefix == ("nice", "-n", "10") for b in DEFAULT_BUDGETS.values())
