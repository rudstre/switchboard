import sqlite3
import sys
from pathlib import Path

import pytest

from switchboard import macos
from switchboard.config import Config
from switchboard.executors.pueue import observation
from switchboard.registry import db


def test_memory_pressure_gate_levels():
    for level, blocked in [(1, False), (2, True), (4, True), (None, False)]:
        why = macos.memory_pressure_gate(lambda level=level: level)()
        assert bool(why) is blocked
    assert "WARN" in macos.memory_pressure_gate(lambda: 2)()


def test_battery_gate():
    assert macos.battery_gate(lambda: True)() is not None
    assert macos.battery_gate(lambda: False)() is None
    assert macos.battery_gate(lambda: None)() is None


@pytest.mark.macos
@pytest.mark.skipif(sys.platform != "darwin", reason="macOS only")
def test_real_macos_readers():
    assert macos.memory_pressure_level() in (1, 2, 4)
    assert macos.on_battery() in (True, False)


@pytest.mark.parametrize("status, expected", [
    ({"Queued": {"enqueued_at": "t"}}, ("QUEUED", None)),
    ({"Running": {"start": "t"}}, ("RUNNING", None)),
    ({"Paused": {"start": "t"}}, ("RUNNING", None)),
    ({"Done": {"result": "Success"}}, ("DONE", 0)),
    ({"Done": {"result": {"Failed": 3}}}, ("DONE", 3)),
    ({"Done": {"result": "Killed"}}, ("DONE", None)),
    ("Locked", ("QUEUED", None)),
])
def test_pueue_status_parsing(status, expected):
    obs = observation({"status": status})
    assert (obs.state, obs.exit_code) == expected
    if status == {"Done": {"result": "Killed"}}:
        assert obs.detail == '"Killed"'


def test_long_home_falls_back_to_short_socket_path(tmp_path):
    cfg = Config(home=tmp_path / ("x" * 120))
    assert len(str(cfg.nudge_socket)) <= 100 and str(cfg.nudge_socket).startswith("/tmp/")
    assert Config(home=Path("/tmp/sbh")).nudge_socket == Path("/tmp/sbh/run/daemon.sock")


def test_schema_v1_database_is_upgraded(tmp_path):
    path = tmp_path / "old.db"
    conn = db.connect(path)
    conn.execute("ALTER TABLE tasks DROP COLUMN cancel_requested")
    conn.execute("UPDATE meta SET value='1' WHERE key='schema_version'")
    conn.close()
    conn = db.connect(path)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(tasks)")}
    assert "cancel_requested" in cols
    assert conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()[0] == "2"
    assert isinstance(conn, sqlite3.Connection)
