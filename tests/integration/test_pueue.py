"""Local execution through a real, isolated pueued. Run with `pytest -m pueue`; skipped if Pueue is missing."""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest

from switchboard.config import Budget, Config, DaemonConfig, LocalConfig
from switchboard.controller import Controller, SimulatedCrash
from switchboard.executors.base import BackendUnreachable
from switchboard.executors.pueue import PueueExecutor, observation

from ..conftest import task_states

pytestmark = [pytest.mark.pueue, pytest.mark.skipif(not shutil.which("pueued"), reason="pueue not installed")]


class Pueued:
    """An isolated pueued; its socket lives under /tmp because macOS caps socket paths at 104 bytes."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.sock_dir = Path(tempfile.mkdtemp(prefix="sbt", dir="/tmp"))
        self.config = root / "pueue.yml"
        self.config.write_text(
            "shared:\n"
            f"  pueue_directory: {root / 'pueue-data'}\n"
            f"  runtime_directory: {self.sock_dir}\n"
            "  use_unix_socket: true\n"
            f"  unix_socket_path: {self.sock_dir / 'p.sock'}\n"
        )
        self.proc: subprocess.Popen | None = None

    def start(self) -> None:
        log = open(self.root / "pueued.log", "ab")
        self.proc = subprocess.Popen(["pueued", "-c", str(self.config)], stdout=log, stderr=log,
                                     stdin=subprocess.DEVNULL, start_new_session=True)
        log.close()
        probe = PueueExecutor(self.root, LocalConfig(pueue_config=self.config))
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                probe._status()
                return
            except BackendUnreachable:
                time.sleep(0.05)
        raise RuntimeError("pueued did not come up")

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            subprocess.run(["pueue", "shutdown"], env={**os.environ, "PUEUE_CONFIG_PATH": str(self.config)},
                           capture_output=True)
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()

    def restart(self) -> None:
        self.stop()
        self.start()


@pytest.fixture
def pueued(tmp_path):
    d = Pueued(tmp_path)
    d.start()
    yield d
    d.stop()
    shutil.rmtree(d.sock_dir, ignore_errors=True)


def make_cfg(tmp_path: Path, pueued: Pueued, cpus: int = 2, **daemon) -> Config:
    return Config(
        home=tmp_path / "sbhome",
        budgets={"focus": Budget(cpus=cpus, mem_gb=8, max_admits_per_tick=8)},
        local=LocalConfig(pueue_config=pueued.config, pueue_group="sbtest"),
        daemon=DaemonConfig(**daemon) if daemon else DaemonConfig(),
        submit_grace_s=0.5,
        lost_grace_s=0.5,
    )


def make_ctl(cfg: Config, **kw) -> tuple[Controller, PueueExecutor]:
    ex = PueueExecutor(cfg.launch_dir, cfg.local)
    return Controller(cfg, {"local": ex}, **kw), ex


def drive(ctl: Controller, timeout: float = 60, watch=None) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        report = ctl.tick()
        if watch:
            watch()
        active, pending = ctl.local_work_remaining()
        if active == 0 and (pending == 0 or report.admitted == 0):
            return
        time.sleep(0.05)
    raise AssertionError("local work did not finish in time")


def runs(counter: Path) -> int:
    return counter.read_text().count("\n") if counter.exists() else 0


def test_end_to_end_without_queueing_in_pueue(tmp_path, pueued, write_spec):
    cfg = make_cfg(tmp_path, pueued, cpus=2)
    ctl, ex = make_ctl(cfg)
    counter = tmp_path / "runs.txt"
    ctl.submit(write_spec(task_args=["--append-to", str(counter), "--seconds", "0.3"],
                          params={"grid": {"alpha": list(range(6))}, "seeds": [1]}), "local")
    peak_unfinished_in_pueue = peak_active = 0

    def computing(task) -> bool:
        """Unfinished in Pueue and its attempt has not yet written a final status (shim not merely exiting)."""
        if observation(task).state == "DONE":
            return False
        row = ctl.conn.execute("SELECT attempt_dir FROM attempts WHERE launch_id=?", (task["label"],)).fetchone()
        status = Path(row["attempt_dir"]) / "status.json" if row else None
        return not (status and status.exists() and json.loads(status.read_text())["state"] != "RUNNING")

    def watch():
        nonlocal peak_unfinished_in_pueue, peak_active
        peak_unfinished_in_pueue = max(peak_unfinished_in_pueue, sum(map(computing, ex._tasks().values())))
        peak_active = max(peak_active, ctl.conn.execute(
            "SELECT count(*) FROM attempts WHERE state IN ('SUBMITTING','SUBMITTED','RUNNING','UNKNOWN')").fetchone()[0])

    drive(ctl, watch=watch)
    assert set(task_states(ctl).values()) == {"SUCCEEDED"}
    assert runs(counter) == 6
    # The controller is the only gate: Pueue never holds more work than was admitted (2 x 1 cpu).
    # Pueue lists a new task as "Queued" for a moment before starting it, and a finished shim stays
    # "Running" while it exits; neither is real queueing or extra compute.
    assert peak_active <= 2
    assert peak_unfinished_in_pueue <= 2
    # Finished entries are swept once Pueue reports them Done and their outcome is recorded.
    deadline = time.monotonic() + 5
    while ex._tasks() and time.monotonic() < deadline:
        ctl.tick()
        time.sleep(0.1)
    assert ex._tasks() == {}


def test_tasks_survive_a_controller_restart(tmp_path, pueued, write_spec):
    cfg = make_cfg(tmp_path, pueued)
    counter = tmp_path / "runs.txt"
    ctl, _ = make_ctl(cfg)
    ctl.submit(write_spec(task_args=["--append-to", str(counter), "--seconds", "1.5"],
                          params={"grid": {"alpha": [1, 2]}, "seeds": [1]}), "local")
    assert ctl.tick().admitted == 2
    ctl.conn.close()
    del ctl  # "crash": all in-memory state is gone; Pueue keeps running the tasks
    restarted, _ = make_ctl(cfg)
    drive(restarted)
    assert set(task_states(restarted).values()) == {"SUCCEEDED"}
    assert runs(counter) == 2
    assert restarted.conn.execute("SELECT count(*) FROM attempts").fetchone()[0] == 2


@pytest.mark.parametrize("crashpoint", ["after_intent", "after_submit"])
def test_crash_during_dispatch_runs_once(tmp_path, pueued, write_spec, crashpoint):
    cfg = make_cfg(tmp_path, pueued)
    counter = tmp_path / "runs.txt"
    crashing, _ = make_ctl(cfg, crashpoints={crashpoint})
    crashing.submit(write_spec(task_args=["--append-to", str(counter)],
                               params={"grid": {"alpha": [1]}, "seeds": [1]}), "local")
    with pytest.raises(SimulatedCrash):
        crashing.tick()
    time.sleep(cfg.submit_grace_s + 0.1)
    restarted, _ = make_ctl(cfg)
    drive(restarted)
    assert task_states(restarted) == {0: "SUCCEEDED"}
    assert runs(counter) == 1


def test_pueued_restart_interrupts_and_requeues_without_using_a_retry(tmp_path, pueued, write_spec):
    cfg = make_cfg(tmp_path, pueued)
    counter = tmp_path / "runs.txt"
    ctl, _ = make_ctl(cfg)
    ctl.submit(write_spec(task_args=["--append-to", str(counter), "--seconds", "2"],
                          params={"grid": {"alpha": [1]}, "seeds": [1]}), "local")
    ctl.tick()
    deadline = time.monotonic() + 10
    while runs(counter) == 0 and time.monotonic() < deadline:
        time.sleep(0.05)
    pueued.restart()  # kills running tasks
    drive(ctl)
    attempts = ctl.conn.execute("SELECT state, reason FROM attempts ORDER BY created_at").fetchall()
    assert [(a["state"], a["reason"]) for a in attempts] == [("CANCELLED", "interrupted"), ("SUCCEEDED", None)]
    task = ctl.conn.execute("SELECT state, retries_used FROM tasks").fetchone()
    assert (task["state"], task["retries_used"]) == ("SUCCEEDED", 0)


def test_pueued_down_fails_nothing_and_work_resumes(tmp_path, pueued, write_spec):
    cfg = make_cfg(tmp_path, pueued)
    ctl, _ = make_ctl(cfg)
    ctl.submit(write_spec(params={"grid": {"alpha": [1, 2]}, "seeds": [1]}), "local")
    pueued.stop()
    report = ctl.tick()
    assert report.admitted == 0 and report.failed == 0
    assert set(task_states(ctl).values()) == {"PENDING"}
    pueued.start()
    drive(ctl)
    assert set(task_states(ctl).values()) == {"SUCCEEDED"}


def test_suspend_stops_running_tasks_and_resume_continues(tmp_path, pueued, write_spec):
    cfg = make_cfg(tmp_path, pueued)
    ctl, ex = make_ctl(cfg)
    ctl.submit(write_spec(task_args=["--seconds", "1.5"], params={"grid": {"alpha": [1]}, "seeds": [1]}), "local")
    ctl.tick()
    attempt_dir = Path(ctl.conn.execute("SELECT attempt_dir FROM attempts").fetchone()[0])
    deadline = time.monotonic() + 10
    while not (attempt_dir / "status.json").exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    shim_pid = json.loads((attempt_dir / "status.json").read_text())["pid"]
    ex.suspend()
    time.sleep(0.3)
    state = subprocess.run(["ps", "-o", "stat=", "-p", str(shim_pid)], capture_output=True, text=True).stdout
    assert state.strip().startswith("T")
    ex.resume()
    drive(ctl)
    assert task_states(ctl) == {0: "SUCCEEDED"}


# ---- daemon -------------------------------------------------------------------------------------


def write_toml(path: Path, cfg: Config) -> None:
    b = cfg.budgets["focus"]
    path.write_text(
        f'home = "{cfg.home}"\n'
        f"[budgets.focus]\ncpus = {b.cpus}\nmem_gb = {b.mem_gb}\nmax_admits_per_tick = {b.max_admits_per_tick}\n"
        f'[local]\npueue_config = "{cfg.local.pueue_config}"\npueue_group = "{cfg.local.pueue_group}"\n'
        "run_on_battery = true\nmemory_pressure_gate = false\n"
        f"[daemon]\nlocal_interval_s = {cfg.daemon.local_interval_s}\nidle_interval_s = {cfg.daemon.idle_interval_s}\n"
    )


@pytest.fixture
def daemon_proc(tmp_path, pueued):
    # Long fallback intervals: only nudges can make this fast.
    cfg = make_cfg(tmp_path, pueued, cpus=1, local_interval_s=30.0, idle_interval_s=30.0)
    toml = tmp_path / "config.toml"
    write_toml(toml, cfg)
    env = {**os.environ, "SWITCHBOARD_CONFIG": str(toml)}
    log = open(tmp_path / "daemon.log", "ab")
    proc = subprocess.Popen([sys.executable, "-m", "switchboard", "daemon", "run"], env=env, stdout=log,
                            stderr=log, start_new_session=True)
    from switchboard.daemon import wait_for_socket
    assert wait_for_socket(cfg), (tmp_path / "daemon.log").read_text()
    yield cfg, env, proc
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=10)


def test_daemon_is_woken_by_cli_and_completion_nudges(tmp_path, daemon_proc, write_spec):
    cfg, env, proc = daemon_proc
    spec = write_spec(task_args=["--seconds", "0.2"], params={"grid": {"alpha": [1, 2, 3, 4]}, "seeds": [1]})
    spec_path = tmp_path / "exp.yaml"
    t0 = time.monotonic()
    out = subprocess.run([sys.executable, "-m", "switchboard", "submit", str(spec_path), "--backend", "local"],
                         env=env, capture_output=True, text=True)
    assert "daemon notified" in out.stdout, out.stdout + out.stderr
    ctl, _ = make_ctl(cfg)
    deadline = time.monotonic() + 20
    while set(task_states(ctl).values()) != {"SUCCEEDED"} and time.monotonic() < deadline:
        time.sleep(0.1)
    elapsed = time.monotonic() - t0
    assert set(task_states(ctl).values()) == {"SUCCEEDED"}
    # 4 tasks run one at a time (cpus=1). With 30 s fallback polling this only finishes fast if the
    # shim's completion nudge wakes the daemon each time.
    assert elapsed < 10, f"took {elapsed:.1f}s"
    assert spec  # spec written by the fixture helper


def test_daemon_refuses_a_second_instance_and_cleans_up(tmp_path, daemon_proc):
    cfg, env, proc = daemon_proc
    second = subprocess.run([sys.executable, "-m", "switchboard", "daemon", "run"], env=env, capture_output=True,
                            text=True, timeout=20)
    assert second.returncode == 2 and "another switchboard daemon" in second.stderr
    proc.send_signal(signal.SIGTERM)
    proc.wait(timeout=10)
    assert proc.returncode == 0
    assert not cfg.nudge_socket.exists()
