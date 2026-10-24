from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

import pytest
import yaml

from switchboard.config import Budget, Config
from switchboard.controller import Controller
from switchboard.executors.fake import FakeExecutor
from switchboard.spec import load_spec

SYNTH = str(Path(__file__).resolve().parents[1] / "tools" / "synth.py")


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@example.invalid", *args],
        check=True, capture_output=True, text=True,
    ).stdout


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """A throwaway git repository standing in for a project working tree."""
    r = tmp_path / "project"
    (r / "src").mkdir(parents=True)
    (r / "src" / "model.py").write_text("X = 1\n")
    (r / ".gitignore").write_text("*.log\n")
    git(r, "init", "-q")
    git(r, "add", "-A")
    git(r, "commit", "-qm", "init")
    return r


@pytest.fixture
def cfg(tmp_path: Path) -> Config:
    return Config(
        home=tmp_path / "sbhome",
        budgets={"focus": Budget(cpus=4, mem_gb=8, max_admits_per_tick=4),
                 "away": Budget(cpus=8, mem_gb=16, max_admits_per_tick=8)},
        submit_grace_s=0.5,
        lost_grace_s=0.5,
    )


@pytest.fixture
def executor(cfg: Config) -> FakeExecutor:
    ex = FakeExecutor(cfg.launch_dir, name="local")
    yield ex
    for p in ex.all_procs():
        if p.poll() is None:
            p.kill()
            p.wait()


@pytest.fixture
def controller(cfg: Config, executor: FakeExecutor) -> Controller:
    return Controller(cfg, {"local": executor})


@pytest.fixture
def write_spec(tmp_path: Path, repo: Path):
    """Write a spec whose command runs tools/synth.py; keyword overrides merge into the YAML."""

    def _write(name: str = "exp", task_args: list[str] | None = None, **overrides):
        spec = {
            "experiment": name,
            "command": [sys.executable, SYNTH, "task", "--seed", "{seed}", "--outdir", "{outdir}",
                        "--write", "result.json", *(task_args or [])],
            "params": {"grid": {"alpha": [0.1, 0.2]}, "seeds": [1, 2]},
            "resources": {"cpus": 1, "mem_gb": 0.5},
            "outputs": {"expect": ["result.json"]},
            "backends": {"local": {"workdir": str(repo), "output_root": str(tmp_path / "out")}},
        }
        for key, value in overrides.items():
            spec[key] = value
        path = tmp_path / f"{name}.yaml"
        path.write_text(yaml.safe_dump(spec, sort_keys=False))
        return load_spec(path)

    return _write


def drive(controller: Controller, executor: FakeExecutor, timeout: float = 60.0) -> None:
    """Tick until no local work is active or admissible."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        report = controller.tick()
        active, pending = controller.local_work_remaining()
        if active == 0 and (pending == 0 or report.admitted == 0):
            return
        time.sleep(0.05)
    raise AssertionError("local work did not finish in time")


def task_states(controller: Controller) -> dict[int, str]:
    return {r["idx"]: r["state"] for r in controller.conn.execute("SELECT idx, state FROM tasks")}
