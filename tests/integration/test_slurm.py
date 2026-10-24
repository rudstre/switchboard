"""Slurm executor end to end against tools/mock_slurm through a LocalTransport.

The "cluster" has its own $HOME (with a clone of the project) and runs the stdlib runner under
whatever `python3` is on PATH, as a login node would.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

from switchboard.config import Budget, Config, HpcConfig
from switchboard.controller import Controller, IdentityMismatch
from switchboard.executors.slurm import SlurmExecutor
from switchboard.executors.transport import LocalTransport
from switchboard.export import export_slurm
from switchboard.spec import load_spec

from ..conftest import task_states

MOCK = Path(__file__).resolve().parents[2] / "tools" / "mock_slurm"


class Cluster:
    def __init__(self, root: Path) -> None:
        self.dir = root / "slurm"
        self.home = root / "remotehome"
        self.dir.mkdir()
        self.home.mkdir()
        self.transport = LocalTransport({"MOCK_SLURM_DIR": str(self.dir), "PATH": f"{MOCK}:{os.environ['PATH']}",
                                         "HOME": str(self.home), "USER": "tester"})

    def faults(self, **kw) -> None:
        path = self.dir / "faults.json"
        current = json.loads(path.read_text()) if path.exists() else {}
        path.write_text(json.dumps({**current, **kw}))

    def state(self) -> dict:
        return json.loads((self.dir / "state.json").read_text())

    def jobs(self) -> list[dict]:
        return list(self.state()["jobs"].values()) if (self.dir / "state.json").exists() else []


@pytest.fixture
def cluster(tmp_path, repo):
    c = Cluster(tmp_path)
    subprocess.run(["git", "clone", "-q", str(repo), str(c.home / "project")], check=True)
    yield c
    for job in c.jobs():  # stop anything still running
        for el in job["elements"].values():
            if el.get("pid"):
                try:
                    os.killpg(el["pid"], 9)
                except (ProcessLookupError, PermissionError):
                    pass


@pytest.fixture
def hpc_cfg(tmp_path):
    return Config(home=tmp_path / "sbhome", budgets={"focus": Budget(cpus=4, mem_gb=8, max_admits_per_tick=4)},
                  hpc=HpcConfig(host="mock"), submit_grace_s=1.0, lost_grace_s=1.0)


def make(cfg, cluster, executor=None, **kw) -> tuple[Controller, SlurmExecutor]:
    slurm = SlurmExecutor(cluster.transport, cfg.hpc, probe_ttl_s=0)
    executors = {"hpc": slurm}
    if executor is not None:
        executors["local"] = executor
    return Controller(cfg, executors, **kw), slurm


def hpc_spec(write_spec, repo, tmp_path, task_args=None, **kw):
    backends = {"local": {"workdir": str(repo), "output_root": str(tmp_path / "out")},
                "hpc": {"workdir": "~/project", "output_root": "~/sb-out",
                        "slurm": {"partition": "normal", "time": "00:10:00"}}}
    return write_spec(task_args=task_args, backends=backends, **kw)


def drive(ctl: Controller, timeout: float = 60) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        ctl.tick()
        if ctl.remote_work_remaining() == (0, 0) and ctl.local_work_remaining() == (0, 0):
            return
        time.sleep(0.1)
    raise AssertionError(f"work did not finish: {task_states(ctl)}")


def runs(counter: Path) -> int:
    return counter.read_text().count("\n") if counter.exists() else 0


def attempts(ctl):
    return ctl.conn.execute("SELECT * FROM attempts ORDER BY created_at, array_idx").fetchall()


def test_arrays_respect_max_array_size_and_complete(hpc_cfg, cluster, write_spec, repo, tmp_path):
    cluster.faults(max_array_size=3)
    ctl, _ = make(hpc_cfg, cluster)
    counter = tmp_path / "runs.txt"
    ctl.submit(hpc_spec(write_spec, repo, tmp_path, ["--append-to", str(counter)],
                        params={"grid": {"alpha": list(range(7))}, "seeds": [1]}), "hpc")
    drive(ctl)
    assert set(task_states(ctl).values()) == {"SUCCEEDED"}
    assert sorted(job["n"] for job in cluster.jobs()) == [1, 3, 3]
    assert runs(counter) == 7 and cluster.state()["sbatch_calls"] == 3
    results = ctl.results("exp")
    assert all(r["outdir"].startswith(str(cluster.home / "sb-out")) for r in results)  # ~ resolved remotely
    assert all(a["wall_s"] is not None and a["peak_rss_mb"] for a in attempts(ctl))


def test_code_identity_is_pinned_across_backends(hpc_cfg, cluster, executor, write_spec, repo, tmp_path):
    ctl, _ = make(hpc_cfg, cluster, executor)
    ident = {"code": {"git": {"paths": ["src"]}}}
    res = ctl.submit(hpc_spec(write_spec, repo, tmp_path, identity=ident), "local", tasks="0-1")
    assert ctl.assign("exp", "hpc", "2") == 1  # the cluster's clone has the same code: accepted
    (cluster.home / "project" / "src" / "model.py").write_text("X = 'edited on the cluster'\n")
    with pytest.raises(IdentityMismatch, match="code id changed"):
        ctl.assign("exp", "hpc", "3")
    assert ctl.find_experiment("exp")["code_id"] == res.code_id


def test_dropped_connection_after_sbatch_is_found_by_job_name(hpc_cfg, cluster, write_spec, repo, tmp_path):
    cluster.faults(drop_after_accept=1)
    ctl, _ = make(hpc_cfg, cluster)
    counter = tmp_path / "runs.txt"
    ctl.submit(hpc_spec(write_spec, repo, tmp_path, ["--append-to", str(counter)]), "hpc")
    assert ctl.tick().ambiguous == 1
    drive(ctl)
    assert set(task_states(ctl).values()) == {"SUCCEEDED"}
    assert cluster.state()["sbatch_calls"] == 1 and runs(counter) == 4


def test_briefly_invisible_launch_is_neither_abandoned_nor_lost(tmp_path, cluster, write_spec, repo):
    cfg = Config(home=tmp_path / "sbhome", hpc=HpcConfig(host="mock"), submit_grace_s=4.0, lost_grace_s=4.0)
    cluster.faults(drop_after_accept=1, hidden_s=1.5)  # reply lost AND not yet visible to squeue/sacct
    ctl, _ = make(cfg, cluster)
    counter = tmp_path / "runs.txt"
    ctl.submit(hpc_spec(write_spec, repo, tmp_path, ["--append-to", str(counter)]), "hpc")
    drive(ctl)
    assert set(task_states(ctl).values()) == {"SUCCEEDED"}
    assert cluster.state()["sbatch_calls"] == 1 and runs(counter) == 4
    assert not any(a["state"] in ("ABANDONED", "FAILED") for a in attempts(ctl))


def test_unreachable_cluster_means_unknown_and_no_submissions(hpc_cfg, cluster, write_spec, repo, tmp_path):
    ctl, _ = make(hpc_cfg, cluster)
    ctl.submit(hpc_spec(write_spec, repo, tmp_path, ["--seconds", "1"]), "hpc")
    ctl.tick()
    cluster.transport.down = True
    report = ctl.tick()
    assert report.unreachable == ["hpc"]
    assert {a["state"] for a in attempts(ctl)} == {"UNKNOWN"}
    time.sleep(1.5)
    ctl.tick()
    assert {a["state"] for a in attempts(ctl)} == {"UNKNOWN"}
    cluster.transport.down = False
    drive(ctl)
    assert set(task_states(ctl).values()) == {"SUCCEEDED"}
    assert cluster.state()["sbatch_calls"] == 1


def test_node_fail_is_retried_as_a_new_attempt(hpc_cfg, cluster, write_spec, repo, tmp_path):
    cluster.faults(node_fail_elements=["1"])
    ctl, _ = make(hpc_cfg, cluster)
    ctl.submit(hpc_spec(write_spec, repo, tmp_path, retries={"max": 1, "on": ["node_fail"]}), "hpc")
    drive(ctl)
    assert set(task_states(ctl).values()) == {"SUCCEEDED"}
    failed = [a for a in attempts(ctl) if a["state"] == "FAILED"]
    assert [a["reason"] for a in failed] == ["node_fail"]
    task = ctl.conn.execute("SELECT retries_used FROM tasks WHERE id=?", (failed[0]["task_id"],)).fetchone()
    assert task["retries_used"] == 1


def test_timeout_is_a_timeout_not_an_interruption(hpc_cfg, cluster, write_spec, repo, tmp_path):
    cluster.faults(timeout_elements={"0": 0.8})
    ctl, _ = make(hpc_cfg, cluster)
    ctl.submit(hpc_spec(write_spec, repo, tmp_path, ["--seconds", "20"], params={"grid": {"alpha": [1]},
                                                                               "seeds": [1]}), "hpc")
    drive(ctl)
    a = attempts(ctl)[0]
    assert task_states(ctl) == {0: "FAILED"} and (a["state"], a["reason"]) == ("FAILED", "timeout")
    assert len(attempts(ctl)) == 1  # not requeued as an interruption


def test_oom_kill_without_status_is_reported_as_oom(hpc_cfg, cluster, write_spec, repo, tmp_path):
    cluster.faults(oom_elements={"0": 0.8})
    ctl, _ = make(hpc_cfg, cluster)
    ctl.submit(hpc_spec(write_spec, repo, tmp_path, ["--seconds", "20"], params={"grid": {"alpha": [1]},
                                                                               "seeds": [1]}), "hpc")
    drive(ctl)
    a = attempts(ctl)[0]
    assert (a["state"], a["reason"]) == ("FAILED", "oom")
    assert a["wall_s"] is not None  # filled in from sacct since the shim was killed


def test_external_scancel_cancels_the_task(hpc_cfg, cluster, write_spec, repo, tmp_path):
    ctl, _ = make(hpc_cfg, cluster)
    ctl.submit(hpc_spec(write_spec, repo, tmp_path, ["--seconds", "20"], params={"grid": {"alpha": [1]},
                                                                               "seeds": [1]}), "hpc")
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        ctl.tick()
        if attempts(ctl) and attempts(ctl)[0]["state"] == "RUNNING":
            break
        time.sleep(0.1)
    job = attempts(ctl)[0]
    external = ctl.conn.execute("SELECT external_id FROM launches WHERE id=?", (job["launch_id"],)).fetchone()[0]
    cluster.transport.run(f"scancel {external}_0")
    drive(ctl)
    assert task_states(ctl) == {0: "CANCELLED"}
    assert attempts(ctl)[0]["reason"] == "cancelled_externally" and len(attempts(ctl)) == 1


def test_resubmitting_a_generated_script_by_hand_does_not_rerun(hpc_cfg, cluster, write_spec, repo, tmp_path):
    ctl, slurm = make(hpc_cfg, cluster)
    counter = tmp_path / "runs.txt"
    ctl.submit(hpc_spec(write_spec, repo, tmp_path, ["--append-to", str(counter)]), "hpc")
    drive(ctl)
    launch = ctl.conn.execute("SELECT id FROM launches").fetchone()[0]
    cluster.transport.run(f"cd {slurm.launch_dir(launch)} && sbatch --parsable job.sbatch")
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline and any(
            el["state"] in ("PENDING", "RUNNING") for j in cluster.jobs() for el in j["elements"].values()):
        time.sleep(0.1)
    assert runs(counter) == 4  # every element found its attempt already claimed and exited


def test_runner_is_installed_once(hpc_cfg, cluster):
    slurm = SlurmExecutor(cluster.transport, hpc_cfg.hpc)
    path = slurm.ensure_runner()
    SlurmExecutor(cluster.transport, hpc_cfg.hpc).ensure_runner()
    puts = [c for c in cluster.transport.calls if "b64decode" in c]
    assert len(puts) == 2  # runner files + OK marker, only on the first install
    assert (Path(path) / "array.py").exists() and path.startswith(str(cluster.home))


def test_probe_reads_limits(hpc_cfg, cluster):
    cluster.faults(max_array_size=50, max_submit=20)
    slurm = SlurmExecutor(cluster.transport, hpc_cfg.hpc)
    info = slurm.probe()
    assert info["MaxArraySize"] == "50" and info["MaxSubmitJobs"] == 20 and info["accounting"] is True
    assert slurm.max_chunk() == 20


def test_export_slurm_is_self_contained(cluster, write_spec, repo, tmp_path):
    counter = tmp_path / "runs.txt"
    loaded = hpc_spec(write_spec, repo, tmp_path, ["--append-to", str(counter)])
    out = export_slurm(loaded, cluster.home / "export1")
    assert {p.name for p in out.iterdir()} == {"runner", "manifest.jsonl", "job.sbatch"}
    cluster.transport.run(f"cd {out} && sbatch job.sbatch")
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline and runs(counter) < 4:
        time.sleep(0.1)
    time.sleep(0.5)
    statuses = [json.loads(p.read_text()) for p in (cluster.home / "sb-out").rglob("status.json")]
    assert len(statuses) == 4 and {s["state"] for s in statuses} == {"SUCCEEDED"}
    assert load_spec(tmp_path / "exp.yaml")  # spec untouched
    assert shutil.which("python3") and sys.executable
