"""M4: manual mixed routing, cancel, retry, logs."""

from __future__ import annotations

import time

from switchboard.config import Budget, Config, HpcConfig
from switchboard.controller import Controller
from switchboard.executors.slurm import SlurmExecutor

from ..cli_helpers import exp_status
from ..conftest import drive as drive_local
from ..conftest import task_states
from .test_slurm import drive, hpc_spec


def mixed(tmp_path, executor, cluster, cpus=4) -> Controller:
    cfg = Config(home=tmp_path / "sbhome", budgets={"focus": Budget(cpus=cpus, mem_gb=8, max_admits_per_tick=8)},
                 hpc=HpcConfig(host="mock"), submit_grace_s=1.0, lost_grace_s=2.0)
    slurm = SlurmExecutor(cluster.transport, cfg.hpc, probe_ttl_s=0)
    return Controller(cfg, {"local": executor, "hpc": slurm})


def attempts(ctl, where="1=1", args=()):
    return ctl.conn.execute(f"SELECT * FROM attempts WHERE {where} ORDER BY created_at", args).fetchall()


def wait_running(ctl, n, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        ctl.tick()
        if len(attempts(ctl, "state='RUNNING'")) >= n:
            return
        time.sleep(0.1)
    raise AssertionError("attempts did not start")


def test_200_tasks_split_by_hand_between_mac_and_cluster(tmp_path, executor, cluster, write_spec, repo):
    ctl = mixed(tmp_path, executor, cluster)
    spec = hpc_spec(write_spec, repo, tmp_path, params={"grid": {"alpha": list(range(200))}, "seeds": [1]})
    spec.spec.backends["hpc"].slurm.array_throttle = 25
    ctl.submit(spec, "local")
    moved = ctl.assign("exp", "hpc", count=120)
    assert moved == 120
    by_backend = dict(ctl.conn.execute("SELECT assigned_backend, count(*) FROM tasks GROUP BY 1").fetchall())
    assert by_backend == {"local": 80, "hpc": 120}
    # the tail went to the cluster, the head stays on the Mac
    assert ctl.conn.execute("SELECT max(idx) FROM tasks WHERE assigned_backend='local'").fetchone()[0] == 79

    status = exp_status(ctl, "exp")
    assert status["pending"] == {"local": 80, "hpc": 120}
    drive(ctl, timeout=180)

    assert set(task_states(ctl).values()) == {"SUCCEEDED"}
    rows = ctl.conn.execute("SELECT t.id, t.assigned_backend, count(a.id) n, "
                            "sum(a.id = t.accepted_attempt_id) accepted, group_concat(DISTINCT a.backend) backends "
                            "FROM tasks t JOIN attempts a ON a.task_id=t.id GROUP BY t.id").fetchall()
    assert len(rows) == 200
    assert all(r["accepted"] == 1 for r in rows)  # exactly one accepted result per task
    assert all(r["backends"] == r["assigned_backend"] for r in rows)  # disjoint: each task ran on one side
    assert {r["backend"] for r in ctl.results("exp")} == {"local", "hpc"}
    assert sum(1 for r in ctl.results("exp") if r["backend"] == "hpc") == 120
    assert cluster.state()["sbatch_calls"] == 1  # one 120-element array


def test_cancel_pending_and_running_local_tasks(cfg, executor, write_spec):
    budget = Config(home=cfg.home, budgets={"focus": Budget(cpus=4, mem_gb=8, max_admits_per_tick=8)})
    ctl = Controller(budget, {"local": executor})
    ctl.submit(write_spec(task_args=["--seconds", "20"], params={"grid": {"alpha": [1, 2, 3]}, "seeds": [1]},
                          resources={"cpus": 2, "mem_gb": 1}), "local")
    ctl.tick()  # two 2-cpu tasks fill the budget; the third waits
    counts = ctl.cancel("exp")
    assert counts == {"pending_cancelled": 1, "signalled": 2, "already_final": 0}
    drive_local(ctl, executor)
    assert set(task_states(ctl).values()) == {"CANCELLED"}
    assert len(attempts(ctl)) == 2  # cancelled runners were not requeued as interruptions
    assert ctl.tick().admitted == 0


def test_cancel_reaches_a_backend_that_was_unreachable(cfg, executor, write_spec):
    ctl = Controller(cfg, {"local": executor})
    ctl.submit(write_spec(task_args=["--seconds", "20"], params={"grid": {"alpha": [1]}, "seeds": [1]}), "local")
    ctl.tick()
    executor.faults.unreachable = True
    assert ctl.cancel("exp/0")["signalled"] == 1  # recorded even though the signal could not be sent
    ctl.tick()
    executor.faults.unreachable = False
    drive_local(ctl, executor)
    assert task_states(ctl) == {0: "CANCELLED"}


def test_cancel_during_an_ambiguous_submission(cfg, executor, write_spec):
    ctl = Controller(cfg, {"local": executor})
    ctl.submit(write_spec(task_args=["--seconds", "20"], params={"grid": {"alpha": [1]}, "seeds": [1]}), "local")
    executor.faults.ambiguous = 1
    ctl.tick()
    assert ctl.cancel("exp")["signalled"] == 1  # no external id yet: nothing to signal right now
    drive_local(ctl, executor)  # the launch is found by its id, then cancelled
    assert task_states(ctl) == {0: "CANCELLED"}


def test_cancel_on_the_cluster(tmp_path, executor, cluster, write_spec, repo):
    ctl = mixed(tmp_path, executor, cluster)
    ctl.submit(hpc_spec(write_spec, repo, tmp_path, ["--seconds", "30"],
                        params={"grid": {"alpha": [1, 2]}, "seeds": [1]}), "hpc")
    wait_running(ctl, 2)
    ctl.cancel("exp")
    drive(ctl)
    assert set(task_states(ctl).values()) == {"CANCELLED"}
    assert len(attempts(ctl)) == 2


def test_manual_retry_resets_the_retry_budget(cfg, executor, write_spec):
    ctl = Controller(cfg, {"local": executor})
    ctl.submit(write_spec(task_args=["--exit-code", "3"], params={"grid": {"alpha": [1, 2]}, "seeds": [1]},
                          retries={"max": 1, "on": ["nonzero"]}), "local")
    drive_local(ctl, executor)
    assert set(task_states(ctl).values()) == {"FAILED"}
    assert ctl.retry("exp/0", failed_only=True) == 1
    task = ctl.find_task("exp/0")
    assert (task["state"], task["retries_used"]) == ("PENDING", 0)
    drive_local(ctl, executor)
    assert len(attempts(ctl, "task_id=?", (task["id"],))) == 4  # 2 before, 2 after the manual retry
    assert ctl.retry("exp/1") == 1 and ctl.retry("exp/1") == 0  # already pending: nothing to do


def test_logs_from_the_mac_and_the_cluster(tmp_path, executor, cluster, write_spec, repo):
    ctl = mixed(tmp_path, executor, cluster)
    ctl.submit(hpc_spec(write_spec, repo, tmp_path, params={"grid": {"alpha": [1, 2]}, "seeds": [1]}), "local",
               tasks="0")
    ctl.assign("exp", "hpc", tasks="1")
    drive(ctl)
    local = ctl.logs("exp/0")
    assert local["backend"] == "local" and "synth: done (seed 1)" in local["streams"]["stdout"]["lines"]
    remote = ctl.logs("exp/1")
    assert remote["backend"] == "hpc" and "synth: done (seed 1)" in remote["streams"]["stdout"]["lines"]
    assert remote["streams"]["slurm"]["lines"] is not None  # Slurm's own log for the element
