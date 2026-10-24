"""M6: automatic routing (local-first with cluster overflow) through the controller."""

from __future__ import annotations

from switchboard.config import Budget, Config, HpcConfig
from switchboard.controller import Controller, TickReport
from switchboard.executors.fake import FakeExecutor
from switchboard.executors.slurm import SlurmExecutor

from ..conftest import task_states
from .test_slurm import drive, hpc_spec


def by_backend(ctl):
    return dict(ctl.conn.execute("SELECT coalesce(assigned_backend, '-'), count(*) FROM tasks GROUP BY 1").fetchall())


def fake_pair(tmp_path, cpus=4):
    cfg = Config(home=tmp_path / "sbhome", budgets={"focus": Budget(cpus=cpus, mem_gb=8, max_admits_per_tick=8),
                                                    "away": Budget(cpus=8, mem_gb=16, max_admits_per_tick=8)},
                 hpc=HpcConfig(host="fake"), submit_grace_s=1.0, lost_grace_s=2.0)
    local = FakeExecutor(cfg.launch_dir, name="local")
    hpc = FakeExecutor(cfg.launch_dir / "hpc", name="hpc", platform="linux-64")
    return cfg, local, hpc


def test_auto_submit_routes_and_explains(tmp_path, write_spec, repo):
    cfg, local, hpc = fake_pair(tmp_path)
    ctl = Controller(cfg, {"local": local, "hpc": hpc})
    spec = hpc_spec(write_spec, repo, tmp_path, params={"grid": {"alpha": list(range(20))}, "seeds": [1]},
                    resources={"cpus": 1, "mem_gb": 0.5, "runtime_min": 60}, placement={"max_local_wait_h": 2})
    res = ctl.submit(spec, "auto")
    assert res.policy == "local-first" and res.backend == "auto"
    ctl._route(TickReport())
    # 4 cpus x 2 h = 8 one-hour tasks on the Mac; the rest overflow
    assert by_backend(ctl) == {"local": 8, "hpc": 12}
    assert ctl.find_task("exp/0")["route_note"].startswith("[R5] local-first: the Mac finishes it in ~15 min")
    assert ctl.find_task("exp/7")["route_note"].startswith("[R5] local-first: the Mac finishes it in ~2.0 h")
    assert "overflow" in ctl.find_task("exp/19")["route_note"]
    for ex in (local, hpc):
        ex.wait_all()


def test_cluster_unreachable_keeps_work_local_then_overflow_resumes(tmp_path, write_spec, repo):
    cfg, local, hpc = fake_pair(tmp_path)
    ctl = Controller(cfg, {"local": local, "hpc": hpc})
    ctl.submit(hpc_spec(write_spec, repo, tmp_path, params={"grid": {"alpha": list(range(20))}, "seeds": [1]},
                        resources={"cpus": 1, "mem_gb": 0.5, "runtime_min": 60}, placement={"max_local_wait_h": 2}),
               "auto")
    ctl.set_control("local_paused", "1")  # hold local admission so the whole pool stays pending
    hpc.faults.unreachable = True
    report = ctl.tick()
    assert "hpc" in report.unreachable
    assert by_backend(ctl) == {"local": 20}
    assert "the cluster is unreachable" in ctl.find_task("exp/19")["route_note"]
    hpc.faults.unreachable = False
    ctl.tick()  # reachable again: overflow is routed and submitted in the same pass
    states = dict(ctl.conn.execute("SELECT assigned_backend, count(*) FROM tasks GROUP BY 1").fetchall())
    assert states == {"local": 8, "hpc": 12}
    assert hpc.submissions == 1
    assert ctl.conn.execute("SELECT count(*) FROM attempts").fetchone()[0] == 12
    for ex in (local, hpc):
        ex.wait_all()


def test_identity_mismatch_on_the_cluster_keeps_work_local(tmp_path, write_spec, repo, cluster):
    cfg = Config(home=tmp_path / "sbhome", budgets={"focus": Budget(cpus=1, mem_gb=8, max_admits_per_tick=1)},
                 hpc=HpcConfig(host="mock"))
    local = FakeExecutor(cfg.launch_dir, name="local")
    slurm = SlurmExecutor(cluster.transport, cfg.hpc, probe_ttl_s=0)
    ctl = Controller(cfg, {"local": local, "hpc": slurm})
    (cluster.home / "project" / "src" / "model.py").write_text("X = 'different on the cluster'\n")
    ctl.submit(hpc_spec(write_spec, repo, tmp_path, identity={"code": {"git": {"paths": ["src"]}}},
                        params={"grid": {"alpha": list(range(6))}, "seeds": [1]},
                        resources={"cpus": 1, "mem_gb": 0.5, "runtime_min": 60}, placement={"max_local_wait_h": 1}),
               "auto")
    ctl.tick()
    assert by_backend(ctl) == {"local": 6}
    note = ctl.find_task("exp/5")["route_note"]
    assert note.startswith("[R3] stays on the Mac: identity mismatch on hpc") and "code id changed" in note
    assert cluster.jobs() == []
    local.wait_all()


def test_manual_assign_turns_routing_off(tmp_path, write_spec, repo):
    cfg, local, hpc = fake_pair(tmp_path)
    ctl = Controller(cfg, {"local": local, "hpc": hpc})
    ctl.submit(hpc_spec(write_spec, repo, tmp_path, params={"grid": {"alpha": list(range(4))}, "seeds": [1]},
                        resources={"cpus": 1, "mem_gb": 0.5, "runtime_min": 60}), "auto")
    ctl.set_control("local_paused", "1")
    hpc.faults.unreachable = True
    ctl.tick()  # routed to the Mac (cluster down) but held by the pause
    hpc.faults.unreachable = False
    ctl.assign("exp", "hpc", tasks="0-3")
    assert ctl.find_experiment("exp")["policy"] == "manual"
    ctl.tick()
    assert by_backend(ctl) == {"hpc": 4}  # the router no longer overrides the manual choice
    ctl.set_policy("exp", "local-first")
    assert ctl.find_experiment("exp")["policy"] == "local-first"
    for ex in (local, hpc):
        ex.wait_all()


def test_200_task_overflow_run_completes_with_one_result_each(tmp_path, write_spec, repo, cluster):
    cfg = Config(home=tmp_path / "sbhome", budgets={"focus": Budget(cpus=4, mem_gb=8, max_admits_per_tick=8)},
                 hpc=HpcConfig(host="mock"), submit_grace_s=1.0, lost_grace_s=2.0)
    local = FakeExecutor(cfg.launch_dir, name="local")
    slurm = SlurmExecutor(cluster.transport, cfg.hpc, probe_ttl_s=0)
    ctl = Controller(cfg, {"local": local, "hpc": slurm})
    # 200 x 1 cpu x 1 min declared; 4 cpus x 20 min limit = 80 task-minutes local; 120 overflow
    spec = hpc_spec(write_spec, repo, tmp_path, params={"grid": {"alpha": list(range(200))}, "seeds": [1]},
                    resources={"cpus": 1, "mem_gb": 0.5, "runtime_min": 1}, placement={"max_local_wait_h": 1 / 3})
    spec.spec.backends["hpc"].slurm.array_throttle = 25
    ctl.submit(spec, "auto")
    ctl.tick()
    assert by_backend(ctl) == {"local": 80, "hpc": 120}
    drive(ctl, timeout=240)
    assert set(task_states(ctl).values()) == {"SUCCEEDED"}
    rows = ctl.conn.execute("SELECT t.assigned_backend b, count(a.id) n, sum(a.id = t.accepted_attempt_id) acc, "
                            "group_concat(DISTINCT a.backend) ran FROM tasks t JOIN attempts a ON a.task_id=t.id "
                            "GROUP BY t.id").fetchall()
    assert len(rows) == 200 and all(r["acc"] == 1 and r["ran"] == r["b"] for r in rows)
    results = ctl.results("exp")
    assert sum(r["backend"] == "local" for r in results) == 80
    assert sum(r["backend"] == "hpc" for r in results) == 120
