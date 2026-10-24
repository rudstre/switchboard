"""Controller behaviour against the fake executor running the real shim."""

import time

import pytest

from switchboard.config import Budget, Config
from switchboard.controller import Controller, IdentityMismatch, SimulatedCrash

from ..conftest import drive, task_states

IDENTITY = {"code": {"git": {"paths": ["src"]}}, "env": {"command": "echo env-v1"}}


def attempts(ctl, state=None):
    q = "SELECT * FROM attempts" + (" WHERE state=?" if state else "")
    return ctl.conn.execute(q, (state,) if state else ()).fetchall()


def test_end_to_end_success_and_results(controller, executor, write_spec):
    res = controller.submit(write_spec(identity=IDENTITY), "local")
    assert res.created and res.tasks_total == 4 and res.code_id.startswith("git:")
    drive(controller, executor)
    assert set(task_states(controller).values()) == {"SUCCEEDED"}
    results = controller.results("exp")
    assert len(results) == 4 and all(r["backend"] == "local" for r in results)
    assert all(a["code_id"] == res.code_id and a["env_id"] == res.env_ids[executor.platform]
               for a in attempts(controller))


def test_missing_outputs_fail_the_task(controller, executor, write_spec):
    controller.submit(write_spec(outputs={"expect": ["result.json", "never.npz"]}), "local")
    drive(controller, executor)
    assert set(task_states(controller).values()) == {"FAILED"}
    assert {a["reason"] for a in attempts(controller)} == {"missing_outputs"}


def test_retries_create_new_attempts(controller, executor, write_spec):
    controller.submit(write_spec(task_args=["--exit-code", "4"], retries={"max": 2, "on": ["nonzero"]}), "local")
    drive(controller, executor)
    assert set(task_states(controller).values()) == {"FAILED"}
    rows = controller.conn.execute("SELECT retries_used, count(a.id) n FROM tasks t JOIN attempts a ON a.task_id=t.id "
                                   "GROUP BY t.id").fetchall()
    assert all(r["retries_used"] == 2 and r["n"] == 3 for r in rows)


def test_resubmit_is_idempotent_and_identity_changes_are_refused(controller, executor, write_spec, repo):
    controller.submit(write_spec(identity=IDENTITY), "local")
    drive(controller, executor)
    again = controller.submit(write_spec(identity=IDENTITY), "local")
    assert not again.created and again.tasks_assigned == 0
    assert controller.tick().admitted == 0

    # Changing the command is an identity change.
    with pytest.raises(IdentityMismatch, match="spec identity changed .*command"):
        controller.submit(write_spec(identity=IDENTITY, task_args=["--seconds", "0.01"]), "local")
    # Editing code is an identity change, even though the spec text is the same.
    (repo / "src" / "model.py").write_text("X = 2\n")
    with pytest.raises(IdentityMismatch, match="code id changed"):
        controller.submit(write_spec(identity=IDENTITY), "local")
    # ... and so is a changed environment on the same platform.
    (repo / "src" / "model.py").write_text("X = 1\n")
    env2 = {**IDENTITY, "env": {"command": "echo env-v2"}}
    with pytest.raises(IdentityMismatch, match="spec identity changed"):  # env definition is part of the spec
        controller.submit(write_spec(identity=env2), "local")

    run2 = controller.submit(write_spec(identity=IDENTITY, task_args=["--seconds", "0.01"]), "local", new_run=True)
    assert run2.created and run2.experiment == "exp#2"
    drive(controller, executor)
    assert len(controller.results("exp#2")) == 4 and len(controller.results("exp")) == 4


def test_environment_drift_with_unchanged_definition_is_refused(controller, executor, write_spec, tmp_path):
    marker = tmp_path / "envver"
    marker.write_text("1")
    ident = {"env": {"command": f"cat {marker}"}}
    controller.submit(write_spec(identity=ident), "local")
    marker.write_text("2")  # e.g. a package upgrade
    with pytest.raises(IdentityMismatch, match="env id changed on"):
        controller.submit(write_spec(identity=ident), "local")


def test_non_identity_changes_apply_to_pending_tasks_only(controller, executor, write_spec):
    controller.submit(write_spec(), "local", tasks="0-1")
    drive(controller, executor)
    controller.submit(write_spec(resources={"cpus": 2, "mem_gb": 1}), "local")
    rows = {r["idx"]: r for r in controller.conn.execute("SELECT * FROM tasks")}
    assert rows[0]["cpus"] == 1 and rows[0]["state"] == "SUCCEEDED"
    assert rows[2]["cpus"] == 2 and rows[2]["assigned_backend"] == "local"


def test_code_identity_is_pinned_at_submit_not_by_first_attempt(controller, executor, write_spec, repo):
    controller.submit(write_spec(identity=IDENTITY), "local")
    (repo / "src" / "model.py").write_text("edited before anything ran\n")
    drive(controller, executor)
    assert set(task_states(controller).values()) == {"FAILED"}
    assert {a["reason"] for a in attempts(controller)} == {"fingerprint_mismatch"}
    assert not any((p / "out" / "result.json").exists() for p in (repo.parent / "out").rglob("a-*"))


def test_reserved_budget_is_never_exceeded(cfg, executor, write_spec):
    cfg2 = Config(home=cfg.home, budgets={"focus": Budget(cpus=3, mem_gb=8, max_admits_per_tick=10)},
                  submit_grace_s=0.5, lost_grace_s=0.5)
    ctl = Controller(cfg2, {"local": executor})
    ctl.submit(write_spec(task_args=["--seconds", "0.3"], params={"grid": {"alpha": list(range(8))}, "seeds": [1]},
                          resources={"cpus": 2, "mem_gb": 1}), "local")
    peak = 0
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        ctl.tick()
        reserved = ctl.conn.execute("SELECT coalesce(sum(t.cpus),0) FROM attempts a JOIN tasks t ON t.id=a.task_id "
                                    "WHERE a.state IN ('SUBMITTING','SUBMITTED','RUNNING','UNKNOWN')").fetchone()[0]
        peak = max(peak, reserved)
        active, pending = ctl.local_work_remaining()
        if active == 0 and pending == 0:
            break
        time.sleep(0.05)
    assert peak == 2  # one 2-cpu task fits in 3 cpus, two do not
    assert set(task_states(ctl).values()) == {"SUCCEEDED"}


def test_pause_local_gate(controller, executor, write_spec):
    controller.submit(write_spec(), "local")
    controller.set_control("local_paused", "1")
    assert controller.tick().admitted == 0
    note = controller.conn.execute("SELECT note FROM tasks LIMIT 1").fetchone()[0]
    assert "paused" in note
    controller.set_control("local_paused", "0")
    drive(controller, executor)
    assert set(task_states(controller).values()) == {"SUCCEEDED"}


# ---- recovery and duplicate handling --------------------------------------------------------


@pytest.mark.parametrize("crashpoint", ["after_intent", "after_submit"])
def test_controller_crash_during_dispatch(cfg, executor, write_spec, tmp_path, crashpoint):
    counter = tmp_path / "runs.txt"
    spec = write_spec(task_args=["--append-to", str(counter)], params={"grid": {"alpha": [1]}, "seeds": [1]})
    crashing = Controller(cfg, {"local": executor}, crashpoints={crashpoint})
    crashing.submit(spec, "local")
    with pytest.raises(SimulatedCrash):
        crashing.tick()
    # "Restart": a fresh controller over the same database and the same (persistent) backend.
    restarted = Controller(cfg, {"local": executor})
    executor.wait_all()
    time.sleep(cfg.submit_grace_s + 0.1)
    drive(restarted, executor)
    executor.wait_all()
    drive(restarted, executor)
    assert task_states(restarted) == {0: "SUCCEEDED"}
    assert counter.read_text().count("\n") == 1  # executed exactly once in both cases
    expected_attempts = 2 if crashpoint == "after_intent" else 1  # the never-launched intent is ABANDONED
    assert len(attempts(restarted)) == expected_attempts
    assert executor.submissions == 1


def test_ambiguous_submission_is_found_by_launch_id_not_resubmitted(controller, executor, write_spec, tmp_path):
    counter = tmp_path / "runs.txt"
    controller.submit(write_spec(task_args=["--append-to", str(counter)],
                                 params={"grid": {"alpha": [1]}, "seeds": [1]}), "local")
    executor.faults.ambiguous = 1
    report = controller.tick()
    assert report.ambiguous == 1 and report.admitted == 0
    drive(controller, executor)
    assert task_states(controller) == {0: "SUCCEEDED"}
    assert executor.submissions == 1 and counter.read_text().count("\n") == 1
    events = controller.conn.execute("SELECT reason FROM events WHERE entity='launch'").fetchall()
    assert any("found by idempotency key" in e["reason"] for e in events)


def test_invisible_launch_that_starts_late_is_found_by_its_status(controller, executor, write_spec, tmp_path):
    """The backend lost its record, but the runner wrote status.json before the grace period ended."""
    counter = tmp_path / "runs.txt"
    controller.submit(write_spec(task_args=["--append-to", str(counter), "--seconds", "1.0"],
                                 params={"grid": {"alpha": [1]}, "seeds": [1]}), "local")
    executor.faults.vanish = 1
    controller.tick()
    time.sleep(0.3)  # runner starts and writes status.json within submit_grace_s
    drive(controller, executor)
    assert task_states(controller) == {0: "SUCCEEDED"}
    assert len(attempts(controller)) == 1 and counter.read_text().count("\n") == 1


def test_lost_submission_runs_twice_but_only_one_result_is_accepted(controller, executor, write_spec, tmp_path):
    """At-least-once: a launch stayed invisible past the grace period and then ran anyway."""
    counter = tmp_path / "runs.txt"
    controller.submit(write_spec(task_args=["--append-to", str(counter), "--seconds", "1.5"],
                                 params={"grid": {"alpha": [1]}, "seeds": [1]}), "local")
    executor.faults.vanish = 1
    executor.faults.start_delay = 1.5  # e.g. queued somewhere the lookup cannot see
    controller.tick()
    time.sleep(0.6)  # > submit_grace_s with no trace -> ABANDONED, task requeued
    controller.tick()
    executor.faults.start_delay = 0
    drive(controller, executor)  # second attempt runs and is accepted
    executor.wait_all()
    controller.tick()  # late evidence: the abandoned attempt also succeeded
    states = sorted(a["state"] for a in attempts(controller))
    assert states == ["SUCCEEDED", "SUPERSEDED"]
    assert counter.read_text().count("\n") == 2
    accepted = controller.conn.execute("SELECT accepted_attempt_id FROM tasks").fetchone()[0]
    assert controller.results("exp")[0]["attempt_id"] == accepted


def test_double_start_of_one_attempt_is_blocked_by_claim(controller, executor, write_spec, tmp_path):
    counter = tmp_path / "runs.txt"
    controller.submit(write_spec(task_args=["--append-to", str(counter), "--seconds", "0.3"],
                                 params={"grid": {"alpha": [1]}, "seeds": [1]}), "local")
    executor.faults.double_start = 1
    drive(controller, executor)
    executor.wait_all()
    assert task_states(controller) == {0: "SUCCEEDED"}
    assert counter.read_text().count("\n") == 1


def test_unreachable_backend_means_unknown_not_failed(controller, executor, write_spec):
    controller.submit(write_spec(task_args=["--seconds", "0.5"], params={"grid": {"alpha": [1]}, "seeds": [1]}),
                      "local")
    controller.tick()
    executor.faults.unreachable = True
    report = controller.tick()
    assert report.unreachable == ["local"]
    assert [a["state"] for a in attempts(controller)] == ["UNKNOWN"]
    time.sleep(1.0)
    controller.tick()
    assert [a["state"] for a in attempts(controller)] == ["UNKNOWN"]  # still not failed, nothing resubmitted
    executor.faults.unreachable = False
    drive(controller, executor)
    assert task_states(controller) == {0: "SUCCEEDED"} and len(attempts(controller)) == 1


def test_runner_killed_without_status_is_runner_died(controller, executor, write_spec):
    controller.submit(write_spec(task_args=["--seconds", "20"], params={"grid": {"alpha": [1]}, "seeds": [1]}),
                      "local")
    controller.tick()
    proc = executor.all_procs()[0]
    time.sleep(0.5)
    import os
    import signal
    os.killpg(proc.pid, signal.SIGKILL)  # shim and task die without writing a final status
    proc.wait()
    drive(controller, executor)
    a = attempts(controller)[0]
    assert a["state"] == "FAILED" and a["reason"] == "runner_died"


def test_explain_data_records_why(controller, executor, write_spec):
    controller.submit(write_spec(resources={"cpus": 16, "mem_gb": 1}), "local")
    controller.tick()
    task = controller.find_task("exp/0")
    assert "larger than the whole 'focus' budget" in task["note"]


def test_launch_prefix_is_applied(cfg, executor, write_spec):
    niced = Config(home=cfg.home, budgets={"focus": Budget(cpus=4, mem_gb=8, max_admits_per_tick=4,
                                                           launch_prefix=("nice", "-n", "10"))})
    ctl = Controller(niced, {"local": executor})
    ctl.submit(write_spec(command=["/bin/sh", "-c", 'ps -o nice= -p $$ > "{outdir}/result.json"'],
                          params={"grid": {"alpha": [1]}, "seeds": [1]}), "local")
    drive(ctl, executor)
    outdir = ctl.results("exp")[0]["outdir"]
    assert open(f"{outdir}/result.json").read().strip() == "10"


def test_gate_blocks_new_admissions_but_leaves_running_tasks_alone(cfg, executor, write_spec):
    pressure = {"level": 1}
    from switchboard.macos import memory_pressure_gate
    ctl = Controller(cfg, {"local": executor}, gates=[memory_pressure_gate(lambda: pressure["level"])])
    ctl.submit(write_spec(task_args=["--seconds", "0.8"], params={"grid": {"alpha": [1, 2]}, "seeds": [1]},
                          resources={"cpus": 4, "mem_gb": 1}), "local")
    assert ctl.tick().admitted == 1  # one 4-cpu task fills the focus budget
    pressure["level"] = 2
    executor.wait_all()
    report = ctl.tick()
    assert report.accepted == 1 and report.admitted == 0  # the running task finished normally
    note = ctl.conn.execute("SELECT note FROM tasks WHERE state='PENDING'").fetchone()[0]
    assert "memory pressure WARN" in note
    pressure["level"] = 1
    drive(ctl, executor)
    assert set(task_states(ctl).values()) == {"SUCCEEDED"}


def test_underestimated_memory_is_flagged_not_killed(controller, executor, write_spec):
    controller.submit(write_spec(task_args=["--mem-mb", "300"], params={"grid": {"alpha": [1]}, "seeds": [1]},
                                 resources={"cpus": 1, "mem_gb": 0.1}), "local")
    drive(controller, executor)
    assert task_states(controller) == {0: "SUCCEEDED"}
    warnings = controller.usage_warnings(controller.find_experiment("exp")["id"])
    assert any("exceeded its 103 MB reservation" in w for w in warnings)
