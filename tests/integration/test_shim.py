"""The shim runs as a separate process under several Pythons, including the oldest one available."""

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from switchboard.executors.fake import SHIM_PATH
from switchboard.runner.fingerprint import git_fingerprint

from ..conftest import SYNTH

PYTHONS = [sys.executable] + [p for p in ("/usr/bin/python3",) if os.path.exists(p)]


@pytest.fixture(params=PYTHONS, ids=lambda p: Path(p).name if p != sys.executable else "dev-python")
def python(request):
    return request.param


def make_spec(tmp_path: Path, command, **extra) -> dict:
    attempt_dir = tmp_path / "attempt"
    spec = {
        "attempt_id": "a-test", "task_key": "00000-abc", "experiment": "exp", "idx": 3,
        "attempt_dir": str(attempt_dir), "outdir": str(attempt_dir / "out"), "workdir": str(tmp_path),
        "setup": None, "command": command, "env": {}, "cpus": 3, "params": {"alpha": 1}, "seed": 9,
        "outputs_expect": ["result.json"], "identity": None, "expected": {},
    }
    spec.update(extra)
    return spec


def run_shim(python, tmp_path, spec, wait=True):
    path = tmp_path / "attempt.json"
    path.write_text(json.dumps(spec))
    proc = subprocess.Popen([python, str(SHIM_PATH), str(path)], start_new_session=True)
    if wait:
        proc.wait(timeout=60)
    return proc


def status(spec):
    return json.loads((Path(spec["attempt_dir"]) / "status.json").read_text())



def test_success_writes_status_metrics_and_success_marker(python, tmp_path):
    spec = make_spec(tmp_path, [sys.executable, SYNTH, "task", "--write", "result.json", "--seed", "9"])
    proc = run_shim(python, tmp_path, spec)
    st = status(spec)
    assert proc.returncode == 0 and st["state"] == "SUCCEEDED"
    assert (Path(spec["attempt_dir"]) / "_SUCCESS").exists()
    assert st["wall_s"] >= 0 and st["cpu_s"] >= 0 and st["peak_rss_mb"] > 1
    out = json.loads((Path(spec["outdir"]) / "result.json").read_text())
    assert out["threads_env"] == "3"  # OMP_NUM_THREADS follows the reservation
    assert json.loads((Path(spec["attempt_dir"]) / "params.json").read_text()) == {"params": {"alpha": 1}, "seed": 9}


def test_exit_zero_with_missing_outputs_is_a_failure(python, tmp_path):
    spec = make_spec(tmp_path, [sys.executable, SYNTH, "task"])
    proc = run_shim(python, tmp_path, spec)
    st = status(spec)
    assert proc.returncode == 88
    assert st["state"] == "FAILED" and st["reason"] == "missing_outputs" and st["missing_outputs"] == ["result.json"]
    assert not (Path(spec["attempt_dir"]) / "_SUCCESS").exists()


def test_nonzero_exit(python, tmp_path):
    spec = make_spec(tmp_path, [sys.executable, SYNTH, "task", "--exit-code", "5"])
    proc = run_shim(python, tmp_path, spec)
    assert proc.returncode == 5
    assert status(spec)["reason"] == "nonzero" and status(spec)["exit_code"] == 5


def test_second_claim_does_not_run_the_command(python, tmp_path):
    counter = tmp_path / "runs.txt"
    spec = make_spec(tmp_path, [sys.executable, SYNTH, "task", "--write", "result.json", "--append-to", str(counter)])
    run_shim(python, tmp_path, spec)
    first = status(spec)
    dup = run_shim(python, tmp_path, spec)
    assert dup.returncode == 86
    assert counter.read_text().count("\n") == 1
    assert status(spec) == first  # the original attempt's status is untouched


def test_fingerprint_mismatch_fails_without_running(python, tmp_path, repo):
    counter = tmp_path / "runs.txt"
    pinned = git_fingerprint(str(repo), ["."])
    (repo / "src" / "model.py").write_text("changed after submit\n")
    spec = make_spec(tmp_path, [sys.executable, SYNTH, "task", "--append-to", str(counter)], workdir=str(repo),
                     identity={"code": {"git": {"paths": ["."]}}}, expected={"code_id": pinned})
    proc = run_shim(python, tmp_path, spec)
    st = status(spec)
    assert proc.returncode == 87 and st["reason"] == "fingerprint_mismatch"
    assert not counter.exists()


def test_sigterm_is_forwarded_and_recorded_as_cancelled(python, tmp_path):
    spec = make_spec(tmp_path, [sys.executable, SYNTH, "task", "--seconds", "30"])
    proc = run_shim(python, tmp_path, spec, wait=False)
    deadline = time.time() + 10
    while not (Path(spec["attempt_dir"]) / "stdout.log").exists() and time.time() < deadline:
        time.sleep(0.05)
    time.sleep(0.3)
    os.killpg(proc.pid, signal.SIGTERM)
    proc.wait(timeout=10)
    assert status(spec)["state"] == "CANCELLED"


def test_setup_snippet_and_argument_quoting(python, tmp_path):
    spec = make_spec(
        tmp_path,
        ["/bin/sh", "-c", 'printf "%s|%s" "$GREETING" "$1" > "$SWITCHBOARD_OUTDIR/result.json"', "sh", "two words"],
        setup="export GREETING=hello",
    )
    run_shim(python, tmp_path, spec)
    assert (Path(spec["outdir"]) / "result.json").read_text() == "hello|two words"


def test_compat_mode_exports_slurm_variables(python, tmp_path):
    spec = make_spec(tmp_path, '[ "$SLURM_ARRAY_TASK_ID" = 3 ] && echo ok > "$SWITCHBOARD_OUTDIR/result.json"',
                     compat="slurm-array")
    run_shim(python, tmp_path, spec)
    assert status(spec)["state"] == "SUCCEEDED"


@pytest.mark.slow
def test_peak_rss_is_reported_in_megabytes(python, tmp_path):
    spec = make_spec(tmp_path, [sys.executable, SYNTH, "task", "--mem-mb", "200", "--write", "result.json"])
    run_shim(python, tmp_path, spec)
    assert 190 < status(spec)["peak_rss_mb"] < 400


def test_unexpanded_paths_are_expanded_in_the_command_too(python, tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    spec = make_spec(tmp_path, [sys.executable, SYNTH, "task", "--outdir", "~/att/out", "--write", "result.json"],
                     attempt_dir="~/att", outdir="~/att/out")
    path = tmp_path / "attempt.json"
    path.write_text(json.dumps(spec))
    subprocess.run([python, str(SHIM_PATH), str(path)], env={**os.environ, "HOME": str(home)}, timeout=60)
    assert (home / "att" / "out" / "result.json").exists()
