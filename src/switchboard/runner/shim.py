"""switchboard task runner shim. Stdlib only, Python >= 3.8; identical on the Mac and on the HPC.

    python3 shim.py ATTEMPT.json

Steps, in order:
  1. claim the attempt directory (mkdir .claim); if it already exists, exit DUPLICATE without running
  2. re-check the pinned code/env fingerprints; on mismatch, fail without running the command
  3. set SWITCHBOARD_* and thread-count environment variables
  4. run `setup` + command, stdout/stderr into the attempt directory
  5. collect exit status and rusage (os.wait4)
  6. check that the expected outputs exist and are non-empty
  7. write status.json atomically, then _SUCCESS if everything passed

Process lifecycle, queueing, pause/resume and kill belong to Pueue or Slurm, not to this file.
"""

from __future__ import annotations

import glob
import json
import os
import signal
import socket
import subprocess
import sys
import time

try:
    from . import fingerprint
except ImportError:  # run as a plain script next to fingerprint.py
    import fingerprint  # type: ignore[no-redef]

EXIT_DUPLICATE = 86
EXIT_FINGERPRINT = 87
EXIT_MISSING_OUTPUTS = 88
EXIT_RUNNER_ERROR = 89

THREAD_VARS = (
    "OMP_NUM_THREADS",
    "MKL_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "NUMBA_NUM_THREADS",
)


def main(argv: list) -> int:
    if len(argv) != 2:
        print("usage: shim.py ATTEMPT.json", file=sys.stderr)
        return EXIT_RUNNER_ERROR
    with open(argv[1]) as fh:
        spec = json.load(fh)
    return run(spec)


def run(spec: dict) -> int:
    attempt_dir = spec["attempt_dir"]
    os.makedirs(attempt_dir, exist_ok=True)
    try:
        os.mkdir(os.path.join(attempt_dir, ".claim"))
    except FileExistsError:
        print("switchboard: attempt %s already claimed; not running it twice" % spec["attempt_id"], file=sys.stderr)
        return EXIT_DUPLICATE
    _write_json(os.path.join(attempt_dir, ".claim", "owner.json"), _whoami())

    status = {
        "schema": 1,
        "attempt_id": spec["attempt_id"],
        "state": "RUNNING",
        "started_at": time.time(),
        "platform": fingerprint.platform_tag(),
        **_whoami(),
    }
    _write_status(attempt_dir, status)
    try:
        code = _execute(spec, status)
    except Exception as exc:  # the shim must always leave a final status behind
        status.update(state="FAILED", reason="runner_error", detail=repr(exc))
        code = EXIT_RUNNER_ERROR
    status["ended_at"] = time.time()
    status["wall_s"] = round(status["ended_at"] - status["started_at"], 3)
    _write_status(attempt_dir, status)
    if status["state"] == "SUCCEEDED":
        _write_json(os.path.join(attempt_dir, "_SUCCESS"), {"attempt_id": spec["attempt_id"]})
    _nudge(spec.get("nudge_socket"))
    return code


def _execute(spec: dict, status: dict) -> int:
    workdir = os.path.expanduser(spec["workdir"])
    setup = spec.get("setup")

    identity = spec.get("identity") or {}
    expected = spec.get("expected") or {}
    for kind in ("code", "env"):
        if identity.get(kind) and expected.get(kind + "_id"):
            try:
                actual = fingerprint.evaluate(identity[kind], workdir, setup)
            except fingerprint.FingerprintError as exc:
                status.update(state="FAILED", reason="fingerprint_error", detail=str(exc))
                return EXIT_FINGERPRINT
            status[kind + "_id"] = actual
            if actual != expected[kind + "_id"]:
                status.update(
                    state="FAILED",
                    reason="fingerprint_mismatch",
                    detail="%s id is %s but %s was pinned at submit" % (kind, actual, expected[kind + "_id"]),
                )
                return EXIT_FINGERPRINT

    outdir = spec["outdir"]
    os.makedirs(outdir, exist_ok=True)
    params_file = os.path.join(spec["attempt_dir"], "params.json")
    _write_json(params_file, {"params": spec.get("params", {}), "seed": spec.get("seed")})

    env = dict(os.environ)
    cpus = str(spec.get("cpus", 1))
    for var in THREAD_VARS:
        env[var] = cpus
    env.update(
        SWITCHBOARD_ATTEMPT_ID=spec["attempt_id"],
        SWITCHBOARD_TASK_KEY=spec["task_key"],
        SWITCHBOARD_EXPERIMENT=spec["experiment"],
        SWITCHBOARD_OUTDIR=outdir,
        SWITCHBOARD_PARAMS_FILE=params_file,
        SWITCHBOARD_CPUS=cpus,
    )
    if spec.get("compat") == "slurm-array":
        env.setdefault("SLURM_ARRAY_TASK_ID", str(spec.get("idx", 0)))
        env.setdefault("SLURM_CPUS_PER_TASK", cpus)
    env.update(spec.get("env") or {})  # the spec's own variables win

    command = spec["command"]
    prologue = "set -e\n" + (setup + "\n" if setup else "")
    if isinstance(command, str):
        argv = [fingerprint.shell(), "-c", prologue + command]
    elif setup:
        argv = [fingerprint.shell(), "-c", prologue + 'exec "$@"', "switchboard-task", *command]
    else:
        argv = list(command)

    cancelled = []

    with open(os.path.join(spec["attempt_dir"], "stdout.log"), "ab") as out, open(
        os.path.join(spec["attempt_dir"], "stderr.log"), "ab"
    ) as err:
        try:
            proc = subprocess.Popen(argv, cwd=workdir, env=env, stdin=subprocess.DEVNULL, stdout=out, stderr=err)
        except OSError as exc:
            status.update(state="FAILED", reason="nonzero", exit_code=127, detail="cannot start command: %s" % exc)
            return 127

        def forward(signum, _frame):
            cancelled.append(signum)
            try:
                proc.send_signal(signum)
            except ProcessLookupError:
                pass

        for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            signal.signal(sig, forward)
        _, wait_status, usage = os.wait4(proc.pid, 0)
        proc.returncode = os.waitstatus_to_exitcode(wait_status) if hasattr(os, "waitstatus_to_exitcode") else (
            -os.WTERMSIG(wait_status) if os.WIFSIGNALED(wait_status) else os.WEXITSTATUS(wait_status)
        )

    # ru_maxrss is bytes on macOS and KiB on Linux.
    rss_mb = usage.ru_maxrss / (1024 * 1024) if sys.platform == "darwin" else usage.ru_maxrss / 1024
    status.update(
        exit_code=proc.returncode,
        cpu_s=round(usage.ru_utime + usage.ru_stime, 3),
        peak_rss_mb=round(rss_mb, 1),
    )
    if cancelled:
        status.update(state="CANCELLED", reason="cancelled", signal=cancelled[0])
        return 128 + cancelled[0]
    if proc.returncode != 0:
        status.update(state="FAILED", reason="nonzero")
        return proc.returncode if proc.returncode > 0 else 128 - proc.returncode

    missing = [pattern for pattern in spec.get("outputs_expect", []) if not _nonempty_match(outdir, pattern)]
    if missing:
        status.update(state="FAILED", reason="missing_outputs", missing_outputs=missing)
        return EXIT_MISSING_OUTPUTS
    status["state"] = "SUCCEEDED"
    return 0


def _nonempty_match(outdir: str, pattern: str) -> bool:
    for path in glob.glob(os.path.join(outdir, pattern), recursive=True):
        if os.path.isfile(path) and os.path.getsize(path) > 0:
            return True
        if os.path.isdir(path) and os.listdir(path):
            return True
    return False


def _whoami() -> dict:
    info = {"host": socket.gethostname(), "pid": os.getpid()}
    for var in ("SLURM_JOB_ID", "SLURM_ARRAY_JOB_ID", "SLURM_ARRAY_TASK_ID", "PUEUE_WORKER_ID"):
        if var in os.environ:
            info[var.lower()] = os.environ[var]
    return info


def _write_status(attempt_dir: str, status: dict) -> None:
    _write_json(os.path.join(attempt_dir, "status.json"), status)


def _write_json(path: str, data: dict) -> None:
    tmp = "%s.tmp.%d" % (path, os.getpid())
    with open(tmp, "w") as fh:
        json.dump(data, fh, indent=1, sort_keys=True)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def _nudge(path: str | None) -> None:
    """Tell a local controller daemon (if any) that a slot has freed up. Best effort."""
    if not path or not os.path.exists(path):
        return
    try:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        sock.settimeout(0.2)
        sock.sendto(b"done", path)
        sock.close()
    except OSError:
        pass


if __name__ == "__main__":
    sys.exit(main(sys.argv))
