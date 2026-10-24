"""Remote executor: Slurm over a Transport (SSH). No process runs on the login node between calls.

One launch = one `sbatch --array` of N attempts. The job name ``sb-<launch_id>`` is the idempotency
key used to find a submission whose reply was lost. Jobs are submitted with --no-requeue; retries are
always new attempts created by the controller.

Accounting: `sacct -X` shows only allocation lines, but CPU and memory statistics live on the step
lines (`<job>_<i>.batch`, `.extern`, srun steps). We query without -X and aggregate per element:
state/exit/elapsed from the allocation line, peak memory = max MaxRSS over steps, CPU = sum of
TotalCPU over non-extern steps. With `JobAcctGatherType=jobacct_gather/none` these are reported as
unavailable (None), never as zero. The shim's own rusage remains the primary measurement.
"""

from __future__ import annotations

import hashlib
import json
import re
import shlex
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from importlib import resources
from pathlib import PurePosixPath
from typing import Sequence

from ..config import HpcConfig
from .base import (
    BackendUnreachable,
    ExternalRef,
    LaunchUnit,
    Observation,
    SubmitRejected,
    Tracked,
)
from .transport import TransportBase, TransportDown, TransportLost

RUNNER_FILES = ("shim.py", "fingerprint.py", "array.py")
JOB_PREFIX = "sb-"

QUEUED_STATES = {"PENDING", "CONFIGURING", "REQUEUED", "REQUEUE_HOLD", "REQUEUE_FED", "RESIZING", "STAGE_IN"}
RUNNING_STATES = {"RUNNING", "COMPLETING", "SUSPENDED", "STAGE_OUT", "SIGNALING", "STOPPED"}
# Terminal Slurm states where Slurm, not the task, ended the attempt -> reason for the controller.
SLURM_REASONS = {
    "TIMEOUT": "timeout",
    "DEADLINE": "timeout",
    "OUT_OF_MEMORY": "oom",
    "NODE_FAIL": "node_fail",
    "BOOT_FAIL": "node_fail",
    "PREEMPTED": "preempted",
    "CANCELLED": "cancelled_externally",
    "REVOKED": "cancelled_externally",
}


# ---- parsing ---------------------------------------------------------------------------------


def parse_duration(text: str) -> float | None:
    """Slurm durations: [D-]HH:MM:SS[.mmm] or MM:SS[.mmm]. Empty/INVALID/UNLIMITED -> None."""
    text = (text or "").strip()
    if not text or text.upper() in ("INVALID", "UNLIMITED", "PARTITION_LIMIT"):
        return None
    days = 0
    if "-" in text:
        d, text = text.split("-", 1)
        days = int(d)
    parts = [float(p) for p in text.split(":")]
    while len(parts) < 3:
        parts.insert(0, 0.0)
    h, m, s = parts[-3:]
    return days * 86400 + h * 3600 + m * 60 + s


def parse_mem_mb(text: str) -> float | None:
    """'123456K', '1.5G', '800M' -> MB. Bare numbers are KiB (we query with --units=K)."""
    text = (text or "").strip()
    if not text:
        return None
    m = re.fullmatch(r"([\d.]+)([KMGTP]?)", text, re.IGNORECASE)
    if not m:
        return None
    scale = {"": 1 / 1024, "K": 1 / 1024, "M": 1, "G": 1024, "T": 1024**2, "P": 1024**3}[m.group(2).upper()]
    return float(m.group(1)) * scale


def parse_job_id(field_: str) -> tuple[str, list[int] | None, str | None]:
    """'123_4' -> ('123', [4], None); '123_[2-5%10]' -> ('123', [2,3,4,5], None);
    '123_4.batch' -> ('123', [4], 'batch'); '123' -> ('123', None, None)."""
    base, _, step = field_.partition(".")
    job, sep, idx = base.partition("_")
    if not sep:
        return job, None, step or None
    if idx.startswith("["):
        indices: list[int] = []
        for part in idx.strip("[]").split("%")[0].split(","):
            lo, dash, hi = part.partition("-")
            indices.extend(range(int(lo), int(hi) + 1) if dash else [int(lo)])
        return job, indices, step or None
    return job, [int(idx)], step or None


@dataclass
class ElementAccount:
    state: str = ""
    exit_code: int | None = None
    signal: int | None = None
    wall_s: float | None = None
    cpu_s: float | None = None
    peak_rss_mb: float | None = None
    steps_cpu: list = field(default_factory=list)
    steps_rss: list = field(default_factory=list)


SACCT_FIELDS = ["JobID", "JobIDRaw", "State", "ExitCode", "Elapsed", "TotalCPU", "MaxRSS"]


def aggregate_sacct(text: str, accounting: bool = True) -> dict[tuple[str, int], ElementAccount]:
    """Parse `sacct -n -P -o JobID,JobIDRaw,State,ExitCode,Elapsed,TotalCPU,MaxRSS` (no -X)."""
    out: dict[tuple[str, int], ElementAccount] = {}
    for line in text.splitlines():
        cols = line.split("|")
        if len(cols) < len(SACCT_FIELDS):
            continue
        row = dict(zip(SACCT_FIELDS, cols))
        job, indices, step = parse_job_id(row["JobID"])
        if indices is None:
            continue  # not an array element: not ours
        state = row["State"].split()[0] if row["State"] else ""  # "CANCELLED by 501" -> CANCELLED
        for idx in indices:
            acct = out.setdefault((job, idx), ElementAccount())
            if step is None:
                acct.state = state
                code, _, sig = row["ExitCode"].partition(":")
                acct.exit_code = int(code) if code.isdigit() else None
                acct.signal = int(sig) if sig.isdigit() else None
                acct.wall_s = parse_duration(row["Elapsed"])
            else:
                if step != "extern":
                    cpu = parse_duration(row["TotalCPU"])
                    if cpu is not None:
                        acct.steps_cpu.append(cpu)
                rss = parse_mem_mb(row["MaxRSS"])
                if rss is not None:
                    acct.steps_rss.append(rss)
    for acct in out.values():
        if accounting:
            acct.cpu_s = sum(acct.steps_cpu) if acct.steps_cpu else None
            acct.peak_rss_mb = max(acct.steps_rss) if acct.steps_rss else None
    return out


def observation_from_account(acct: ElementAccount) -> Observation:
    st = acct.state
    if st in QUEUED_STATES or not st:
        return Observation("QUEUED", detail=st)
    if st in RUNNING_STATES:
        return Observation("RUNNING", detail=st)
    code = acct.exit_code
    if st == "COMPLETED":
        code = 0 if code is None else code
    return Observation("DONE", exit_code=code, detail=st, reason=SLURM_REASONS.get(st),
                       wall_s=acct.wall_s, cpu_s=acct.cpu_s, peak_rss_mb=acct.peak_rss_mb)


def render_job(unit: LaunchUnit, runner_dir: str | None, launch_dir: str | None, python: str) -> str:
    """The self-contained batch script. With launch_dir=None, paths are relative to the submit dir."""
    o = unit.options
    n = len(unit.attempts)
    array = f"0-{n - 1}" + (f"%{o['array_throttle']}" if o.get("array_throttle") else "")
    lines = ["#!/bin/bash", f"#SBATCH --job-name={JOB_PREFIX}{unit.launch_id}", f"#SBATCH --array={array}",
             f"#SBATCH --cpus-per-task={o.get('cpus', 1)}", "#SBATCH --no-requeue"]
    if o.get("mem_mb"):
        lines.append(f"#SBATCH --mem={o['mem_mb']}M")
    for key, flag in (("time", "--time"), ("partition", "--partition"), ("account", "--account")):
        if o.get(key):
            lines.append(f"#SBATCH {flag}={o[key]}")
    out = f"{launch_dir}/slurm-%A_%a.out" if launch_dir else "slurm-%A_%a.out"
    lines.append(f"#SBATCH --output={out}")
    lines += [f"#SBATCH {x}" for x in o.get("extra", [])]
    base = shlex.quote(launch_dir) if launch_dir else '"$SLURM_SUBMIT_DIR"'
    runner = shlex.quote(runner_dir) if runner_dir else '"$SLURM_SUBMIT_DIR/runner"'
    lines += [
        "# Generated by switchboard. Self-contained: resubmitting it by hand is safe, because each",
        "# attempt directory can only be claimed once (a duplicate exits without running).",
        f'exec {python} {runner}/array.py {base}/manifest.jsonl "$SLURM_ARRAY_TASK_ID"',
        "",
    ]
    return "\n".join(lines)


def runner_sources() -> dict[str, bytes]:
    pkg = resources.files("switchboard.runner")
    return {name: pkg.joinpath(name).read_bytes() for name in RUNNER_FILES}


def runner_hash() -> str:
    h = hashlib.sha256()
    for name, data in sorted(runner_sources().items()):
        h.update(name.encode() + b"\0" + data)
    return h.hexdigest()[:12]


# ---- executor --------------------------------------------------------------------------------


class SlurmExecutor:
    def __init__(self, transport: TransportBase, cfg: HpcConfig, name: str = "hpc", probe_ttl_s: float = 600.0):
        self.name = name
        self.platform = cfg.platform
        self.transport = transport
        self.cfg = cfg
        self.probe_ttl_s = probe_ttl_s
        self._env: dict[str, str] | None = None
        self._probe: tuple[float, dict] | None = None
        self._runner_ready = False

    # -- paths ---------------------------------------------------------------------------------

    def remote_env(self) -> dict[str, str]:
        if self._env is None:
            res = self._run('printf "%s\\n%s\\n" "$HOME" "$USER"')
            home, user = (res.stdout.splitlines() + ["", ""])[:2]
            if not home:
                raise BackendUnreachable("could not read $HOME on the cluster")
            self._env = {"HOME": home, "USER": user}
        return self._env

    def resolve_path(self, path: str, spec_dir: str | None = None) -> str:
        env = self.remote_env()
        p = path.replace("${HOME}", env["HOME"]).replace("$HOME", env["HOME"])
        p = p.replace("${USER}", env["USER"]).replace("$USER", env["USER"])
        if p == "~" or p.startswith("~/"):
            p = env["HOME"] + p[1:]
        if not p.startswith("/"):
            p = str(PurePosixPath(env["HOME"]) / p)
        return p

    @property
    def runner_dir(self) -> str:
        return self.resolve_path(f"{self.cfg.remote_root}/runner/{runner_hash()}")

    def launch_dir(self, launch_id: str) -> str:
        return self.resolve_path(f"{self.cfg.remote_root}/launch/{launch_id}")

    def element_log(self, launch_id: str, array_job_id: str, idx: int) -> str:
        return f"{self.launch_dir(launch_id)}/slurm-{array_job_id}_{idx}.out"

    # -- setup / limits --------------------------------------------------------------------------

    def ensure_runner(self) -> str:
        """Install the stdlib runner under a content-hashed directory (running jobs keep their copy)."""
        target = self.runner_dir
        if self._runner_ready:
            return target
        marker = f"{target}/OK"
        if self._read([marker]).get(marker) is None:
            files = {f"{target}/{name}": data for name, data in runner_sources().items()}
            self._put(files, mode=0o755)
            self._put({marker: b"ok\n"})
        self._runner_ready = True
        return target

    def probe(self, refresh: bool = False) -> dict:
        if self._probe and not refresh and time.monotonic() - self._probe[0] < self.probe_ttl_s:
            return self._probe[1]
        info: dict = {"host": getattr(self.transport, "host", "local"), **self.remote_env()}
        conf = self._run("scontrol show config")
        for line in conf.stdout.splitlines():
            key, _, value = line.partition("=")
            key, value = key.strip(), value.strip()
            if key in ("MaxArraySize", "MaxJobCount", "JobAcctGatherType", "AccountingStorageType", "SLURM_VERSION"):
                info[key] = value
        assoc = self._run('sacctmgr -n -P show assoc user="$USER" format=MaxSubmit,MaxJobs')
        submits = [int(x) for line in assoc.stdout.splitlines() for x in line.split("|")[:1] if x.strip().isdigit()]
        info["MaxSubmitJobs"] = min(submits) if submits else None
        queued = self._run('squeue -h -r -u "$USER" -o %i')
        info["jobs_in_queue"] = len([x for x in queued.stdout.splitlines() if x.strip()])
        py = self._run(f"{self.cfg.python} -c 'import sys; print(\"%d.%d\" % sys.version_info[:2])'")
        info["python"] = py.stdout.strip() or None
        info["accounting"] = "none" not in info.get("JobAcctGatherType", "").lower()
        self._probe = (time.monotonic(), info)
        return info

    def max_chunk(self) -> int:
        """Largest array we may submit now: config cap, MaxArraySize and the user's submit headroom."""
        info = self.probe()
        limit = self.cfg.max_chunk
        if str(info.get("MaxArraySize", "")).isdigit():
            limit = min(limit, int(info["MaxArraySize"]))
        if info.get("MaxSubmitJobs"):
            limit = min(limit, int(info["MaxSubmitJobs"]) - int(info.get("jobs_in_queue", 0)))
        return max(0, limit)

    # -- Executor protocol ---------------------------------------------------------------------

    def fingerprint(self, provider: dict, workdir: str, setup: str | None) -> str:
        from ..runner.fingerprint import FingerprintError

        runner = self.ensure_runner()
        cmd = f"{self.cfg.python} {shlex.quote(runner + '/fingerprint.py')} {shlex.quote(json.dumps(provider))} " \
              f"--workdir {shlex.quote(workdir)}" + (f" --setup {shlex.quote(setup)}" if setup else "")
        res = self._run(cmd, timeout=300)
        if res.returncode != 0:
            raise FingerprintError(f"on {self.name}: {res.stderr.strip()[-400:]}")
        return res.stdout.strip()

    def submit(self, unit: LaunchUnit) -> ExternalRef:
        try:
            self.transport.check()
            runner = self.ensure_runner()
            launch_dir = self.launch_dir(unit.launch_id)
            manifest = "".join(json.dumps(p.spec) + "\n" for p in unit.attempts).encode()
            script = render_job(unit, runner, launch_dir, self.cfg.python).encode()
            self._put({f"{launch_dir}/manifest.jsonl": manifest, f"{launch_dir}/job.sbatch": script})
        except (TransportDown, TransportLost, BackendUnreachable) as exc:
            raise SubmitRejected(f"could not prepare the launch on {self.name}: {exc}") from exc
        # From here a lost connection is ambiguous: sbatch may have accepted the job.
        res = self.transport.run(f"cd {shlex.quote(launch_dir)} && sbatch --parsable job.sbatch", timeout=120)
        if res.returncode != 0:
            raise SubmitRejected(f"sbatch refused the job: {res.stderr.strip()[-400:]}")
        job_id = res.stdout.strip().split(";")[0].strip()
        if not job_id.isdigit():
            raise ConnectionError(f"unexpected sbatch output {res.stdout!r}")
        return ExternalRef(unit.launch_id, job_id)

    def find(self, launch_id: str) -> list[ExternalRef]:
        name = JOB_PREFIX + launch_id
        res = self._run(f'squeue -h -r -u "$USER" -n {shlex.quote(name)} -o "%F"')
        ids = sorted({x.strip() for x in res.stdout.splitlines() if x.strip()})
        if not ids:
            since = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
            acct = self._run(f"sacct -n -P -X --name={shlex.quote(name)} -S {since} -o JobID")
            if acct.returncode != 0:
                raise BackendUnreachable(f"sacct unavailable, cannot prove the launch is absent: "
                                         f"{acct.stderr.strip()[-200:]}")
            ids = sorted({parse_job_id(x.strip())[0] for x in acct.stdout.splitlines() if x.strip()})
        return [ExternalRef(launch_id, job) for job in ids]

    def observe(self, tracked: Sequence[Tracked]) -> dict[str, Observation]:
        if not tracked:
            return {}
        res = self._run('squeue -h -r -u "$USER" -o "%F|%K|%T"')
        if res.returncode != 0:
            raise BackendUnreachable(f"squeue failed: {res.stderr.strip()[-200:]}")
        live: dict[tuple[str, int], str] = {}
        for line in res.stdout.splitlines():
            parts = line.strip().split("|")
            if len(parts) == 3 and parts[1].isdigit():
                live[(parts[0], int(parts[1]))] = parts[2]
        out: dict[str, Observation] = {}
        missing = []
        for t in tracked:
            key = (t.external_id or "", t.array_idx or 0)
            if key in live:
                out[t.attempt_id] = observation_from_account(ElementAccount(state=live[key]))
            else:
                missing.append(t)
        if missing:
            accounts = self._sacct(sorted({t.external_id for t in missing if t.external_id}))
            for t in missing:
                acct = accounts.get((t.external_id or "", t.array_idx or 0))
                out[t.attempt_id] = observation_from_account(acct) if acct else Observation("ABSENT")
        return out

    def read_status(self, attempt_dirs: dict[str, str]) -> dict[str, dict | None]:
        paths = {aid: f"{d}/status.json" for aid, d in attempt_dirs.items()}
        texts = self._read(list(paths.values()))
        out: dict[str, dict | None] = {}
        for aid, path in paths.items():
            try:
                out[aid] = json.loads(texts[path]) if texts.get(path) else None
            except json.JSONDecodeError:
                out[aid] = None
        return out

    def read_text(self, paths: list[str], max_bytes: int = 1 << 20) -> dict[str, str | None]:
        try:
            return self.transport.read_files(paths, max_bytes=max_bytes)
        except (TransportDown, TransportLost) as exc:
            raise BackendUnreachable(str(exc)) from exc

    def cancel(self, tracked: Sequence[Tracked]) -> None:
        ids = [f"{t.external_id}_{t.array_idx or 0}" for t in tracked if t.external_id]
        if ids:
            self._run("scancel " + " ".join(shlex.quote(i) for i in ids))

    # -- internals -----------------------------------------------------------------------------

    def _sacct(self, job_ids: list[str]) -> dict[tuple[str, int], ElementAccount]:
        out: dict[tuple[str, int], ElementAccount] = {}
        accounting = self.probe().get("accounting", True)
        for i in range(0, len(job_ids), 100):
            chunk = ",".join(job_ids[i:i + 100])
            res = self._run(f"sacct -n -P --units=K -j {chunk} -o {','.join(SACCT_FIELDS)}")
            if res.returncode != 0:
                raise BackendUnreachable(f"sacct failed: {res.stderr.strip()[-200:]}")
            out.update(aggregate_sacct(res.stdout, accounting=accounting))
        return out

    def _run(self, command: str, timeout: float = 60.0):
        try:
            return self.transport.run(command, timeout=timeout)
        except (TransportDown, TransportLost) as exc:
            raise BackendUnreachable(str(exc)) from exc

    def _put(self, files: dict[str, bytes], mode: int = 0o644) -> None:
        try:
            self.transport.put_files(files, mode=mode)
        except (TransportDown, TransportLost) as exc:
            raise BackendUnreachable(str(exc)) from exc

    def _read(self, paths: list[str]) -> dict[str, str | None]:
        try:
            return self.transport.read_files(paths)
        except (TransportDown, TransportLost) as exc:
            raise BackendUnreachable(str(exc)) from exc
