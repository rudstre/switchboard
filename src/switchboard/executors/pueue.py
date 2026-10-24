"""Local executor backed by Pueue v4 (CLI + JSON). Pueue owns process lifecycle; we only add admitted work.

Verified against pueue 4.0.4:
  * `pueue add -p` prints the task id; `-l` label round-trips in `status --json` (our idempotency key)
  * commands run through the shell, so we pass one shlex-quoted string
  * `pueue add` captures the caller's environment, so it is always called with a minimal explicit env
  * pause/kill signal the task's whole process group
  * a daemon shutdown kills running tasks (they come back as Done/Killed); the shim then records CANCELLED,
    which the controller treats as an interruption unless the user asked to cancel
"""

from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Sequence

from ..config import LocalConfig
from ..runner import fingerprint as fp
from .base import (
    BackendUnreachable,
    ExternalRef,
    LaunchUnit,
    LocalStatusReader,
    Observation,
    SubmitRejected,
    Tracked,
)
from .fake import SHIM_PATH

SUPPORTED_MAJOR = 4
_UNREACHABLE = ("Failed to initialize client", "connecting to daemon", "Connection refused", "No such file",
                "Couldn't find a configuration file", "Did you start the daemon")
# Variables passed to `pueue add`, which become the task's environment.
_ENV_KEEP = ("PATH", "HOME", "USER", "LOGNAME", "LANG", "LC_ALL", "TMPDIR", "SHELL")


class PueueError(RuntimeError):
    pass


class PueueExecutor(LocalStatusReader):
    def __init__(self, launch_dir: Path, cfg: LocalConfig, name: str = "local", python: str = sys.executable,
                 timeout_s: float = 15.0) -> None:
        self.name = name
        self.platform = fp.platform_tag()
        self.launch_dir = Path(launch_dir)
        self.cfg = cfg
        self.python = python
        self.timeout_s = timeout_s
        self._group_ready = False

    # -- Executor protocol ---------------------------------------------------------------------

    def fingerprint(self, provider: dict, workdir: str, setup: str | None) -> str:
        return fp.evaluate(provider, workdir, setup)

    def submit(self, unit: LaunchUnit) -> ExternalRef:
        if len(unit.attempts) != 1:
            raise ValueError("Pueue launches carry exactly one attempt")
        payload = unit.attempts[0]
        launch = self.launch_dir / unit.launch_id
        launch.mkdir(parents=True, exist_ok=True)
        spec_path = launch / f"{payload.attempt_id}.json"
        spec_path.write_text(json.dumps(payload.spec))
        try:
            self.ensure_group()
        except (BackendUnreachable, PueueError) as exc:
            raise SubmitRejected(f"pueue group not ready: {exc}") from exc
        command = shlex.join([*unit.launch_prefix, self.python, str(SHIM_PATH), str(spec_path)])
        try:
            proc = self._pueue(["add", "-g", self.cfg.pueue_group, "-l", unit.launch_id, "-p",
                                "-w", str(launch), "--", command], check=False)
        except BackendUnreachable as exc:
            raise SubmitRejected(str(exc)) from exc
        # A completed `pueue add` round trip that failed added nothing: safe to reject.
        # Timeouts propagate (subprocess.TimeoutExpired) and are treated as ambiguous by the controller.
        if proc.returncode != 0:
            raise SubmitRejected(f"pueue add failed: {_tail(proc.stderr)}")
        return ExternalRef(unit.launch_id, proc.stdout.strip())

    def find(self, launch_id: str) -> list[ExternalRef]:
        return [ExternalRef(launch_id, tid) for tid, task in self._tasks().items() if task.get("label") == launch_id]

    def observe(self, tracked: Sequence[Tracked]) -> dict[str, Observation]:
        tasks = self._tasks()
        out = {}
        for t in tracked:
            task = tasks.get(t.external_id or "")
            out[t.attempt_id] = observation(task) if task else Observation("ABSENT")
        return out

    def cancel(self, tracked: Sequence[Tracked]) -> None:
        ids = [t.external_id for t in tracked if t.external_id]
        if ids:
            self._pueue(["kill", *ids], check=False)

    # -- optional hooks used by the controller ---------------------------------------------------

    def finished_entries(self) -> list[tuple[str, str]]:
        """(launch_id, pueue id) of Done tasks in our group."""
        return [(t.get("label") or "", tid) for tid, t in self._tasks().items() if observation(t).state == "DONE"]

    def release(self, tracked: Sequence[Tracked]) -> None:
        """Remove Done Pueue entries whose outcome the controller has recorded."""
        ids = [t.external_id for t in tracked if t.external_id]
        if ids:
            self._pueue(["remove", *ids], check=False)

    def suspend(self) -> None:
        """SIGSTOP running tasks (frees CPU, not memory) and stop the group starting anything."""
        self._pueue(["pause", "-g", self.cfg.pueue_group])

    def resume(self) -> None:
        self._pueue(["start", "-g", self.cfg.pueue_group])

    # -- doctor / setup ------------------------------------------------------------------------

    def version(self) -> str:
        proc = subprocess.run([self.cfg.pueue_bin, "--version"], capture_output=True, text=True, timeout=5)
        return proc.stdout.strip()

    def check_version(self) -> str:
        try:
            text = self.version()
        except OSError as exc:
            raise PueueError(f"pueue not found ({exc}); install it with `brew install pueue`") from exc
        m = re.search(r"(\d+)\.(\d+)\.(\d+)", text)
        if not m or int(m.group(1)) != SUPPORTED_MAJOR:
            raise PueueError(f"unsupported Pueue version {text!r}; switchboard supports {SUPPORTED_MAJOR}.x")
        return text

    def ensure_group(self) -> None:
        if self._group_ready:
            return
        groups = self._status().get("groups", {})
        group = groups.get(self.cfg.pueue_group)
        if group is None:
            self._pueue(["group", "add", self.cfg.pueue_group, "--parallel", str(self.cfg.pueue_parallel)])
        elif group.get("parallel_tasks", 0) < self.cfg.pueue_parallel:
            self._pueue(["parallel", str(self.cfg.pueue_parallel), "-g", self.cfg.pueue_group])
        self._group_ready = True

    def group_status(self) -> dict:
        return self._status().get("groups", {}).get(self.cfg.pueue_group, {})

    # -- internals -----------------------------------------------------------------------------

    def _tasks(self) -> dict[str, dict]:
        return {tid: task for tid, task in self._status().get("tasks", {}).items()
                if task.get("group") == self.cfg.pueue_group}

    def _status(self) -> dict:
        proc = self._pueue(["status", "--json"])
        try:
            return json.loads(proc.stdout)
        except json.JSONDecodeError as exc:
            raise BackendUnreachable(f"unreadable `pueue status --json`: {exc}") from exc

    def _pueue(self, args: list[str], check: bool = True) -> subprocess.CompletedProcess:
        try:
            proc = subprocess.run([self.cfg.pueue_bin, *args], capture_output=True, text=True,
                                  timeout=self.timeout_s, env=self._env(), stdin=subprocess.DEVNULL)
        except FileNotFoundError as exc:
            raise BackendUnreachable(f"pueue binary not found: {exc}") from exc
        except subprocess.TimeoutExpired:
            if args[0] == "add":
                raise  # the task may have been added: the controller treats this as ambiguous
            raise BackendUnreachable(f"pueue {args[0]} timed out after {self.timeout_s:g}s") from None
        if proc.returncode != 0 and any(marker in proc.stderr for marker in _UNREACHABLE):
            raise BackendUnreachable(f"pueued is not running or unreachable: {_tail(proc.stderr)}")
        if check and proc.returncode != 0:
            raise PueueError(f"pueue {args[0]} failed: {_tail(proc.stderr)}")
        return proc

    def _env(self) -> dict[str, str]:
        env = {k: os.environ[k] for k in _ENV_KEEP if k in os.environ}
        if self.cfg.pueue_config:
            env["PUEUE_CONFIG_PATH"] = str(self.cfg.pueue_config)
        elif "PUEUE_CONFIG_PATH" in os.environ:
            env["PUEUE_CONFIG_PATH"] = os.environ["PUEUE_CONFIG_PATH"]
        return env


def observation(task: dict) -> Observation:
    status = task.get("status")
    kind, body = (status, None) if isinstance(status, str) else next(iter(status.items()))
    if kind in ("Running", "Paused"):
        return Observation("RUNNING", detail=kind)
    if kind != "Done":
        return Observation("QUEUED", detail=kind)  # Queued, Stashed, Locked
    result = body.get("result") if isinstance(body, dict) else None
    if result == "Success":
        return Observation("DONE", exit_code=0)
    if isinstance(result, dict) and "Failed" in result:
        return Observation("DONE", exit_code=int(result["Failed"]))
    return Observation("DONE", exit_code=None, detail=json.dumps(result))  # Killed, FailedToSpawn, Errored...


def _tail(text: str, n: int = 400) -> str:
    text = re.sub(r"\x1b\[[0-9;]*m", "", text or "").strip()
    return text[-n:]
