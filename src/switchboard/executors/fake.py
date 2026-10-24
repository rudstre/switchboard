"""In-process test executor. Runs the real shim as subprocesses; not a production backend.

Its state lives in this object, so a test can "restart the controller" by building a new
Controller around the same FakeExecutor, the way pueued outlives the CLI.
"""

from __future__ import annotations

import itertools
import json
import os
import signal
import subprocess
import sys
from pathlib import Path
from typing import Sequence

from ..runner import fingerprint as fp
from ..runner import shim as shim_module
from .base import (
    BackendUnreachable,
    ExternalRef,
    FaultPlan,
    LaunchUnit,
    LocalStatusReader,
    Observation,
    SubmitRejected,
    Tracked,
)

SHIM_PATH = Path(shim_module.__file__)


class FakeExecutor(LocalStatusReader):
    def __init__(
        self,
        launch_dir: Path,
        name: str = "local",
        platform: str | None = None,
        python: str = sys.executable,
        faults: FaultPlan | None = None,
    ) -> None:
        self.name = name
        self.platform = platform or fp.platform_tag()
        self.launch_dir = Path(launch_dir)
        self.python = python
        self.faults = faults or FaultPlan()
        self.jobs: dict[str, dict] = {}  # external_id -> {"launch_id", "procs": {attempt_id: Popen}}
        self.hidden: list[subprocess.Popen] = []  # processes the backend "lost track of"
        self.submissions = 0
        self._ids = itertools.count(1)

    # -- Executor protocol ---------------------------------------------------------------------

    def fingerprint(self, provider: dict, workdir: str, setup: str | None) -> str:
        self._check_reachable()
        return fp.evaluate(provider, workdir, setup)

    def submit(self, unit: LaunchUnit) -> ExternalRef:
        self._check_reachable()
        f = self.faults
        if f.reject:
            f.reject -= 1
            raise SubmitRejected("fault injection: rejected")
        self.submissions += 1
        procs = {}
        for payload in unit.attempts:
            path = self.launch_dir / unit.launch_id / f"{payload.attempt_id}.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload.spec))
            procs[payload.attempt_id] = self._start(unit.launch_prefix, path)
            if f.double_start:
                f.double_start -= 1
                self.hidden.append(self._start(unit.launch_prefix, path))
        if f.vanish:
            f.vanish -= 1
            self.hidden.extend(procs.values())
            raise ConnectionError("fault injection: backend lost track of the launch")
        external_id = f"fake-{next(self._ids)}"
        self.jobs[external_id] = {"launch_id": unit.launch_id, "procs": procs}
        if f.ambiguous:
            f.ambiguous -= 1
            raise ConnectionError("fault injection: reply lost after acceptance")
        return ExternalRef(unit.launch_id, external_id)

    def find(self, launch_id: str) -> list[ExternalRef]:
        self._check_reachable()
        return [ExternalRef(launch_id, ext) for ext, job in self.jobs.items() if job["launch_id"] == launch_id]

    def observe(self, tracked: Sequence[Tracked]) -> dict[str, Observation]:
        self._check_reachable()
        out = {}
        for t in tracked:
            proc = self.jobs.get(t.external_id or "", {}).get("procs", {}).get(t.attempt_id)
            if proc is None:
                out[t.attempt_id] = Observation("ABSENT")
            elif proc.poll() is None:
                out[t.attempt_id] = Observation("RUNNING")
            else:
                out[t.attempt_id] = Observation("DONE", exit_code=proc.returncode)
        return out

    def read_status(self, attempt_dirs: dict[str, str]) -> dict[str, dict | None]:
        self._check_reachable()
        return super().read_status(attempt_dirs)

    def cancel(self, tracked: Sequence[Tracked]) -> None:
        self._check_reachable()
        for t in tracked:
            proc = self.jobs.get(t.external_id or "", {}).get("procs", {}).get(t.attempt_id)
            if proc is not None and proc.poll() is None:
                os.killpg(proc.pid, signal.SIGTERM)

    # -- test helpers --------------------------------------------------------------------------

    def all_procs(self) -> list[subprocess.Popen]:
        return [p for job in self.jobs.values() for p in job["procs"].values()] + self.hidden

    def wait_all(self, timeout: float = 60) -> None:
        for proc in self.all_procs():
            proc.wait(timeout=timeout)

    def forget(self, external_id: str) -> None:
        """Simulate the backend dropping a record (e.g. `pueue clean`, sacct purge)."""
        job = self.jobs.pop(external_id)
        self.hidden.extend(job["procs"].values())

    def _start(self, prefix: tuple[str, ...], spec_path: Path) -> subprocess.Popen:
        argv = [*prefix, self.python, str(SHIM_PATH), str(spec_path)]
        if self.faults.start_delay:
            argv = ["/bin/sh", "-c", f'sleep {self.faults.start_delay}; exec "$@"', "sh", *argv]
        log = open(spec_path.with_suffix(".runner.log"), "ab")
        try:
            return subprocess.Popen(
                argv,
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=log,
                start_new_session=True,
            )
        finally:
            log.close()

    def _check_reachable(self) -> None:
        if self.faults.unreachable:
            raise BackendUnreachable("fault injection: backend unreachable")
