"""The executor contract shared by Pueue (local), Slurm (remote) and the test double."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Protocol, Sequence


class BackendUnreachable(RuntimeError):
    """The backend could not be asked. Never treated as a task failure."""


class SubmitRejected(RuntimeError):
    """The backend definitely did not accept the launch, so abandoning it immediately is safe.

    Any other exception from ``submit`` is treated as *ambiguous*: the launch may or may not exist.
    """


@dataclass(frozen=True)
class AttemptPayload:
    attempt_id: str
    spec: dict  # the shim's attempt spec (see runner/shim.py)


@dataclass(frozen=True)
class LaunchUnit:
    """One backend submission: a single Pueue task, or one Slurm array of several attempts."""

    launch_id: str  # idempotency key: Pueue label / Slurm --job-name
    attempts: tuple[AttemptPayload, ...]
    launch_prefix: tuple[str, ...] = ()
    options: dict = field(default_factory=dict)  # backend-specific, e.g. Slurm cpus/mem/time/partition


@dataclass(frozen=True)
class ExternalRef:
    launch_id: str
    external_id: str


@dataclass(frozen=True)
class Tracked:
    attempt_id: str
    external_id: str | None
    array_idx: int | None = None


@dataclass(frozen=True)
class Observation:
    state: Literal["QUEUED", "RUNNING", "DONE", "ABSENT"]
    exit_code: int | None = None
    detail: str = ""
    # When the backend itself ended the attempt (Slurm TIMEOUT, OUT_OF_MEMORY, NODE_FAIL, PREEMPTED,
    # external scancel), the reason it gives overrides whatever the shim recorded while being killed.
    reason: str | None = None
    wall_s: float | None = None
    cpu_s: float | None = None
    peak_rss_mb: float | None = None


class Executor(Protocol):
    name: str
    platform: str  # e.g. "osx-arm64", "linux-64"

    def fingerprint(self, provider: dict, workdir: str, setup: str | None) -> str:
        """Evaluate a fingerprint provider where this backend runs tasks."""

    def submit(self, unit: LaunchUnit) -> ExternalRef:
        """Start the launch. Raise SubmitRejected if it definitely was not accepted."""

    def find(self, launch_id: str) -> list[ExternalRef]:
        """Look a launch up by its idempotency key. [] means positively absent."""

    def observe(self, tracked: Sequence[Tracked]) -> dict[str, Observation]:
        """Backend view of each attempt, keyed by attempt_id."""

    def read_status(self, attempt_dirs: dict[str, str]) -> dict[str, dict | None]:
        """Shim status.json per attempt_id (None if absent). The authoritative source."""

    def cancel(self, tracked: Sequence[Tracked]) -> None: ...

    def resolve_path(self, path: str, spec_dir: str | Path) -> str:
        """Turn a spec path into the absolute path tasks will see on this backend."""


@dataclass
class LocalStatusReader:
    """read_status and path resolution for backends whose attempt directories are on this machine."""

    def resolve_path(self, path: str, spec_dir: str | Path) -> str:
        """Local paths may be relative to the spec file."""
        p = Path(os.path.expandvars(path)).expanduser()
        return str(p if p.is_absolute() else (Path(spec_dir) / p).resolve())

    def read_status(self, attempt_dirs: dict[str, str]) -> dict[str, dict | None]:
        out: dict[str, dict | None] = {}
        for attempt_id, attempt_dir in attempt_dirs.items():
            path = os.path.join(attempt_dir, "status.json")
            try:
                with open(path) as fh:
                    out[attempt_id] = json.load(fh)
            except FileNotFoundError:
                out[attempt_id] = None
            except json.JSONDecodeError:  # written atomically, so this should not happen; treat as absent
                out[attempt_id] = None
        return out


@dataclass
class FaultPlan:
    """Fault injection knobs for the test double. Each counter applies to the next N submissions."""

    reject: int = 0  # raise SubmitRejected without starting anything
    ambiguous: int = 0  # start and record the launch, then raise (reply lost)
    vanish: int = 0  # start the process but keep no record, then raise (backend lost track)
    double_start: int = 0  # start the same attempt twice (e.g. manual re-sbatch)
    unreachable: bool = False  # every call raises BackendUnreachable
    start_delay: float = 0.0  # seconds before each runner starts (e.g. time spent queued)
    extra: dict = field(default_factory=dict)
