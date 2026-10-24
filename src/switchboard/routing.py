"""Deterministic, explainable routing for experiments submitted with `--backend auto`. Pure: no I/O.

Rules, first match wins; every decision carries a rule id and a reason sentence for `explain`:
  R1 local-only / hpc-only policies
  R2 not runnable on the Mac (platform, or cpus/memory above the largest local budget) -> cluster
  R3 cluster not usable (not configured, unreachable, no backends.hpc, identity mismatch) -> Mac
  R4 no runtime estimate -> Mac (overflow needs resources.runtime_min)
  R5 local-first: keep tasks the Mac finishes within max_local_wait_h, in queue order; overflow the tail

The local drain estimate is Σ(cpus × runtime) / capacity, using the *current* budget profile and the
declared runtime (no prediction yet). Only pending tasks are routed, every pass (late binding); tasks
are never moved once dispatched.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RouteTask:
    task_id: int
    exp_id: int
    cpus: int
    mem_mb: int
    runtime_s: float | None


@dataclass(frozen=True)
class RouteExperiment:
    exp_id: int
    policy: str  # local-first | local-only | hpc-only
    max_local_wait_s: float
    platforms: tuple[str, ...]
    has_local: bool
    has_hpc: bool
    hpc_identity: str | None = None  # None: ok; otherwise why the cluster may not run it


@dataclass(frozen=True)
class RouteContext:
    capacity_cpus: int  # current profile
    profile: str
    max_local_cpus: int  # largest local budget
    max_local_mem_mb: int
    local_platform: str = "osx-arm64"
    hpc_platform: str = "linux-64"
    hpc_configured: bool = False
    hpc_reachable: bool = False
    committed_local_cpu_s: float = 0.0  # running local work still to do + manual local queue


@dataclass(frozen=True)
class Decision:
    backend: str | None  # None: cannot run anywhere right now
    rule: str
    reason: str

    @property
    def note(self) -> str:
        return f"[{self.rule}] {self.reason}"


def _hours(seconds: float) -> str:
    return f"{seconds / 3600:.1f} h" if seconds >= 3600 else f"{seconds / 60:.0f} min"


def route(tasks: list[RouteTask], experiments: dict[int, RouteExperiment], ctx: RouteContext) -> dict[int, Decision]:
    out: dict[int, Decision] = {}
    queued_cpu_s = ctx.committed_local_cpu_s  # work the Mac will actually do
    whole_queue_cpu_s = ctx.committed_local_cpu_s  # as if every eligible task stayed (for overflow messages)
    capacity = max(ctx.capacity_cpus, 1)
    for t in tasks:
        e = experiments[t.exp_id]
        why_not_local = _why_not_local(t, e, ctx)
        why_not_hpc = _why_not_hpc(e, ctx)
        if e.policy == "local-only":
            out[t.task_id] = (Decision("local", "R1", "policy local-only") if not why_not_local
                              else Decision(None, "R1", f"policy local-only, but {why_not_local}"))
            continue
        if e.policy == "hpc-only":
            out[t.task_id] = (Decision("hpc", "R1", "policy hpc-only") if not why_not_hpc
                              else Decision(None, "R1", f"policy hpc-only; waiting: {why_not_hpc}"))
            continue
        if why_not_local:
            out[t.task_id] = (Decision("hpc", "R2", f"{why_not_local}; runs on the cluster") if not why_not_hpc
                              else Decision(None, "R2", f"{why_not_local}, and {why_not_hpc}; waiting"))
            continue
        if why_not_hpc:
            out[t.task_id] = Decision("local", "R3", f"stays on the Mac: {why_not_hpc}")
            queued_cpu_s += t.cpus * (t.runtime_s or 0)
            continue
        if not t.runtime_s:
            out[t.task_id] = Decision("local", "R4", "stays on the Mac: no runtime estimate "
                                      "(set resources.runtime_min to allow overflow to the cluster)")
            continue
        finish_s = (queued_cpu_s + t.cpus * t.runtime_s) / capacity
        whole_queue_cpu_s += t.cpus * t.runtime_s
        if finish_s <= e.max_local_wait_s:
            out[t.task_id] = Decision("local", "R5", f"local-first: the Mac finishes it in ~{_hours(finish_s)} at the "
                                      f"'{ctx.profile}' budget (limit {_hours(e.max_local_wait_s)})")
            queued_cpu_s += t.cpus * t.runtime_s
        else:
            out[t.task_id] = Decision("hpc", "R5", f"local-first overflow: the Mac would finish it in "
                                      f"~{_hours(whole_queue_cpu_s / capacity)} at the '{ctx.profile}' budget, over the "
                                      f"{_hours(e.max_local_wait_s)} limit")
    return out


def local_drain_s(decisions: dict[int, Decision], tasks: list[RouteTask], ctx: RouteContext) -> float:
    """Projected time for the Mac to finish its committed work plus everything routed to it."""
    work = ctx.committed_local_cpu_s + sum(t.cpus * (t.runtime_s or 0) for t in tasks
                                           if decisions.get(t.task_id) and decisions[t.task_id].backend == "local")
    return work / max(ctx.capacity_cpus, 1)


def _why_not_local(t: RouteTask, e: RouteExperiment, ctx: RouteContext) -> str | None:
    if not e.has_local:
        return "the spec has no backends.local"
    if ctx.local_platform not in e.platforms:
        return f"not runnable on {ctx.local_platform} (placement.platforms)"
    if t.cpus > ctx.max_local_cpus or t.mem_mb > ctx.max_local_mem_mb:
        return (f"needs {t.cpus} cpus / {t.mem_mb / 1024:.1f} GB, more than the Mac's largest budget "
                f"({ctx.max_local_cpus} cpus / {ctx.max_local_mem_mb / 1024:.0f} GB)")
    return None


def _why_not_hpc(e: RouteExperiment, ctx: RouteContext) -> str | None:
    if not ctx.hpc_configured:
        return "no cluster configured"
    if not e.has_hpc:
        return "the spec has no backends.hpc"
    if ctx.hpc_platform not in e.platforms:
        return f"not runnable on {ctx.hpc_platform} (placement.platforms)"
    if e.hpc_identity:
        return e.hpc_identity
    if not ctx.hpc_reachable:
        return "the cluster is unreachable"
    return None
