"""Static local admission. Pure functions: no I/O, so every decision is testable and explainable.

Budgets are advisory bookkeeping over *declared* reservations. macOS enforces none of this;
measured usage is reported separately (see Controller.warnings).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .config import Budget


@dataclass(frozen=True)
class Candidate:
    task_id: int
    cpus: int
    mem_mb: int


@dataclass
class Admission:
    admit: list[int] = field(default_factory=list)
    notes: dict[int, str] = field(default_factory=dict)  # task_id -> why it is (not) admitted


def admit_local(
    candidates: list[Candidate],
    reserved_cpus: int,
    reserved_mem_mb: int,
    budget: Budget,
    mode: str,
    gates: list[str],
) -> Admission:
    """Pick pending local tasks to start now, in candidate order, with backfill of smaller tasks.

    ``gates`` are reasons that block all new admissions (paused, memory pressure, battery...).
    """
    result = Admission()
    if gates:
        why = "waiting: " + "; ".join(gates)
        result.notes = {c.task_id: why for c in candidates}
        return result
    cpus, mem = reserved_cpus, reserved_mem_mb
    for c in candidates:
        if c.cpus > budget.cpus or c.mem_mb > budget.mem_mb:
            result.notes[c.task_id] = (
                f"waiting: needs {c.cpus} cpus / {c.mem_mb / 1024:.1f} GB, larger than the whole "
                f"'{mode}' budget ({budget.cpus} cpus / {budget.mem_gb:g} GB)"
            )
        elif len(result.admit) >= budget.max_admits_per_tick:
            result.notes[c.task_id] = f"waiting: admission ramp ({budget.max_admits_per_tick} per tick)"
        elif cpus + c.cpus <= budget.cpus and mem + c.mem_mb <= budget.mem_mb:
            result.admit.append(c.task_id)
            cpus += c.cpus
            mem += c.mem_mb
            result.notes[c.task_id] = (
                f"admitted locally ('{mode}': {cpus}/{budget.cpus} cpus, "
                f"{mem / 1024:.1f}/{budget.mem_gb:g} GB reserved)"
            )
        else:
            result.notes[c.task_id] = (
                f"waiting: local '{mode}' budget full ({cpus}/{budget.cpus} cpus, "
                f"{mem / 1024:.1f}/{budget.mem_gb:g} GB reserved; needs {c.cpus} cpus / {c.mem_mb / 1024:.1f} GB)"
            )
    return result
