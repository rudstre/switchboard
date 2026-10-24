"""The controller: submission with identity pinning, reconciliation, acceptance and local admission.

Guarantees (see the plan, section 1):
  * at-least-once execution: an ambiguous submission is resolved by lookup, and abandoned only after
    the backend positively reports no trace of it for ``submit_grace_s``;
  * at most one *active* attempt per task (partial unique index in the schema);
  * at most one *accepted* result per task (compare-and-set on tasks.accepted_attempt_id);
    any later success is SUPERSEDED.
"""

from __future__ import annotations

import json
import socket
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psutil

from . import policy
from .config import Config
from .executors.base import (
    AttemptPayload,
    BackendUnreachable,
    Executor,
    LaunchUnit,
    SubmitRejected,
    Tracked,
)
from .ids import canonical_json, new_id, short
from .registry import db
from .registry.states import (
    ABANDONED,
    ACTIVE_ATTEMPT_STATES,
    CANCELLED,
    FAILED,
    PENDING,
    RUNNING,
    SUBMITTED,
    SUBMITTING,
    SUCCEEDED,
    SUPERSEDED,
    UNKNOWN,
    log_event,
    transition,
)
from .runner.fingerprint import FingerprintError
from .runner.shim import EXIT_DUPLICATE
from .spec import ExperimentSpec, LoadedSpec, SpecError, expand, render_command

ACTIVE_SQL = "(" + ",".join(f"'{s}'" for s in ACTIVE_ATTEMPT_STATES) + ")"
LATE_EVIDENCE_WINDOW_S = 7 * 24 * 3600
# Interruptions (daemon shutdown, reboot, manual kill) requeue without using a retry, up to this many times.
MAX_INTERRUPTIONS = 3


class SwitchboardError(RuntimeError):
    """An error whose message is meant for the user."""


class IdentityMismatch(SwitchboardError):
    pass


class SimulatedCrash(BaseException):
    """Raised at a named crashpoint in tests; BaseException so no handler swallows it."""


@dataclass
class SubmitResult:
    experiment: str
    created: bool
    tasks_total: int
    tasks_assigned: int
    backend: str
    code_id: str | None
    env_ids: dict[str, str]
    warnings: list[str] = field(default_factory=list)
    dry_run: bool = False


@dataclass
class TickReport:
    admitted: int = 0
    accepted: int = 0
    superseded: int = 0
    failed: int = 0
    cancelled: int = 0
    retried: int = 0
    requeued: int = 0
    abandoned: int = 0
    ambiguous: int = 0
    unreachable: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


class Controller:
    def __init__(
        self,
        config: Config,
        executors: dict[str, Executor],
        conn: sqlite3.Connection | None = None,
        actor: str = "cli",
        crashpoints: set[str] | None = None,
        gates: list | None = None,
        clock=time.time,
    ) -> None:
        self.cfg = config
        self.executors = executors
        self.conn = conn or db.connect(config.db_path)
        self.actor = actor
        self.crashpoints = crashpoints or set()
        # Callables returning a reason string when new local admissions must stop (M2: pressure, battery).
        self.gates = gates or []
        self.clock = clock
        self._specs: dict[int, ExperimentSpec] = {}

    # ==== submission ===========================================================================

    def submit(
        self,
        loaded: LoadedSpec,
        backend: str,
        tasks: str | None = None,
        new_run: bool = False,
        code_id: str | None = None,
        dry_run: bool = False,
    ) -> SubmitResult:
        spec = loaded.spec
        executor = self._executor_for(spec, backend)
        defs = expand(loaded)
        selected = parse_selection(tasks, len(defs))
        spec_hash = loaded.spec_hash()
        identity_json = canonical_json(loaded.identity_fields())

        # Pin identities *before* anything is dispatched (evaluated where the backend runs tasks).
        pinned_code, code_source = self._evaluate_code(spec, backend, executor, code_id, loaded.base_dir)
        env_id = self._evaluate_env(spec, backend, executor, loaded.base_dir)

        warnings = []
        if spec.identity is None or spec.identity.code is None:
            if code_id is None:
                warnings.append("no code identity defined: result reuse relies on the spec text alone")
        if spec.identity is None or spec.identity.env is None:
            warnings.append(f"no environment identity defined for {executor.platform}")

        existing = self._latest_run(spec.experiment)
        if existing is not None and not new_run:
            problems = self._identity_diff(existing, spec_hash, identity_json, pinned_code, executor.platform, env_id)
            if problems:
                raise IdentityMismatch(
                    f"experiment '{spec.experiment}' already exists with a different identity:\n  - "
                    + "\n  - ".join(problems)
                    + "\nCompleted results will not be reused. Re-run with --new-run to start "
                    f"'{spec.experiment}#{existing['run_no'] + 1}'."
                )
            return self._resubmit(existing, loaded, backend, executor, selected, env_id, warnings, dry_run)

        run_no = existing["run_no"] + 1 if existing is not None else 1
        name = display_name(spec.experiment, run_no)
        env_ids = {executor.platform: env_id} if env_id else {}
        result = SubmitResult(name, True, len(defs), len(selected), backend, pinned_code, env_ids, warnings, dry_run)
        if dry_run:
            return result
        now = self.clock()
        with db.tx(self.conn):
            cur = self.conn.execute(
                "INSERT INTO experiments(name, run_no, spec_text, spec_json, spec_dir, spec_hash, identity_json, "
                "code_id, code_id_source, created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (spec.experiment, run_no, loaded.source_text, spec.model_dump_json(), str(loaded.base_dir),
                 spec_hash, identity_json, pinned_code, code_source, now),
            )
            exp_id = cur.lastrowid
            if env_id:
                self.conn.execute(
                    "INSERT INTO experiment_envs(experiment_id, platform, env_id, pinned_at) VALUES (?,?,?,?)",
                    (exp_id, executor.platform, env_id, now),
                )
            r = spec.resources
            self.conn.executemany(
                "INSERT INTO tasks(experiment_id, idx, key, params_json, seed, cpus, mem_mb, runtime_s, "
                "assigned_backend, state, note, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                [
                    (exp_id, d.idx, d.key, canonical_json(d.params), d.seed, r.cpus, r.mem_mb,
                     r.runtime_min * 60 if r.runtime_min else None,
                     backend if d.idx in selected else None, PENDING,
                     f"assigned to {backend} at submit" if d.idx in selected else "unassigned", now)
                    for d in defs
                ],
            )
            log_event(self.conn, "experiment", exp_id, None, "CREATED",
                      f"{len(defs)} tasks, {len(selected)} assigned to {backend}; code {short(pinned_code)}"
                      f" ({code_source or 'none'}); env[{executor.platform}] {short(env_id)}", self.actor)
        return result

    def assign(self, experiment: str, backend: str, tasks: str | None = None, count: int | None = None) -> int:
        """Manually (re)assign pending, not-yet-dispatched tasks to a backend, pinning identities first."""
        exp = self._experiment(experiment)
        spec = self._spec(exp)
        executor = self._executor_for(spec, backend)
        code, _ = self._evaluate_code(spec, backend, executor, None, exp["spec_dir"])
        env_id = self._evaluate_env(spec, backend, executor, exp["spec_dir"])
        problems = self._identity_diff(exp, exp["spec_hash"], exp["identity_json"], code, executor.platform, env_id)
        if problems:
            raise IdentityMismatch(f"cannot assign to {backend}:\n  - " + "\n  - ".join(problems))
        n_tasks = self.conn.execute("SELECT count(*) FROM tasks WHERE experiment_id=?", (exp["id"],)).fetchone()[0]
        selected = parse_selection(tasks, n_tasks)
        if count is not None:
            # The *tail* of the pending pool: work the current backend would reach last.
            tail = self.conn.execute(
                "SELECT idx FROM tasks WHERE experiment_id=? AND state=? AND (assigned_backend IS NULL OR "
                "assigned_backend!=?) ORDER BY idx DESC", (exp["id"], PENDING, backend)).fetchall()
            selected = {r["idx"] for r in tail if r["idx"] in selected}
            selected = set(sorted(selected, reverse=True)[:count])
        with db.tx(self.conn):
            self._pin_env(exp["id"], executor.platform, env_id)
            return self._assign_pending(exp["id"], backend, selected)

    def cancel(self, ref: str) -> dict[str, int]:
        """Cancel a task (EXP/KEY) or every unfinished task of an experiment (EXP)."""
        tasks = self._resolve_tasks(ref)
        counts = {"pending_cancelled": 0, "signalled": 0, "already_final": 0}
        signal: dict[str, list[Tracked]] = {}
        with db.tx(self.conn):
            for t in tasks:
                if t["state"] in (SUCCEEDED, FAILED, CANCELLED):
                    counts["already_final"] += 1
                    continue
                self.conn.execute("UPDATE tasks SET cancel_requested=1 WHERE id=?", (t["id"],))
                log_event(self.conn, "task", t["id"], t["state"], None, "cancel requested", self.actor)
                if t["state"] == PENDING:
                    transition(self.conn, "task", t["id"], CANCELLED, "cancelled by user before dispatch",
                               self.actor, note="cancelled by user")
                    counts["pending_cancelled"] += 1
                    continue
                for a in self.conn.execute(
                        f"SELECT a.*, l.external_id FROM attempts a JOIN launches l ON l.id=a.launch_id "
                        f"WHERE a.task_id=? AND a.state IN {ACTIVE_SQL}", (t["id"],)):
                    if a["external_id"]:
                        signal.setdefault(a["backend"], []).append(Tracked(a["id"], a["external_id"], a["array_idx"]))
                counts["signalled"] += 1
        for backend, tracked in signal.items():
            try:
                self.executors[backend].cancel(tracked)
            except (BackendUnreachable, KeyError, RuntimeError, OSError):
                pass  # re-sent by every reconcile pass until the attempts are final
        return counts

    def retry(self, ref: str, failed_only: bool = False) -> int:
        """Requeue FAILED (and, unless failed_only, CANCELLED) tasks with a fresh retry budget."""
        states = (FAILED,) if failed_only else (FAILED, CANCELLED)
        n = 0
        with db.tx(self.conn):
            for t in self._resolve_tasks(ref):
                if t["state"] in states:
                    transition(self.conn, "task", t["id"], PENDING, "manual retry", self.actor,
                               note="manual retry", retries_used=0, cancel_requested=0)
                    n += 1
        return n

    def _resolve_tasks(self, ref: str) -> list[sqlite3.Row]:
        if "/" in ref:
            return [self.find_task(ref)]
        exp = self._experiment(ref)
        return self.conn.execute("SELECT * FROM tasks WHERE experiment_id=? ORDER BY idx", (exp["id"],)).fetchall()

    def _resubmit(self, existing, loaded, backend, executor, selected, env_id, warnings, dry_run) -> SubmitResult:
        spec = loaded.spec
        n_total = self.conn.execute("SELECT count(*) FROM tasks WHERE experiment_id=?", (existing["id"],)).fetchone()[0]
        envs = self._envs(existing["id"])
        if env_id:
            envs.setdefault(executor.platform, env_id)
        result = SubmitResult(display_name(existing["name"], existing["run_no"]), False, n_total, 0, backend,
                              existing["code_id"], envs, warnings, dry_run)
        if dry_run:
            return result
        r = spec.resources
        with db.tx(self.conn):
            # Identity matched: only non-identity fields change, and only for tasks not yet dispatched.
            self.conn.execute("UPDATE experiments SET spec_text=?, spec_json=? WHERE id=?",
                              (loaded.source_text, spec.model_dump_json(), existing["id"]))
            self.conn.execute(
                "UPDATE tasks SET cpus=?, mem_mb=?, runtime_s=? WHERE experiment_id=? AND state=?",
                (r.cpus, r.mem_mb, r.runtime_min * 60 if r.runtime_min else None, existing["id"], PENDING),
            )
            self._pin_env(existing["id"], executor.platform, env_id)
            result.tasks_assigned = self._assign_pending(existing["id"], backend, selected)
            log_event(self.conn, "experiment", existing["id"], None, None,
                      f"resubmitted unchanged identity; {result.tasks_assigned} pending tasks assigned to {backend}",
                      self.actor)
        self._specs.pop(existing["id"], None)
        return result

    def _assign_pending(self, exp_id: int, backend: str, selected: set[int]) -> int:
        rows = self.conn.execute(
            "SELECT id, idx, assigned_backend FROM tasks WHERE experiment_id=? AND state=?", (exp_id, PENDING)
        ).fetchall()
        changed = [r["id"] for r in rows if r["idx"] in selected and r["assigned_backend"] != backend]
        self.conn.executemany(
            "UPDATE tasks SET assigned_backend=?, note=?, updated_at=? WHERE id=?",
            [(backend, f"assigned to {backend} manually", self.clock(), tid) for tid in changed],
        )
        return len(changed)

    def _evaluate_code(self, spec, backend, executor, explicit, spec_dir):
        if explicit:
            return explicit, "explicit"
        if spec.identity is None or spec.identity.code is None:
            return None, None
        b = spec.backends[backend]
        try:
            value = executor.fingerprint(spec.identity.code.model_dump(exclude_none=True),
                                         executor.resolve_path(b.workdir, spec_dir), b.setup)
        except (FingerprintError, BackendUnreachable) as exc:
            raise SwitchboardError(f"cannot evaluate code identity on {backend}: {exc}") from exc
        return value, f"evaluated:{backend}"

    def _evaluate_env(self, spec, backend, executor, spec_dir) -> str | None:
        if spec.identity is None or spec.identity.env is None:
            return None
        b = spec.backends[backend]
        try:
            return executor.fingerprint(spec.identity.env.model_dump(exclude_none=True),
                                        executor.resolve_path(b.workdir, spec_dir), b.setup)
        except (FingerprintError, BackendUnreachable) as exc:
            raise SwitchboardError(f"cannot evaluate environment identity on {backend}: {exc}") from exc

    def _identity_diff(self, exp, spec_hash, identity_json, code_id, platform, env_id) -> list[str]:
        problems = []
        if exp["spec_hash"] != spec_hash:
            old, new = json.loads(exp["identity_json"]), json.loads(identity_json)
            changed = sorted(k for k in set(old) | set(new) if old.get(k) != new.get(k))
            problems.append(f"spec identity changed ({', '.join(changed)})")
        if exp["code_id"] != code_id and not (exp["code_id_source"] == "explicit" and code_id is None):
            problems.append(f"code id changed {short(exp['code_id'])} -> {short(code_id)}")
        pinned_env = self._envs(exp["id"]).get(platform)
        if pinned_env and env_id and pinned_env != env_id:
            problems.append(f"env id changed on {platform}: {short(pinned_env)} -> {short(env_id)}")
        return problems

    def _pin_env(self, exp_id: int, platform: str, env_id: str | None) -> None:
        if env_id:
            self.conn.execute(
                "INSERT OR IGNORE INTO experiment_envs(experiment_id, platform, env_id, pinned_at) VALUES (?,?,?,?)",
                (exp_id, platform, env_id, self.clock()),
            )

    # ==== tick =================================================================================

    def tick(self, remote: bool = True) -> TickReport:
        """One pass. ``remote=False`` skips cluster backends (the daemon throttles those)."""
        report = TickReport()
        for name, executor in self.executors.items():
            if name != "local" and not remote:
                continue
            try:
                self._reconcile_submitting(name, executor, report)
                self._reconcile_active(name, executor, report)
                if name != "local":
                    self._dispatch_remote(executor, report)
            except BackendUnreachable:
                report.unreachable.append(name)
        if "local" in self.executors and "local" not in report.unreachable:
            self._admit_local(self.executors["local"], report)
        return report

    def remote_work_remaining(self) -> tuple[int, int]:
        """(active remote attempts, pending remote tasks)."""
        active = self.conn.execute(
            f"SELECT count(*) FROM attempts WHERE backend!='local' AND state IN {ACTIVE_SQL}").fetchone()[0]
        pending = self.conn.execute(
            "SELECT count(*) FROM tasks WHERE state=? AND assigned_backend IS NOT NULL AND assigned_backend!='local'",
            (PENDING,)).fetchone()[0]
        return active, pending

    def local_work_remaining(self) -> tuple[int, int]:
        """(active local attempts, pending local tasks)."""
        active = self.conn.execute(
            f"SELECT count(*) FROM attempts WHERE backend='local' AND state IN {ACTIVE_SQL}").fetchone()[0]
        pending = self.conn.execute(
            "SELECT count(*) FROM tasks WHERE state=? AND assigned_backend='local'", (PENDING,)).fetchone()[0]
        return active, pending

    # ---- reconciliation ----------------------------------------------------------------------

    def _reconcile_submitting(self, backend: str, executor: Executor, report: TickReport) -> None:
        launches = self.conn.execute(
            "SELECT * FROM launches WHERE backend=? AND state=?", (backend, SUBMITTING)).fetchall()
        for launch in launches:
            attempts = self._launch_attempts(launch["id"])
            try:
                refs = executor.find(launch["id"])
                statuses = executor.read_status({a["id"]: a["attempt_dir"] for a in attempts})
            except BackendUnreachable:
                with db.tx(self.conn):
                    for a in attempts:
                        if a["state"] == SUBMITTING:
                            transition(self.conn, "attempt", a["id"], UNKNOWN, f"{backend} unreachable while "
                                       "confirming submission", self.actor, unknown_since=self.clock())
                raise
            if refs or any(statuses.values()):
                external = refs[0].external_id if refs else None
                with db.tx(self.conn):
                    transition(self.conn, "launch", launch["id"], "ACCEPTED", "found by idempotency key after an "
                               "ambiguous submission" if refs else "attempt status found on backend", self.actor,
                               external_id=external)
                    for a in attempts:
                        if a["state"] in (SUBMITTING, UNKNOWN):
                            transition(self.conn, "attempt", a["id"], SUBMITTED, "submission confirmed", self.actor,
                                       unknown_since=None)
                continue
            age = self.clock() - launch["created_at"]
            if age < self.cfg.submit_grace_s:
                continue
            with db.tx(self.conn):
                transition(self.conn, "launch", launch["id"], ABANDONED,
                           f"positively absent from {backend} for {age:.0f}s", self.actor)
                for a in attempts:
                    transition(self.conn, "attempt", a["id"], ABANDONED,
                               f"submission not found on {backend} after {age:.0f}s", self.actor)
                    self._task_back_to_pending(a["task_id"], f"previous submission {a['id']} abandoned; requeued "
                                               "(no retry consumed)")
                    report.abandoned += 1

    def _reconcile_active(self, backend: str, executor: Executor, report: TickReport) -> None:
        horizon = self.clock() - LATE_EVIDENCE_WINDOW_S
        rows = self.conn.execute(
            f"SELECT a.*, l.external_id, l.state AS launch_state FROM attempts a JOIN launches l ON l.id=a.launch_id "
            f"WHERE a.backend=? AND (a.state IN ('{SUBMITTED}','{RUNNING}','{UNKNOWN}') "
            f"OR (a.state='{ABANDONED}' AND a.created_at > ?)) AND l.state != '{SUBMITTING}'",
            (backend, horizon),
        ).fetchall()
        if not rows:
            self._sweep(executor)
            return
        live = [r for r in rows if r["state"] != ABANDONED]
        # Observe first, then read status: the shim writes its final status before exiting, so a
        # process seen as DONE must already have a final status.json if it ended normally.
        try:
            observed = executor.observe([Tracked(r["id"], r["external_id"], r["array_idx"]) for r in live])
        except BackendUnreachable:
            observed = None
        try:
            statuses = executor.read_status({r["id"]: r["attempt_dir"] for r in rows})
        except BackendUnreachable:
            statuses = None
        cancelling = [Tracked(r["id"], r["external_id"], r["array_idx"]) for r in live
                      if r["external_id"] and self._task(r["task_id"])["cancel_requested"]]
        if cancelling and observed is not None:
            try:
                executor.cancel(cancelling)
            except (BackendUnreachable, RuntimeError, OSError):
                pass  # retried on the next pass
        if observed is None and statuses is None:
            with db.tx(self.conn):
                for r in live:
                    if r["state"] != UNKNOWN:
                        transition(self.conn, "attempt", r["id"], UNKNOWN, f"{backend} unreachable", self.actor,
                                   unknown_since=self.clock())
            raise BackendUnreachable(backend)

        now = self.clock()
        for r in rows:
            status = (statuses or {}).get(r["id"])
            obs = (observed or {}).get(r["id"])
            if status and status.get("state") in (SUCCEEDED, FAILED, CANCELLED):
                if status["state"] != SUCCEEDED and obs is not None and obs.state in ("RUNNING", "QUEUED") \
                        and r["state"] != ABANDONED:
                    # The shim has finished writing but the backend has not yet recorded why the attempt
                    # ended (e.g. Slurm TIMEOUT/OOM). Wait a pass so its verdict can take precedence.
                    continue
                if status["state"] != SUCCEEDED and obs is not None and obs.reason:
                    # e.g. Slurm TIMEOUT: the shim saw SIGTERM and wrote CANCELLED, but Slurm knows why.
                    status = {**status, "state": FAILED if obs.reason != "cancelled_externally" else CANCELLED,
                              "reason": obs.reason, "detail": f"{backend} reported {obs.detail}"}
                self._finalize(r, status, report)
                continue
            if r["state"] == ABANDONED:
                continue  # no new evidence
            if observed is None:
                if r["state"] != UNKNOWN:
                    with db.tx(self.conn):
                        transition(self.conn, "attempt", r["id"], UNKNOWN, f"{backend} unreachable", self.actor,
                                   unknown_since=now)
                continue
            seen = obs.state if obs else "ABSENT"
            if status and status.get("state") == RUNNING and (
                    seen == "ABSENT" or (seen == "DONE" and obs.exit_code == EXIT_DUPLICATE)):
                # The backend no longer tracks a runner (or our copy lost the claim to another one):
                # trust status.json, and check the recorded pid when the runner is on this host.
                alive = runner_alive(status)
                if alive is not None:
                    seen = "RUNNING" if alive else "DONE"
            with db.tx(self.conn):
                if seen == "ABSENT":
                    if r["absent_since"] is None:
                        self.conn.execute("UPDATE attempts SET absent_since=? WHERE id=?", (now, r["id"]))
                        continue
                    if now - r["absent_since"] < self.cfg.lost_grace_s:
                        continue
                    lost = {"state": FAILED, "reason": "lost",
                            "detail": f"no trace on {backend} for {now - r['absent_since']:.0f}s and no final status"}
                    self._finalize_in_tx(r, lost, report)
                elif seen == "DONE":
                    code = obs.exit_code if obs else None
                    if obs is not None and obs.detail == '"Killed"':
                        died = {"state": CANCELLED, "reason": "interrupted",
                                "detail": f"killed by {backend} (daemon shutdown or manual kill)"}
                    elif obs is not None and obs.reason:
                        died = {"state": CANCELLED if obs.reason == "cancelled_externally" else FAILED,
                                "reason": obs.reason, "exit_code": code, "detail": f"{backend} reported {obs.detail}"}
                    else:
                        died = {"state": FAILED, "reason": "runner_died", "exit_code": code,
                                "detail": f"runner ended (exit {code}) without a final status.json"}
                    if obs is not None:  # backend accounting fills in what the dead shim could not record
                        died.update({k: v for k, v in (("wall_s", obs.wall_s), ("cpu_s", obs.cpu_s),
                                                        ("peak_rss_mb", obs.peak_rss_mb)) if v is not None})
                    self._finalize_in_tx(r, died, report)
                elif seen == "RUNNING":
                    extra = {"started_at": status["started_at"]} if status and status.get("started_at") else {}
                    transition(self.conn, "attempt", r["id"], RUNNING, f"running on {backend}", self.actor,
                               unknown_since=None, absent_since=None, **extra)
                    task = self._task(r["task_id"])
                    if task["state"] == SUBMITTED and task["accepted_attempt_id"] is None:
                        transition(self.conn, "task", task["id"], RUNNING, f"attempt {r['id']} running", self.actor)
                elif seen == "QUEUED":
                    transition(self.conn, "attempt", r["id"], SUBMITTED, f"queued on {backend}", self.actor,
                               unknown_since=None, absent_since=None)
        self._sweep(executor)

    def _sweep(self, executor: Executor) -> None:
        """Remove finished backend entries whose attempts are recorded as final (keeps Pueue's state small)."""
        finished = getattr(executor, "finished_entries", None)
        if finished is None:
            return
        try:
            entries = finished()
            if not entries:
                return
            marks = ",".join("?" * len(entries))
            open_launches = {r[0] for r in self.conn.execute(
                f"SELECT DISTINCT launch_id FROM attempts WHERE launch_id IN ({marks}) AND state IN {ACTIVE_SQL}",
                [launch_id for launch_id, _ in entries])}
            known = {r[0] for r in self.conn.execute(
                f"SELECT id FROM launches WHERE id IN ({marks})", [launch_id for launch_id, _ in entries])}
            done = [Tracked("", ext) for launch_id, ext in entries if launch_id in known and launch_id not in open_launches]
            if done:
                executor.release(done)
        except (BackendUnreachable, RuntimeError):
            pass  # harmless: retried on the next pass

    def _finalize(self, attempt: sqlite3.Row, status: dict, report: TickReport) -> None:
        with db.tx(self.conn):
            self._finalize_in_tx(attempt, status, report)

    def _finalize_in_tx(self, attempt: sqlite3.Row, status: dict, report: TickReport) -> None:
        metrics = {k: status.get(k) for k in ("exit_code", "started_at", "ended_at", "wall_s", "cpu_s",
                                               "peak_rss_mb", "code_id", "env_id")}
        reason = status.get("reason")
        detail = status.get("detail") or (
            f"missing outputs: {', '.join(status['missing_outputs'])}" if status.get("missing_outputs") else "")
        reason_text = reason + (f": {detail}" if detail else "") if reason else "outputs verified"
        was_current = attempt["state"] != ABANDONED
        task = self._task(attempt["task_id"])

        if status["state"] == SUCCEEDED:
            won = self.conn.execute(
                "UPDATE tasks SET accepted_attempt_id=? WHERE id=? AND accepted_attempt_id IS NULL",
                (attempt["id"], task["id"]),
            ).rowcount == 1
            if won:
                transition(self.conn, "attempt", attempt["id"], SUCCEEDED, "succeeded and accepted", self.actor,
                           reason=None, unknown_since=None, **metrics)
                transition(self.conn, "task", task["id"], SUCCEEDED, f"accepted attempt {attempt['id']} "
                           f"({attempt['backend']})", self.actor, note=f"accepted {attempt['id']}")
                report.accepted += 1
            else:
                transition(self.conn, "attempt", attempt["id"], SUPERSEDED,
                           f"succeeded, but task already accepted {task['accepted_attempt_id']}", self.actor,
                           reason="superseded", unknown_since=None, **metrics)
                report.superseded += 1
            return

        final = CANCELLED if status["state"] == CANCELLED else FAILED
        interrupted = reason == "preempted" or (
            final == CANCELLED and not task["cancel_requested"] and reason != "cancelled_externally")
        if interrupted and reason not in ("interrupted", "preempted"):
            reason = "interrupted"
            reason_text = (f"interrupted (signal {status.get('signal')}) without a cancel request: daemon "
                           "shutdown, reboot or manual kill")
        transition(self.conn, "attempt", attempt["id"], final, reason_text, self.actor,
                   reason=reason, unknown_since=None, **metrics)
        if final == CANCELLED:
            report.cancelled += 1
        else:
            report.failed += 1
        if not was_current or task["accepted_attempt_id"] is not None:
            return  # a stale or superseded attempt never changes the task
        if task["cancel_requested"]:
            transition(self.conn, "task", task["id"], CANCELLED, f"cancelled by user ({reason_text})", self.actor,
                       note="cancelled by user")
            return
        if interrupted:
            n = self.conn.execute("SELECT count(*) FROM attempts WHERE task_id=? AND reason IN "
                                  "('interrupted','preempted')", (task["id"],)).fetchone()[0]
            if n <= MAX_INTERRUPTIONS:
                transition(self.conn, "task", task["id"], PENDING, f"{reason_text}; requeued without using a retry "
                           f"({n}/{MAX_INTERRUPTIONS})", self.actor, note=f"requeued after interruption {n}")
                report.requeued += 1
                return
            reason_text = f"interrupted {n} times; giving up"
            final = FAILED
        retries = self._spec_for_task(task).retries
        if final == FAILED and reason in retries.on and task["retries_used"] < retries.max:
            n = task["retries_used"] + 1
            transition(self.conn, "task", task["id"], PENDING, f"retry {n}/{retries.max} after {reason_text}",
                       self.actor, retries_used=n, note=f"retry {n}/{retries.max} after {reason}")
            report.retried += 1
        else:
            transition(self.conn, "task", task["id"], final, reason_text, self.actor, note=reason_text)

    def _task_back_to_pending(self, task_id: int, why: str) -> None:
        task = self._task(task_id)
        if task["accepted_attempt_id"] is not None:
            return
        if task["cancel_requested"]:
            transition(self.conn, "task", task_id, CANCELLED, f"{why}; cancelled as requested", self.actor,
                       note="cancelled by user")
        elif task["state"] != PENDING:
            transition(self.conn, "task", task_id, PENDING, why, self.actor, note=why)

    # ---- local admission and dispatch --------------------------------------------------------

    def _admit_local(self, executor: Executor, report: TickReport) -> None:
        mode = self.get_control("mode", "focus")
        budget = self.cfg.budgets[mode]
        gates = []
        if self.get_control("local_paused", "0") == "1":
            gates.append("local execution paused (switchboard resume-local)")
        for gate in self.gates:
            why = gate()
            if why:
                gates.append(why)
        reserved = self.conn.execute(
            f"SELECT coalesce(sum(t.cpus),0), coalesce(sum(t.mem_mb),0) FROM attempts a JOIN tasks t ON t.id=a.task_id "
            f"WHERE a.backend='local' AND a.state IN {ACTIVE_SQL}").fetchone()
        rows = self.conn.execute(
            "SELECT t.*, e.created_at AS exp_created FROM tasks t JOIN experiments e ON e.id=t.experiment_id "
            "WHERE t.state=? AND t.assigned_backend='local' ORDER BY e.created_at, t.idx LIMIT 200",
            (PENDING,),
        ).fetchall()
        if not rows:
            return
        decision = policy.admit_local(
            [policy.Candidate(r["id"], r["cpus"], r["mem_mb"]) for r in rows],
            reserved[0], reserved[1], budget, mode, gates,
        )
        by_id = {r["id"]: r for r in rows}
        changed = [(note, tid) for tid, note in decision.notes.items() if note != by_id[tid]["note"]]
        if changed:
            with db.tx(self.conn):
                self.conn.executemany("UPDATE tasks SET note=? WHERE id=?", changed)
        for task_id in decision.admit:
            if self._dispatch(executor, [by_id[task_id]], decision.notes[task_id], report,
                              prefix=budget.launch_prefix):
                report.admitted += 1

    def _dispatch_remote(self, executor: Executor, report: TickReport) -> None:
        """Submit pending tasks assigned to a cluster backend as Slurm arrays (one launch per chunk)."""
        rows = self.conn.execute(
            "SELECT t.* FROM tasks t JOIN experiments e ON e.id=t.experiment_id WHERE t.state=? AND "
            "t.assigned_backend=? ORDER BY e.created_at, t.idx LIMIT 5000", (PENDING, executor.name)).fetchall()
        if not rows:
            return
        groups: dict[tuple, list[sqlite3.Row]] = {}
        for r in rows:
            groups.setdefault((r["experiment_id"], r["cpus"], r["mem_mb"], r["runtime_s"]), []).append(r)
        for (exp_id, cpus, mem_mb, runtime_s), tasks in groups.items():
            spec = self._spec(self._experiment_by_id(exp_id))
            slurm = spec.backends[executor.name].slurm
            options = {"cpus": cpus, "mem_mb": mem_mb, "time": slurm_time(slurm.time if slurm else None, runtime_s),
                       "partition": slurm.partition if slurm else None, "account": slurm.account if slurm else None,
                       "array_throttle": slurm.array_throttle if slurm else None,
                       "extra": list(slurm.extra) if slurm else []}
            while tasks:
                size = executor.max_chunk() if hasattr(executor, "max_chunk") else len(tasks)
                if size <= 0:
                    with db.tx(self.conn):
                        self.conn.executemany("UPDATE tasks SET note=? WHERE id=?", [
                            (f"waiting: {executor.name} submit limit reached", t["id"]) for t in tasks])
                    return
                chunk, tasks = tasks[:size], tasks[size:]
                if not self._dispatch(executor, chunk, f"submitted to {executor.name} as part of a "
                                      f"{len(chunk)}-task array", report, options=options):
                    return  # rejected or ambiguous: try again next pass
                report.admitted += len(chunk)

    def _dispatch(self, executor: Executor, tasks: list[sqlite3.Row], why: str, report: TickReport,
                  prefix: tuple[str, ...] = (), options: dict | None = None) -> bool:
        """Record intent for one launch of len(tasks) attempts, submit it, record the outcome."""
        backend = executor.name
        launch_id = new_id("l")
        payloads, rows = [], []
        for i, task in enumerate(tasks):
            exp = self._experiment_by_id(task["experiment_id"])
            spec = self._spec(exp)
            b = spec.backends[backend]
            attempt_id = new_id("a")
            name = display_name(exp["name"], exp["run_no"])
            output_root = executor.resolve_path(b.output_root, exp["spec_dir"])
            attempt_dir = f"{output_root}/{dir_name(exp['name'], exp['run_no'])}/{task['key']}/{attempt_id}"
            payloads.append(AttemptPayload(attempt_id, attempt_spec(
                spec, name, task["key"], task["idx"], json.loads(task["params_json"]), task["seed"], task["cpus"],
                attempt_id, attempt_dir, executor.resolve_path(b.workdir, exp["spec_dir"]), b.setup,
                exp["code_id"], self._envs(exp["id"]).get(executor.platform),
                str(self.cfg.nudge_socket) if backend == "local" else None)))
            rows.append((attempt_id, task, i, attempt_dir))
        now = self.clock()
        with db.tx(self.conn):
            self.conn.execute("INSERT INTO launches(id, backend, state, created_at, updated_at) VALUES (?,?,?,?,?)",
                              (launch_id, backend, SUBMITTING, now, now))
            for attempt_id, task, i, attempt_dir in rows:
                self.conn.execute(
                    "INSERT INTO attempts(id, task_id, launch_id, backend, platform, array_idx, state, attempt_dir, "
                    "created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                    (attempt_id, task["id"], launch_id, backend, executor.platform, i, SUBMITTING, attempt_dir, now),
                )
                log_event(self.conn, "attempt", attempt_id, None, SUBMITTING, f"intent recorded for task "
                          f"{task['key']} (launch {launch_id}, element {i})", self.actor, ts=now)
                transition(self.conn, "task", task["id"], SUBMITTED, why, self.actor, note=why)
        self._crashpoint("after_intent")
        try:
            ref = executor.submit(LaunchUnit(launch_id, tuple(payloads), prefix, options or {}))
        except SubmitRejected as exc:
            with db.tx(self.conn):
                transition(self.conn, "launch", launch_id, ABANDONED, f"rejected: {exc}", self.actor)
                for attempt_id, task, _, _ in rows:
                    transition(self.conn, "attempt", attempt_id, ABANDONED, f"rejected by {backend}: {exc}",
                               self.actor)
                    self._task_back_to_pending(task["id"], f"submission rejected by {backend}: {exc}")
            return False
        except Exception as exc:  # ambiguous: may or may not exist; reconcile resolves it by launch_id
            with db.tx(self.conn):
                log_event(self.conn, "launch", launch_id, SUBMITTING, SUBMITTING,
                          f"submission outcome unknown ({type(exc).__name__}: {exc}); will look it up", self.actor)
            report.ambiguous += 1
            return False
        self._crashpoint("after_submit")
        with db.tx(self.conn):
            transition(self.conn, "launch", launch_id, "ACCEPTED", "accepted", self.actor, external_id=ref.external_id)
            for attempt_id, _, i, _ in rows:
                transition(self.conn, "attempt", attempt_id, SUBMITTED,
                           f"submitted to {backend} as {ref.external_id}" + (f"_{i}" if len(rows) > 1 else ""),
                           self.actor)
        return True

    # ==== control, queries ======================================================================

    def get_control(self, key: str, default: str) -> str:
        row = self.conn.execute("SELECT value FROM control WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default

    def set_control(self, key: str, value: str) -> None:
        with db.tx(self.conn):
            old = self.get_control(key, "")
            self.conn.execute("INSERT INTO control(key, value) VALUES (?,?) "
                              "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
            log_event(self.conn, "control", key, old or None, value, f"{key} set to {value}", self.actor)

    def set_mode(self, mode: str) -> None:
        if mode not in self.cfg.budgets:
            raise SwitchboardError(f"unknown mode '{mode}'; configured: {sorted(self.cfg.budgets)}")
        self.set_control("mode", mode)

    def experiments(self) -> list[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM experiments ORDER BY created_at").fetchall()

    def find_experiment(self, ref: str) -> sqlite3.Row:
        return self._experiment(ref)

    def find_task(self, ref: str) -> sqlite3.Row:
        """``EXP/KEY`` or ``EXP/IDX`` (EXP may be ``name#run``)."""
        exp_ref, sep, task_ref = ref.rpartition("/")
        if not sep:
            raise SwitchboardError("task references look like EXPERIMENT/KEY or EXPERIMENT/INDEX")
        exp = self._experiment(exp_ref)
        if task_ref.isdigit():
            row = self.conn.execute("SELECT * FROM tasks WHERE experiment_id=? AND idx=?",
                                    (exp["id"], int(task_ref))).fetchone()
        else:
            row = self.conn.execute("SELECT * FROM tasks WHERE experiment_id=? AND key=?",
                                    (exp["id"], task_ref)).fetchone()
        if row is None:
            raise SwitchboardError(f"no task {task_ref} in {exp_ref}")
        return row

    def logs(self, ref: str, attempt_id: str | None = None, tail: int = 50) -> dict:
        """stdout/stderr of an attempt (default: the accepted or latest one), read wherever it ran."""
        task = self.find_task(ref)
        rows = self.conn.execute(
            "SELECT a.*, l.external_id FROM attempts a JOIN launches l ON l.id=a.launch_id WHERE a.task_id=? "
            "ORDER BY a.created_at", (task["id"],)).fetchall()
        if not rows:
            raise SwitchboardError(f"{ref} has not been dispatched yet ({task['note']})")
        if attempt_id:
            matches = [r for r in rows if r["id"] == attempt_id or r["id"].endswith(attempt_id)]
            if not matches:
                raise SwitchboardError(f"no attempt {attempt_id} for {ref}")
            row = matches[0]
        else:
            row = next((r for r in rows if r["id"] == task["accepted_attempt_id"]), rows[-1])
        executor = self.executors.get(row["backend"])
        if executor is None:
            raise SwitchboardError(f"backend {row['backend']} is not available in this build")
        paths = {"stdout": f"{row['attempt_dir']}/stdout.log", "stderr": f"{row['attempt_dir']}/stderr.log"}
        element_log = getattr(executor, "element_log", None)  # e.g. Slurm's own log for the array element
        if element_log and row["external_id"]:
            paths["slurm"] = element_log(row["launch_id"], row["external_id"], row["array_idx"] or 0)
        texts = executor.read_text(list(paths.values()))
        out = {"attempt_id": row["id"], "backend": row["backend"], "state": row["state"], "streams": {}}
        for stream, path in paths.items():
            text = texts.get(path)
            lines = text.splitlines() if text is not None else None
            out["streams"][stream] = {"path": path, "lines": lines[-tail:] if lines is not None and tail else lines}
        return out

    def task_attempts(self, task_id: int) -> list[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM attempts WHERE task_id=? ORDER BY created_at", (task_id,)).fetchall()

    def events_for(self, entity: str, entity_id: str) -> list[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM events WHERE entity=? AND entity_id=? ORDER BY id",
                                 (entity, str(entity_id))).fetchall()

    def experiment_envs(self, exp_id: int) -> dict[str, str]:
        return self._envs(exp_id)

    def results(self, experiment: str) -> list[dict]:
        exp = self._experiment(experiment)
        rows = self.conn.execute(
            "SELECT t.idx, t.key, t.params_json, t.seed, a.id AS attempt_id, a.backend, a.attempt_dir, a.wall_s "
            "FROM tasks t JOIN attempts a ON a.id=t.accepted_attempt_id WHERE t.experiment_id=? ORDER BY t.idx",
            (exp["id"],),
        ).fetchall()
        return [{"idx": r["idx"], "task_key": r["key"], "params": json.loads(r["params_json"]), "seed": r["seed"],
                 "attempt_id": r["attempt_id"], "backend": r["backend"], "outdir": str(Path(r["attempt_dir"]) / "out"),
                 "wall_s": r["wall_s"]} for r in rows]

    def usage_warnings(self, experiment_id: int) -> list[str]:
        rows = self.conn.execute(
            "SELECT t.key, t.cpus, t.mem_mb, a.id, a.cpu_s, a.wall_s, a.peak_rss_mb FROM attempts a "
            "JOIN tasks t ON t.id=a.task_id WHERE t.experiment_id=? AND a.wall_s IS NOT NULL", (experiment_id,),
        ).fetchall()
        out = []
        for r in rows:
            if r["peak_rss_mb"] and r["peak_rss_mb"] > r["mem_mb"]:
                out.append(f"{r['key']}: peak RSS {r['peak_rss_mb']:.0f} MB exceeded its {r['mem_mb']} MB reservation")
            if r["cpu_s"] and r["wall_s"] and r["wall_s"] > 1 and r["cpu_s"] / r["wall_s"] > r["cpus"] * 1.5:
                out.append(f"{r['key']}: used {r['cpu_s'] / r['wall_s']:.1f} cores on average but reserved "
                           f"{r['cpus']} (over-threaded?)")
        return out

    # ==== helpers ===============================================================================

    def _crashpoint(self, name: str) -> None:
        if name in self.crashpoints:
            raise SimulatedCrash(name)

    def _executor_for(self, spec: ExperimentSpec, backend: str) -> Executor:
        if backend not in spec.backends:
            raise SpecError(f"the spec has no backends.{backend} section (workdir, output_root, setup)")
        if backend not in self.executors:
            raise SwitchboardError(f"backend '{backend}' is not available in this build")
        return self.executors[backend]

    def _latest_run(self, name: str) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM experiments WHERE name=? ORDER BY run_no DESC LIMIT 1",
                                 (name,)).fetchone()

    def _experiment(self, ref: str) -> sqlite3.Row:
        name, _, run = ref.partition("#")
        if run:
            row = self.conn.execute("SELECT * FROM experiments WHERE name=? AND run_no=?", (name, int(run))).fetchone()
        else:
            row = self._latest_run(name)
        if row is None:
            raise SwitchboardError(f"no experiment named {ref}")
        return row

    def _experiment_by_id(self, exp_id: int) -> sqlite3.Row:
        return self.conn.execute("SELECT * FROM experiments WHERE id=?", (exp_id,)).fetchone()

    def _spec(self, exp: sqlite3.Row) -> ExperimentSpec:
        if exp["id"] not in self._specs:
            self._specs[exp["id"]] = ExperimentSpec.model_validate_json(exp["spec_json"])
        return self._specs[exp["id"]]

    def _spec_for_task(self, task: sqlite3.Row) -> ExperimentSpec:
        return self._spec(self._experiment_by_id(task["experiment_id"]))

    def _task(self, task_id: int) -> sqlite3.Row:
        return self.conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()

    def _envs(self, exp_id: int) -> dict[str, str]:
        return {r["platform"]: r["env_id"] for r in self.conn.execute(
            "SELECT platform, env_id FROM experiment_envs WHERE experiment_id=?", (exp_id,))}

    def _launch_attempts(self, launch_id: str) -> list[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM attempts WHERE launch_id=?", (launch_id,)).fetchall()


def runner_alive(status: dict) -> bool | None:
    """Is the shim that wrote this RUNNING status still alive? None when it ran on another host."""
    if status.get("host") != socket.gethostname() or not status.get("pid"):
        return None
    try:
        proc = psutil.Process(status["pid"])
        # Guard against pid reuse: the shim was created before it recorded started_at.
        return proc.create_time() <= status.get("started_at", 0) + 1 and proc.status() != psutil.STATUS_ZOMBIE
    except psutil.Error:
        return False


def attempt_spec(spec: ExperimentSpec, experiment: str, task_key: str, idx: int, params: dict, seed: int | None,
                 cpus: int, attempt_id: str, attempt_dir: str, workdir: str, setup: str | None,
                 code_id: str | None, env_id: str | None, nudge_socket: str | None) -> dict:
    """The shim's input for one attempt (see runner/shim.py)."""
    outdir = f"{attempt_dir}/out"
    command = render_command(spec.command, {
        "seed": seed, "outdir": outdir, "params_file": f"{attempt_dir}/params.json", "task_key": task_key,
        "attempt_id": attempt_id, "experiment": experiment, "idx": idx, "param": params, "cpus": cpus,
    })
    return {
        "schema": 1, "attempt_id": attempt_id, "task_key": task_key, "experiment": experiment, "idx": idx,
        "attempt_dir": attempt_dir, "outdir": outdir, "workdir": workdir, "setup": setup, "command": command,
        "env": spec.env, "cpus": cpus, "params": params, "seed": seed, "outputs_expect": spec.outputs.expect,
        "compat": spec.compat, "identity": spec.identity.model_dump(exclude_none=True) if spec.identity else None,
        "expected": {"code_id": code_id, "env_id": env_id}, "nudge_socket": nudge_socket,
    }


def slurm_time(explicit: str | None, runtime_s: float | None) -> str | None:
    """Slurm --time: the spec's value, else 1.5x the declared runtime plus 5 minutes, else the cluster default."""
    if explicit:
        return explicit
    if not runtime_s:
        return None
    minutes = int(runtime_s * 1.5 / 60) + 5
    return f"{minutes // 60:02d}:{minutes % 60:02d}:00"


def display_name(name: str, run_no: int) -> str:
    return name if run_no == 1 else f"{name}#{run_no}"


def dir_name(name: str, run_no: int) -> str:
    return name if run_no == 1 else f"{name}--run{run_no}"


def parse_selection(selection: str | None, total: int) -> set[int]:
    """'0-99,150,200-210' -> set of indices; None -> all."""
    if selection is None:
        return set(range(total))
    out: set[int] = set()
    for part in selection.split(","):
        part = part.strip()
        if not part:
            continue
        lo, sep, hi = part.partition("-")
        try:
            a, b = int(lo), int(hi) if sep else int(lo)
        except ValueError:
            raise SwitchboardError(f"bad task selection '{part}'; use e.g. 0-99,150") from None
        if a > b or b >= total or a < 0:
            raise SwitchboardError(f"task selection '{part}' is outside 0-{total - 1}")
        out.update(range(a, b + 1))
    return out
