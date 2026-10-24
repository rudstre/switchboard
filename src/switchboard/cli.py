"""switchboard command-line interface. Every command accepts --json for use by Claude Code.

Local tasks run under Pueue. With the daemon running (`switchboard daemon install`), commands just
nudge it; without one, they run a controller pass inline. Slurm arrives in M3.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime

from . import launchd, macos
from .config import Config, load_config
from .controller import ACTIVE_SQL, Controller, SwitchboardError, display_name
from .daemon import Daemon, DaemonAlreadyRunning, daemon_running, nudge
from .executors.base import BackendUnreachable
from .executors.pueue import PueueError, PueueExecutor
from .ids import short
from .spec import SpecError, load_spec


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except (SwitchboardError, SpecError, PueueError, DaemonAlreadyRunning) as exc:
        if getattr(args, "json", False):
            print(json.dumps({"error": str(exc)}))
        else:
            print(f"error: {exc}", file=sys.stderr)
        return 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="switchboard", description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)

    def add(name, func, help_):
        p = sub.add_parser(name, help=help_)
        p.add_argument("--json", action="store_true", help="machine-readable output")
        p.set_defaults(func=func)
        return p

    p = add("submit", cmd_submit, "register an experiment and assign tasks to a backend")
    p.add_argument("spec")
    p.add_argument("--backend", required=True, choices=["local", "hpc"])
    p.add_argument("--tasks", help="task indices to assign, e.g. 0-99,150 (default: all)")
    p.add_argument("--new-run", action="store_true", help="start NAME#N+1 instead of refusing on identity change")
    p.add_argument("--code-id", help="pin this code identity instead of evaluating it")
    p.add_argument("--dry-run", action="store_true")

    p = add("assign", cmd_assign, "assign pending tasks of an experiment to a backend")
    p.add_argument("experiment")
    p.add_argument("--backend", required=True, choices=["local", "hpc"])
    p.add_argument("--tasks")

    add("tick", cmd_tick, "one reconcile + admission pass")
    p = add("run", cmd_run, "tick in the foreground until local work is done (no daemon needed)")
    p.add_argument("--interval", type=float, default=0.5)
    add("doctor", cmd_doctor, "check Pueue, the daemon, gates and paths")

    p = add("daemon", cmd_daemon, "run or manage the background controller")
    p.add_argument("action", choices=["run", "install", "uninstall", "status", "plist"])

    p = add("status", cmd_status, "experiment progress and local budget")
    p.add_argument("experiment", nargs="?")
    p = add("explain", cmd_explain, "why a task is where it is: routing, admission, attempts, events")
    p.add_argument("task", help="EXPERIMENT/KEY or EXPERIMENT/INDEX")
    p = add("results", cmd_results, "accepted result per task (use this instead of globbing directories)")
    p.add_argument("experiment")

    p = add("mode", cmd_mode, "show or set the local budget profile")
    p.add_argument("mode", nargs="?")
    p = add("pause-local", cmd_pause, "stop admitting new local tasks (running tasks continue)")
    p.add_argument("--suspend", action="store_true",
                   help="also SIGSTOP running local tasks (frees CPU, not memory)")
    add("resume-local", cmd_resume, "resume local admissions")
    return parser


def make_controller(actor: str = "cli", cfg: Config | None = None) -> Controller:
    cfg = cfg or load_config()
    executors = {"local": PueueExecutor(cfg.launch_dir, cfg.local)}
    return Controller(cfg, executors, actor=actor, gates=local_gates(cfg))


def local_gates(cfg: Config) -> list:
    gates = []
    if cfg.local.memory_pressure_gate:
        gates.append(macos.memory_pressure_gate())
    if not cfg.local.run_on_battery:
        gates.append(macos.battery_gate())
    return gates


def _kick(ctl: Controller) -> str:
    """After a change: wake the daemon, or run one pass inline if there is none."""
    if daemon_running(ctl.cfg) and nudge(ctl.cfg):
        return "daemon notified"
    report = ctl.tick()
    return _fmt_report(report.as_dict()) + " (no daemon running; ran one pass inline)"


# ---- commands ----------------------------------------------------------------------------------


def cmd_submit(args) -> int:
    ctl = make_controller()
    res = ctl.submit(load_spec(args.spec), args.backend, tasks=args.tasks, new_run=args.new_run,
                     code_id=args.code_id, dry_run=args.dry_run)
    if args.json:
        kicked = _kick(ctl) if not res.dry_run else None
        print(json.dumps(res.__dict__ | {"controller": kicked}))
        return 0
    verb = "would create" if res.dry_run and res.created else ("created" if res.created else "unchanged identity:")
    print(f"{verb} {res.experiment}: {res.tasks_total} tasks, {res.tasks_assigned} assigned to {res.backend}")
    print(f"  code id: {short(res.code_id)}")
    for plat, env in res.env_ids.items():
        print(f"  env id [{plat}]: {short(env)}")
    for w in res.warnings:
        print(f"  warning: {w}")
    if not res.dry_run:
        print(f"  {_kick(ctl)}")
    return 0


def cmd_assign(args) -> int:
    ctl = make_controller()
    n = ctl.assign(args.experiment, args.backend, args.tasks)
    kicked = _kick(ctl)
    print(json.dumps({"assigned": n, "controller": kicked}) if args.json
          else f"{n} pending tasks assigned to {args.backend}; {kicked}")
    return 0


def cmd_tick(args) -> int:
    report = make_controller(actor="tick").tick()
    print(json.dumps(report.as_dict()) if args.json else _fmt_report(report.as_dict()))
    return 0


def cmd_run(args) -> int:
    ctl = make_controller(actor="run")
    totals: dict = {}
    while True:
        report = ctl.tick().as_dict()
        for k, v in report.items():
            if isinstance(v, int):
                totals[k] = totals.get(k, 0) + v
        if report["unreachable"]:
            print(f"unreachable: {report['unreachable']}", file=sys.stderr)
        active, pending = ctl.local_work_remaining()
        if active == 0 and (pending == 0 or report["admitted"] == 0):
            break
        time.sleep(args.interval)
    if args.json:
        print(json.dumps({"totals": totals, "pending_local": pending}))
    else:
        print(_fmt_report(totals))
        if pending:
            print(f"{pending} local tasks still pending (see `switchboard explain`)")
    return 0


def cmd_status(args) -> int:
    ctl = make_controller()
    exps = [ctl.find_experiment(args.experiment)] if args.experiment else ctl.experiments()
    data = {"experiments": [_exp_status(ctl, e) for e in exps], "local": _local_status(ctl)}
    if args.json:
        print(json.dumps(data))
        return 0
    for e in data["experiments"]:
        c = e["counts"]
        print(f"Experiment: {e['name']}")
        print(f"  Tasks: {e['tasks']}   Completed: {c['SUCCEEDED']}   Failed: {c['FAILED']}   "
              f"Cancelled: {c['CANCELLED']}")
        for backend, n in sorted(e["running"].items()):
            print(f"  Running {'locally' if backend == 'local' else 'on ' + backend.upper()}: {n}")
        for backend, n in sorted(e["submitted"].items()):
            print(f"  Submitted/queued on {backend}: {n}")
        pend = e["pending"]
        print(f"  Pending: {sum(pend.values())}" + (
            " (" + ", ".join(f"{k} {v}" for k, v in sorted(pend.items())) + ")" if pend else ""))
        print(f"  Identity: code {short(e['code_id'])}" + "".join(
            f"   env[{p}] {short(v)}" for p, v in e["env_ids"].items()))
        for w in e["warnings"][:5]:
            print(f"  warning: {w}")
        print()
    loc = data["local"]
    print(f"Local budget ({loc['mode']}{', PAUSED' if loc['paused'] else ''}):")
    print(f"  CPU: {loc['cpus_reserved']} / {loc['cpus_budget']} threads reserved")
    print(f"  RAM: {loc['mem_gb_reserved']:.1f} / {loc['mem_gb_budget']:g} GB reserved")
    return 0


def cmd_explain(args) -> int:
    ctl = make_controller()
    task = ctl.find_task(args.task)
    exp = ctl._experiment_by_id(task["experiment_id"])
    attempts = ctl.task_attempts(task["id"])
    data = {
        "task": f"{display_name(exp['name'], exp['run_no'])}/{task['key']}",
        "state": task["state"], "assigned_backend": task["assigned_backend"], "note": task["note"],
        "params": json.loads(task["params_json"]), "seed": task["seed"],
        "reservation": {"cpus": task["cpus"], "mem_mb": task["mem_mb"]},
        "retries_used": task["retries_used"], "accepted_attempt_id": task["accepted_attempt_id"],
        "attempts": [dict(a) | {"events": [dict(ev) for ev in ctl.events_for("attempt", a["id"])]} for a in attempts],
        "task_events": [dict(ev) for ev in ctl.events_for("task", task["id"])],
    }
    if args.json:
        print(json.dumps(data))
        return 0
    print(f"Task {data['task']}  [{task['state']}]")
    print(f"  params: {task['params_json']}  seed: {task['seed']}")
    print(f"  backend: {task['assigned_backend'] or 'unassigned'}   reservation: {task['cpus']} cpus, "
          f"{task['mem_mb'] / 1024:.1f} GB   retries used: {task['retries_used']}")
    print(f"  why: {task['note']}")
    for a in attempts:
        mark = "  <- accepted" if a["id"] == task["accepted_attempt_id"] else ""
        metrics = ""
        if a["wall_s"] is not None:
            metrics = f"  wall {a['wall_s']:.1f}s, cpu {a['cpu_s'] or 0:.1f}s, peak {a['peak_rss_mb'] or 0:.0f} MB"
        print(f"  attempt {a['id']} on {a['backend']}: {a['state']}{' (' + a['reason'] + ')' if a['reason'] else ''}"
              f"{metrics}{mark}")
        print(f"    dir: {a['attempt_dir']}")
        for ev in ctl.events_for("attempt", a["id"]):
            print(f"    {_ts(ev['ts'])}  {ev['from_state'] or '-':>10} -> {ev['to_state'] or '-':<10} {ev['reason']}")
    print("  task history:")
    for ev in data["task_events"]:
        print(f"    {_ts(ev['ts'])}  {ev['from_state'] or '-':>10} -> {ev['to_state'] or '-':<10} {ev['reason']}")
    return 0


def cmd_results(args) -> int:
    rows = make_controller().results(args.experiment)
    if args.json:
        print(json.dumps(rows))
    else:
        for r in rows:
            print(f"{r['idx']:>5}  {r['task_key']}  {r['backend']:<5}  {r['outdir']}")
    return 0


def cmd_mode(args) -> int:
    ctl = make_controller()
    if args.mode:
        ctl.set_mode(args.mode)
        _kick(ctl)
    mode = ctl.get_control("mode", "focus")
    print(json.dumps({"mode": mode}) if args.json else f"mode: {mode}")
    return 0


def cmd_pause(args) -> int:
    ctl = make_controller()
    ctl.set_control("local_paused", "1")
    if args.suspend:
        ctl.executors["local"].suspend()
        ctl.set_control("local_suspended", "1")
    msg = ("local admissions paused; running tasks suspended (SIGSTOP: CPU freed, memory still held)"
           if args.suspend else "local admissions paused; running tasks continue")
    print(json.dumps({"local_paused": True, "suspended": args.suspend}) if args.json else msg)
    return 0


def cmd_resume(args) -> int:
    ctl = make_controller()
    resumed = ctl.get_control("local_suspended", "0") == "1"
    if resumed:
        ctl.executors["local"].resume()
        ctl.set_control("local_suspended", "0")
    ctl.set_control("local_paused", "0")
    kicked = _kick(ctl)
    print(json.dumps({"local_paused": False, "resumed_suspended": resumed}) if args.json
          else f"local admissions resumed{'; suspended tasks continued' if resumed else ''}; {kicked}")
    return 0


def cmd_daemon(args) -> int:
    cfg = load_config()
    if args.action == "run":
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
        Daemon(cfg, lambda: make_controller(actor="daemon", cfg=cfg)).run()
        return 0
    if args.action == "plist":
        sys.stdout.write(launchd.render(cfg).decode())
        return 0
    if args.action == "install":
        path = launchd.install(cfg)
        print(json.dumps({"installed": str(path)}) if args.json else f"installed and loaded {path}")
        return 0
    if args.action == "uninstall":
        removed = launchd.uninstall()
        print(json.dumps({"removed": removed}) if args.json else ("removed" if removed else "not installed"))
        return 0
    running = daemon_running(cfg)
    print(json.dumps({"running": running, "socket": str(cfg.nudge_socket)}) if args.json
          else f"daemon {'running' if running else 'not running'} (socket {cfg.nudge_socket})")
    return 0


def cmd_doctor(args) -> int:
    cfg = load_config()
    checks: list[tuple[str, str, str]] = []  # (level, check, message)
    ex = PueueExecutor(cfg.launch_dir, cfg.local)
    try:
        checks.append(("ok", "pueue", ex.check_version()))
        try:
            ex.ensure_group()
            group = ex.group_status()
            checks.append(("ok", "pueued", f"reachable; group '{cfg.local.pueue_group}' "
                                           f"parallel={group.get('parallel_tasks')} status={group.get('status')}"))
        except BackendUnreachable as exc:
            checks.append(("fail", "pueued", f"{exc}. Start it with `brew services start pueue`."))
    except PueueError as exc:
        checks.append(("fail", "pueue", str(exc)))
    running = daemon_running(cfg)
    checks.append(("ok" if running else "warn", "daemon",
                   "running" if running else "not running; commands tick inline. "
                   "Install with `switchboard daemon install`."))
    level = macos.memory_pressure_level()
    checks.append(("ok" if level == 1 else "warn", "memory pressure",
                   macos.PRESSURE_NAMES.get(level, "unreadable") if level else "unreadable"))
    batt = macos.on_battery()
    checks.append(("warn" if batt and not cfg.local.run_on_battery else "ok", "power",
                   {True: "battery (local admissions gated)", False: "AC", None: "unknown"}[batt]))
    checks.append(("ok", "state", f"{cfg.db_path}; nudge socket {cfg.nudge_socket}"))
    if args.json:
        print(json.dumps([{"level": lv, "check": c, "message": m} for lv, c, m in checks]))
    else:
        for lv, c, m in checks:
            print(f"[{lv:>4}] {c}: {m}")
    return 1 if any(lv == "fail" for lv, _, _ in checks) else 0


# ---- helpers -----------------------------------------------------------------------------------


def _exp_status(ctl: Controller, exp) -> dict:
    conn = ctl.conn
    counts = {s: 0 for s in ("PENDING", "SUBMITTED", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED")}
    for r in conn.execute("SELECT state, count(*) n FROM tasks WHERE experiment_id=? GROUP BY state", (exp["id"],)):
        counts[r["state"]] = r["n"]
    pending = {(r["b"] or "unassigned"): r["n"] for r in conn.execute(
        "SELECT assigned_backend b, count(*) n FROM tasks WHERE experiment_id=? AND state='PENDING' GROUP BY b",
        (exp["id"],))}
    by_backend = {"RUNNING": {}, "SUBMITTED": {}}
    for r in conn.execute(
            f"SELECT a.backend, a.state, count(*) n FROM attempts a JOIN tasks t ON t.id=a.task_id "
            f"WHERE t.experiment_id=? AND a.state IN {ACTIVE_SQL} GROUP BY a.backend, a.state", (exp["id"],)):
        bucket = "RUNNING" if r["state"] == "RUNNING" else "SUBMITTED"
        by_backend[bucket][r["backend"]] = by_backend[bucket].get(r["backend"], 0) + r["n"]
    return {
        "name": display_name(exp["name"], exp["run_no"]), "tasks": sum(counts.values()), "counts": counts,
        "running": by_backend["RUNNING"], "submitted": by_backend["SUBMITTED"], "pending": pending,
        "code_id": exp["code_id"], "env_ids": ctl.experiment_envs(exp["id"]),
        "warnings": ctl.usage_warnings(exp["id"]),
    }


def _local_status(ctl: Controller) -> dict:
    mode = ctl.get_control("mode", "focus")
    budget = ctl.cfg.budgets[mode]
    cpus, mem = ctl.conn.execute(
        f"SELECT coalesce(sum(t.cpus),0), coalesce(sum(t.mem_mb),0) FROM attempts a JOIN tasks t ON t.id=a.task_id "
        f"WHERE a.backend='local' AND a.state IN {ACTIVE_SQL}").fetchone()
    return {"mode": mode, "paused": ctl.get_control("local_paused", "0") == "1", "cpus_reserved": cpus,
            "cpus_budget": budget.cpus, "mem_gb_reserved": mem / 1024, "mem_gb_budget": budget.mem_gb}


def _fmt_report(r: dict) -> str:
    parts = [f"{k} {v}" for k, v in r.items() if isinstance(v, int) and v]
    if r.get("unreachable"):
        parts.append(f"unreachable {','.join(r['unreachable'])}")
    return "tick: " + (", ".join(parts) if parts else "nothing to do")


def _ts(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime("%H:%M:%S")


if __name__ == "__main__":
    sys.exit(main())
