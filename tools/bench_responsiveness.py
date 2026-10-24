#!/usr/bin/env python3
"""Measure how background compute affects interactivity and throughput on this Mac.

For each launch prefix (default, nice, taskpolicy background, ...) and thread count, it runs a
multithreaded hashing load (tools/synth.py cpu) and, while it runs, measures from a
default-priority process:
  * timer-wakeup jitter (2 ms sleeps): p50 / p99 overshoot
  * interactive proxy: wall time of `python -I -c "import json, ast"`
  * `git status` on a synthetic 3,000-file repository
and the load's own throughput, relative to the default prefix at the same thread count.

macOS QoS placement is *observed*, not assumed: throughput per thread relative to `default`
hints at clamping to the slower core tier. For direct core residency, run
`sudo powermetrics --samplers cpu_power -i 1000` in another terminal during the run.

Results go to bench-results/<timestamp>.{md,json}. Run it on AC power with normal apps open.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
SYNTH = HERE / "synth.py"
PREFIXES = {
    "default": [],
    "nice": ["nice", "-n", "10"],
    "background": ["taskpolicy", "-b"],
    "utility": ["taskpolicy", "-c", "utility"],
}


def pct(values: list[float], q: float) -> float:
    if not values:
        return float("nan")
    values = sorted(values)
    return values[min(len(values) - 1, int(q * len(values)))]


def make_repo(root: Path, n_files: int = 3000) -> Path:
    repo = root / "repo"
    repo.mkdir()
    for i in range(n_files):
        d = repo / f"pkg{i % 30}"
        d.mkdir(exist_ok=True)
        (d / f"mod{i}.py").write_text(f"VALUE = {i}\n" * 20)
    git = ["git", "-C", str(repo), "-c", "user.name=bench", "-c", "user.email=bench@example.invalid"]
    subprocess.run([*git, "init", "-q"], check=True)
    subprocess.run([*git, "add", "-A"], check=True)
    subprocess.run([*git, "commit", "-qm", "init"], check=True)
    for i in range(0, n_files, 60):  # a few dirty files, like a working tree mid-edit
        (repo / f"pkg{i % 30}" / f"mod{i}.py").write_text("changed\n")
    return repo


def probe(duration: float, repo: Path) -> dict:
    jitter: list[float] = []
    stop = threading.Event()

    def jitter_loop() -> None:
        while not stop.is_set():
            t0 = time.perf_counter()
            time.sleep(0.002)
            jitter.append((time.perf_counter() - t0 - 0.002) * 1000)

    th = threading.Thread(target=jitter_loop, daemon=True)
    th.start()
    starts, gits = [], []
    deadline = time.monotonic() + duration
    while time.monotonic() < deadline:
        t0 = time.perf_counter()
        subprocess.run([sys.executable, "-I", "-c", "import json, ast"], check=True)
        starts.append((time.perf_counter() - t0) * 1000)
        t0 = time.perf_counter()
        subprocess.run(["git", "-C", str(repo), "status", "--porcelain"], check=True, stdout=subprocess.DEVNULL)
        gits.append((time.perf_counter() - t0) * 1000)
    stop.set()
    th.join()
    return {
        "jitter_p50_ms": pct(jitter, 0.5),
        "jitter_p99_ms": pct(jitter, 0.99),
        "py_start_ms": statistics.median(starts),
        "git_status_ms": statistics.median(gits),
        "samples": len(starts),
    }


def run_case(prefix: list[str], threads: int, duration: float, repo: Path, tmp: Path) -> dict:
    report = tmp / f"load-{os.getpid()}-{time.monotonic_ns()}.json"
    warmup = 1.0
    load = subprocess.Popen([*prefix, sys.executable, str(SYNTH), "cpu", "--threads", str(threads),
                             "--seconds", str(duration + warmup), "--report", str(report)])
    time.sleep(warmup)
    result = probe(duration, repo)
    load.wait()
    data = json.loads(report.read_text())
    result["units_per_s"] = data["units"] / data["seconds"]
    return result


def on_battery() -> bool:
    try:
        out = subprocess.run(["pmset", "-g", "batt"], capture_output=True, text=True).stdout
    except OSError:
        return False
    return "Battery Power" in out


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="switchboard responsiveness benchmark")
    p.add_argument("--threads", default="4,8,10,12")
    p.add_argument("--prefixes", default="default,nice,background")
    p.add_argument("--duration", type=float, default=20.0, help="seconds per case")
    p.add_argument("--quick", action="store_true", help="smoke test: 4 threads, 4 s per case")
    p.add_argument("--allow-battery", action="store_true")
    p.add_argument("--out", default="bench-results")
    a = p.parse_args(argv)
    if a.quick:
        a.threads, a.duration = "4", 4.0
    if on_battery() and not a.allow_battery:
        print("on battery power: results would not represent plugged-in use. Plug in, or pass --allow-battery.",
              file=sys.stderr)
        return 2
    threads = [int(x) for x in a.threads.split(",")]
    prefixes = a.prefixes.split(",")
    unknown = [x for x in prefixes if x not in PREFIXES]
    if unknown:
        p.error(f"unknown prefixes {unknown}; choose from {sorted(PREFIXES)}")

    rows = []
    with tempfile.TemporaryDirectory(prefix="sb-bench-") as td:
        tmp = Path(td)
        repo = make_repo(tmp)
        print("baseline (no load)...", file=sys.stderr)
        rows.append({"prefix": "-", "threads": 0, **probe(a.duration, repo), "units_per_s": None})
        for n in threads:
            for name in prefixes:
                print(f"{name} x {n} threads...", file=sys.stderr)
                rows.append({"prefix": name, "threads": n, **run_case(PREFIXES[name], n, a.duration, repo, tmp)})

    default_tp = {r["threads"]: r["units_per_s"] for r in rows if r["prefix"] == "default"}
    lines = [
        f"# switchboard responsiveness benchmark ({datetime.now():%Y-%m-%d %H:%M})",
        "",
        f"{os.cpu_count()} logical CPUs; {a.duration:g}s per case; battery={on_battery()}",
        "",
        "| prefix | threads | throughput (MB/s hashed) | vs default | jitter p50 ms | jitter p99 ms "
        "| python start ms | git status ms |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for r in rows:
        tp = r["units_per_s"]
        rel = f"{tp / default_tp[r['threads']]:.2f}" if tp and default_tp.get(r["threads"]) else "-"
        lines.append(
            f"| {r['prefix']} | {r['threads']} | {tp:.0f} | {rel} " if tp else f"| {r['prefix']} | {r['threads']} | - | - "
        )
        lines[-1] += (f"| {r['jitter_p50_ms']:.2f} | {r['jitter_p99_ms']:.2f} | {r['py_start_ms']:.0f} "
                      f"| {r['git_status_ms']:.0f} |")
    table = "\n".join(lines)
    print(table)
    out = Path(a.out)
    out.mkdir(exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    (out / f"{stamp}.md").write_text(table + "\n")
    (out / f"{stamp}.json").write_text(json.dumps(rows, indent=1))
    print(f"\nsaved {out / stamp}.md/.json", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
