#!/usr/bin/env python3
"""Synthetic workloads for testing switchboard. Stdlib only; deterministic given its arguments.

  synth.py task  [--seconds S] [--threads N] [--mem-mb M] [--outdir D --write a.txt,b.json]
                 [--fail-prob P --seed K] [--exit-code C] [--append-to FILE]
  synth.py cpu   --threads N --seconds S [--report FILE]      # throughput load for benchmarks

`task` with no --write and exit 0 is the "missing outputs" case.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import sys
import threading
import time

CHUNK = b"\x5a" * (1 << 20)


def burn(seconds: float, threads: int) -> int:
    """Run `threads` hashing loops for `seconds`; hashlib releases the GIL on large buffers."""
    deadline = time.monotonic() + seconds
    counts = [0] * threads

    def worker(i: int) -> None:
        h = hashlib.sha256()
        while time.monotonic() < deadline:
            h.update(CHUNK)
            counts[i] += 1

    pool = [threading.Thread(target=worker, args=(i,)) for i in range(threads)]
    for t in pool:
        t.start()
    for t in pool:
        t.join()
    return sum(counts)


def cmd_task(a) -> int:
    if a.append_to:
        with open(a.append_to, "a") as fh:
            fh.write(f"{os.getpid()} {os.environ.get('SWITCHBOARD_ATTEMPT_ID', '-')}\n")
    hog = None
    if a.mem_mb:
        hog = bytearray(a.mem_mb * 1024 * 1024)
        for i in range(0, len(hog), 4096):  # touch every page so it is resident
            hog[i] = 1
    if a.seconds:
        if a.threads > 0:
            burn(a.seconds, a.threads)
        else:
            time.sleep(a.seconds)
    if a.fail_prob and random.Random(a.seed).random() < a.fail_prob:
        print(f"synth: failing on purpose (seed {a.seed})", file=sys.stderr)
        return 3
    if a.write:
        outdir = a.outdir or os.environ.get("SWITCHBOARD_OUTDIR", ".")
        os.makedirs(outdir, exist_ok=True)
        for name in a.write.split(","):
            with open(os.path.join(outdir, name), "w") as fh:
                json.dump({"seed": a.seed, "threads_env": os.environ.get("OMP_NUM_THREADS")}, fh)
    print(f"synth: done (seed {a.seed})")
    del hog
    return a.exit_code


def cmd_cpu(a) -> int:
    start = time.monotonic()
    units = burn(a.seconds, a.threads)
    result = {"units": units, "seconds": time.monotonic() - start, "threads": a.threads}
    if a.report:
        with open(a.report, "w") as fh:
            json.dump(result, fh)
    else:
        print(json.dumps(result))
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    t = sub.add_parser("task")
    t.add_argument("--seconds", type=float, default=0)
    t.add_argument("--threads", type=int, default=0, help="0 = sleep instead of burning CPU")
    t.add_argument("--mem-mb", type=int, default=0)
    t.add_argument("--outdir")
    t.add_argument("--write", help="comma-separated output files to create")
    t.add_argument("--fail-prob", type=float, default=0)
    t.add_argument("--seed", type=int, default=0)
    t.add_argument("--exit-code", type=int, default=0)
    t.add_argument("--append-to", help="append one line per execution (duplicate-run detection)")
    t.set_defaults(func=cmd_task)
    c = sub.add_parser("cpu")
    c.add_argument("--threads", type=int, required=True)
    c.add_argument("--seconds", type=float, required=True)
    c.add_argument("--report")
    c.set_defaults(func=cmd_cpu)
    a = p.parse_args(argv)
    return a.func(a)


if __name__ == "__main__":
    sys.exit(main())
