"""Shared state for the mock Slurm commands (tests only). Stdlib, Python >= 3.8.

State lives in $MOCK_SLURM_DIR/state.json (flock-protected); faults in $MOCK_SLURM_DIR/faults.json.
"""

import contextlib
import fcntl
import json
import os
import time

DIR = os.environ["MOCK_SLURM_DIR"]
TERMINAL = {"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY", "NODE_FAIL", "PREEMPTED"}


def p(*parts):
    return os.path.join(DIR, *parts)


@contextlib.contextmanager
def locked():
    with open(p("lock"), "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            with open(p("state.json")) as fh:
                state = json.load(fh)
        except (IOError, ValueError):
            state = {"next_id": 1000, "jobs": {}, "sbatch_calls": 0}
        yield state
        tmp = p("state.json.tmp")
        with open(tmp, "w") as fh:
            json.dump(state, fh, indent=1)
        os.replace(tmp, p("state.json"))


def read_state():
    with locked() as s:
        return s


def faults():
    try:
        with open(p("faults.json")) as fh:
            return json.load(fh)
    except (IOError, ValueError):
        return {}


def save_faults(f):
    with open(p("faults.json"), "w") as fh:
        json.dump(f, fh)


def visible(job):
    return time.time() >= job["submitted"] + float(faults().get("hidden_s", 0))


def fmt_elapsed(sec):
    sec = int(sec or 0)
    return "%02d:%02d:%02d" % (sec // 3600, sec % 3600 // 60, sec % 60)


def fmt_cpu(sec):
    sec = float(sec or 0)
    if sec < 3600:
        return "%02d:%06.3f" % (int(sec // 60), sec % 60)
    return fmt_elapsed(sec)
