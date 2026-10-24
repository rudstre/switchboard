"""Executes one mock array job's elements, honouring the throttle and fault injection."""
import os
import signal
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _mockslurm as m  # noqa: E402

aj = sys.argv[1]
procs = {}  # idx -> (Popen, started)
kills = {}  # idx -> ("TIMEOUT"|"OUT_OF_MEMORY", deadline, sigkill_at)


def kill_group(pid, sig):
    try:
        os.killpg(pid, sig)
    except ProcessLookupError:
        pass


while True:
    f = m.faults()
    with m.locked() as st:
        job = st["jobs"][aj]
        els = job["elements"]
        hidden = not m.visible(job)
        free = job["throttle"] - len(procs)
        for idx in sorted(els, key=int):
            el = els[idx]
            if free <= 0 or hidden or el["state"] != "PENDING":
                continue
            if el.get("cancel"):
                el.update(state="CANCELLED", exit="0:15", end=time.time())
                continue
            if idx in f.get("node_fail_elements", []):
                el.update(state="NODE_FAIL", exit="0:0", start=time.time(), end=time.time())
                continue
            env = dict(os.environ, SLURM_ARRAY_JOB_ID=aj, SLURM_ARRAY_TASK_ID=idx, SLURM_JOB_ID=str(el["raw"]),
                       SLURM_CPUS_PER_TASK=job["cpus"], SLURM_SUBMIT_DIR=job["cwd"])
            out = job["output"].replace("%A", aj).replace("%a", idx)
            out = out if os.path.isabs(out) else os.path.join(job["cwd"], out)
            with open(out, "ab") as log:
                proc = subprocess.Popen(["bash", job["script"]], cwd=job["cwd"], env=env, stdout=log, stderr=log,
                                        stdin=subprocess.DEVNULL, start_new_session=True)
            procs[idx] = (proc, time.time())
            el.update(state="RUNNING", start=time.time(), pid=proc.pid)
            free -= 1
            for kind, key in (("TIMEOUT", "timeout_elements"), ("OUT_OF_MEMORY", "oom_elements")):
                if idx in f.get(key, {}):
                    kills[idx] = (kind, time.time() + float(f[key][idx]), None)
        for idx, (proc, _started) in list(procs.items()):
            el = els[idx]
            if el.get("cancel") and not el.get("signalled"):
                kill_group(proc.pid, signal.SIGTERM)
                el["signalled"] = True
            if idx in kills:
                kind, deadline, sigkill_at = kills[idx]
                if sigkill_at is None and time.time() >= deadline:
                    kill_group(proc.pid, signal.SIGKILL if kind == "OUT_OF_MEMORY" else signal.SIGTERM)
                    kills[idx] = (kind, deadline, time.time() + 1.0)
                elif sigkill_at is not None and time.time() >= sigkill_at:
                    kill_group(proc.pid, signal.SIGKILL)
            pid, status, usage = os.wait4(proc.pid, os.WNOHANG)
            if pid == 0:
                continue
            del procs[idx]
            code = os.WEXITSTATUS(status) if os.WIFEXITED(status) else 0
            sig = os.WTERMSIG(status) if os.WIFSIGNALED(status) else 0
            if el.get("cancel"):
                state = "CANCELLED"
            elif idx in kills and kills[idx][2] is not None:
                state = kills[idx][0]
            else:
                state = "COMPLETED" if code == 0 and sig == 0 else "FAILED"
            rss_kb = usage.ru_maxrss / 1024 if sys.platform == "darwin" else usage.ru_maxrss
            el.update(state=state, exit="%d:%d" % (code, sig), end=time.time(),
                      cpu=usage.ru_utime + usage.ru_stime, maxrss_kb=int(rss_kb))
        done = all(el["state"] in m.TERMINAL for el in els.values())
    if done:
        break
    time.sleep(0.05)
