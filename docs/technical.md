# switchboard: technical reference

This page is the detailed reference: rules, guarantees, configuration keys and code layout. For an
introduction, see the [README](../README.md).

**What it promises:**
- **at-least-once execution**;
- **at most one accepted result per task**;
- no silent reuse of results when code or environments change.

**Milestones:**

| Milestone | Delivered |
|---|---|
| M0 + M1 | Core model, runner shim, responsiveness benchmark |
| M2 | Pueue execution, gates, adaptive daemon |
| M3 | Slurm arrays over SSH; smoke-tested on the SWC cluster |
| M4 | Manual mixed routing, cancel, retry, logs |
| M5 | Activity detection and `mode auto` |
| M6 | Automatic local-first routing with cluster overflow; tested on SWC |

Deferred: the `throughput-first` policy, runtime prediction from history, and data staging.

The benchmark that set the local budgets is in [benchmarks/](benchmarks/). The first real-cluster
run is described in [m3-cluster-checklist.md](m3-cluster-checklist.md).

## Command overview

```bash
brew install pueue && brew services start pueue      # Pueue 4.x runs the local tasks
uv sync
uv run switchboard doctor
uv run switchboard daemon install    # optional: background controller (launchd LaunchAgent)
uv run switchboard submit examples/synthetic-grid.yaml --backend local
uv run switchboard status
uv run switchboard explain synthetic-grid/3
uv run switchboard results synthetic-grid --json
```

**Automatic routing (`--backend auto`):**

```yaml
placement:
  policy: local-first          # or local-only, hpc-only
  max_local_wait_h: 6          # overflow work the Mac would not finish within this
  platforms: [osx-arm64, linux-64]
resources: {cpus: 1, mem_gb: 2, runtime_min: 30}   # the declared runtime drives the estimate
```

```bash
switchboard submit grid.yaml --backend auto
switchboard explain grid/150    # routing: [R5] local-first overflow: the Mac would finish it in ~9.4 h ...
switchboard policy grid hpc-only
```

Every pass, pending tasks are routed in queue order. The first rule that applies wins:

| Rule | When | Result |
|---|---|---|
| R1 | the policy is `local-only` or `hpc-only` | that backend |
| R2 | the Mac can't run the task (platform, or more CPUs/memory than its largest budget) | cluster |
| R3 | the cluster isn't usable (not configured, unreachable, no `backends.hpc`, or a code/env identity mismatch, checked once over SSH) | Mac |
| R4 | there is no runtime estimate | Mac |
| R5 | `local-first` | see below |

Under R5, the Mac keeps whatever it would finish within `max_local_wait_h`. It counts the work it already owes, divided by the CPUs of the current budget profile. The tail of the queue overflows to the cluster as Slurm arrays.

Routing is late-bound: pending tasks are re-evaluated on every pass, and dispatched tasks never move. `switchboard assign` turns automatic routing off for that experiment, and `switchboard policy EXP local-first` turns it back on.

**Splitting one experiment by hand:**

```bash
switchboard submit grid.yaml --backend local            # every task starts in the Mac's pool
switchboard assign grid --backend hpc --count 120       # move the last 120 pending tasks to the cluster
switchboard assign grid --backend local --tasks 150-159 # or move specific pending tasks back
```

**Watching and managing tasks:**

```bash
switchboard status grid
switchboard explain grid/42                             # routing, admission reason, attempt history
switchboard logs grid/42                                # stdout/stderr (+ Slurm's log), local or over SSH
switchboard cancel grid/42                              # or `cancel grid` for every unfinished task
switchboard retry grid --failed-only                    # requeue with a fresh retry budget
```

Assigning a task to a backend pins the code and environment identity there first, and refuses on a mismatch. Tasks never move once dispatched.

- **Without the daemon:** each command runs one controller pass inline, and `switchboard run` ticks in the foreground until local work is done.
- **With the daemon:** commands nudge it.
- **Daemon cadence:**
  - it wakes immediately on a nudge: from the CLI, or from the shim when a task exits;
  - it polls every 5 s while local work exists;
  - otherwise it polls every 300 s.

**Local guarantees:**
- Pueue only ever receives admitted tasks, in the `switchboard` group, and the controller does all queueing.
- No new local tasks start under memory pressure ≥ WARN, on battery (unless `local.run_on_battery = true`), or after `switchboard pause-local`.
- Running tasks are never killed by the controller.
- `pause-local --suspend` SIGSTOPs running tasks, which frees CPU but not memory.
- A task killed by a Pueue restart, a reboot or a manual kill is requeued without using a retry, up to 3 times.

**Activity (`mode auto`, the default):**

`switchboard mode` shows the chosen profile, why it was chosen, and the live sensor readings. `switchboard mode focus|away` overrides; `switchboard mode auto` goes back to following activity.

The `focus` budget applies immediately when any of these holds:
- you used the keyboard or trackpad in the last 2 minutes;
- an app keeps the display awake (video, presentation) or is using the microphone (calls);
- other work is using more than 3 cores, which is protected even if you are away.

The `away` budget applies only after away evidence has held for 2 minutes. Away evidence is either:
- the screen is locked or the display is asleep; or
- there has been no input for 10 minutes, nothing holds the display or microphone, and the CPU is quiet.

Not typing, on its own, never counts as away. `corespeechd` ("Hey Siri") is ignored as a microphone user, and more processes can be ignored with `[activity] ignore_holders`.

While local tasks run on AC power, the daemon holds a `caffeinate -i` assertion so idle sleep doesn't freeze them. Closing the lid still sleeps the Mac. Turn this off with `[local] prevent_idle_sleep = false`.

- **State location:** `~/.switchboard/` (override with `SWITCHBOARD_HOME`).
- **Config:** `~/.config/switchboard/config.toml` (override with `SWITCHBOARD_CONFIG`), for example:

```toml
[budgets.focus]                         # defaults shown; see benchmarks/
cpus = 4
mem_gb = 12
launch_prefix = ["nice", "-n", "10"]

[budgets.away]
cpus = 12
mem_gb = 28

[local]
pueue_group = "switchboard"
run_on_battery = false
memory_pressure_gate = true
# pueue_config = "~/path/to/pueue.yml"  # default: Pueue's own config

[daemon]
local_interval_s = 5
idle_interval_s = 300

[activity]                              # mode auto thresholds (defaults shown)
active_idle_s = 120
away_idle_s = 600
away_hold_s = 120
busy_cores = 3.0
quiet_cores = 1.0
ignore_holders = []                     # added to the built-in ["corespeechd"]
```

## Cluster (Slurm over SSH)

```toml
[hpc]
host = "mycluster"            # ~/.ssh/config alias; no credentials are stored
remote_root = "~/.switchboard"
```

**Connecting:**
- `switchboard hpc connect` opens a persistent SSH session (authenticate once, MFA included).
- `hpc check` and `hpc probe` are read-only: probe reports MaxArraySize, submit limits and the accounting type.
- `hpc install` copies the stdlib runner into a directory named by its content hash.

**How tasks run:**
- Tasks assigned to `hpc` are submitted as `sbatch --array` chunks, sized within MaxArraySize and your submit headroom.
- Each chunk's job is named `sb-<launch>`. That name is how a submission whose reply was lost is found again, instead of being resubmitted.
- Jobs use `--no-requeue`; retries are new attempts.

**Outcomes:**

| Slurm outcome | Recorded as |
|---|---|
| TIMEOUT | `timeout` |
| OUT_OF_MEMORY | `oom` |
| NODE_FAIL | `node_fail` (retryable by default) |
| PREEMPTED | an interruption: requeued without using a retry |
| A `scancel` you ran yourself | the task is CANCELLED, not requeued |

**Accounting and escape hatches:**
- CPU and memory figures come from `sacct` step lines; `sacct -X` would omit them.
- Every generated `job.sbatch` is self-contained.
- `switchboard export-slurm SPEC --out DIR` writes a launch directory you can `sbatch` by hand, with no switchboard involved.

## Spec reference

See [examples/synthetic-grid.yaml](../examples/synthetic-grid.yaml). The sections are:

- **`command`:** the command to run. Placeholders: `{seed} {outdir} {params_file} {task_key} {attempt_id} {experiment} {idx} {cpus} {param.NAME}`.
- **`params`:** `grid` (lists, `{linspace: [a, b, n]}`, `{range: [a, b, step]}`), `zip`, `seeds`, or a JSONL `manifest`.
- **`resources`:** declared reservations. They are advisory admission bookkeeping, never kill limits.
- **`outputs.expect`:** glob patterns that must exist and be non-empty in `{outdir}`. Exit code 0 alone is not success.
- **`identity`:**
  - `code` uses the `git` provider (HEAD + diff + untracked file contents) or `command`. It is pinned before any dispatch and must match on every backend.
  - `env` uses `command`. It is pinned per platform.
  - The shim re-checks both before running, and refuses to run on a mismatch.
- **`retries`:** `max` and `on` (`nonzero`, `missing_outputs`, `lost`, `runner_died`, …).
- **`interruptible`:** default `false`.
- **`compat: slurm-array`:** exports `SLURM_ARRAY_TASK_ID` locally.
- **`backends`:** for each of `local` and `hpc`, a `workdir`, `output_root` and `setup` (a shell snippet). Local relative paths resolve against the spec file.

**Run identity.**
- Resubmitting an unchanged spec is a no-op.
- Changing an identity field, the code, or a pinned environment is refused with a diff. Use `--new-run` to start `name#2`.
- Resources and other non-identity fields can change; the change applies only to tasks that are still pending.

## Files and code layout

- **Task attempts** go in `<output_root>/<experiment>/<task_key>/<attempt_id>/`, which holds:
  - `.claim/`
  - `status.json`
  - `stdout.log` / `stderr.log`
  - `params.json`
  - `out/`, which is `{outdir}`
  - `_SUCCESS`
- **Downstream analysis** should read `switchboard results`, not glob directories.

| Module | Role |
|---|---|
| `spec.py` | validation, expansion, identity hash, templating |
| `registry/` | SQLite schema, WAL transactions, state machine with event log |
| `runner/shim.py`, `runner/fingerprint.py` | stdlib-only, Python ≥ 3.8; shipped verbatim to the HPC in M3 |
| `executors/` | executor protocol; `pueue.py` (local, Pueue 4.x); `slurm.py` + `transport.py` (SSH); `fake.py` test double |
| `export.py` | registry-free, self-contained Slurm launch directories |
| `tools/mock_slurm/` | sbatch/squeue/sacct/scancel/scontrol/sacctmgr stand-ins with fault injection (tests) |
| `macos.py` | memory pressure, power, idle time, lock/display state, assertion holders, foreign CPU |
| `activity.py` | pure activity classifier with hysteresis (ACTIVE -> focus, AWAY -> away) |
| `daemon.py`, `launchd.py` | adaptive background loop with nudge socket and lock; LaunchAgent install |
| `controller.py` | submit/assign with identity pinning, reconcile, acceptance CAS, admission |
| `policy.py` | pure static-budget admission |
| `routing.py` | pure routing rules R1–R5 with explainable decisions |
| `tools/synth.py` | synthetic workloads |
| `tools/bench_responsiveness.py` | QoS / responsiveness benchmark (run on AC power) |

## Tests

```bash
uv run pytest            # excludes @cluster; @pueue tests start their own isolated pueued
                         # (skipped if Pueue is missing); shim tests also run under /usr/bin/python3
```
