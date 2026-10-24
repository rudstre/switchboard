# switchboard

A repository-agnostic controller for research jobs.

- **Where it runs tasks:** on this Mac (via Pueue) or on an HPC cluster (via Slurm over SSH).
- **What it tracks:** every task and attempt, in a SQLite registry.
- **What it promises:**
  - **at-least-once execution**;
  - **at most one accepted result per task**;
  - no silent reuse of results when code or environments change.

**Status:** M0–M2.
- M0 + M1: core model, runner shim, responsiveness benchmark.
- M2: Pueue execution, memory-pressure and battery gates, adaptive daemon with completion nudges.

Coming next:

| Milestone | Adds |
|---|---|
| M3 | Slurm |
| M4 | Mixed routing and the rest of the CLI |
| M5 | Activity detection |
| M6 | Local-first routing with HPC overflow |

## Quick start

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

- **State location:** `~/.switchboard/` (override with `SWITCHBOARD_HOME`).
- **Config:** `~/.config/switchboard/config.toml` (override with `SWITCHBOARD_CONFIG`), for example:

```toml
[budgets.focus]                         # defaults shown; see docs/benchmarks/
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
```

## Spec

See [examples/synthetic-grid.yaml](examples/synthetic-grid.yaml). The sections are:

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

## Layout

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
| `executors/` | executor protocol; `pueue.py` (local, Pueue 4.x); `fake.py` is the in-process test double |
| `macos.py` | memory-pressure and power readings, and the admission gates built on them |
| `daemon.py`, `launchd.py` | adaptive background loop with nudge socket and lock; LaunchAgent install |
| `controller.py` | submit/assign with identity pinning, reconcile, acceptance CAS, admission |
| `policy.py` | pure static-budget admission |
| `tools/synth.py` | synthetic workloads |
| `tools/bench_responsiveness.py` | QoS / responsiveness benchmark (run on AC power) |

## Tests

```bash
uv run pytest            # excludes @cluster; @pueue tests start their own isolated pueued
                         # (skipped if Pueue is missing); shim tests also run under /usr/bin/python3
```
