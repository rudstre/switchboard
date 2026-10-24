# switchboard

**switchboard runs your analysis jobs on your Mac when it's free, and on the cluster when it isn't.**

You describe your experiment once, for example "run `fit_model.py` for these 200 parameter
combinations". switchboard then works out where each run should happen, starts it, watches it, and
tells you where the results are. You don't write `sbatch` scripts, keep spreadsheets of job IDs, or
find yourself wondering "did run 137 actually finish?"

---

## Why would I want this?

A familiar situation:

- You have a parameter sweep: 200 combinations of `theta` and random seed.
- Each run takes about 30 minutes on one CPU.
- Normally you'd write a Slurm script, submit it to the cluster, and wait in the queue.
- Meanwhile your MacBook, which has 15 fast cores, sits idle all evening.

With switchboard you run one command:

```bash
switchboard submit sweep.yaml --backend auto
```

Then:

1. It keeps as much of the sweep as your Mac can finish within a few hours, and runs it there.
2. It sends the rest to the cluster as a single Slurm job array.
3. While you're working on the Mac, it uses only a few cores, so your computer stays responsive.
4. When you lock the screen or walk away, it uses more cores.
5. When you come back, you run `switchboard status` and see what finished and where everything is.

## How it works, in one picture

```
              you
               │   switchboard submit sweep.yaml
               ▼
        ┌─────────────┐   keeps a record of every run (what, where, when, result)
        │ switchboard │──────────────────────────────────────────────────────────
        └─────────────┘
          │         │
  "Mac can do it"   "too much for the Mac right now"
          │         │
          ▼         ▼
     your Mac     the cluster (Slurm, over SSH)
   (in the gaps)   (as job arrays)
          │         │
          └────┬────┘
               ▼
      one result folder per run, listed by `switchboard results`
```

The important ideas:

- **An experiment** is one YAML file. It says which command to run, which parameters to sweep, and
  what output each run should produce.
- **A task** is one combination of parameters, so a 200-combination sweep has 200 tasks.
- **A task succeeds only when its output files exist.** "The program exited without an error" isn't
  enough on its own.
- **Each task produces exactly one accepted result.** If something goes wrong and a run happens twice
  (a dropped connection, say), switchboard keeps only one result and tells you which.

---

## What you need

- **A Mac** with Apple Silicon. This was built on an M5 Pro, but any recent Mac works.
- **[uv](https://docs.astral.sh/uv/)**, a tool for running Python projects. Install it with
  `brew install uv` if you don't already have it.
- **[Pueue](https://github.com/Nukesor/pueue)**, which actually runs the jobs on your Mac. Install it
  with `brew install pueue`.
- **Optional: an account on a Slurm cluster** that you can reach with `ssh` from your Mac.

You do **not** need administrator rights, and nothing needs installing on the cluster beyond Python 3.

## Install

```bash
git clone https://github.com/rudstre/switchboard.git
cd switchboard
uv tool install --editable .   # puts the `switchboard` command on your PATH
brew services start pueue      # start Pueue now and at every login
switchboard doctor             # checks that everything is in place
```

If you'd rather not install the command, run `uv sync` once inside the folder, then prefix every
command below with `uv run`, for example `uv run switchboard status`.

`switchboard doctor` prints a checklist like this:

```
[  ok] pueue: pueue 4.0.4
[  ok] pueued: reachable; group 'switchboard' parallel=64 status=Running
[warn] daemon: not running; commands tick inline. Install with `switchboard daemon install`.
[  ok] memory pressure: NORMAL
[  ok] power: AC
[  ok] hpc: not configured ([hpc] host); local only
```

---

## A five-minute tour

The repository includes a small, fake experiment, [examples/synthetic-grid.yaml](examples/synthetic-grid.yaml).
It has 12 tasks that each burn a CPU for about a second and write a file. Try it:

```bash
switchboard submit examples/synthetic-grid.yaml --backend local
switchboard run       # stays in the foreground until the local work is done
switchboard status
```

You'll see something like:

```
Experiment: synthetic-grid
  Tasks: 12   Completed: 12   Failed: 0   Cancelled: 0
  Pending: 0
  Policy: manual

Local budget (auto -> focus):
  activity: input 0s ago
  CPU: 0 / 4 threads reserved
  RAM: 0.0 / 12 GB reserved
```

To ask about a single task (task 3 here):

```bash
switchboard explain synthetic-grid/3
```

```
Task synthetic-grid/00003-d7b6d7ed  [SUCCEEDED]
  params: {"alpha":0.2}  seed: 2
  backend: local   reservation: 1 cpus, 0.5 GB   retries used: 0
  attempt a-01a169a3e0e6-e856803fec on local: SUCCEEDED  wall 1.1s, cpu 1.0s, peak 30 MB  <- accepted
    dir: .../sb-out/synthetic-grid/00003-d7b6d7ed/a-01a169a3e0e6-e856803fec
```

To list where every result lives:

```bash
switchboard results synthetic-grid
```

That's the whole loop: **submit, check status, collect results.** Everything below is detail.

---

## Describing your own experiment

Here is a realistic example. Suppose your project has a script you'd normally run like this:

```bash
python fit_model.py --theta 0.5 --seed 3 --out results/theta0.5_seed3
```

and you want it for 20 values of `theta` × 10 seeds. Save this as `sweep.yaml`:

```yaml
experiment: theta-sweep            # a short name you'll use in commands

# The command to run. Words in {braces} are filled in for each task.
command: ["python", "fit_model.py",
          "--theta", "{param.theta}",
          "--seed", "{seed}",
          "--out", "{outdir}"]       # switchboard gives every run its own empty output folder

params:
  grid:
    theta: {linspace: [0, 1, 20]}  # 20 evenly spaced values from 0 to 1
  seeds: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
# 20 thetas x 10 seeds = 200 tasks

resources:
  cpus: 1                          # how many cores one run uses
  mem_gb: 2                        # roughly how much memory one run needs
  runtime_min: 30                  # a rough guess at how long one run takes

outputs:
  expect: ["fit.npz", "summary.json"]   # a run only counts as done if these files appear

retries:
  max: 1                           # retry a failed run once before giving up

backends:
  local:                           # how to run it on your Mac
    workdir: ~/code/my-project
    output_root: ~/results/switchboard
    setup: "source .venv/bin/activate"
  hpc:                             # how to run it on the cluster
    workdir: ~/my-project
    output_root: /ceph/scratch/$USER/switchboard
    setup: "source ~/my-project/.venv/bin/activate"
    slurm: {partition: cpu, time: "01:00:00"}
```

`setup` is any shell snippet that prepares the environment, such as activating a virtualenv or conda
env. If you rely on `module load` and it reports `module: command not found` on the cluster, start the
snippet with `source /etc/profile && `. Batch jobs don't always get a login shell.

**The placeholders you can use in `command`:**

| Placeholder | Becomes |
|---|---|
| `{param.NAME}` | that task's value of a parameter, e.g. `{param.theta}` → `0.473` |
| `{seed}` | that task's seed |
| `{outdir}` | an empty folder created just for this run; write your outputs here |
| `{params_file}` | a JSON file containing all of this task's parameters, if your script prefers that |

**Ways to describe parameters:**

```yaml
params:
  grid:                            # every combination of these
    theta: [0.1, 0.2, 0.5]
    n_neurons: {range: [100, 1000, 100]}   # 100, 200, ..., 900
  zip:                             # these go together, pair by pair
    dataset: [mouse1, mouse2]
    session: [s03, s07]
  seeds: [1, 2, 3]
```

For irregular combinations, list one task per line in a file and point to it with
`params: {manifest: tasks.jsonl}`. Each line looks like
`{"theta": 0.3, "n_neurons": 500, "seed": 1}`.

> **Your script needs very little.** switchboard only asks that it accept an output folder (and
> ideally a seed), and write its results there. If your script already reads `SLURM_ARRAY_TASK_ID`,
> add `compat: slurm-array` and it will get that variable on your Mac too.

---

## Choosing where things run

When you submit, pick one of three options:

```bash
switchboard submit sweep.yaml --backend auto    # let switchboard decide (recommended)
switchboard submit sweep.yaml --backend local   # everything on your Mac
switchboard submit sweep.yaml --backend hpc     # everything on the cluster
```

### What `--backend auto` does

By default it's **local-first**: the Mac keeps whatever it can finish within 6 hours, and the rest
goes to the cluster.

A worked example with the sweep above (200 tasks × 30 min × 1 core):

- While you're at your desk, the Mac offers **4 cores**.
- In 6 hours, 4 cores can do 4 × 6 = 24 core-hours of work. That's **48 runs** of 30 minutes each.
- So switchboard keeps the first 48 tasks on the Mac and sends the other **152 to the cluster** as
  one job array.

Ask about one of the overflowed tasks and it tells you why:

```bash
switchboard explain theta-sweep/150
```

```
  routing: [R5] local-first overflow: the Mac would finish it in ~18.9 h at the 'focus' budget,
           over the 6.0 h limit
```

You can change the limit, or the policy, in the YAML:

```yaml
placement:
  policy: local-first     # or local-only (never use the cluster), or hpc-only
  max_local_wait_h: 6     # how long you're willing to wait for the Mac
```

A few behaviours worth knowing:

- **The split is re-checked continuously** for tasks that haven't started. If you go home and the
  Mac gets more cores, more of the remaining work stays local.
- **A task that has started never moves.**
- **If the cluster is down, everything waits for the Mac.**
- **If a task can't run on the Mac at all**, it goes to the cluster automatically. That covers code
  that needs Linux, or a task that needs more cores or memory than the Mac offers.

### Splitting by hand

Sometimes you just want to say "send 120 of these to the cluster":

```bash
switchboard submit sweep.yaml --backend local
switchboard assign theta-sweep --backend hpc --count 120     # moves the last 120 waiting tasks
switchboard assign theta-sweep --backend local --tasks 0-9   # or move specific tasks back
```

---

## Your Mac stays usable

switchboard never uses the whole machine. It works with two budgets:

| When | Cores | Memory |
|---|---|---|
| **focus**: you're using the Mac | 4 | 12 GB |
| **away**: screen locked, or idle for 10+ minutes | 12 | 28 GB |

It decides which one applies by itself (this is `mode auto`). It counts as **focus** when:
- you've typed or used the trackpad in the last 2 minutes;
- you're on a video call or watching something, even without touching the keyboard;
- something else, like a big build, is already keeping the CPU busy.

It switches to **away** only once that's been true for a couple of minutes. Not typing alone doesn't
count as away.

You can always take over:

```bash
switchboard mode            # what does it think is happening, and why?
switchboard mode focus      # force the small budget
switchboard mode away       # force the big budget
switchboard mode auto       # go back to automatic
switchboard pause-local     # start nothing new on the Mac (running jobs carry on)
switchboard resume-local
```

It also protects the Mac on its own:
- **No new local jobs start on battery power**, or when the Mac is short of memory.
- **Running jobs are never killed** to make room.
- **While jobs run on mains power, it stops the Mac from going to sleep on its own.** Closing the lid
  still puts it to sleep.

The default budgets came from measuring this Mac's responsiveness under load; see
[docs/benchmarks](docs/benchmarks/2026-10-23-m5pro-ac.md). You can change them in the config file
(see [Configuration](#configuration)).

---

## Keeping an eye on things

```bash
switchboard status                     # every experiment
switchboard status theta-sweep         # one experiment
switchboard explain theta-sweep/42     # everything about task 42: where, why, and what happened
switchboard logs theta-sweep/42        # its printed output, from the Mac or the cluster
```

A typical `status` mid-run:

```
Experiment: theta-sweep
  Tasks: 200   Completed: 84   Failed: 0   Cancelled: 0
  Running locally: 4
  Running on HPC: 16
  Pending: 96 (hpc 80, local 16)
  Policy: local-first (max local wait 6 h)

Local budget (auto -> away):
  activity: screen locked (for 3 min)
  CPU: 4 / 12 threads reserved
  RAM: 8.0 / 28 GB reserved
  Mac queue: ~2.0 h of declared work at this budget
```

Tasks are referred to as `EXPERIMENT/NUMBER`, for example `theta-sweep/42`. The numbers count from 0
in the order the tasks were generated.

## Getting your results

```bash
switchboard results theta-sweep          # a table: task, where it ran, output folder
switchboard results theta-sweep --json   # the same, for scripts
```

Each run's output folder contains whatever your script wrote to `{outdir}`. To load everything in
Python:

```python
import json, subprocess
import numpy as np

rows = json.loads(subprocess.check_output(["switchboard", "results", "theta-sweep", "--json"]))
for r in rows:
    data = np.load(f"{r['outdir']}/fit.npz")
    print(r["params"]["theta"], r["seed"], r["backend"], data["loss"])
```

> **Use `switchboard results` rather than searching folders yourself.** If a run happened twice, it
> lists only the accepted copy.

---

## When things go wrong

| You see | It means | What to do |
|---|---|---|
| `FAILED (nonzero)` | your script exited with an error | `switchboard logs EXP/N` to read its output |
| `FAILED (missing_outputs)` | your script finished, but the files in `outputs.expect` weren't there | check the script writes to `{outdir}` |
| `FAILED (timeout)` | the cluster stopped it at its time limit | raise `slurm.time` |
| `FAILED (oom)` | the cluster stopped it for using too much memory | raise `resources.mem_gb` |
| `FAILED (fingerprint_mismatch)` | your code changed after you submitted | see the question about changing code below |
| `UNKNOWN` | the cluster can't be reached right now | nothing; it catches up when the connection is back |

```bash
switchboard retry theta-sweep --failed-only   # run the failed tasks again
switchboard cancel theta-sweep/42             # stop one task
switchboard cancel theta-sweep                # stop everything that hasn't finished
```

---

## Setting up the cluster (one time)

1. **Make sure `ssh` works without typing a password every time.** Usually this means a key and an
   entry in `~/.ssh/config`:

   ```
   Host swc
       HostName swc.ucl.ac.uk
       User yourname
       IdentityFile ~/.ssh/id_ed25519
   ```

2. **Tell switchboard about it.** Create `~/.config/switchboard/config.toml` containing:

   ```toml
   [hpc]
   host = "swc"
   ```

3. **Check it works:**

   ```bash
   switchboard hpc check     # can we connect?
   switchboard hpc probe     # what does the cluster allow? (read-only)
   ```

   If your cluster asks for two-factor authentication, run `switchboard hpc connect` once. switchboard
   reuses that login for 12 hours.

4. **Try a tiny test.** [examples/smoke-hpc.yaml](examples/smoke-hpc.yaml) runs 3 small jobs on the
   cluster, and [examples/auto-hpc.yaml](examples/auto-hpc.yaml) splits 12 small jobs between the Mac
   and the cluster.

switchboard keeps its own small helper files in `~/.switchboard/` on the cluster. It doesn't touch
anything else there, and you can still use `sbatch` yourself exactly as before.

## Running in the background

Without a background process, switchboard only does work when you type a command, or while
`switchboard run` is open. To have it keep going on its own, including starting the next job the
moment one finishes:

```bash
switchboard daemon install    # starts now and at every login
switchboard daemon status
switchboard daemon uninstall  # to remove it
```

---

## Common questions

**Will a job ever run twice?**
Rarely, and it's handled. Suppose the connection to the cluster drops just as a job is submitted.
switchboard looks the job up again by name instead of blindly resubmitting it. In the rare case where
a run really does happen twice, only one result is accepted; the other is kept but marked
`SUPERSEDED`.

**What if I change my code while an experiment is running?**
Add an `identity` section and switchboard notices:

```yaml
identity:
  code: {git: {paths: ["src"]}}   # fingerprint the code in src/, including uncommitted edits
```

Every run then checks the code is the same as when you submitted, on the Mac and on the cluster.
Runs with different code refuse to start rather than silently mixing versions. Resubmitting a
changed experiment is refused too, with an explanation; use `--new-run` to start a fresh run
alongside the old one.

**Can I still use the cluster normally?**
Yes. switchboard only manages the jobs it submitted. You can also turn any experiment into a plain
Slurm job folder and run it entirely by hand:

```bash
switchboard export-slurm sweep.yaml --out my-sweep-job
```

**What happens if my laptop sleeps or I close the lid?**
Jobs on the cluster carry on regardless. Jobs on the Mac pause while it sleeps and continue when it
wakes. Next time switchboard looks, it catches up on everything that finished meanwhile.

**Does it change my research code or repository?**
No. It runs the command you give it in the folder you name, and only ever writes inside its output
folders.

**Is it safe to resubmit the same file?**
Yes. Submitting an unchanged experiment again does nothing, and finished runs are not redone.

---

## Cheat sheet

| I want to… | Command |
|---|---|
| check my setup | `switchboard doctor` |
| start an experiment | `switchboard submit FILE.yaml --backend auto` |
| see progress | `switchboard status [EXP]` |
| understand one task | `switchboard explain EXP/N` |
| read a task's output | `switchboard logs EXP/N` |
| find the results | `switchboard results EXP` |
| retry failures | `switchboard retry EXP --failed-only` |
| stop things | `switchboard cancel EXP` or `cancel EXP/N` |
| move work by hand | `switchboard assign EXP --backend hpc --count N` |
| change routing | `switchboard policy EXP local-first` |
| control the Mac budget | `switchboard mode auto` / `focus` / `away`, or `pause-local` |
| run in the background | `switchboard daemon install` |
| test the cluster link | `switchboard hpc check` |

Every command also accepts `--json`, which is handy for scripts and for asking an AI assistant to
manage experiments for you.

## Configuration

Settings live in `~/.config/switchboard/config.toml`. Everything is optional; this example shows the
defaults you're most likely to change:

```toml
[budgets.focus]        # while you're using the Mac
cpus = 4
mem_gb = 12

[budgets.away]         # while you're away
cpus = 12
mem_gb = 28

[local]
run_on_battery = false

[hpc]
host = "swc"           # your ~/.ssh/config alias
```

---

## Learn more

- [docs/technical.md](docs/technical.md): the full reference. It covers the routing rules,
  guarantees, every config key, the Slurm details and the code layout.
- [docs/benchmarks/](docs/benchmarks/): how the Mac budgets were chosen.
- [docs/m3-cluster-checklist.md](docs/m3-cluster-checklist.md): a step-by-step first run on a new
  cluster.
