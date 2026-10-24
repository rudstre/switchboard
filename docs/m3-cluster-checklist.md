# First run on the real cluster (M3 acceptance)

Everything in M3 is tested against `tools/mock_slurm`. Before trusting it with real experiments, run
these steps once, by hand, on the actual cluster.

## 1. Configure

In `~/.config/switchboard/config.toml`:

```toml
[hpc]
host = "mycluster"            # an alias from ~/.ssh/config
remote_root = "~/.switchboard"
python = "python3"            # needs >= 3.8 on login and compute nodes
remote_shell = "bash -lc"     # login shell, so sbatch/squeue are on PATH
```

## 2. Connect

Run the connect step and authenticate once (password or MFA). Later calls reuse that SSH session for 12 h:

```bash
switchboard hpc connect
```

Then:

```bash
switchboard hpc check
```

## 3. Probe (read-only)

```bash
switchboard hpc probe
```

Confirm each of these looks right:
- `MaxArraySize`
- `MaxSubmitJobs`
- `JobAcctGatherType`: if it is `jobacct_gather/none`, CPU and memory figures will show as unavailable.
- `python`

## 4. Capture real accounting output (for the parser tests)

After any finished array job of yours, run:

```bash
sacct -n -P --units=K -j <ARRAYJOBID> -o JobID,JobIDRaw,State,ExitCode,Elapsed,TotalCPU,MaxRSS
```

- Save the output to `tests/fixtures/sacct-real.txt`. The parser tests currently use documented formats, not real cluster output.
- Also capture the output for a pending array, which shows lines like `123_[2-5]`.

## 5. Smoke test: 3 synthetic tasks

- Use a spec with `backends.hpc`. Its `workdir` must contain `tools/synth.py`, or the command must point to a copy of it.
- Use `slurm.time: "00:05:00"` and a small or test partition.

```bash
switchboard submit smoke.yaml --backend hpc
switchboard tick
switchboard status
switchboard explain smoke/0
```

**Pass criteria:**
- [ ] one `sbatch` array of 3 elements;
- [ ] all 3 tasks `SUCCEEDED`;
- [ ] `results` paths are on the cluster's filesystem;
- [ ] `explain` shows wall, CPU and peak memory figures.

## 6. Escape hatch

Run:

```bash
switchboard export-slurm smoke.yaml --out ./smoke-export
```

Then copy the directory to the cluster and, inside it, run `sbatch job.sbatch`. The tasks should run without switchboard involved.
