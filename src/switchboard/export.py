"""`switchboard export-slurm`: a self-contained launch directory, independent of the registry.

The directory holds the runner, a manifest and job.sbatch with relative paths, so it can be copied to
the cluster and submitted by hand (`cd DIR && sbatch job.sbatch`) when the controller is unavailable.
Outputs go to the spec's backends.hpc.output_root (~ and $USER are expanded on the cluster).
"""

from __future__ import annotations

import json
from pathlib import Path

from .controller import attempt_spec, dir_name, parse_selection, slurm_time
from .executors.base import AttemptPayload, LaunchUnit
from .executors.slurm import render_job, runner_sources
from .ids import new_id
from .spec import LoadedSpec, SpecError, expand


def export_slurm(loaded: LoadedSpec, out_dir: Path, python: str = "python3", tasks: str | None = None,
                 code_id: str | None = None) -> Path:
    spec = loaded.spec
    if "hpc" not in spec.backends:
        raise SpecError("export-slurm needs a backends.hpc section (workdir, output_root, setup, slurm)")
    b = spec.backends["hpc"]
    defs = expand(loaded)
    selected = parse_selection(tasks, len(defs))
    launch_id = new_id("x")
    payloads = []
    for d in defs:
        if d.idx not in selected:
            continue
        attempt_id = new_id("a")
        attempt_dir = f"{b.output_root.rstrip('/')}/{dir_name(spec.experiment, 1)}/{d.key}/{attempt_id}"
        payloads.append(AttemptPayload(attempt_id, attempt_spec(
            spec, spec.experiment, d.key, d.idx, d.params, d.seed, spec.resources.cpus, attempt_id, attempt_dir,
            b.workdir, b.setup, code_id, None, None)))
    s = b.slurm
    unit = LaunchUnit(launch_id, tuple(payloads), options={
        "cpus": spec.resources.cpus, "mem_mb": spec.resources.mem_mb,
        "time": slurm_time(s.time if s else None, spec.resources.runtime_min * 60 if spec.resources.runtime_min else None),
        "partition": s.partition if s else None, "account": s.account if s else None,
        "array_throttle": s.array_throttle if s else None, "extra": list(s.extra) if s else []})
    out_dir.mkdir(parents=True, exist_ok=False)
    (out_dir / "runner").mkdir()
    for name, data in runner_sources().items():
        (out_dir / "runner" / name).write_bytes(data)
    (out_dir / "manifest.jsonl").write_text("".join(json.dumps(p.spec) + "\n" for p in payloads))
    (out_dir / "job.sbatch").write_text(render_job(unit, None, None, python))
    return out_dir
