"""Experiment specification: validation, task expansion, identity hashing and command templating.

The spec is generic: it names a command, parameters, resources and expected outputs. Nothing here
knows about any particular scientific project.
"""

from __future__ import annotations

import itertools
import json
import math
import re
import string
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from . import SHIM_MAJOR
from .ids import canonical_json, sha256_hex

BackendName = Literal["local", "hpc"]
RetryReason = Literal["nonzero", "missing_outputs", "lost", "runner_died", "node_fail", "timeout"]

TEMPLATE_FIELDS = {"seed", "outdir", "params_file", "task_key", "attempt_id", "experiment", "idx", "param", "cpus"}


class _SpecLoader(yaml.SafeLoader):
    """SafeLoader with YAML 1.2 booleans: only true/false, so keys like `on:` stay strings."""


_SpecLoader.yaml_implicit_resolvers = {
    ch: [(tag, rx) for tag, rx in resolvers if tag != "tag:yaml.org,2002:bool"]
    for ch, resolvers in yaml.SafeLoader.yaml_implicit_resolvers.items()
}
_SpecLoader.add_implicit_resolver(
    "tag:yaml.org,2002:bool", re.compile(r"^(?:true|True|TRUE|false|False|FALSE)$"), list("tTfF")
)


class SpecError(ValueError):
    """The spec is invalid; the message is meant for the user."""


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Resources(_Model):
    cpus: int = Field(1, ge=1)
    mem_gb: float = Field(1.0, gt=0)
    runtime_min: float | None = Field(None, gt=0)

    @property
    def mem_mb(self) -> int:
        return int(math.ceil(self.mem_gb * 1024))


class Outputs(_Model):
    expect: list[str] = []


class GitFingerprint(_Model):
    paths: list[str] = ["."]


class FingerprintSpec(_Model):
    git: GitFingerprint | None = None
    command: str | None = None

    @model_validator(mode="after")
    def _exactly_one(self) -> FingerprintSpec:
        if (self.git is None) == (self.command is None):
            raise ValueError("a fingerprint needs exactly one provider: 'git' or 'command'")
        return self


class Identity(_Model):
    code: FingerprintSpec | None = None
    env: FingerprintSpec | None = None

    @field_validator("env")
    @classmethod
    def _env_is_command(cls, v: FingerprintSpec | None) -> FingerprintSpec | None:
        if v is not None and v.git is not None:
            raise ValueError("environment identity must use the 'command' provider (git describes code)")
        return v


class Retries(_Model):
    max: int = Field(0, ge=0)
    on: list[RetryReason] = ["nonzero", "lost", "runner_died"]


class SlurmOptions(_Model):
    partition: str | None = None
    account: str | None = None
    time: str | None = None
    array_throttle: int | None = Field(None, ge=1)
    extra: list[str] = []


class BackendSpec(_Model):
    workdir: str
    output_root: str
    setup: str | None = None
    slurm: SlurmOptions | None = None


class ParamsSpec(_Model):
    grid: dict[str, Any] = {}
    zip: dict[str, list[Any]] = {}
    seeds: list[int] | None = None
    manifest: str | None = None

    @field_validator("grid")
    @classmethod
    def _grid_values(cls, grid: dict[str, Any]) -> dict[str, Any]:
        for name, value in grid.items():
            _axis_values(name, value)  # raises on malformed axes
        return grid


class ExperimentSpec(_Model):
    experiment: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
    command: list[str] | str
    params: ParamsSpec = ParamsSpec()
    env: dict[str, str] = {}
    resources: Resources = Resources()
    outputs: Outputs = Outputs()
    identity: Identity | None = None
    interruptible: bool = False
    retries: Retries = Retries()
    compat: Literal["slurm-array"] | None = None
    backends: dict[BackendName, BackendSpec] = {}


@dataclass(frozen=True)
class TaskDef:
    idx: int
    key: str
    params: dict[str, Any]
    seed: int | None


@dataclass(frozen=True)
class LoadedSpec:
    spec: ExperimentSpec
    source_text: str
    base_dir: Path
    manifest_rows: list[dict[str, Any]] | None
    manifest_sha256: str | None

    def identity_fields(self) -> dict[str, Any]:
        """Fields whose change invalidates results. Resources, retries, placement and paths are excluded."""
        s = self.spec
        return {
            "shim_major": SHIM_MAJOR,
            "command": s.command,
            "params": s.params.model_dump(exclude={"manifest"}),
            "manifest_sha256": self.manifest_sha256,
            "env": s.env,
            "outputs": s.outputs.expect,
            "identity": s.identity.model_dump(exclude_none=True) if s.identity else None,
            "compat": s.compat,
        }

    def spec_hash(self) -> str:
        return "spec:" + sha256_hex(canonical_json(self.identity_fields()))


def load_spec(path: str | Path) -> LoadedSpec:
    path = Path(path)
    text = path.read_text()
    return parse_spec(text, base_dir=path.resolve().parent)


def parse_spec(text: str, base_dir: Path) -> LoadedSpec:
    try:
        raw = yaml.load(text, Loader=_SpecLoader)
    except yaml.YAMLError as exc:
        raise SpecError(f"spec is not valid YAML: {exc}") from exc
    if not isinstance(raw, dict):
        raise SpecError("spec must be a YAML mapping")
    try:
        spec = ExperimentSpec.model_validate(raw)
    except ValidationError as exc:
        raise SpecError(_format_validation_error(exc)) from exc

    rows = digest = None
    if spec.params.manifest:
        manifest_path = (base_dir / spec.params.manifest).expanduser()
        if not manifest_path.exists():
            raise SpecError(f"params.manifest not found: {manifest_path}")
        data = manifest_path.read_bytes()
        digest = sha256_hex(data)
        rows = []
        for lineno, line in enumerate(data.decode().splitlines(), 1):
            if line.strip():
                row = json.loads(line)
                if not isinstance(row, dict):
                    raise SpecError(f"{manifest_path}:{lineno}: each manifest line must be a JSON object")
                rows.append(row)
    loaded = LoadedSpec(spec, text, base_dir, rows, digest)
    _check_template(loaded)
    return loaded


def expand(loaded: LoadedSpec) -> list[TaskDef]:
    """Expand the spec into task definitions. Keys are stable for identical params and seeds."""
    combos = list(_param_rows(loaded))
    width = max(5, len(str(max(len(combos) - 1, 0))))
    tasks = []
    for idx, (params, seed) in enumerate(combos):
        digest = sha256_hex(canonical_json({"params": params, "seed": seed}))[:8]
        tasks.append(TaskDef(idx=idx, key=f"{idx:0{width}d}-{digest}", params=params, seed=seed))
    return tasks


def render_command(command: list[str] | str, ctx: dict[str, Any]) -> list[str] | str:
    fmt = string.Formatter()
    mapping = dict(ctx)
    mapping["param"] = SimpleNamespace(**ctx.get("param", {}))
    if isinstance(command, str):
        return fmt.vformat(command, (), mapping)
    return [fmt.vformat(part, (), mapping) for part in command]


# --- internals -------------------------------------------------------------------------------


def _axis_values(name: str, value: Any) -> list[Any]:
    if isinstance(value, list):
        if not value:
            raise ValueError(f"grid axis '{name}' is empty")
        return value
    if isinstance(value, dict) and len(value) == 1:
        (kind, args), = value.items()
        if kind == "linspace" and isinstance(args, list) and len(args) == 3:
            start, stop, num = float(args[0]), float(args[1]), int(args[2])
            if num < 1:
                raise ValueError(f"grid axis '{name}': linspace needs num >= 1")
            if num == 1:
                return [start]
            values = [start + (stop - start) * i / (num - 1) for i in range(num)]
            values[-1] = stop
            return values
        if kind == "range" and isinstance(args, list) and len(args) in (2, 3):
            start, stop = args[0], args[1]
            step = args[2] if len(args) == 3 else 1
            if step == 0:
                raise ValueError(f"grid axis '{name}': range step must be non-zero")
            count = max(0, math.ceil((stop - start) / step))
            if count == 0:
                raise ValueError(f"grid axis '{name}': range is empty")
            return [start + i * step for i in range(count)]
    raise ValueError(f"grid axis '{name}' must be a list, {{linspace: [start, stop, num]}} or {{range: [start, stop, step]}}")


def _param_rows(loaded: LoadedSpec):
    p = loaded.spec.params
    if loaded.manifest_rows is not None:
        if p.grid or p.zip:
            raise SpecError("params.manifest cannot be combined with params.grid or params.zip")
        base = [dict(row) for row in loaded.manifest_rows]
    else:
        overlap = set(p.grid) & set(p.zip)
        if overlap:
            raise SpecError(f"parameters defined in both grid and zip: {sorted(overlap)}")
        lengths = {len(v) for v in p.zip.values()}
        if len(lengths) > 1:
            raise SpecError(f"params.zip lists must have equal lengths, got {sorted(lengths)}")
        zipped = [dict(zip(p.zip, vals)) for vals in zip(*p.zip.values())] if p.zip else [{}]
        axes = [_axis_values(n, v) for n, v in p.grid.items()]
        base = [{**dict(zip(p.grid, combo)), **z} for combo in itertools.product(*axes) for z in zipped]

    for row in base:
        if p.seeds is not None:
            if "seed" in row:
                raise SpecError("a parameter row defines 'seed' and params.seeds is also set; use one")
            for seed in p.seeds:
                yield row, seed
        else:
            seed = row.pop("seed", None)
            yield row, seed


def _check_template(loaded: LoadedSpec) -> None:
    parts = [loaded.spec.command] if isinstance(loaded.spec.command, str) else loaded.spec.command
    rows = list(_param_rows(loaded))
    if not rows:
        raise SpecError("the spec expands to zero tasks")
    param_names = set(rows[0][0])
    fmt = string.Formatter()
    for part in parts:
        try:
            fields = [f for _, f, _, _ in fmt.parse(part) if f is not None]
        except ValueError as exc:
            raise SpecError(f"bad placeholder in command {part!r}: {exc} (use '{{{{' for a literal brace)") from exc
        for field in fields:
            root, _, attr = field.partition(".")
            if root not in TEMPLATE_FIELDS:
                raise SpecError(f"unknown placeholder {{{field}}}; allowed: {sorted(TEMPLATE_FIELDS)}")
            if root == "param" and attr not in param_names:
                raise SpecError(f"{{{field}}} does not name a parameter; parameters are {sorted(param_names)}")
            if root == "seed" and any(seed is None for _, seed in rows):
                raise SpecError("command uses {seed} but some tasks have no seed; set params.seeds")


def _format_validation_error(exc: ValidationError) -> str:
    lines = ["invalid spec:"]
    for err in exc.errors():
        loc = ".".join(str(x) for x in err["loc"]) or "(root)"
        lines.append(f"  {loc}: {err['msg']}")
    return "\n".join(lines)
