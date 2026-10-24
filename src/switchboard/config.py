"""User configuration (~/.config/switchboard/config.toml) and state locations (~/.switchboard)."""

from __future__ import annotations

import hashlib
import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

# macOS limits AF_UNIX socket paths to 104 bytes (SUN_LEN).
MAX_SOCKET_PATH = 100


@dataclass(frozen=True)
class Budget:
    """Advisory local budget: sums of *declared* reservations, not enforced limits."""

    cpus: int
    mem_gb: float
    max_admits_per_tick: int = 2
    # Command prefix for local tasks, e.g. ["nice", "-n", "10"]; see tools/bench_responsiveness.py.
    launch_prefix: tuple[str, ...] = ()

    @property
    def mem_mb(self) -> int:
        return int(self.mem_gb * 1024)


# Chosen from docs/benchmarks/2026-10-23-m5pro-ac.md: `nice -n 10` keeps full throughput with small
# interactive cost; taskpolicy background/utility QoS cut throughput to 0.13-0.66x.
NICE = ("nice", "-n", "10")
DEFAULT_BUDGETS = {
    "focus": Budget(cpus=4, mem_gb=12, max_admits_per_tick=1, launch_prefix=NICE),
    "away": Budget(cpus=12, mem_gb=28, max_admits_per_tick=2, launch_prefix=NICE),
}


@dataclass(frozen=True)
class LocalConfig:
    pueue_bin: str = "pueue"
    pueue_config: Path | None = None  # None: Pueue's own default config
    pueue_group: str = "switchboard"
    # Pueue never queues: the controller admits tasks, so the group limit only has to be high enough.
    pueue_parallel: int = 64
    run_on_battery: bool = False
    memory_pressure_gate: bool = True


@dataclass(frozen=True)
class DaemonConfig:
    local_interval_s: float = 5.0  # fallback poll while local work is pending or running
    idle_interval_s: float = 300.0  # heartbeat when there is no local work


@dataclass(frozen=True)
class Config:
    home: Path
    budgets: dict[str, Budget] = field(default_factory=lambda: dict(DEFAULT_BUDGETS))
    local: LocalConfig = field(default_factory=LocalConfig)
    daemon: DaemonConfig = field(default_factory=DaemonConfig)
    # Seconds a SUBMITTING launch must be positively absent before it is ABANDONED.
    submit_grace_s: float = 120.0
    # Seconds an attempt may be missing from its backend (with no final status) before it is LOST.
    lost_grace_s: float = 300.0

    @property
    def db_path(self) -> Path:
        return self.home / "state.db"

    @property
    def launch_dir(self) -> Path:
        return self.home / "launch"

    @property
    def run_dir(self) -> Path:
        return self.home / "run"

    @property
    def nudge_socket(self) -> Path:
        path = self.run_dir / "daemon.sock"
        if len(str(path)) <= MAX_SOCKET_PATH:
            return path
        digest = hashlib.sha256(str(self.home).encode()).hexdigest()[:10]
        return Path("/tmp") / f"switchboard-{os.getuid()}-{digest}.sock"

    @property
    def daemon_lock(self) -> Path:
        return self.run_dir / "daemon.lock"


def default_home() -> Path:
    return Path(os.environ.get("SWITCHBOARD_HOME", "~/.switchboard")).expanduser()


def default_config_path() -> Path:
    return Path(os.environ.get("SWITCHBOARD_CONFIG", "~/.config/switchboard/config.toml")).expanduser()


def load_config(path: Path | None = None, home: Path | None = None) -> Config:
    path = path or default_config_path()
    raw: dict = {}
    if path.exists():
        with path.open("rb") as fh:
            raw = tomllib.load(fh)
    budgets = dict(DEFAULT_BUDGETS)
    for mode, values in raw.get("budgets", {}).items():
        base = budgets.get(mode, DEFAULT_BUDGETS["focus"])
        budgets[mode] = Budget(
            cpus=int(values.get("cpus", base.cpus)),
            mem_gb=float(values.get("mem_gb", base.mem_gb)),
            max_admits_per_tick=int(values.get("max_admits_per_tick", base.max_admits_per_tick)),
            launch_prefix=tuple(values.get("launch_prefix", base.launch_prefix)),
        )
    controller = raw.get("controller", {})
    local = raw.get("local", {})
    daemon = raw.get("daemon", {})
    defaults = LocalConfig()
    return Config(
        home=home or Path(raw.get("home", default_home())).expanduser(),
        budgets=budgets,
        local=LocalConfig(
            pueue_bin=str(local.get("pueue_bin", defaults.pueue_bin)),
            pueue_config=Path(local["pueue_config"]).expanduser() if local.get("pueue_config") else None,
            pueue_group=str(local.get("pueue_group", defaults.pueue_group)),
            pueue_parallel=int(local.get("pueue_parallel", defaults.pueue_parallel)),
            run_on_battery=bool(local.get("run_on_battery", defaults.run_on_battery)),
            memory_pressure_gate=bool(local.get("memory_pressure_gate", defaults.memory_pressure_gate)),
        ),
        daemon=DaemonConfig(
            local_interval_s=float(daemon.get("local_interval_s", 5.0)),
            idle_interval_s=float(daemon.get("idle_interval_s", 300.0)),
        ),
        submit_grace_s=float(controller.get("submit_grace_s", 120.0)),
        lost_grace_s=float(controller.get("lost_grace_s", 300.0)),
    )
