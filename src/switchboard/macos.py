"""The two macOS readings the MVP gates on: memory pressure and power source. No root needed."""

from __future__ import annotations

import subprocess
import sys
from typing import Callable

import psutil

PRESSURE_NAMES = {1: "NORMAL", 2: "WARN", 4: "CRITICAL"}


def memory_pressure_level() -> int | None:
    """kern.memorystatus_vm_pressure_level: 1 normal, 2 warn, 4 critical. None if unavailable."""
    if sys.platform != "darwin":
        return None
    try:
        out = subprocess.run(["sysctl", "-n", "kern.memorystatus_vm_pressure_level"], capture_output=True,
                             text=True, timeout=2)
        return int(out.stdout.strip()) if out.returncode == 0 else None
    except (OSError, ValueError, subprocess.TimeoutExpired):
        return None


def on_battery() -> bool | None:
    """True on battery, False on AC, None if there is no battery or it cannot be read."""
    try:
        battery = psutil.sensors_battery()
    except (OSError, RuntimeError, NotImplementedError):
        battery = None
    if battery is not None and battery.power_plugged is not None:
        return not battery.power_plugged
    try:
        out = subprocess.run(["pmset", "-g", "batt"], capture_output=True, text=True, timeout=2).stdout
    except (OSError, subprocess.TimeoutExpired):
        return None
    if "Battery Power" in out:
        return True
    if "AC Power" in out:
        return False
    return None


def memory_pressure_gate(read: Callable[[], int | None] = memory_pressure_level) -> Callable[[], str | None]:
    def gate() -> str | None:
        level = read()
        if level is not None and level >= 2:
            return f"memory pressure {PRESSURE_NAMES.get(level, level)}; no new local tasks"
        return None

    return gate


def battery_gate(read: Callable[[], bool | None] = on_battery) -> Callable[[], str | None]:
    def gate() -> str | None:
        return "on battery power; no new local tasks (set local.run_on_battery to allow)" if read() else None

    return gate
