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


# ---- activity sensors (M5) --------------------------------------------------------------------

import ctypes  # noqa: E402
import re  # noqa: E402
import time  # noqa: E402

from .activity import Sample  # noqa: E402

_CG = "/System/Library/Frameworks/CoreGraphics.framework/CoreGraphics"
_CF = "/System/Library/Frameworks/CoreFoundation.framework/CoreFoundation"
_libs: dict = {}


def _frameworks():
    if not _libs and sys.platform == "darwin":
        cg, cf = ctypes.CDLL(_CG), ctypes.CDLL(_CF)
        cg.CGEventSourceSecondsSinceLastEventType.restype = ctypes.c_double
        cg.CGEventSourceSecondsSinceLastEventType.argtypes = [ctypes.c_int32, ctypes.c_uint32]
        cg.CGMainDisplayID.restype = ctypes.c_uint32
        cg.CGDisplayIsAsleep.restype = ctypes.c_uint32
        cg.CGDisplayIsAsleep.argtypes = [ctypes.c_uint32]
        cg.CGSessionCopyCurrentDictionary.restype = ctypes.c_void_p
        cf.CFStringCreateWithCString.restype = ctypes.c_void_p
        cf.CFStringCreateWithCString.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_uint32]
        cf.CFDictionaryGetValue.restype = ctypes.c_void_p
        cf.CFDictionaryGetValue.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
        cf.CFBooleanGetValue.restype = ctypes.c_bool
        cf.CFBooleanGetValue.argtypes = [ctypes.c_void_p]
        cf.CFRelease.argtypes = [ctypes.c_void_p]
        _libs.update(cg=cg, cf=cf)
    return _libs


def idle_seconds() -> float | None:
    """Seconds since the last keyboard/mouse/trackpad event in this login session."""
    libs = _frameworks()
    if not libs:
        return None
    # kCGEventSourceStateHIDSystemState = 1, kCGAnyInputEventType = ~0
    return float(libs["cg"].CGEventSourceSecondsSinceLastEventType(1, 0xFFFFFFFF))


def display_asleep() -> bool:
    libs = _frameworks()
    return bool(libs and libs["cg"].CGDisplayIsAsleep(libs["cg"].CGMainDisplayID()))


def screen_locked() -> bool:
    libs = _frameworks()
    if not libs:
        return False
    cg, cf = libs["cg"], libs["cf"]
    session = cg.CGSessionCopyCurrentDictionary()
    if not session:
        return False  # no GUI session (e.g. over ssh): not something we can judge
    key = cf.CFStringCreateWithCString(None, b"CGSSessionScreenIsLocked", 0x08000100)
    try:
        value = cf.CFDictionaryGetValue(session, key)
        return bool(value and cf.CFBooleanGetValue(value))
    finally:
        cf.CFRelease(key)
        cf.CFRelease(session)


_ASSERTION = re.compile(r"^\s+pid (\d+)\((.*?)\): \[0x[0-9a-f]+\] \S+ (\w+) named: ")


def assertion_holders(text: str | None = None) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """(display-sleep holders, microphone users) from `pmset -g assertions`, as process names."""
    if text is None:
        try:
            text = subprocess.run(["pmset", "-g", "assertions"], capture_output=True, text=True, timeout=3).stdout
        except (OSError, subprocess.TimeoutExpired):
            return (), ()
    display, mic = [], []
    current: dict | None = None
    blocks = []
    for line in text.splitlines():
        m = _ASSERTION.match(line)
        if m:
            current = {"pid": int(m.group(1)), "name": m.group(2), "type": m.group(3), "for": None, "res": ""}
            blocks.append(current)
        elif current is not None and line.startswith("\t"):
            detail = line.strip()
            if detail.startswith("Created for PID:"):
                current["for"] = int(re.sub(r"\D", "", detail) or 0)
            elif detail.startswith("Resources:"):
                current["res"] += detail
        elif not line.startswith("\t"):
            current = None
    for b in blocks:
        if b["type"] == "PreventUserIdleDisplaySleep":
            display.append(_process_name(b["for"]) if b["for"] else b["name"])
        elif "audio-in" in b["res"]:
            mic.append(_process_name(b["for"]) if b["for"] else b["name"])
    return tuple(display), tuple(mic)


def _process_name(pid: int) -> str:
    try:
        return psutil.Process(pid).name()
    except psutil.Error:
        return f"pid {pid}"


class ForeignCpu:
    """CPU load (cores) from everything except switchboard's own task trees. Rate-limited, since a
    full process scan costs ~50 ms; returns the last value between samples, None until two exist."""

    def __init__(self, min_interval_s: float = 30.0, marker: str = "runner/shim.py") -> None:
        self.min_interval_s = min_interval_s
        self.marker = marker
        self._last: tuple[float, float, float] | None = None  # (t, system busy s, ours s)
        self._value: float | None = None

    def sample(self, ours_running: bool) -> float | None:
        now = time.monotonic()
        if self._last and now - self._last[0] < self.min_interval_s:
            return self._value
        t = psutil.cpu_times()
        busy = t.user + t.system + getattr(t, "nice", 0.0)
        ours = self._ours() if ours_running else 0.0
        if self._last:
            dt = now - self._last[0]
            foreign = max(0.0, (busy - self._last[1]) - max(0.0, ours - self._last[2]))
            self._value = foreign / dt if dt > 0 else self._value
        self._last = (now, busy, ours)
        return self._value

    def _ours(self) -> float:
        procs = {}
        children: dict[int, list[int]] = {}
        for p in psutil.process_iter(["pid", "ppid", "cmdline", "cpu_times"]):
            procs[p.info["pid"]] = p.info
            children.setdefault(p.info["ppid"], []).append(p.info["pid"])
        roots = [pid for pid, info in procs.items() if any(self.marker in part for part in (info["cmdline"] or []))]
        total, stack, seen = 0.0, list(roots), set()
        while stack:
            pid = stack.pop()
            if pid in seen or pid not in procs:
                continue
            seen.add(pid)
            ct = procs[pid]["cpu_times"]
            if ct is not None:
                total += ct.user + ct.system
            stack.extend(children.get(pid, []))
        return total


class MacSensors:
    """One Sample per call; cheap except for the rate-limited foreign-CPU scan."""

    def __init__(self, cpu: ForeignCpu | None = None) -> None:
        self.cpu = cpu or ForeignCpu()

    def __call__(self, local_tasks_running: bool) -> Sample:
        display, mic = assertion_holders()
        return Sample(
            t=time.time(), idle_s=idle_seconds(), screen_locked=screen_locked(), display_asleep=display_asleep(),
            display_holders=display, microphone_holders=mic, foreign_cores=self.cpu.sample(local_tasks_running),
        )
