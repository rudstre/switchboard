"""Is the Mac being used? A pure classifier with hysteresis over sensor samples (see macos.py for sensors).

ACTIVE (-> focus budget) as soon as any of:
  * recent keyboard/mouse input (idle < active_idle_s) on an unlocked screen
  * an app is keeping the display awake (video, presentation, call) or the microphone is in use
  * other work is keeping the CPU busy (foreign load > busy_cores): protect it even if you are away
AWAY (-> away budget) only after away evidence has held for away_hold_s:
  * screen locked or display asleep, or
  * no input for away_idle_s, with no display/microphone holders and a quiet CPU (< quiet_cores)
Anything in between keeps the previous state. Absence of keyboard input alone never means away.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field


@dataclass(frozen=True)
class ActivityConfig:
    active_idle_s: float = 120.0
    away_idle_s: float = 600.0
    away_hold_s: float = 120.0
    busy_cores: float = 3.0
    quiet_cores: float = 1.0
    use_microphone: bool = True
    # Assertion holders to ignore (process names). corespeechd ("Hey Siri") holds the microphone
    # permanently; add e.g. a browser that keeps a wake lock forever.
    ignore_holders: tuple[str, ...] = ("corespeechd",)


@dataclass(frozen=True)
class Sample:
    t: float
    idle_s: float | None = None  # seconds since last keyboard/mouse input
    screen_locked: bool = False
    display_asleep: bool = False
    display_holders: tuple[str, ...] = ()  # processes preventing display sleep
    microphone_holders: tuple[str, ...] = ()  # processes using audio input
    foreign_cores: float | None = None  # CPU load from everything except switchboard's tasks


@dataclass(frozen=True)
class ActivityState:
    state: str = "ACTIVE"  # ACTIVE | AWAY
    since: float = 0.0
    away_evidence_since: float | None = None
    reason: str = "no observations yet"

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict | None) -> ActivityState:
        return ActivityState(**d) if d else ActivityState()


@dataclass
class Evidence:
    active: list[str] = field(default_factory=list)
    away: list[str] = field(default_factory=list)


def evidence(s: Sample, cfg: ActivityConfig) -> Evidence:
    ev = Evidence()
    ignore = set(cfg.ignore_holders)
    display = [h for h in s.display_holders if h not in ignore]
    mic = [h for h in s.microphone_holders if h not in ignore] if cfg.use_microphone else []
    if s.screen_locked:
        ev.away.append("screen locked")
    elif s.display_asleep:
        ev.away.append("display asleep")
    else:
        if s.idle_s is not None and s.idle_s < cfg.active_idle_s:
            ev.active.append(f"input {s.idle_s:.0f}s ago")
        if display:
            ev.active.append(f"display kept awake by {', '.join(sorted(set(display)))}")
        if mic:
            ev.active.append(f"microphone in use by {', '.join(sorted(set(mic)))}")
    if s.foreign_cores is not None and s.foreign_cores > cfg.busy_cores:
        ev.active.append(f"other work using {s.foreign_cores:.1f} cores")
    if not ev.active and not ev.away and s.idle_s is not None and s.idle_s >= cfg.away_idle_s:
        if s.foreign_cores is None or s.foreign_cores < cfg.quiet_cores:
            ev.away.append(f"no input for {s.idle_s / 60:.0f} min")
    return ev


def classify(prev: ActivityState, s: Sample, cfg: ActivityConfig) -> ActivityState:
    ev = evidence(s, cfg)
    if ev.active:
        reason = "; ".join(ev.active)
        if prev.state == "ACTIVE":
            return ActivityState("ACTIVE", prev.since or s.t, None, reason)
        return ActivityState("ACTIVE", s.t, None, reason)
    if ev.away:
        started = prev.away_evidence_since if prev.away_evidence_since is not None else s.t
        reason = "; ".join(ev.away)
        if prev.state == "AWAY":
            return ActivityState("AWAY", prev.since, started, reason)
        held = s.t - started
        if held >= cfg.away_hold_s:
            return ActivityState("AWAY", s.t, started, f"{reason} (for {held / 60:.0f} min)")
        return ActivityState("ACTIVE", prev.since or s.t, started,
                             f"{reason}; away in {cfg.away_hold_s - held:.0f}s if it holds")
    # In between (e.g. 2-10 min without input): keep the previous state, reset the away timer.
    return ActivityState(prev.state, prev.since or s.t, None,
                         f"{prev.state.lower()} (unchanged): no input for {(s.idle_s or 0) / 60:.0f} min")


PROFILE = {"ACTIVE": "focus", "AWAY": "away"}
