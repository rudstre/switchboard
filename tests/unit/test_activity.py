"""Activity classifier on recorded-style sensor traces, and the pmset assertion parser."""

import os

import psutil

from switchboard.activity import ActivityConfig, ActivityState, Sample, classify
from switchboard.macos import assertion_holders

CFG = ActivityConfig()  # active < 2 min input, away after 10 min, 2 min hold, busy > 3 cores, quiet < 1


def run(trace, state=None):
    """trace: list of (t, Sample kwargs). Returns the state after each sample."""
    state = state or ActivityState()
    out = []
    for t, kw in trace:
        state = classify(state, Sample(t=t, **kw), CFG)
        out.append(state)
    return out


def every(seconds, until, **kw):
    return [(t, {k: (v(t) if callable(v) else v) for k, v in kw.items()}) for t in range(0, until, seconds)]


def test_typing_keeps_the_mac_active():
    states = run(every(15, 3600, idle_s=5.0, foreign_cores=0.5))
    assert {s.state for s in states} == {"ACTIVE"}


def test_video_call_without_input_stays_active():
    # 40 minutes of no keyboard input, but the call app keeps the display awake and uses the mic.
    states = run(every(15, 2400, idle_s=lambda t: float(t), display_holders=("zoom.us",),
                       microphone_holders=("zoom.us",), foreign_cores=1.2))
    assert {s.state for s in states} == {"ACTIVE"}
    assert "display kept awake by zoom.us" in states[-1].reason


def test_idle_compile_counts_as_busy_not_away():
    # no input for an hour while a build uses 6 cores: protect that work with the focus budget
    states = run(every(15, 3600, idle_s=lambda t: 600.0 + t, foreign_cores=6.0))
    assert {s.state for s in states} == {"ACTIVE"}
    assert "other work using 6.0 cores" in states[-1].reason


def test_locked_screen_becomes_away_after_the_hold():
    trace = [(0, {"idle_s": 3.0})] + [(t, {"screen_locked": True, "idle_s": float(t)}) for t in range(15, 300, 15)]
    states = run(trace)
    first_away = next(t for (t, _), s in zip(trace, states) if s.state == "AWAY")
    assert 135 <= first_away <= 150  # locked at t=15, AWAY once that held for 120 s


def test_brief_lock_does_not_switch():
    trace = ([(0, {"idle_s": 1.0})] + [(t, {"screen_locked": True}) for t in range(15, 90, 15)]
             + [(90, {"idle_s": 0.5})])
    assert {s.state for s in run(trace)} == {"ACTIVE"}


def test_any_input_returns_to_active_immediately():
    away = run([(0, {"screen_locked": True}), (200, {"screen_locked": True})])[-1]
    assert away.state == "AWAY"
    back = classify(away, Sample(t=210, idle_s=0.2), CFG)
    assert back.state == "ACTIVE" and back.since == 210


def test_no_input_alone_needs_ten_minutes_and_a_quiet_cpu():
    states = run(every(30, 1200, idle_s=lambda t: float(t), foreign_cores=0.3))
    times = [t for t in range(0, 1200, 30)]
    away_at = next(t for t, s in zip(times, states) if s.state == "AWAY")
    assert 600 + 120 <= away_at <= 600 + 150
    busy = run(every(30, 1200, idle_s=lambda t: float(t), foreign_cores=2.0))  # between quiet and busy
    assert {s.state for s in busy} == {"ACTIVE"}


def test_ignored_holders_do_not_count():
    trace = every(30, 1200, idle_s=lambda t: float(t), microphone_holders=("corespeechd",), foreign_cores=0.1)
    assert run(trace)[-1].state == "AWAY"


def test_state_round_trips_through_json():
    s = ActivityState("AWAY", 10.0, 5.0, "screen locked")
    assert ActivityState.from_dict(s.to_dict()) == s


PMSET = """Assertion status system-wide:
   PreventUserIdleDisplaySleep    1
Listed by owning process:
   pid 28923(Claude): [0x0006479a00019510] 00:01:30 NoIdleSleepAssertion named: "Electron"
   pid 777(zoom.us): [0x0006479a00019511] 00:30:00 PreventUserIdleDisplaySleep named: "Zoom meeting"
   pid 419(coreaudiod): [0x000641ba000184c9] 00:26:35 PreventUserIdleSystemSleep named: "com.apple.audio.ctx"
\tCreated for PID: {mypid}.
\tResources: audio-in F6EE8074-97C7-44C7-855B-A2EC2ABD98F8
   pid 419(coreaudiod): [0x000641ba00019318] 00:26:35 PreventUserIdleSystemSleep named: "speaker"
\tCreated for PID: 83773.
\tResources: audio-out BuiltInSpeakerDevice
Kernel Assertions: 0x100=MAGICWAKE
"""


def test_pmset_parser_finds_display_and_microphone_holders():
    display, mic = assertion_holders(PMSET.format(mypid=os.getpid()))
    assert display == ("zoom.us",)
    assert mic == (psutil.Process().name(),)  # the audio-in "Created for" process, not coreaudiod


import sys  # noqa: E402

import pytest  # noqa: E402


@pytest.mark.macos
@pytest.mark.skipif(sys.platform != "darwin", reason="macOS only")
def test_real_sensors_return_sane_values():
    from switchboard import macos
    sample = macos.MacSensors(macos.ForeignCpu(min_interval_s=0))(False)
    assert sample.idle_s is not None and sample.idle_s >= 0
    assert isinstance(sample.screen_locked, bool) and isinstance(sample.display_asleep, bool)
    assert isinstance(sample.display_holders, tuple) and isinstance(sample.microphone_holders, tuple)
