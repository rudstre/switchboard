"""Slurm output parsing. Formats follow sacct/squeue documentation; replace with fixtures captured
from the real cluster before relying on them (see docs/m3-cluster-checklist.md)."""

import pytest

from switchboard.executors.base import LaunchUnit
from switchboard.executors.slurm import (
    aggregate_sacct,
    observation_from_account,
    parse_duration,
    parse_job_id,
    parse_mem_mb,
    render_job,
)

SACCT = """\
4242_0|4243|COMPLETED|0:0|00:01:05|01:58.500|
4242_0.batch|4243.batch|COMPLETED|0:0|00:01:05|01:58.400|812345K
4242_0.extern|4243.extern|COMPLETED|0:0|00:01:05|00:00.001|1024K
4242_0.0|4243.0|COMPLETED|0:0|00:01:00|00:30.000|1.5G
4242_1|4244|CANCELLED by 501|0:15|00:00:07|00:06.000|
4242_1.batch|4244.batch|CANCELLED|0:15|00:00:07|00:06.000|20480K
4242_2|4245|OUT_OF_MEMORY|0:125|00:00:30|00:29.000|
4242_[3-5,7%2]|4242|PENDING|0:0|00:00:00|00:00:00|
"""


def test_durations():
    assert parse_duration("00:01:05") == 65
    assert parse_duration("01:58.500") == pytest.approx(118.5)
    assert parse_duration("1-02:03:04") == 86400 + 7384
    assert parse_duration("") is None and parse_duration("INVALID") is None


def test_memory_units():
    assert parse_mem_mb("1024K") == 1
    assert parse_mem_mb("1.5G") == 1536
    assert parse_mem_mb("800M") == 800
    assert parse_mem_mb("") is None


def test_job_ids_including_collapsed_pending_ranges():
    assert parse_job_id("4242_0.batch") == ("4242", [0], "batch")
    assert parse_job_id("4242_[3-5,7%2]") == ("4242", [3, 4, 5, 7], None)
    assert parse_job_id("99") == ("99", None, None)


def test_step_level_aggregation():
    acct = aggregate_sacct(SACCT)
    e0 = acct[("4242", 0)]
    assert e0.state == "COMPLETED" and e0.exit_code == 0 and e0.wall_s == 65
    assert e0.cpu_s == pytest.approx(118.4 + 30.0)  # batch + srun step; extern excluded
    assert e0.peak_rss_mb == 1536  # max over steps, not the (empty) allocation line
    assert acct[("4242", 1)].state == "CANCELLED"
    assert {i for (_, i) in acct} == {0, 1, 2, 3, 4, 5, 7}
    assert acct[("4242", 4)].state == "PENDING"


def test_observations_carry_slurm_reasons():
    acct = aggregate_sacct(SACCT)
    assert observation_from_account(acct[("4242", 0)]).state == "DONE"
    assert observation_from_account(acct[("4242", 1)]).reason == "cancelled_externally"
    assert observation_from_account(acct[("4242", 2)]).reason == "oom"
    assert observation_from_account(acct[("4242", 3)]).state == "QUEUED"


def test_accounting_disabled_reports_unavailable_not_zero():
    text = "7_0|8|COMPLETED|0:0|00:00:10||\n7_0.batch|8.batch|COMPLETED|0:0|00:00:10||\n"
    e = aggregate_sacct(text, accounting=False)[("7", 0)]
    assert e.cpu_s is None and e.peak_rss_mb is None and e.wall_s == 10


def test_job_script():
    unit = LaunchUnit("l-1", tuple(range(3)), options={"cpus": 4, "mem_mb": 6144, "time": "01:00:00",
                                                      "partition": "short", "array_throttle": 2,
                                                      "extra": ["--constraint=intel"]})
    script = render_job(unit, "/home/u/.switchboard/runner/abc", "/home/u/.switchboard/launch/l-1", "python3")
    for line in ["#SBATCH --job-name=sb-l-1", "#SBATCH --array=0-2%2", "#SBATCH --cpus-per-task=4",
                 "#SBATCH --mem=6144M", "#SBATCH --no-requeue", "#SBATCH --time=01:00:00",
                 "#SBATCH --partition=short", "#SBATCH --constraint=intel"]:
        assert line in script
    assert 'array.py /home/u/.switchboard/launch/l-1/manifest.jsonl "$SLURM_ARRAY_TASK_ID"' in script


def test_real_sacct_output_from_swc():
    """Captured from the SWC cluster (Slurm 23.11.4, jobacct_gather/cgroup) after the M3 smoke test.

    Notes: extern steps report MaxRSS as a bare 0; the array job id equals the *last* element's raw id;
    cgroup sampling barely registers 20-second jobs (48K-240K vs ~17 MB measured by the shim).
    """
    from pathlib import Path

    text = (Path(__file__).resolve().parents[1] / "fixtures" / "sacct-swc-completed.txt").read_text()
    acct = aggregate_sacct(text)
    assert sorted(acct) == [("3827293", 0), ("3827293", 1), ("3827293", 2)]
    for e in acct.values():
        obs = observation_from_account(e)
        assert (obs.state, obs.exit_code, obs.reason) == ("DONE", 0, None)
        assert e.wall_s == 20 and 0 < e.cpu_s < 1 and e.peak_rss_mb is not None
    assert acct[("3827293", 2)].peak_rss_mb == pytest.approx(240 / 1024)
