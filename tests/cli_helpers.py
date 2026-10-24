"""Thin access to CLI rendering helpers for tests."""

from switchboard.cli import _exp_status


def exp_status(ctl, name: str) -> dict:
    return _exp_status(ctl, ctl.find_experiment(name))
