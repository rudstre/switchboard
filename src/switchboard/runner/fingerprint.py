"""Read-only code/environment fingerprints. Stdlib only, Python >= 3.8: shipped next to shim.py.

Providers:
  git      HEAD + hash of `git diff --binary HEAD -- <paths>` + hash of the *contents* of untracked,
           non-ignored files under <paths>. Any edit to tracked or untracked content changes the id.
  command  sha256 of the stdout of a shell command (run after the backend's setup snippet).

CLI (used for remote evaluation over SSH):
  python3 fingerprint.py '<provider json>' --workdir DIR [--setup SNIPPET]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys


class FingerprintError(RuntimeError):
    pass


def platform_tag() -> str:
    system = {"Darwin": "osx", "Linux": "linux"}.get(platform.system(), platform.system().lower())
    machine = platform.machine().lower()
    arch = {"x86_64": "64", "amd64": "64", "arm64": "arm64", "aarch64": "aarch64"}.get(machine, machine)
    return "%s-%s" % (system, arch)


def shell() -> str:
    return shutil.which("bash") or "/bin/sh"


def evaluate(provider: dict, workdir: str, setup: str | None = None) -> str:
    workdir = os.path.expanduser(workdir)
    if provider.get("git") is not None:
        return git_fingerprint(workdir, provider["git"].get("paths") or ["."])
    if provider.get("command") is not None:
        return command_fingerprint(workdir, provider["command"], setup)
    raise FingerprintError("unknown fingerprint provider: %r" % (provider,))


def git_fingerprint(workdir: str, paths: list) -> str:
    head = _git(workdir, ["rev-parse", "--verify", "HEAD"]).strip()
    diff = _git(workdir, ["diff", "--binary", "--no-ext-diff", "--no-color", "--no-textconv", "HEAD", "--", *paths])
    untracked = _git(workdir, ["ls-files", "-z", "--others", "--exclude-standard", "--", *paths])
    h = hashlib.sha256()
    h.update(b"head\0" + head + b"\0")
    h.update(b"diff\0" + hashlib.sha256(diff).digest())
    for rel in sorted(p for p in untracked.split(b"\0") if p):
        full = os.path.join(os.fsencode(workdir), rel)
        h.update(b"untracked\0" + rel + b"\0")
        if os.path.islink(full):
            h.update(b"link\0" + os.fsencode(os.readlink(full)))
        elif os.path.isfile(full):
            with open(full, "rb") as fh:
                h.update(hashlib.sha256(fh.read()).digest())
    return "git:" + h.hexdigest()


def command_fingerprint(workdir: str, command: str, setup: str | None = None) -> str:
    script = "set -e\n" + (setup + "\n" if setup else "") + command
    try:
        proc = subprocess.run(
            [shell(), "-c", script], cwd=workdir, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=300
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise FingerprintError("fingerprint command failed to run in %s: %s" % (workdir, exc)) from exc
    if proc.returncode != 0:
        raise FingerprintError(
            "fingerprint command exited %d in %s: %s"
            % (proc.returncode, workdir, proc.stderr.decode(errors="replace").strip()[-500:])
        )
    return "cmd:" + hashlib.sha256(proc.stdout).hexdigest()


def _git(workdir: str, args: list) -> bytes:
    try:
        proc = subprocess.run(
            ["git", "-C", workdir, *args], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise FingerprintError("git failed in %s: %s" % (workdir, exc)) from exc
    if proc.returncode != 0:
        raise FingerprintError(
            "git %s failed in %s: %s" % (args[0], workdir, proc.stderr.decode(errors="replace").strip())
        )
    return proc.stdout


def main(argv: list | None = None) -> int:
    parser = argparse.ArgumentParser(description="Print a switchboard fingerprint")
    parser.add_argument("provider", help="provider JSON, e.g. '{\"git\": {\"paths\": [\".\"]}}'")
    parser.add_argument("--workdir", required=True)
    parser.add_argument("--setup")
    args = parser.parse_args(argv)
    try:
        print(evaluate(json.loads(args.provider), args.workdir, args.setup))
    except FingerprintError as exc:
        print("error: %s" % exc, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
