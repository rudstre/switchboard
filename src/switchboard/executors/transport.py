"""How the controller reaches the cluster: SSH in production, a local shell for the mock-Slurm tests.

Both run *shell command strings* and move small files with the same stdlib helper scripts, so the
Slurm executor's code path is identical in tests and on the real cluster.

Failure semantics:
  * TransportDown: the command certainly did not run (cannot connect, or auth needed). Safe to retry.
  * TransportLost: the connection broke or timed out mid-command. The command *may* have run.
"""

from __future__ import annotations

import base64
import json
import os
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


class TransportDown(RuntimeError):
    pass


class TransportLost(RuntimeError):
    pass


@dataclass(frozen=True)
class Result:
    returncode: int
    stdout: str
    stderr: str


# Remote helpers: stdlib only, Python >= 3.6. Payloads travel on stdin as JSON.
_PUT = r"""
import base64, json, os, sys
for f in json.load(sys.stdin)["files"]:
    path = os.path.expandvars(os.path.expanduser(f["path"]))
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp.%d" % os.getpid()
    with open(tmp, "wb") as fh:
        fh.write(base64.b64decode(f["data"]))
    os.chmod(tmp, f.get("mode", 0o644))
    os.replace(tmp, path)
print("ok")
"""

_READ = r"""
import json, os, sys
req = json.load(sys.stdin)
out = {}
for p in req["paths"]:
    path = os.path.expandvars(os.path.expanduser(p))
    try:
        with open(path, "rb") as fh:
            out[p] = fh.read(req.get("max_bytes", 1 << 20)).decode("utf-8", "replace")
    except (IOError, OSError):
        out[p] = None
json.dump(out, sys.stdout)
"""


class Transport(Protocol):
    def run(self, command: str, stdin: bytes | None = None, timeout: float = 60.0) -> Result: ...

    def check(self) -> None:
        """Raise TransportDown unless commands can be run right now."""


class TransportBase:
    python = "python3"

    def run(self, command: str, stdin: bytes | None = None, timeout: float = 60.0) -> Result:
        raise NotImplementedError

    def put_files(self, files: dict[str, bytes], mode: int = 0o644) -> None:
        payload = {"files": [{"path": p, "data": base64.b64encode(d).decode(), "mode": mode} for p, d in files.items()]}
        res = self.run(f"{self.python} -c {shlex.quote(_PUT)}", stdin=json.dumps(payload).encode(), timeout=120)
        if res.returncode != 0 or "ok" not in res.stdout:
            raise TransportDown(f"writing files on the cluster failed: {res.stderr.strip()[-400:]}")

    def read_files(self, paths: list[str], max_bytes: int = 1 << 20) -> dict[str, str | None]:
        if not paths:
            return {}
        payload = {"paths": paths, "max_bytes": max_bytes}
        res = self.run(f"{self.python} -c {shlex.quote(_READ)}", stdin=json.dumps(payload).encode(), timeout=120)
        if res.returncode != 0:
            raise TransportDown(f"reading files on the cluster failed: {res.stderr.strip()[-400:]}")
        return json.loads(res.stdout)


class SSHTransport(TransportBase):
    """Non-interactive SSH through a persistent ControlMaster. Secrets stay in ~/.ssh and the agent.

    With MFA clusters, run `switchboard hpc connect` once (interactive); later calls reuse the master.
    """

    def __init__(self, host: str, remote_shell: str = "bash -lc", python: str = "python3",
                 control_persist: str = "12h", connect_timeout: int = 10) -> None:
        self.host = host
        self.remote_shell = remote_shell
        self.python = python
        self.control_persist = control_persist
        self.connect_timeout = connect_timeout
        self.control_path = str(Path("~/.ssh").expanduser() / "sb-%C")  # %C hash keeps it under SUN_LEN

    def ssh_options(self, batch: bool = True) -> list[str]:
        opts = ["-o", "ControlMaster=auto", "-o", f"ControlPath={self.control_path}",
                "-o", f"ControlPersist={self.control_persist}", "-o", f"ConnectTimeout={self.connect_timeout}",
                "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=4"]
        if batch:
            opts += ["-o", "BatchMode=yes"]
        return opts

    def run(self, command: str, stdin: bytes | None = None, timeout: float = 60.0) -> Result:
        remote = f"{self.remote_shell} {shlex.quote(command)}" if self.remote_shell else command
        argv = ["ssh", *self.ssh_options(), self.host, remote]
        try:
            proc = subprocess.run(argv, input=stdin, capture_output=True, timeout=timeout,
                                  stdin=None if stdin is not None else subprocess.DEVNULL)
        except subprocess.TimeoutExpired as exc:
            raise TransportLost(f"ssh {self.host}: no reply within {timeout:g}s") from exc
        except OSError as exc:
            raise TransportDown(f"cannot run ssh: {exc}") from exc
        out, err = proc.stdout.decode(errors="replace"), proc.stderr.decode(errors="replace")
        if proc.returncode == 255:
            # ssh's own failure code: either it never connected or the connection broke mid-command.
            raise TransportLost(f"ssh {self.host} failed: {err.strip()[-400:]}")
        return Result(proc.returncode, out, err)

    def check(self) -> None:
        try:
            proc = subprocess.run(["ssh", *self.ssh_options(), self.host, "true"], capture_output=True,
                                  timeout=self.connect_timeout + 10, stdin=subprocess.DEVNULL)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise TransportDown(f"ssh {self.host}: {exc}") from exc
        if proc.returncode != 0:
            err = proc.stderr.decode(errors="replace").strip()
            hint = " (run `switchboard hpc connect` to authenticate)" if "ermission denied" in err else ""
            raise TransportDown(f"ssh {self.host} unavailable{hint}: {err[-300:]}")

    def connect_interactive(self) -> int:
        """Open the ControlMaster interactively (password/MFA prompts appear in this terminal)."""
        argv = ["ssh", *self.ssh_options(batch=False), "-o", "ControlMaster=yes", "-fN", self.host]
        return subprocess.call(argv)


class LocalTransport(TransportBase):
    """Runs commands in a local shell; used with tools/mock_slurm. ``down`` simulates an unreachable host."""

    def __init__(self, env: dict[str, str] | None = None, python: str = "python3") -> None:
        self.env = {**os.environ, **(env or {})}
        self.python = python
        self.down = False
        self.calls: list[str] = []

    def run(self, command: str, stdin: bytes | None = None, timeout: float = 60.0) -> Result:
        if self.down:
            raise TransportLost("fault injection: connection lost")
        self.calls.append(command)
        try:
            proc = subprocess.run(["bash", "-c", command], input=stdin, capture_output=True, timeout=timeout,
                                  env=self.env, stdin=None if stdin is not None else subprocess.DEVNULL)
        except subprocess.TimeoutExpired as exc:
            raise TransportLost(f"local command timed out: {command[:80]}") from exc
        out, err = proc.stdout.decode(errors="replace"), proc.stderr.decode(errors="replace")
        if proc.returncode == 255:
            raise TransportLost(f"connection dropped: {err.strip()[-200:]}")
        return Result(proc.returncode, out, err)

    def check(self) -> None:
        if self.down:
            raise TransportDown("fault injection: host down")
