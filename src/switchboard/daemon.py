"""The resident controller loop: adaptive cadence, woken early by nudges.

Every pass is an idempotent Controller.tick(), so killing the daemon loses nothing. Cadence:
  * a nudge datagram (CLI after submit/assign/mode/pause; shim when a local attempt exits) wakes it at once
  * otherwise every ``local_interval_s`` while local work is pending or running
  * otherwise every ``idle_interval_s``
  * after errors, exponential backoff capped at ``idle_interval_s``
Idle cost is one blocked select() call.
"""

from __future__ import annotations

import fcntl
import logging
import os
import select
import signal
import socket
import time
from pathlib import Path
from typing import Callable

from .config import Config
from .controller import Controller

log = logging.getLogger("switchboard.daemon")


class DaemonAlreadyRunning(RuntimeError):
    pass


class Daemon:
    def __init__(self, cfg: Config, make_controller: Callable[[], Controller]) -> None:
        self.cfg = cfg
        self.make_controller = make_controller
        self.ticks = 0
        self.nudges = 0
        self._stop = False

    def run(self, max_ticks: int | None = None) -> None:
        self.cfg.run_dir.mkdir(parents=True, exist_ok=True)
        lock = open(self.cfg.daemon_lock, "a+")
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            lock.close()
            raise DaemonAlreadyRunning(f"another switchboard daemon holds {self.cfg.daemon_lock}") from None
        lock.seek(0)
        lock.truncate()
        lock.write(f"{os.getpid()}\n")
        lock.flush()

        sock_path = self.cfg.nudge_socket
        sock_path.unlink(missing_ok=True)  # stale: we hold the lock, so no other daemon owns it
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        sock.bind(str(sock_path))
        os.chmod(sock_path, 0o600)
        wake_r, wake_w = os.pipe()
        os.set_blocking(wake_w, False)
        previous = {s: signal.signal(s, self._on_signal) for s in (signal.SIGTERM, signal.SIGINT)}
        old_wakeup = signal.set_wakeup_fd(wake_w)
        log.info("daemon %d listening on %s", os.getpid(), sock_path)
        try:
            self._loop(sock, wake_r, max_ticks)
        finally:
            signal.set_wakeup_fd(old_wakeup)
            for s, handler in previous.items():
                signal.signal(s, handler)
            sock.close()
            sock_path.unlink(missing_ok=True)
            os.close(wake_r)
            os.close(wake_w)
            fcntl.flock(lock, fcntl.LOCK_UN)
            lock.close()
            log.info("daemon %d stopped after %d ticks", os.getpid(), self.ticks)

    def _loop(self, sock: socket.socket, wake_r: int, max_ticks: int | None) -> None:
        ctl: Controller | None = None
        failures = 0
        d = self.cfg.daemon
        last_remote = 0.0
        self._force_remote = True
        while not self._stop:
            busy = remote_busy = False
            try:
                ctl = ctl or self.make_controller()
                remote = self._force_remote or time.monotonic() - last_remote >= self.cfg.hpc.remote_interval_s
                report = ctl.tick(remote=remote)
                if remote:
                    last_remote, self._force_remote = time.monotonic(), False
                active, pending = ctl.local_work_remaining()
                busy = bool(active or pending)
                r_active, r_pending = ctl.remote_work_remaining()
                remote_busy = bool(r_active or r_pending)
                failures = 0
                summary = {k: v for k, v in report.as_dict().items() if v}
                if summary:
                    log.info("tick: %s", summary)
            except Exception:
                failures += 1
                log.exception("tick failed (%d in a row)", failures)
                ctl = None  # rebuild connections on the next pass
            self.ticks += 1
            if max_ticks is not None and self.ticks >= max_ticks:
                return
            if failures:
                timeout = min(d.idle_interval_s, d.local_interval_s * 2 ** min(failures, 10))
            elif busy:
                timeout = d.local_interval_s
            elif remote_busy:
                timeout = min(d.idle_interval_s, self.cfg.hpc.remote_interval_s)
            else:
                timeout = d.idle_interval_s
            ready, _, _ = select.select([sock, wake_r], [], [], timeout)
            if sock in ready:
                self._drain(sock)
            if wake_r in ready:
                os.read(wake_r, 64)

    def _drain(self, sock: socket.socket) -> None:
        sock.setblocking(False)
        try:
            while True:
                if sock.recv(64) == b"cli":  # a user action: reconcile the cluster now too
                    self._force_remote = True
                self.nudges += 1
        except BlockingIOError:
            pass
        finally:
            sock.setblocking(True)

    def _on_signal(self, signum, _frame) -> None:
        self._stop = True


def daemon_running(cfg: Config) -> bool:
    if not cfg.daemon_lock.exists():
        return False
    with open(cfg.daemon_lock, "a+") as fh:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(fh, fcntl.LOCK_UN)
        return False


def nudge(cfg: Config) -> bool:
    """Wake a running daemon. Returns False if none is listening."""
    path: Path = cfg.nudge_socket
    if not path.exists():
        return False
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as s:
            s.sendto(b"cli", str(path))
        return True
    except OSError:
        return False


def wait_for_socket(cfg: Config, timeout: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cfg.nudge_socket.exists() and daemon_running(cfg):
            return True
        time.sleep(0.05)
    return False
