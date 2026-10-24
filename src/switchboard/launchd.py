"""launchd LaunchAgent for the daemon (per-user, no root). Pueue's own agent is `brew services start pueue`."""

from __future__ import annotations

import os
import plistlib
import shutil
import subprocess
import sys
from pathlib import Path

from .config import Config

LABEL = "com.switchboard.daemon"


def plist_path() -> Path:
    return Path("~/Library/LaunchAgents").expanduser() / f"{LABEL}.plist"


def plist_dict(cfg: Config, python: str = sys.executable) -> dict:
    env = {"PATH": os.pathsep.join(dict.fromkeys(
        [str(Path(shutil.which("pueue") or "/opt/homebrew/bin/pueue").parent), "/opt/homebrew/bin",
         "/usr/local/bin", "/usr/bin", "/bin", "/usr/sbin", "/sbin"]))}
    for var in ("SWITCHBOARD_HOME", "SWITCHBOARD_CONFIG", "PUEUE_CONFIG_PATH"):
        if var in os.environ:
            env[var] = os.environ[var]
    return {
        "Label": LABEL,
        "ProgramArguments": [python, "-m", "switchboard", "daemon", "run"],
        "EnvironmentVariables": env,
        "RunAtLoad": True,
        "KeepAlive": True,
        "ThrottleInterval": 10,
        "ProcessType": "Standard",
        "LowPriorityIO": True,
        "StandardOutPath": str(cfg.home / "daemon.log"),
        "StandardErrorPath": str(cfg.home / "daemon.log"),
    }


def render(cfg: Config) -> bytes:
    return plistlib.dumps(plist_dict(cfg))


def install(cfg: Config) -> Path:
    path = plist_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    cfg.home.mkdir(parents=True, exist_ok=True)
    path.write_bytes(render(cfg))
    domain = f"gui/{os.getuid()}"
    subprocess.run(["launchctl", "bootout", f"{domain}/{LABEL}"], capture_output=True)
    subprocess.run(["launchctl", "bootstrap", domain, str(path)], check=True, capture_output=True)
    return path


def uninstall() -> bool:
    path = plist_path()
    subprocess.run(["launchctl", "bootout", f"gui/{os.getuid()}/{LABEL}"], capture_output=True)
    if path.exists():
        path.unlink()
        return True
    return False
