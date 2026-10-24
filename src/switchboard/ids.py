"""Identifiers and canonical hashing."""

from __future__ import annotations

import hashlib
import json
import secrets
import time
from typing import Any


def new_id(prefix: str) -> str:
    """Time-sortable unique id, e.g. ``a-019a1b2c3d4e-9f8e7d6c5b``."""
    return f"{prefix}-{int(time.time() * 1000):012x}-{secrets.token_hex(5)}"


def canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_hex(data: str | bytes) -> str:
    if isinstance(data, str):
        data = data.encode()
    return hashlib.sha256(data).hexdigest()


def short(identity: str | None, n: int = 12) -> str:
    """Shorten ``provider:hexdigest`` ids for display."""
    if not identity:
        return "-"
    provider, _, digest = identity.partition(":")
    return f"{provider}:{digest[:n]}" if digest else identity[:n]
