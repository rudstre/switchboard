"""Slurm array entry point. Stdlib only, Python >= 3.8; shipped next to shim.py and fingerprint.py.

    python3 array.py MANIFEST.jsonl INDEX

Line INDEX (0-based) of the manifest is one attempt spec; it is run by the shim.
"""

from __future__ import annotations

import json
import sys

try:
    from . import shim
except ImportError:  # run as a plain script
    import shim  # type: ignore[no-redef]


def main(argv: list) -> int:
    if len(argv) != 3:
        print("usage: array.py MANIFEST.jsonl INDEX", file=sys.stderr)
        return shim.EXIT_RUNNER_ERROR
    index = int(argv[2])
    with open(argv[1]) as fh:
        for i, line in enumerate(fh):
            if i == index:
                return shim.run(json.loads(line))
    print("array.py: manifest %s has no line %d" % (argv[1], index), file=sys.stderr)
    return shim.EXIT_RUNNER_ERROR


if __name__ == "__main__":
    sys.exit(main(sys.argv))
