#!/usr/bin/env python3

from __future__ import annotations

import argparse
import subprocess
import tempfile
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the transient FT2 reference decoder in an isolated cwd.")
    parser.add_argument("wav", help="Input WAV path")
    parser.add_argument(
        "--binary",
        default="/private/tmp/mode-refs-test/ft2/ft2-ref-decode",
        help="Path to the FT2 reference decoder helper.",
    )
    args = parser.parse_args()

    binary = Path(args.binary).resolve()
    wav = Path(args.wav).resolve()
    with tempfile.TemporaryDirectory(prefix="ft2-ref-decode-") as tmpdir:
        completed = subprocess.run(
            [str(binary), str(wav)],
            check=True,
            capture_output=True,
            text=True,
            cwd=tmpdir,
        )
    print(completed.stdout, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
