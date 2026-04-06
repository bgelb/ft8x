#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import subprocess
import tempfile
from pathlib import Path


MODE_FLAGS = {
    "ft8": "-8",
    "ft4": "-5",
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Run stock WSJT-X jt9 on a WAV file.")
    parser.add_argument("wav", help="Input WAV path")
    parser.add_argument("mode", choices=sorted(MODE_FLAGS))
    parser.add_argument(
        "--wsjtx-app",
        default="artifacts/releases/2.7.0/wsjtx.app",
        help="Path to the WSJT-X app bundle",
    )
    args = parser.parse_args()

    app = Path(args.wsjtx_app).resolve()
    binary = app / "Contents" / "MacOS" / "jt9"
    exec_dir = app / "Contents" / "MacOS"
    dylib_path = ":".join(
        [
            str(app / "Contents" / "MacOS"),
            str(app / "Contents" / "Frameworks"),
        ]
    )

    with tempfile.TemporaryDirectory(prefix="jt9-") as tmpdir:
        env = os.environ.copy()
        env["DYLD_LIBRARY_PATH"] = dylib_path
        completed = subprocess.run(
            [
                str(binary),
                MODE_FLAGS[args.mode],
                "-d",
                "2",
                "-e",
                str(exec_dir),
                "-a",
                tmpdir,
                "-t",
                tmpdir,
                str(Path(args.wav).resolve()),
            ],
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
    print(completed.stdout, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
