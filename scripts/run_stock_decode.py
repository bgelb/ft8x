#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import subprocess
import tempfile
from pathlib import Path

from mode_reference import jt9_debug_level, locate_ft2_ref_binary, locate_jt9_binary


MODE_FLAGS = {
    "ft8": "-8",
    "ft4": "-5",
}


def run_jt9(wav: Path, mode: str, profile: str, wsjtx_app: str | None) -> str:
    binary, app = locate_jt9_binary(wsjtx_app)
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
                MODE_FLAGS[mode],
                "-d",
                jt9_debug_level(profile),
                "-e",
                str(exec_dir),
                "-a",
                tmpdir,
                "-t",
                tmpdir,
                str(wav),
            ],
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
    return completed.stdout


def run_ft2_ref(wav: Path, ft2_binary: str | None) -> str:
    binary = locate_ft2_ref_binary("decode", ft2_binary)
    with tempfile.TemporaryDirectory(prefix="ft2-ref-decode-") as tmpdir:
        completed = subprocess.run(
            [str(binary), str(wav)],
            check=True,
            capture_output=True,
            text=True,
            cwd=tmpdir,
        )
    return completed.stdout


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the canonical stock decoder entrypoint for FT8/FT4/FT2.")
    parser.add_argument("wav", help="Input WAV path")
    parser.add_argument("mode", choices=["ft8", "ft4", "ft2"])
    parser.add_argument(
        "--profile",
        default="medium",
        choices=["medium", "deepest"],
        help="Stock decode profile to use.",
    )
    parser.add_argument(
        "--wsjtx-app",
        help="Override the official WSJT-X app bundle used for FT8/FT4 stock decodes.",
    )
    parser.add_argument(
        "--ft2-binary",
        help="Override the FT2 stock helper built through scripts/bootstrap_mode_refs.py.",
    )
    args = parser.parse_args()

    wav = Path(args.wav).resolve()
    if args.mode in MODE_FLAGS:
        stdout = run_jt9(wav, args.mode, args.profile, args.wsjtx_app)
    else:
        stdout = run_ft2_ref(wav, args.ft2_binary)
    print(stdout, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
