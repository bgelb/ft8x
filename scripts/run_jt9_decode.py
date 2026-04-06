#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path

from run_stock_decode import run_jt9


MODE_FLAGS = {
    "ft8": "-8",
    "ft4": "-5",
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Run stock WSJT-X jt9 on a WAV file.")
    parser.add_argument("wav", help="Input WAV path")
    parser.add_argument("mode", choices=sorted(MODE_FLAGS))
    parser.add_argument(
        "--profile",
        default="medium",
        choices=["medium", "deepest"],
        help="Stock decode profile to use.",
    )
    parser.add_argument(
        "--wsjtx-app",
        help="Path to the WSJT-X app bundle",
    )
    args = parser.parse_args()

    print(
        run_jt9(Path(args.wav).resolve(), args.mode, args.profile, args.wsjtx_app),
        end="",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
