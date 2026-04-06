#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path

from mode_reference import locate_ft2_ref_binary
from run_stock_decode import run_ft2_ref

def main() -> int:
    parser = argparse.ArgumentParser(description="Run the transient FT2 reference decoder in an isolated cwd.")
    parser.add_argument("wav", help="Input WAV path")
    parser.add_argument(
        "--binary",
        help="Path to the FT2 reference decoder helper.",
    )
    parser.add_argument(
        "--profile",
        default="medium",
        choices=["medium", "deepest"],
        help="Accepted for interface compatibility; FT2 stock helper ignores this today.",
    )
    args = parser.parse_args()

    binary = locate_ft2_ref_binary("decode", args.binary)
    print(run_ft2_ref(Path(args.wav).resolve(), str(binary)), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
