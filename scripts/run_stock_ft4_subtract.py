#!/usr/bin/env python3

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

from mode_reference import locate_ft4_stock_subtract


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the transient FT4 stock subtraction helper")
    parser.add_argument("input_wav")
    parser.add_argument("output_wav")
    parser.add_argument("message")
    parser.add_argument("--freq-hz", type=float, required=True)
    parser.add_argument("--dt-seconds", type=float, required=True)
    parser.add_argument("--helper", help="Override FT4 stock subtraction helper path.")
    args = parser.parse_args()

    helper = locate_ft4_stock_subtract(args.helper)
    subprocess.run(
        [
            str(helper),
            str(Path(args.input_wav).resolve()),
            str(Path(args.output_wav).resolve()),
            args.message,
            str(args.freq_hz),
            str(args.dt_seconds),
        ],
        check=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
