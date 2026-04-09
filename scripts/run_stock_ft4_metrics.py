#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import subprocess

from mode_reference import locate_ft4_stock_metrics


def parse_line(prefix: str, line: str) -> list[float]:
    payload = line.split("=", 1)[1].strip()
    if not payload:
        return []
    return [float(value) for value in payload.split()]


def parse_output(stdout: str) -> dict:
    result: dict = {}
    for raw_line in stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("badsync="):
            result["badsync"] = line.endswith("T")
        elif line.startswith("bitmetrics1="):
            result["bitmetrics1"] = parse_line("bitmetrics1", line)
        elif line.startswith("bitmetrics2="):
            result["bitmetrics2"] = parse_line("bitmetrics2", line)
        elif line.startswith("bitmetrics3="):
            result["bitmetrics3"] = parse_line("bitmetrics3", line)
        elif line.startswith("llra="):
            result["llra"] = parse_line("llra", line)
        elif line.startswith("llrb="):
            result["llrb"] = parse_line("llrb", line)
        elif line.startswith("llrc="):
            result["llrc"] = parse_line("llrc", line)
    result["raw_stdout"] = stdout
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the transient FT4 stock metrics helper")
    parser.add_argument("wav")
    parser.add_argument("--freq-hz", type=float, required=True)
    parser.add_argument("--dt-seconds", type=float, required=True)
    parser.add_argument("--helper", help="Override FT4 stock metrics helper path.")
    args = parser.parse_args()

    helper = locate_ft4_stock_metrics(args.helper)
    completed = subprocess.run(
        [str(helper), args.wav, str(args.freq_hz), str(args.dt_seconds)],
        check=True,
        capture_output=True,
        text=True,
    )
    print(json.dumps(parse_output(completed.stdout)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
