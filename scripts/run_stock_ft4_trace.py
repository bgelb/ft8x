#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import subprocess

from mode_reference import locate_ft4_stock_trace


CAND_RE = re.compile(
    r"^pass=(?P<pass>\d+)\s+cand=(?P<cand>\d+)\s+freq=\s*(?P<freq>-?\d+(?:\.\d+)?)\s+score=\s*(?P<score>-?\d+(?:\.\d+)?)$"
)
DECODE_RE = re.compile(
    r"^decode pass=(?P<pass>\d+)\s+cand=(?P<cand>\d+)\s+segment=(?P<segment>\d+)\s+ipass=(?P<ipass>\d+)\s+ntype=(?P<ntype>-?\d+)\s+nharderror=(?P<nharderror>-?\d+)\s+dt=\s*(?P<dt>-?\d+(?:\.\d+)?)\s+freq=\s*(?P<freq>-?\d+(?:\.\d+)?)\s+message=(?P<message>.+)$"
)


def parse_output(stdout: str) -> dict:
    result: dict = {"passes": [], "raw_stdout": stdout}
    current_pass: dict | None = None
    for raw_line in stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("pass=") and " ncand=" in line:
            left, right = line.split(" ncand=", 1)
            pass_index = int(left.split("=", 1)[1])
            current_pass = {"pass_index": pass_index, "candidate_count": int(right), "candidates": [], "decodes": []}
            result["passes"].append(current_pass)
            continue
        if match := CAND_RE.match(line):
            if current_pass is None:
                continue
            current_pass["candidates"].append(
                {
                    "candidate_index": int(match.group("cand")),
                    "freq_hz": float(match.group("freq")),
                    "score": float(match.group("score")),
                }
            )
            continue
        if match := DECODE_RE.match(line):
            if current_pass is None:
                continue
            current_pass["decodes"].append(
                {
                    "candidate_index": int(match.group("cand")),
                    "segment": int(match.group("segment")),
                    "ipass": int(match.group("ipass")),
                    "ntype": int(match.group("ntype")),
                    "nharderror": int(match.group("nharderror")),
                    "dt_seconds": float(match.group("dt")),
                    "freq_hz": float(match.group("freq")),
                    "message": re.sub(r"\s+", " ", match.group("message").strip().upper()),
                }
            )
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the transient FT4 stock full-pass trace helper")
    parser.add_argument("wav")
    parser.add_argument("--profile", choices=["medium", "deepest"], default="deepest")
    parser.add_argument("--helper", help="Override FT4 stock trace helper path.")
    args = parser.parse_args()

    helper = locate_ft4_stock_trace(args.helper)
    ndepth = "2" if args.profile == "medium" else "3"
    completed = subprocess.run(
        [str(helper), args.wav, ndepth],
        check=True,
        capture_output=True,
        text=True,
    )
    print(json.dumps(parse_output(completed.stdout)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
