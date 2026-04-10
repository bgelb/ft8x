#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import subprocess

from mode_reference import locate_ft4_stock_search


CAND_RE = re.compile(
    r"^cand\((?P<index>\d+)\)=freq:\s*(?P<freq>-?\d+(?:\.\d+)?)\s+score:\s*(?P<score>-?\d+(?:\.\d+)?)$"
)
RESIDUAL_RE = re.compile(
    r"^residual sum=\s*(?P<sum>-?\d+(?:\.\d+)?(?:[Ee][+-]?\d+)?)\s+sqsum=\s*(?P<sqsum>-?\d+(?:\.\d+)?(?:[Ee][+-]?\d+)?)\s+probes=(?P<probes>.+)$"
)


def parse_output(stdout: str) -> dict:
    scale = 32767.0
    payload: dict[str, object] = {"candidates": [], "raw_stdout": stdout}
    for raw_line in stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("ncand="):
            payload["candidate_count"] = int(line.split("=", 1)[1])
            continue
        if match := CAND_RE.match(line):
            payload["candidates"].append(
                {
                    "candidate_index": int(match.group("index")),
                    "freq_hz": float(match.group("freq")),
                    "score": float(match.group("score")),
                }
            )
            continue
        if match := RESIDUAL_RE.match(line):
            payload["residual_signature"] = {
                "sample_sum": float(match.group("sum")) / scale,
                "sample_sq_sum": float(match.group("sqsum")) / (scale * scale),
                "probe_values": [
                    float(token) / scale for token in match.group("probes").split(",") if token
                ],
            }
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Run stock FT4 getcandidates4-only helper")
    parser.add_argument("wav")
    parser.add_argument("--helper", help="Override FT4 stock search helper path.")
    args = parser.parse_args()

    helper = locate_ft4_stock_search(args.helper)
    completed = subprocess.run(
        [str(helper), args.wav],
        check=True,
        capture_output=True,
        text=True,
    )
    print(json.dumps(parse_output(completed.stdout)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
