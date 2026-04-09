#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import subprocess

from mode_reference import locate_ft4_stock_debug


CANDIDATE_RE = re.compile(r"^cand\((?P<index>\d+)\)=freq:\s*(?P<freq>-?\d+(?:\.\d+)?)\s+score:\s*(?P<score>-?\d+(?:\.\d+)?)$")
SEGMENT_RE = re.compile(
    r"^segment=(?P<segment>\d+)\s+smax=\s*(?P<smax>-?\d+(?:\.\d+)?)\s+ibest=(?P<ibest>-?\d+)\s+idfbest=(?P<idfbest>-?\d+)$"
)
VARIANT_RE = re.compile(
    r"^variant_segment=(?P<segment>\d+)\s+f1=\s*(?P<f1>-?\d+(?:\.\d+)?)\s+smax=\s*(?P<smax>-?\d+(?:\.\d+)?)\s+nsync_qual=(?P<nsync_qual>-?\d+)\s+ibest=(?P<ibest>-?\d+)$"
)
VARIANT_PASS_RE = re.compile(r"^variant_pass=(?P<pass>\d+)\s+decoded=(?P<decoded>.+)$")


def parse_output(stdout: str) -> dict:
    payload: dict = {
        "candidate_count": 0,
        "candidates": [],
        "segments": [],
        "variants": [],
        "raw_stdout": stdout,
    }
    for raw_line in stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("ncand="):
            payload["candidate_count"] = int(line.split("=", 1)[1])
            continue
        if line == "variant_badsync=1":
            payload["variants"].append({"badsync": True})
            continue
        if match := CANDIDATE_RE.match(line):
            payload["candidates"].append(
                {
                    "index": int(match.group("index")),
                    "freq_hz": float(match.group("freq")),
                    "score": float(match.group("score")),
                }
            )
            continue
        if match := SEGMENT_RE.match(line):
            payload["segments"].append(
                {
                    "segment": int(match.group("segment")),
                    "smax": float(match.group("smax")),
                    "ibest": int(match.group("ibest")),
                    "idfbest": int(match.group("idfbest")),
                }
            )
            continue
        if match := VARIANT_RE.match(line):
            ibest = int(match.group("ibest"))
            payload["variants"].append(
                {
                    "segment": int(match.group("segment")),
                    "f1_hz": float(match.group("f1")),
                    "smax": float(match.group("smax")),
                    "nsync_qual": int(match.group("nsync_qual")),
                    "ibest": ibest,
                    "dt_seconds": ibest / 666.67 - 0.5,
                }
            )
            continue
        if match := VARIANT_PASS_RE.match(line):
            payload.setdefault("variant_passes", []).append(
                {
                    "pass": int(match.group("pass")),
                    "decoded": re.sub(r"\s+", " ", match.group("decoded").strip().upper()),
                }
            )
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the transient FT4 stock-stage debug helper")
    parser.add_argument("wav")
    parser.add_argument("--freq-hz", type=float, required=True)
    parser.add_argument("--helper", help="Override FT4 stock debug helper path.")
    args = parser.parse_args()

    helper = locate_ft4_stock_debug(args.helper)
    completed = subprocess.run(
        [str(helper), args.wav, str(args.freq_hz)],
        check=True,
        capture_output=True,
        text=True,
    )
    print(json.dumps(parse_output(completed.stdout)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
