#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import subprocess

from mode_reference import locate_ft4_stock_fixed


SEGMENT_RE = re.compile(
    r"^segment=(?P<segment>\d+)\s+smax=\s*(?P<smax>-?\d+(?:\.\d+)?)\s+ibest=(?P<ibest>-?\d+)\s+idfbest=(?P<idfbest>-?\d+)$"
)
VARIANT_RE = re.compile(
    r"^variant_segment=(?P<segment>\d+)\s+f1=\s*(?P<f1>-?\d+(?:\.\d+)?)\s+smax=\s*(?P<smax>-?\d+(?:\.\d+)?)\s+nsync_qual=(?P<nsync>\d+)\s+ibest=(?P<ibest>-?\d+)$"
)
PASS_RE = re.compile(r"^variant_pass=(?P<pass>\d+)\s+decoded=(?P<decoded>.+)$")
SUBTRACT_RE = re.compile(
    r"^subtract dt_internal=\s*(?P<dt>-?\d+(?:\.\d+)?(?:[Ee][+-]?\d+)?)\s+freq_exact=\s*(?P<freq>-?\d+(?:\.\d+)?(?:[Ee][+-]?\d+)?)\s+ntype=(?P<ntype>-?\d+)\s+nharderror=(?P<nharderror>-?\d+)\s+bits=(?P<bits>[01]{77})$"
)


def parse_output(stdout: str) -> dict:
    payload: dict[str, object] = {"segments": [], "raw_stdout": stdout}
    current_variant: dict | None = None
    for raw_line in stdout.splitlines():
        line = raw_line.strip()
        if not line:
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
            current_variant = None
            continue
        if match := VARIANT_RE.match(line):
            current_variant = {
                "segment": int(match.group("segment")),
                "f1_hz": float(match.group("f1")),
                "smax": float(match.group("smax")),
                "nsync_qual": int(match.group("nsync")),
                "ibest": int(match.group("ibest")),
                "passes": [],
            }
            payload.setdefault("variants", []).append(current_variant)
            continue
        if line == "variant_badsync=1":
            payload.setdefault("badsync_segments", 0)
            payload["badsync_segments"] += 1
            current_variant = None
            continue
        if match := PASS_RE.match(line):
            if current_variant is not None:
                current_variant["passes"].append(
                    {
                        "ipass": int(match.group("pass")),
                        "decoded": re.sub(r"\s+", " ", match.group("decoded").strip().upper()),
                    }
                )
            continue
        if match := SUBTRACT_RE.match(line):
            if current_variant is not None and current_variant.get("passes"):
                current_variant["passes"][-1]["subtract_event"] = {
                    "dt_internal_seconds": float(match.group("dt")),
                    "freq_exact_hz": float(match.group("freq")),
                    "ntype": int(match.group("ntype")),
                    "nharderror": int(match.group("nharderror")),
                    "message_bits": match.group("bits"),
                }
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Run stock FT4 fixed-candidate helper")
    parser.add_argument("wav")
    parser.add_argument("freq_hz", type=float)
    parser.add_argument("--profile", choices=["medium", "deepest"], default="medium")
    parser.add_argument("--helper", help="Override FT4 stock fixed helper path.")
    args = parser.parse_args()

    helper = locate_ft4_stock_fixed(args.helper)
    ndepth = "2" if args.profile == "medium" else "3"
    completed = subprocess.run(
        [str(helper), args.wav, str(args.freq_hz), ndepth],
        check=True,
        capture_output=True,
        text=True,
    )
    print(json.dumps(parse_output(completed.stdout)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
