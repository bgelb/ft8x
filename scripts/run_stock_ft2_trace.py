#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
from pathlib import Path

from mode_reference import locate_ft2_ref_binary


def parse_trace(stdout: str) -> dict:
    candidates: list[dict] = []
    current: dict | None = None
    pending_iterations: int | None = None
    for raw_line in stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("ncand="):
            continue
        if line.startswith("candidate_f0="):
            if current is not None:
                candidates.append(current)
            current = {
                "candidate_f0": float(line.split("=", 1)[1]),
                "sequences": [],
            }
            pending_iterations = None
            continue
        if current is None:
            continue
        if line.startswith("best_df="):
            current["best_df"] = float(line.split("=", 1)[1])
            continue
        if line.startswith("best_ibest="):
            current["best_ibest"] = int(line.split("=", 1)[1])
            continue
        if line.startswith("best_sync="):
            current["best_sync"] = float(line.split("=", 1)[1])
            continue
        if line.startswith("nseq="):
            pieces = line.split()
            seq = {
                "nseq": int(pieces[0].split("=", 1)[1]),
                "sync_ok": int(pieces[1].split("=", 1)[1]),
                "nharderror": int(pieces[2].split("=", 1)[1]),
                "decoded": line.split("decoded=", 1)[1].strip(),
            }
            current["sequences"].append(seq)
            pending_iterations = len(current["sequences"]) - 1
            continue
        if line.startswith("iterations=") and pending_iterations is not None:
            current["sequences"][pending_iterations]["iterations"] = int(line.split("=", 1)[1])
            continue
        if line.startswith("mean=") and pending_iterations is not None:
            current["sequences"][pending_iterations]["mean"] = float(line.split("=", 1)[1])
            continue
        if line.startswith("sigma=") and pending_iterations is not None:
            current["sequences"][pending_iterations]["sigma"] = float(line.split("=", 1)[1])
            continue
        if line.startswith("llr_head=") and pending_iterations is not None:
            values = line.split("=", 1)[1].strip()
            current["sequences"][pending_iterations]["llr_head"] = (
                [float(part) for part in values.split(",") if part]
                if values
                else []
            )
            continue
        if line.startswith("llrs=") and pending_iterations is not None:
            values = line.split("=", 1)[1].strip()
            current["sequences"][pending_iterations]["llrs"] = (
                [float(part) for part in values.split(",") if part]
                if values
                else []
            )
            continue
    if current is not None:
        candidates.append(current)
    return {"candidates": candidates}


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the stock FT2 trace helper and emit JSON.")
    parser.add_argument("wav", help="Input WAV path")
    parser.add_argument("--binary", help="Override FT2 stock trace helper path")
    args = parser.parse_args()

    wav = Path(args.wav).resolve()
    binary = locate_ft2_ref_binary("trace", args.binary)
    with tempfile.TemporaryDirectory(prefix="ft2-ref-trace-") as tmpdir:
        completed = subprocess.run(
            [str(binary), str(wav)],
            check=True,
            capture_output=True,
            text=True,
            cwd=tmpdir,
        )
    print(json.dumps(parse_trace(completed.stdout)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
