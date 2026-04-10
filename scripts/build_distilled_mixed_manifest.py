#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import random
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a distilled mixed-only manifest from compare results.")
    parser.add_argument("--compare", required=True, help="Path to mode_parity compare JSON")
    parser.add_argument("--manifest", required=True, help="Path to the source mixed manifest JSON")
    parser.add_argument("--mode", required=True, choices=["ft4", "ft2"])
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    compare_path = Path(args.compare).resolve()
    manifest_path = Path(args.manifest).resolve()
    output_path = Path(args.output).resolve()

    compare = json.loads(compare_path.read_text())
    manifest = json.loads(manifest_path.read_text())

    failing_ids = {
        result["id"]
        for result in compare["results"]
        if not result["match"] and result["mode"] == args.mode and result.get("cohort") == "mixed"
    }

    cases = [
        case
        for case in manifest["cases"]
        if case["mode"] == args.mode and case.get("cohort") == "mixed"
    ]
    failing_cases = [case for case in cases if case["id"] in failing_ids]
    non_failing_cases = [case for case in cases if case["id"] not in failing_ids]

    rng = random.Random(args.seed)
    sample_count = min(len(failing_cases), len(non_failing_cases))
    if sample_count == 0:
        sample_count = min(50, len(non_failing_cases))
    sampled_cases = rng.sample(non_failing_cases, sample_count)

    payload = {
        "kind": "distilled-mixed",
        "source_compare": str(compare_path),
        "source_manifest": str(manifest_path),
        "mode": args.mode,
        "seed": args.seed,
        "failing_case_count": len(failing_cases),
        "sampled_control_count": len(sampled_cases),
        "cases": failing_cases + sampled_cases,
    }
    output_path.write_text(json.dumps(payload, indent=2) + "\n")
    print(output_path)
    print(
        json.dumps(
            {
                "mode": args.mode,
                "failing_case_count": len(failing_cases),
                "sampled_control_count": len(sampled_cases),
                "total_cases": len(payload["cases"]),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
