#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import random
from pathlib import Path


def load_json(path: Path) -> dict:
    return json.loads(path.read_text())


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build a small mixed manifest from a parity compare JSON."
    )
    parser.add_argument("--compare", required=True, help="JSON emitted by scripts/mode_parity.py compare.")
    parser.add_argument("--mode", default="ft8", help="Mode label to record in the output manifest.")
    parser.add_argument("--profile", help="Profile label to record. Defaults to the compare profile.")
    parser.add_argument("--output", required=True, help="Output manifest path.")
    parser.add_argument(
        "--controls-per-mismatch",
        type=int,
        default=1,
        help="Number of exact-match controls to include per exact mismatch.",
    )
    parser.add_argument(
        "--min-controls",
        type=int,
        default=5,
        help="Minimum exact-match controls to keep even when there are few or no mismatches.",
    )
    parser.add_argument("--seed", type=int, default=12345)
    args = parser.parse_args()

    compare = load_json(Path(args.compare))
    source_manifest = load_json(Path(compare["manifest"]))
    cases_by_id = {case["id"]: case for case in source_manifest["cases"]}
    results_by_id = {result["id"]: result for result in compare["results"]}

    mismatch_ids = [
        result["id"]
        for result in compare["results"]
        if not result.get("match", False)
        or result.get("stock_only_messages")
        or not result.get("rust_covers_reference", True)
    ]
    control_ids = [
        result["id"]
        for result in compare["results"]
        if result["id"] not in set(mismatch_ids)
        and result.get("match", False)
        and result.get("rust_covers_reference", True)
    ]

    rng = random.Random(args.seed)
    rng.shuffle(control_ids)
    control_count = max(args.min_controls, len(mismatch_ids) * args.controls_per_mismatch)
    selected_control_ids = control_ids[:control_count]

    distilled_cases = []
    for case_id, role in [(case_id, "mismatch") for case_id in mismatch_ids] + [
        (case_id, "control") for case_id in selected_control_ids
    ]:
        case = dict(cases_by_id[case_id])
        case["parity_role"] = role
        case["compare_result"] = results_by_id[case_id]
        distilled_cases.append(case)

    payload = {
        "kind": "distilled-mixed-manifest",
        "source_compare": str(Path(args.compare).resolve()),
        "mode": args.mode,
        "profile": args.profile or compare.get("profile"),
        "comparison_mode": compare.get("comparison_mode", "exact"),
        "mismatch_cases": len(mismatch_ids),
        "control_cases": len(selected_control_ids),
        "cases": distilled_cases,
    }
    write_json(Path(args.output), payload)
    print(
        json.dumps(
            {
                "output": str(Path(args.output).resolve()),
                "mismatch_cases": len(mismatch_ids),
                "control_cases": len(selected_control_ids),
                "case_count": len(distilled_cases),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
