#!/usr/bin/env python3

from __future__ import annotations

import argparse
import concurrent.futures
import json
import re
import subprocess
from collections import Counter
from pathlib import Path

import mode_parity


def run_json(command: list[str]) -> dict:
    completed = subprocess.run(command, check=True, capture_output=True, text=True)
    return json.loads(completed.stdout)


def normalize_messages(decodes: list[dict]) -> list[str]:
    return sorted(
        {
            re.sub(r"\s+", " ", decode["text"].strip().upper())
            for decode in decodes
            if decode.get("text")
        }
    )


def stage_summary(stage: dict) -> dict:
    diagnostics = stage.get("report", {}).get("diagnostics", {})
    return {
        "stage": stage.get("stage"),
        "decode_count": len(stage.get("report", {}).get("decodes", [])),
        "messages": normalize_messages(stage.get("report", {}).get("decodes", [])),
        "ldpc_codewords": diagnostics.get("ldpc_codewords", 0),
        "parsed_payloads": diagnostics.get("parsed_payloads", 0),
        "search_passes": len(stage.get("search_passes", [])),
        "residual_prep_subtractions": len(stage.get("residual_prep_subtractions", [])),
        "input_signature": stage.get("input_signature"),
        "residual_signature": stage.get("residual_signature"),
    }


def first_mismatch(reference_messages: set[str], stages: list[dict]) -> str:
    stage_messages = [(stage["stage"], set(stage["messages"])) for stage in stages]
    final_messages = stage_messages[-1][1] if stage_messages else set()
    stock_only = reference_messages - final_messages
    if stock_only:
        return "full-output:reference-missing"
    rust_only = final_messages - reference_messages
    if rust_only:
        for stage_name, messages in stage_messages:
            if messages & rust_only:
                return f"{stage_name}:rust-extra"
        return "full-output:rust-extra"
    return "none"


def build_case_chart(case: dict, args: argparse.Namespace, reference_template: str) -> dict:
    command = [
        str(Path(args.decoder_binary).resolve()),
        "debug-stages",
        case["wav_path"],
        "--mode",
        case["mode"],
        "--profile",
        args.profile,
        "--max-candidates",
        str(args.max_candidates),
        "--search-passes",
        str(args.search_passes),
        "--json",
    ]
    trace = run_json(command)
    reference_records = mode_parity.reference_records_for_case(case, reference_template)
    reference_messages = sorted({record["message"] for record in reference_records})
    stages = [stage_summary(stage) for stage in trace.get("stages", [])]
    final_messages = set(stages[-1]["messages"]) if stages else set()
    reference_set = set(reference_messages)
    return {
        "id": case["id"],
        "mode": case["mode"],
        "cohort": case.get("cohort"),
        "parity_role": case.get("parity_role"),
        "wav_path": case["wav_path"],
        "reference_messages": reference_messages,
        "final_rust_messages": sorted(final_messages),
        "stock_only_messages": sorted(reference_set - final_messages),
        "rust_only_messages": sorted(final_messages - reference_set),
        "first_mismatch": first_mismatch(reference_set, stages),
        "stages": stages,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build a compact Rust FT8 stage chart against stock final outputs."
    )
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--mode", default="ft8")
    parser.add_argument("--profile", default="medium", choices=["medium", "deepest"])
    parser.add_argument("--decoder-binary", default="decoder/target/release/ft8-decoder")
    parser.add_argument(
        "--reference-cmd",
        help="Shell-style command template with {wav} and optional {mode}. Defaults to stock WSJT-X.",
    )
    parser.add_argument("--max-candidates", type=int, default=600)
    parser.add_argument("--search-passes", type=int, default=3)
    parser.add_argument("--jobs", type=int, default=4)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    manifest = json.loads(Path(args.manifest).read_text())
    reference_template = args.reference_cmd or mode_parity.default_stock_reference_template(args.profile)
    cases = [case for case in manifest["cases"] if case.get("mode", args.mode) == args.mode]
    rows = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as executor:
        futures = [
            executor.submit(build_case_chart, case, args, reference_template)
            for case in cases
        ]
        for future in concurrent.futures.as_completed(futures):
            rows.append(future.result())
    rows.sort(key=lambda row: row["id"])

    mismatch_counts = Counter(row["first_mismatch"] for row in rows)
    payload = {
        "kind": "stage-chart",
        "manifest": str(Path(args.manifest).resolve()),
        "mode": args.mode,
        "profile": args.profile,
        "decoder_binary": str(Path(args.decoder_binary).resolve()),
        "reference_cmd": reference_template,
        "case_count": len(rows),
        "first_mismatch_counts": dict(sorted(mismatch_counts.items())),
        "cases": rows,
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2) + "\n")
    print(
        json.dumps(
            {
                "output": str(output.resolve()),
                "case_count": len(rows),
                "first_mismatch_counts": payload["first_mismatch_counts"],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
