#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import subprocess
from pathlib import Path

from mode_parity import parse_decode_records


def norm_messages(records: list[dict]) -> list[str]:
    return sorted(record["message"] for record in records)


def norm_rust_decode_messages(decodes: list[dict]) -> list[str]:
    return sorted(decode["text"].strip().upper() for decode in decodes)


def run_json(command: list[str], cwd: Path) -> dict:
    completed = subprocess.run(command, cwd=cwd, check=True, capture_output=True, text=True)
    return json.loads(completed.stdout)


def run_text(command: list[str], cwd: Path) -> str:
    completed = subprocess.run(command, cwd=cwd, check=True, capture_output=True, text=True)
    return completed.stdout


def compare_freq_lists(stock: list[float], rust: list[float], tolerance_hz: float) -> bool:
    if len(stock) != len(rust):
        return False
    return all(abs(left - right) <= tolerance_hz for left, right in zip(stock, rust))


def compare_residual_signatures(stock: dict | None, rust: dict | None) -> bool:
    if stock is None or rust is None:
        return stock is None and rust is None
    sample_sum_delta = abs(stock["sample_sum"] - rust["sample_sum"])
    if sample_sum_delta > 5e-3:
        return False
    sample_sq_delta = abs(stock["sample_sq_sum"] - rust["sample_sq_sum"])
    sample_sq_scale = max(abs(stock["sample_sq_sum"]), abs(rust["sample_sq_sum"]), 1.0)
    if sample_sq_delta > max(0.25, sample_sq_scale * 5e-4):
        return False
    stock_probes = stock.get("probe_values", [])
    rust_probes = rust.get("probe_values", [])
    if len(stock_probes) != len(rust_probes):
        return False
    return all(abs(left - right) <= 5e-5 for left, right in zip(stock_probes, rust_probes))


def ft2_row(case: dict, cwd: Path, profile: str) -> tuple[dict, list[str]]:
    wav = case["wav_path"]
    stock_trace = run_json(["python3", "scripts/run_stock_ft2_trace.py", wav], cwd)
    rust_trace = run_json(
        [
            "decoder/target/release/ft8-decoder",
            "debug-ft2-trace",
            wav,
            "--profile",
            profile,
        ],
        cwd,
    )
    stock_final = norm_messages(
        parse_decode_records(run_text(["python3", "scripts/run_stock_decode.py", wav, "ft2", "--profile", profile], cwd))
    )
    rust_final = norm_rust_decode_messages(
        json.loads(
            run_text(
                [
                    "decoder/target/release/ft8-decoder",
                    "decode",
                    wav,
                    "--mode",
                    "ft2",
                    "--profile",
                    profile,
                    "--json",
                ],
                cwd,
            )
        )["decodes"]
    )

    row: dict[str, object] = {
        "id": case["id"],
        "mode": "ft2",
        "final_match": stock_final == rust_final,
        "stock_final": stock_final,
        "rust_final": rust_final,
    }
    stages: list[str] = []

    stock_candidates = stock_trace.get("candidates", [])
    rust_candidates = rust_trace.get("candidates", [])
    row["candidate_match"] = (
        len(stock_candidates) == len(rust_candidates)
        and compare_freq_lists(
            [candidate["candidate_f0"] for candidate in stock_candidates],
            [candidate["coarse_freq_hz"] for candidate in rust_candidates],
            1.0,
        )
    )
    stages.append("candidate_match")

    if stock_candidates and rust_candidates:
        stock_c = stock_candidates[0]
        rust_c = rust_candidates[0]
        row["refine_match"] = (
            abs(stock_c.get("best_df", 0.0) - rust_c.get("best_df_hz", 0.0)) <= 0.1
            and int(stock_c.get("best_ibest", -1)) == int(rust_c.get("best_ibest", -2))
        )
    else:
        row["refine_match"] = False
    stages.append("refine_match")

    for nseq in range(1, 6):
        stage_sync = f"nseq{nseq}_sync_match"
        stage_llr = f"nseq{nseq}_llr_match"
        stage_bp = f"nseq{nseq}_bp_match"
        stock_seq = next((seq for seq in stock_candidates[0].get("sequences", []) if seq["nseq"] == nseq), None) if stock_candidates else None
        rust_seq = next((seq for seq in rust_candidates[0].get("sequences", []) if seq["nseq"] == nseq), None) if rust_candidates else None
        row[stage_sync] = (
            (stock_seq is None and rust_seq is None)
            or (
                stock_seq is not None
                and rust_seq is not None
                and stock_seq["sync_ok"] == rust_seq["sync_ok"]
            )
        )
        if stock_seq is not None and rust_seq is not None and stock_seq.get("llrs") and rust_seq.get("llrs"):
            deltas = [abs(left - right) for left, right in zip(stock_seq["llrs"], rust_seq["llrs"])]
            row[stage_llr] = max(deltas, default=0.0) <= 0.05
        else:
            row[stage_llr] = stock_seq is None and rust_seq is None
        row[stage_bp] = (
            (stock_seq is None and rust_seq is None)
            or (
                stock_seq is not None
                and rust_seq is not None
                and (stock_seq.get("decoded", "").strip().upper() or None) == rust_seq.get("decoded_text")
            )
        )
        stages.extend([stage_sync, stage_llr, stage_bp])

    row["earliest_divergence"] = next((stage for stage in stages + ["final_match"] if not row.get(stage, False)), "")
    return row, stages + ["final_match"]


def ft4_row(case: dict, cwd: Path, profile: str) -> tuple[dict, list[str]]:
    wav = case["wav_path"]
    stock_component = run_json(
        [
            "python3",
            "scripts/run_stock_ft4_component.py",
            wav,
            "--profile",
            profile,
            "--search-passes",
            "3",
            "--max-candidates",
            "50",
        ],
        cwd,
    )
    stock_trace = run_json(["python3", "scripts/run_stock_ft4_trace.py", wav, "--profile", profile], cwd)
    rust_trace = run_json(
        [
            "decoder/target/release/ft8-decoder",
            "debug-search",
            wav,
            "--mode",
            "ft4",
            "--profile",
            profile,
            "--max-candidates",
            "50",
            "--search-passes",
            "3",
        ],
        cwd,
    )
    stock_final = norm_messages(
        parse_decode_records(run_text(["python3", "scripts/run_stock_decode.py", wav, "ft4", "--profile", profile], cwd))
    )
    rust_final = norm_rust_decode_messages(rust_trace["final_report"]["decodes"])

    row: dict[str, object] = {
        "id": case["id"],
        "mode": "ft4",
        "final_match": stock_final == rust_final,
        "stock_final": stock_final,
        "rust_final": rust_final,
    }
    stages: list[str] = []

    stock_component_passes = {
        entry["pass_index"]: entry for entry in stock_component.get("passes", [])
    }
    stock_trace_passes = {entry["pass_index"]: entry for entry in stock_trace.get("passes", [])}
    rust_passes = {entry["pass_index"] + 1: entry for entry in rust_trace.get("passes", [])}
    for pass_index in (1, 2, 3):
        residual_stage = f"pass{pass_index}_residual_match"
        cand_stage = f"pass{pass_index}_candidate_match"
        raw_stage = f"pass{pass_index}_raw_match"
        stock_component_pass = stock_component_passes.get(pass_index, {})
        stock_trace_pass = stock_trace_passes.get(pass_index, {})
        rust_pass = rust_passes.get(pass_index, {})
        row[residual_stage] = compare_residual_signatures(
            stock_trace_pass.get("residual_signature"),
            rust_pass.get("residual_signature"),
        )
        stock_freqs = [
            candidate["freq_hz"]
            for candidate in stock_component_pass.get("search", {}).get("candidates", [])
        ]
        rust_freqs = [candidate["coarse_freq_hz"] for candidate in rust_pass.get("candidates", [])]
        row[cand_stage] = compare_freq_lists(stock_freqs, rust_freqs, 2.0)
        stock_raw = sorted(stock_component_pass.get("accepted_messages", []))
        rust_raw = sorted(
            message
            for candidate in rust_pass.get("candidates", [])
            for message in candidate.get("accepted_successes", [])
        )
        row[raw_stage] = stock_raw == rust_raw
        stages.extend([residual_stage, cand_stage, raw_stage])

    row["earliest_divergence"] = next((stage for stage in stages + ["final_match"] if not row.get(stage, False)), "")
    return row, stages + ["final_match"]


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a per-sample stage divergence chart for a distilled manifest.")
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--mode", required=True, choices=["ft4", "ft2"])
    parser.add_argument("--profile", default="medium", choices=["medium", "deepest"])
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--output-csv", required=True)
    args = parser.parse_args()

    cwd = Path(__file__).resolve().parent.parent
    manifest = json.loads(Path(args.manifest).read_text())
    rows = []
    stage_columns: list[str] = []

    for case in manifest["cases"]:
        if args.mode == "ft2":
            row, cols = ft2_row(case, cwd, args.profile)
        else:
            row, cols = ft4_row(case, cwd, args.profile)
        if not stage_columns:
            stage_columns = cols
        rows.append(row)

    output_json = Path(args.output_json).resolve()
    output_csv = Path(args.output_csv).resolve()
    output_json.write_text(
        json.dumps(
            {
                "manifest": str(Path(args.manifest).resolve()),
                "mode": args.mode,
                "profile": args.profile,
                "stage_columns": stage_columns,
                "rows": rows,
            },
            indent=2,
        )
        + "\n"
    )

    with output_csv.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["id", *stage_columns, "earliest_divergence"])
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in writer.fieldnames})

    print(output_json)
    print(output_csv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
