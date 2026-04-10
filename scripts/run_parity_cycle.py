#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import subprocess
from collections import Counter
from pathlib import Path


def run(command: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=cwd, check=True, capture_output=True, text=True)


def load_json(path: Path) -> dict:
    return json.loads(path.read_text())


def compute_frontier(chart_payload: dict, compare_payload: dict, mode: str) -> dict:
    rows = chart_payload["rows"]
    stage_columns = chart_payload["stage_columns"]
    earliest_counts = Counter(row["earliest_divergence"] for row in rows if row["earliest_divergence"])
    earliest_stage = ""
    for stage in stage_columns:
        if earliest_counts.get(stage):
            earliest_stage = stage
            break

    compare_results = {
        result["id"]: result
        for result in compare_payload["results"]
        if result["mode"] == mode and result.get("cohort") == "mixed"
    }
    frontier_cases = []
    for row in rows:
        if row["earliest_divergence"] != earliest_stage:
            continue
        compare_result = compare_results.get(row["id"], {})
        frontier_cases.append(
            {
                "id": row["id"],
                "earliest_divergence": row["earliest_divergence"],
                "final_match": row["final_match"],
                "compare_match": compare_result.get("match"),
                "rust_only_messages": compare_result.get("rust_only_messages", []),
                "stock_only_messages": compare_result.get("stock_only_messages", []),
                "truth_messages": compare_result.get("truth_messages", []),
            }
        )

    return {
        "mode": mode,
        "row_count": len(rows),
        "stage_columns": stage_columns,
        "earliest_divergence_counts": dict(earliest_counts),
        "frontier_stage": earliest_stage,
        "frontier_case_count": len(frontier_cases),
        "frontier_cases": frontier_cases,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the parity frontier cycle: distilled mixed set, stage chart, triage, and frontier summary."
    )
    parser.add_argument("--compare", required=True, help="mode_parity compare JSON covering the source corpus.")
    parser.add_argument("--manifest", required=True, help="Manifest JSON referenced by the compare payload.")
    parser.add_argument("--profile", default="medium", choices=["medium", "deepest"])
    parser.add_argument("--modes", nargs="+", default=["ft4", "ft2"], choices=["ft4", "ft2"])
    parser.add_argument("--seed", type=int, default=0, help="Random seed for distilled control selection.")
    parser.add_argument(
        "--decoder-binary",
        default="decoder/target/release/ft8-decoder",
        help="Path to the Rust decoder binary for triage reruns.",
    )
    parser.add_argument(
        "--output-root",
        required=True,
        help="Directory receiving distilled manifests, stage charts, triage bundles, and summary files.",
    )
    parser.add_argument("--max-candidates", type=int, default=600)
    parser.add_argument("--raised-max-candidates", type=int, default=4000)
    parser.add_argument("--search-passes", type=int, default=3)
    parser.add_argument("--extra-search-passes", type=int, default=6)
    parser.add_argument(
        "--skip-triage",
        action="store_true",
        help="Skip triage bundle generation and only rebuild distilled manifests/charts/summary.",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    compare_path = Path(args.compare).resolve()
    manifest_path = Path(args.manifest).resolve()
    output_root = Path(args.output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    compare_payload = load_json(compare_path)

    triage_dir = output_root / "triage"
    if not args.skip_triage:
        triage_dir.mkdir(parents=True, exist_ok=True)
        run(
            [
                "python3",
                "scripts/mode_parity.py",
                "triage",
                "--compare",
                str(compare_path),
                "--decoder-binary",
                str(Path(args.decoder_binary).resolve()),
                "--output-root",
                str(triage_dir),
                "--use-stock-reference",
                "--profile",
                args.profile,
                "--max-candidates",
                str(args.max_candidates),
                "--raised-max-candidates",
                str(args.raised_max_candidates),
                "--search-passes",
                str(args.search_passes),
                "--extra-search-passes",
                str(args.extra_search_passes),
            ],
            repo_root,
        )

    mode_summaries = []
    for mode in args.modes:
        mode_dir = output_root / mode
        mode_dir.mkdir(parents=True, exist_ok=True)
        distilled_path = mode_dir / f"{mode}-distilled-mixed-{args.profile}.json"
        chart_json = mode_dir / f"{mode}-stage-chart-{args.profile}.json"
        chart_csv = mode_dir / f"{mode}-stage-chart-{args.profile}.csv"

        run(
            [
                "python3",
                "scripts/build_distilled_mixed_manifest.py",
                "--compare",
                str(compare_path),
                "--manifest",
                str(manifest_path),
                "--mode",
                mode,
                "--seed",
                str(args.seed),
                "--output",
                str(distilled_path),
            ],
            repo_root,
        )
        run(
            [
                "python3",
                "scripts/build_stage_chart.py",
                "--manifest",
                str(distilled_path),
                "--mode",
                mode,
                "--profile",
                args.profile,
                "--output-json",
                str(chart_json),
                "--output-csv",
                str(chart_csv),
            ],
            repo_root,
        )

        chart_payload = load_json(chart_json)
        frontier = compute_frontier(chart_payload, compare_payload, mode)
        frontier["distilled_manifest"] = str(distilled_path)
        frontier["stage_chart_json"] = str(chart_json)
        frontier["stage_chart_csv"] = str(chart_csv)
        frontier["triage_root"] = str(triage_dir) if not args.skip_triage else None
        frontier_path = mode_dir / f"{mode}-frontier-{args.profile}.json"
        frontier_path.write_text(json.dumps(frontier, indent=2) + "\n")
        frontier["frontier_summary_path"] = str(frontier_path)
        mode_summaries.append(frontier)

    summary = {
        "compare": str(compare_path),
        "manifest": str(manifest_path),
        "profile": args.profile,
        "seed": args.seed,
        "modes": mode_summaries,
    }
    summary_path = output_root / f"parity-cycle-summary-{args.profile}.json"
    summary_path.write_text(json.dumps(summary, indent=2) + "\n")
    print(summary_path)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
