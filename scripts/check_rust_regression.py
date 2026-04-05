#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path


KEY_FIELDS = ("profile_id", "dataset_id")
VALUE_FIELDS = (
    "dataset_kind",
    "samples",
    "decode_count",
    "truth_count",
    "scored_truth_count",
    "tp",
    "fp",
    "fn",
)


def load_expected(path: Path) -> dict[tuple[str, str], dict[str, int | str]]:
    rows = json.loads(path.read_text())
    return {(row["profile_id"], row["dataset_id"]): row for row in rows}


def load_summary(path: Path) -> dict[tuple[str, str], dict[str, int | str]]:
    rows: dict[tuple[str, str], dict[str, int | str]] = {}
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            key = (row["profile_id"], row["dataset_id"])
            rows[key] = {
                "profile_id": row["profile_id"],
                "dataset_id": row["dataset_id"],
                "dataset_kind": row["dataset_kind"],
                "samples": int(row["samples"]),
                "decode_count": int(row["decode_count"]),
                "truth_count": int(row["truth_count"]),
                "scored_truth_count": int(row["scored_truth_count"]),
                "tp": int(row["tp"]),
                "fp": int(row["fp"]),
                "fn": int(row["fn"]),
            }
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Check Rust decoder regression metrics.")
    parser.add_argument("--summary", required=True, help="Path to run-rust summary.csv")
    parser.add_argument("--expected", required=True, help="Path to expected JSON metrics")
    args = parser.parse_args()

    summary = load_summary(Path(args.summary))
    expected = load_expected(Path(args.expected))

    errors: list[str] = []

    missing = sorted(set(expected) - set(summary))
    unexpected = sorted(set(summary) - set(expected))
    if missing:
        errors.append(f"Missing rows: {missing}")
    if unexpected:
        errors.append(f"Unexpected rows: {unexpected}")

    for key in sorted(set(expected) & set(summary)):
        expected_row = expected[key]
        actual_row = summary[key]
        diffs = []
        for field in VALUE_FIELDS:
            if expected_row[field] != actual_row[field]:
                diffs.append(f"{field}: expected {expected_row[field]!r}, got {actual_row[field]!r}")
        if diffs:
            errors.append(f"{key}: " + "; ".join(diffs))

    if errors:
        print("Rust decoder regression mismatch:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1

    print("Rust decoder regression metrics match checked-in expectation.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
