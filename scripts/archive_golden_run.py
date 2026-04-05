#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import platform
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RESULTS_DIR = REPO_ROOT / "artifacts" / "results" / "latest"
DEFAULT_GOLDEN_ROOT = REPO_ROOT / "golden"


def run_command(command: list[str]) -> str:
    return subprocess.check_output(command, text=True).strip()


def optional_command(command: list[str]) -> str | None:
    try:
        value = run_command(command)
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None
    return value or None


def slugify(value: str) -> str:
    lowered = value.strip().lower()
    lowered = re.sub(r"[^a-z0-9]+", "-", lowered)
    lowered = re.sub(r"-{2,}", "-", lowered)
    return lowered.strip("-") or "unknown"


def safe_component(value: str, *, lowercase: bool, allow_dots: bool) -> str:
    normalized = value.strip()
    if lowercase:
        normalized = normalized.lower()
    allowed = r"[^A-Za-z0-9.\-]+" if allow_dots else r"[^A-Za-z0-9\-]+"
    normalized = re.sub(allowed, "-", normalized)
    normalized = re.sub(r"-{2,}", "-", normalized)
    normalized = normalized.strip("-")
    return normalized or "unknown"


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_dir(path_value: str, expected_file: str) -> Path:
    path = Path(path_value).expanduser().resolve()
    if path.is_dir():
        return path
    if path.is_file() and path.name == expected_file:
        return path.parent
    raise FileNotFoundError(f"Expected a directory or {expected_file}: {path}")


def infer_report_dir(results_dir: Path) -> Path | None:
    parent = results_dir.parent
    if parent.name != "results":
        return None
    candidate = parent.parent / "reports" / results_dir.name
    if candidate.exists():
        return candidate.resolve()
    return None


def detect_host_info() -> dict[str, str]:
    system = platform.system() or "unknown"
    machine = platform.machine() or "unknown"
    if system == "Darwin":
        os_name = "macOS"
        os_version = optional_command(["sw_vers", "-productVersion"]) or platform.mac_ver()[0] or "unknown"
        os_build = optional_command(["sw_vers", "-buildVersion"]) or "unknown"
        cpu_brand = optional_command(["sysctl", "-n", "machdep.cpu.brand_string"]) or machine
    else:
        os_name = system
        os_version = platform.release() or "unknown"
        os_build = platform.version() or "unknown"
        cpu_brand = platform.processor() or machine
    return {
        "platform_type": system,
        "os_name": os_name,
        "os_version": os_version,
        "os_build": os_build,
        "cpu_arch": machine,
        "cpu_brand": cpu_brand,
    }


def git_default_ref() -> str:
    ref = optional_command(["git", "rev-parse", "--abbrev-ref", "HEAD"])
    return ref or "HEAD"


def git_default_commit() -> str:
    commit = optional_command(["git", "rev-parse", "--short=7", "HEAD"])
    return commit or "unknown"


def payload_profile_ids(payload: dict[str, Any]) -> list[str]:
    profiles = payload.get("profiles") or []
    ids: list[str] = []
    for profile in profiles:
        if isinstance(profile, dict):
            ids.append(str(profile.get("id") or profile.get("profile_id") or profile))
        else:
            ids.append(str(profile))
    return ids


def payload_dataset_ids(payload: dict[str, Any]) -> list[str]:
    datasets = payload.get("datasets") or []
    ids: list[str] = []
    for dataset in datasets:
        if isinstance(dataset, dict):
            ids.append(str(dataset.get("id") or dataset.get("dataset_id") or dataset))
        else:
            ids.append(str(dataset))
    return ids


def build_metadata(
    payload: dict[str, Any],
    snapshot_kind: str,
    source_ref: str,
    source_commit: str,
    command: str | None,
    host_info: dict[str, str],
    include_report: bool,
) -> dict[str, Any]:
    results_source: dict[str, Any] = {
        "ref": source_ref,
        "commit": source_commit,
        "run_id": payload["run_id"],
        "generated_at": payload.get("generated_at"),
        "profiles": payload_profile_ids(payload),
        "datasets": payload_dataset_ids(payload),
        "run_count": len(payload.get("runs", [])),
    }
    if command:
        results_source["command"] = command
    if "releases" in payload:
        results_source["release_count"] = len(payload.get("releases", []))
    if "decoder_id" in payload:
        results_source["decoder_id"] = payload.get("decoder_id")
    if "decoder_label" in payload:
        results_source["decoder_label"] = payload.get("decoder_label")

    snapshot_contents: dict[str, Any] = {"results_dir": "results"}
    if include_report:
        snapshot_contents["report_dir"] = "report"

    return {
        "snapshot_kind": snapshot_kind,
        "results_source": results_source,
        "runner": host_info,
        "snapshot_contents": snapshot_contents,
        "archived_at": utc_now(),
    }


def snapshot_slug(
    source_commit: str,
    host_info: dict[str, str],
    run_id: str,
) -> str:
    return "__".join(
        [
            slugify(source_commit),
            slugify(host_info["platform_type"]),
            slugify(host_info["cpu_arch"]),
            safe_component(f"{host_info['os_name']}-{host_info['os_version']}", lowercase=True, allow_dots=True),
            slugify(host_info["cpu_brand"]),
            safe_component(run_id, lowercase=False, allow_dots=False),
        ]
    )


def copy_snapshot(
    *,
    results_dir: Path,
    report_dir: Path | None,
    snapshot_root: Path,
    metadata: dict[str, Any],
    force: bool,
) -> None:
    if snapshot_root.exists():
        if not force:
            raise FileExistsError(f"Snapshot already exists: {snapshot_root}")
        if snapshot_root.is_dir():
            shutil.rmtree(snapshot_root)
        else:
            snapshot_root.unlink()

    parent = snapshot_root.parent
    parent.mkdir(parents=True, exist_ok=True)
    temp_root = Path(tempfile.mkdtemp(prefix=f".{snapshot_root.name}.", dir=parent))
    try:
        shutil.copytree(results_dir, temp_root / "results")
        if report_dir is not None:
            shutil.copytree(report_dir, temp_root / "report")
        (temp_root / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
        temp_root.rename(snapshot_root)
    except Exception:
        shutil.rmtree(temp_root, ignore_errors=True)
        raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Archive a regression run into the tracked golden snapshot layout."
    )
    parser.add_argument(
        "results_path",
        nargs="?",
        default=str(DEFAULT_RESULTS_DIR),
        help="Path to a results run directory or results.json file. Defaults to artifacts/results/latest.",
    )
    parser.add_argument(
        "--snapshot-kind",
        default="wsjtx-vs-version",
        help="Subdirectory under golden/ for this snapshot. Defaults to wsjtx-vs-version.",
    )
    parser.add_argument(
        "--report-path",
        help="Path to a matching report directory or index.html file. Defaults to the sibling artifacts/reports/<run-id> path when available.",
    )
    parser.add_argument(
        "--golden-root",
        default=str(DEFAULT_GOLDEN_ROOT),
        help="Root directory that holds tracked golden snapshots. Defaults to repo-root/golden.",
    )
    parser.add_argument(
        "--source-ref",
        default=git_default_ref(),
        help="Git ref or branch the archived results came from. Defaults to the current checkout ref.",
    )
    parser.add_argument(
        "--source-commit",
        default=git_default_commit(),
        help="Commit hash to record for the archived results. Defaults to the current checkout short hash.",
    )
    parser.add_argument(
        "--command",
        help="Optional command string to record in metadata.",
    )
    parser.add_argument(
        "--platform-type",
        help="Override the detected platform type.",
    )
    parser.add_argument(
        "--os-name",
        help="Override the detected operating system name.",
    )
    parser.add_argument(
        "--os-version",
        help="Override the detected operating system version.",
    )
    parser.add_argument(
        "--os-build",
        help="Override the detected operating system build identifier.",
    )
    parser.add_argument(
        "--cpu-arch",
        help="Override the detected CPU architecture.",
    )
    parser.add_argument(
        "--cpu-brand",
        help="Override the detected CPU brand string.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the resolved snapshot path and metadata without copying files.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Replace an existing snapshot directory if it already exists.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()

    results_dir = resolve_dir(args.results_path, "results.json")
    results_json_path = results_dir / "results.json"
    if not results_json_path.exists():
        raise FileNotFoundError(f"Missing results.json under {results_dir}")
    payload = json.loads(results_json_path.read_text())
    run_id = payload.get("run_id")
    if not run_id:
        raise ValueError(f"Missing run_id in {results_json_path}")

    report_dir: Path | None
    if args.report_path:
        report_dir = resolve_dir(args.report_path, "index.html")
    else:
        report_dir = infer_report_dir(results_dir)
    if report_dir is not None and not (report_dir / "index.html").exists():
        raise FileNotFoundError(f"Missing index.html under {report_dir}")

    host_info = detect_host_info()
    overrides = {
        "platform_type": args.platform_type,
        "os_name": args.os_name,
        "os_version": args.os_version,
        "os_build": args.os_build,
        "cpu_arch": args.cpu_arch,
        "cpu_brand": args.cpu_brand,
    }
    for key, value in overrides.items():
        if value:
            host_info[key] = value

    metadata = build_metadata(
        payload=payload,
        snapshot_kind=args.snapshot_kind,
        source_ref=args.source_ref,
        source_commit=args.source_commit,
        command=args.command,
        host_info=host_info,
        include_report=report_dir is not None,
    )

    golden_root = Path(args.golden_root).expanduser().resolve()
    snapshot_root = (
        golden_root
        / args.snapshot_kind
        / snapshot_slug(args.source_commit, host_info, run_id)
    )

    if args.dry_run:
        print(f"snapshot_root={snapshot_root}")
        print(json.dumps(metadata, indent=2))
        return 0

    copy_snapshot(
        results_dir=results_dir,
        report_dir=report_dir,
        snapshot_root=snapshot_root,
        metadata=metadata,
        force=args.force,
    )
    print(snapshot_root)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
