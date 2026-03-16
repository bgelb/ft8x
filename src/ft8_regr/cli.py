from __future__ import annotations

import argparse
from pathlib import Path

from .core import (
    default_paths,
    discover_datasets,
    discover_releases,
    ensure_directories,
    generate_report,
    run_benchmarks,
    sync_releases,
    sync_samples,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="FT8 WSJT-X regression prototype")
    parser.add_argument(
        "--root",
        default=".",
        help="Repository root. Defaults to the current working directory.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    discover_parser = subparsers.add_parser("discover", help="Discover releases and datasets")
    discover_parser.add_argument(
        "--verify-downloads",
        action="store_true",
        help="Issue HEAD requests for discovered release download URLs.",
    )

    sync_samples_parser = subparsers.add_parser("sync-samples", help="Download sample corpora")
    sync_samples_parser.add_argument("--datasets", nargs="*", help="Dataset ids to sync")
    sync_samples_parser.add_argument("--sample-limit", type=int, help="Limit samples per dataset")

    sync_releases_parser = subparsers.add_parser("sync-releases", help="Download and extract WSJT-X releases")
    sync_releases_parser.add_argument("--versions", nargs="*", help="Versions to sync")

    run_parser = subparsers.add_parser("run", help="Run the benchmark matrix")
    run_parser.add_argument("--versions", nargs="*", help="Versions to benchmark")
    run_parser.add_argument("--datasets", nargs="*", help="Dataset ids to benchmark")
    run_parser.add_argument("--profiles", nargs="*", help="Profile ids to benchmark")
    run_parser.add_argument("--sample-limit", type=int, help="Limit samples per dataset")
    run_parser.add_argument("--force", action="store_true", help="Re-run existing raw decode jobs")
    run_parser.add_argument("--jobs", type=int, help="Concurrent decoder jobs to run")

    report_parser = subparsers.add_parser("report", help="Render static HTML report")
    report_parser.add_argument("--results", help="Optional path to a results.json file")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    paths = default_paths(Path(args.root))
    ensure_directories(paths)

    if args.command == "discover":
        discover_releases(paths, verify=args.verify_downloads)
        discover_datasets(paths)
        print(f"discovery written to {paths.discovery}")
        return

    if args.command == "sync-samples":
        sync_samples(
            paths,
            dataset_filter=set(args.datasets or []) or None,
            sample_limit=args.sample_limit,
        )
        print(f"samples synced under {paths.samples}")
        return

    if args.command == "sync-releases":
        sync_releases(paths, version_filter=set(args.versions or []) or None)
        print(f"releases synced under {paths.releases}")
        return

    if args.command == "run":
        payload = run_benchmarks(
            paths,
            versions=args.versions,
            datasets=args.datasets,
            profiles=args.profiles,
            sample_limit=args.sample_limit,
            force=args.force,
            jobs=args.jobs,
        )
        report_path = generate_report(paths, paths.results / payload["run_id"] / "results.json")
        print(f"results written to {paths.results / payload['run_id']}")
        print(f"report written to {report_path}")
        return

    if args.command == "report":
        report_path = generate_report(
            paths,
            Path(args.results).resolve() if args.results else None,
        )
        print(f"report written to {report_path}")
        return


if __name__ == "__main__":
    main()
