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
from collections import defaultdict
from datetime import datetime, timezone
from html import escape
from os.path import relpath
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


def git_repo_web_url() -> str | None:
    remote = optional_command(["git", "remote", "get-url", "origin"])
    if not remote:
        return None
    remote = remote.strip()
    if remote.startswith("git@github.com:"):
        path = remote.removeprefix("git@github.com:")
        if path.endswith(".git"):
            path = path[:-4]
        return f"https://github.com/{path}"
    if remote.startswith("https://github.com/"):
        url = remote[:-4] if remote.endswith(".git") else remote
        return url
    return None


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


def parse_timestamp(value: str | None) -> datetime:
    if not value:
        return datetime.min.replace(tzinfo=timezone.utc)
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def relative_href(base_dir: Path, target: Path) -> str:
    return Path(relpath(target, start=base_dir)).as_posix()


def format_list(values: list[str]) -> str:
    return ", ".join(values) if values else "n/a"


def load_snapshot_entries(golden_root: Path) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for metadata_path in sorted(golden_root.glob("*/*/metadata.json")):
        snapshot_root = metadata_path.parent
        metadata = json.loads(metadata_path.read_text())
        results_source = metadata.get("results_source", {})
        runner = metadata.get("runner", {})
        entries.append(
            {
                "snapshot_kind": metadata.get("snapshot_kind", snapshot_root.parent.name),
                "snapshot_root": snapshot_root,
                "metadata_path": metadata_path,
                "report_path": snapshot_root / "report" / "index.html",
                "summary_path": snapshot_root / "results" / "summary.csv",
                "results_path": snapshot_root / "results" / "results.json",
                "source_commit": str(results_source.get("commit", "unknown")),
                "source_ref": str(results_source.get("ref", "unknown")),
                "run_id": str(results_source.get("run_id", snapshot_root.name)),
                "generated_at": str(results_source.get("generated_at", "")),
                "profiles": [str(value) for value in results_source.get("profiles", [])],
                "datasets": [str(value) for value in results_source.get("datasets", [])],
                "release_count": results_source.get("release_count"),
                "run_count": results_source.get("run_count"),
                "platform_type": str(runner.get("platform_type", "unknown")),
                "os_name": str(runner.get("os_name", "unknown")),
                "os_version": str(runner.get("os_version", "unknown")),
                "cpu_arch": str(runner.get("cpu_arch", "unknown")),
                "cpu_brand": str(runner.get("cpu_brand", "unknown")),
            }
        )
    entries.sort(
        key=lambda item: (
            item["snapshot_kind"],
            parse_timestamp(item["generated_at"]),
            item["run_id"],
        ),
        reverse=True,
    )
    return entries


def render_toc(entries: list[dict[str, Any]], base_dir: Path) -> str:
    if not entries:
        return "<p class=\"empty\">No saved golden snapshots yet.</p>"
    items: list[str] = []
    for entry in entries:
        if not entry["report_path"].exists():
            continue
        href = relative_href(base_dir, entry["report_path"])
        label = f"{entry['snapshot_kind']} / {entry['run_id']}"
        detail = (
            f"{entry['source_commit']} on {entry['platform_type']} "
            f"{entry['cpu_arch']} {entry['os_name']} {entry['os_version']}"
        )
        items.append(
            f"<li><a href=\"{escape(href)}\">{escape(label)}</a>"
            f"<span>{escape(detail)}</span></li>"
        )
    if not items:
        return "<p class=\"empty\">No saved golden snapshots with reports yet.</p>"
    return "<ol class=\"toc-list\">\n" + "\n".join(items) + "\n</ol>"


def render_snapshot_tables(entries: list[dict[str, Any]], base_dir: Path) -> str:
    if not entries:
        return ""
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for entry in entries:
        grouped[entry["snapshot_kind"]].append(entry)

    sections: list[str] = []
    for snapshot_kind in sorted(grouped):
        rows: list[str] = []
        for entry in grouped[snapshot_kind]:
            report_link = (
                f"<a href=\"{escape(relative_href(base_dir, entry['report_path']))}\">report</a>"
                if entry["report_path"].exists()
                else "n/a"
            )
            summary_link = (
                f"<a href=\"{escape(relative_href(base_dir, entry['summary_path']))}\">summary.csv</a>"
                if entry["summary_path"].exists()
                else "n/a"
            )
            results_link = (
                f"<a href=\"{escape(relative_href(base_dir, entry['results_path']))}\">results.json</a>"
                if entry["results_path"].exists()
                else "n/a"
            )
            metadata_link = f"<a href=\"{escape(relative_href(base_dir, entry['metadata_path']))}\">metadata.json</a>"
            host = (
                f"{entry['platform_type']} / {entry['cpu_arch']} / "
                f"{entry['os_name']} {entry['os_version']} / {entry['cpu_brand']}"
            )
            rows.append(
                "<tr>"
                f"<td><code>{escape(entry['run_id'])}</code></td>"
                f"<td><code>{escape(entry['source_commit'])}</code><br><span class=\"subtle\">{escape(entry['source_ref'])}</span></td>"
                f"<td>{escape(entry['generated_at'] or 'n/a')}</td>"
                f"<td>{escape(format_list(entry['profiles']))}</td>"
                f"<td>{escape(format_list(entry['datasets']))}</td>"
                f"<td>{escape(host)}</td>"
                f"<td>{entry['release_count'] if entry['release_count'] is not None else 'n/a'}</td>"
                f"<td>{entry['run_count'] if entry['run_count'] is not None else 'n/a'}</td>"
                f"<td>{report_link} | {summary_link} | {results_link} | {metadata_link}</td>"
                "</tr>"
            )
        sections.append(
            "<section class=\"snapshot-section\">"
            f"<h2>{escape(snapshot_kind)}</h2>"
            "<div class=\"table-wrap\">"
            "<table>"
            "<thead><tr>"
            "<th>Run ID</th>"
            "<th>Source</th>"
            "<th>Generated At</th>"
            "<th>Profiles</th>"
            "<th>Datasets</th>"
            "<th>Host</th>"
            "<th>Releases</th>"
            "<th>Jobs</th>"
            "<th>Artifacts</th>"
            "</tr></thead>"
            "<tbody>"
            + "".join(rows)
            + "</tbody></table></div></section>"
        )
    return "\n".join(sections)


def render_index_page(
    *,
    title: str,
    heading: str,
    intro: str,
    entries: list[dict[str, Any]],
    base_dir: Path,
    extra_links: list[tuple[str, str]],
) -> str:
    nav_links = " ".join(
        f"<a href=\"{escape(href)}\">{escape(label)}</a>" for label, href in extra_links
    )
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <style>
    :root {{
      --bg: #f6f1e8;
      --panel: #fffaf2;
      --ink: #18222b;
      --muted: #5f6a72;
      --line: #d8cdc0;
      --accent: #0d6b57;
      --accent-soft: #d8efe8;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Avenir Next", "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at top left, #fff7d6 0, transparent 28rem),
        linear-gradient(180deg, #f2eadf 0%, var(--bg) 55%, #efe6d8 100%);
      color: var(--ink);
    }}
    main {{
      max-width: 1100px;
      margin: 0 auto;
      padding: 3rem 1.25rem 4rem;
    }}
    .hero {{
      background: rgba(255, 250, 242, 0.9);
      border: 1px solid var(--line);
      border-radius: 24px;
      padding: 2rem;
      box-shadow: 0 18px 50px rgba(24, 34, 43, 0.08);
    }}
    h1, h2 {{ margin: 0 0 0.8rem; }}
    h1 {{
      font-size: clamp(2rem, 4vw, 3.4rem);
      line-height: 0.95;
      letter-spacing: -0.04em;
    }}
    h2 {{
      font-size: 1.2rem;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      color: var(--muted);
    }}
    p {{
      margin: 0 0 1rem;
      line-height: 1.55;
      max-width: 70ch;
    }}
    .nav {{
      display: flex;
      flex-wrap: wrap;
      gap: 0.9rem;
      margin-top: 1.25rem;
    }}
    .nav a, .toc-list a, table a {{
      color: var(--accent);
      text-decoration: none;
      font-weight: 700;
    }}
    .nav a:hover, .toc-list a:hover, table a:hover {{
      text-decoration: underline;
    }}
    .panel {{
      background: rgba(255, 250, 242, 0.86);
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 1.5rem;
      margin-top: 1.25rem;
    }}
    .toc-list {{
      margin: 0;
      padding-left: 1.4rem;
    }}
    .toc-list li {{
      margin: 0 0 0.8rem;
      line-height: 1.45;
    }}
    .toc-list span {{
      display: block;
      color: var(--muted);
      font-size: 0.94rem;
    }}
    .table-wrap {{
      overflow-x: auto;
      border: 1px solid var(--line);
      border-radius: 16px;
      background: var(--panel);
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      min-width: 920px;
    }}
    th, td {{
      text-align: left;
      vertical-align: top;
      padding: 0.85rem 0.9rem;
      border-bottom: 1px solid var(--line);
    }}
    th {{
      background: var(--accent-soft);
      color: #10342b;
      font-size: 0.9rem;
      letter-spacing: 0.02em;
    }}
    tbody tr:nth-child(even) td {{
      background: rgba(216, 239, 232, 0.22);
    }}
    code {{
      font-family: "SFMono-Regular", "Menlo", monospace;
      font-size: 0.92em;
    }}
    .subtle, .empty, footer {{
      color: var(--muted);
    }}
    footer {{
      margin-top: 2rem;
      font-size: 0.95rem;
    }}
    @media (max-width: 720px) {{
      main {{ padding-top: 1.5rem; }}
      .hero, .panel {{ padding: 1.2rem; border-radius: 18px; }}
    }}
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <h1>{heading}</h1>
      <p>{intro}</p>
      <nav class="nav">{nav_links}</nav>
    </section>
    <section class="panel">
      <h2>Report TOC</h2>
      {toc}
    </section>
    <section class="panel">
      <h2>Snapshot Details</h2>
      {tables}
    </section>
    <footer>Generated from checked-in <code>golden</code> snapshots by <code>scripts/archive_golden_run.py</code>.</footer>
  </main>
</body>
</html>
""".format(
        title=escape(title),
        heading=escape(heading),
        intro=escape(intro),
        nav_links=nav_links,
        toc=render_toc(entries, base_dir),
        tables=render_snapshot_tables(entries, base_dir),
    )


def write_text_if_changed(path: Path, contents: str) -> None:
    if path.exists() and path.read_text() == contents:
        return
    path.write_text(contents)


def refresh_site_indexes(golden_root: Path) -> list[Path]:
    entries = load_snapshot_entries(golden_root)
    written: list[Path] = []
    repo_web_url = git_repo_web_url()

    golden_root.mkdir(parents=True, exist_ok=True)
    golden_index_path = golden_root / "index.html"
    golden_index_contents = render_index_page(
        title="FT8X Golden Snapshot Index",
        heading="Golden Regression Snapshot Index",
        intro="Saved regression runs checked into the repository, with direct links to every archived report, summary, results payload, and metadata file.",
        entries=entries,
        base_dir=golden_root,
        extra_links=[
            ("GitHub README", f"{repo_web_url}#readme" if repo_web_url else relative_href(golden_root, REPO_ROOT / "README.md")),
            ("Site Home", relative_href(golden_root, REPO_ROOT / "index.html")),
        ],
    )
    write_text_if_changed(golden_index_path, golden_index_contents)
    written.append(golden_index_path)

    if golden_root.resolve() == DEFAULT_GOLDEN_ROOT.resolve():
        root_index_path = REPO_ROOT / "index.html"
        root_index_contents = render_index_page(
            title="FT8X Regression Reports",
            heading="FT8X Regression Reports",
            intro="Landing page for the checked-in golden regression runs. Use the report TOC below to jump straight from GitHub Pages to any archived WSJT-X regression summary page.",
            entries=entries,
            base_dir=REPO_ROOT,
            extra_links=[
                ("GitHub README", f"{repo_web_url}#readme" if repo_web_url else "README.md"),
                ("Golden Snapshot Index", "golden/index.html"),
                ("GitHub Repo", repo_web_url or "https://github.com/bgelb/ft8x"),
            ],
        )
        write_text_if_changed(root_index_path, root_index_contents)
        written.append(root_index_path)

    return written


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
    parser.add_argument(
        "--refresh-indexes-only",
        action="store_true",
        help="Regenerate the golden snapshot landing pages without copying a snapshot.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    golden_root = Path(args.golden_root).expanduser().resolve()

    if args.refresh_indexes_only:
        written = refresh_site_indexes(golden_root)
        for path in written:
            print(path)
        return 0

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
    refresh_site_indexes(golden_root)
    print(snapshot_root)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
