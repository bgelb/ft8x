from __future__ import annotations

import csv
import html
import json
import os
import platform
import plistlib
import re
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


TIMEOUT_SECONDS = 60
USER_AGENT = "ft8-regr-prototype/0.1"
VERSION_PATTERN = re.compile(r"^(?P<core>\d+\.\d+\.\d+)(?:-(?P<tag>rc)(?P<tag_number>\d+))?$")
ROOT_RELEASE_ROW_PATTERN = re.compile(
    r'<tr title="(?P<release>wsjtx-(?P<version>\d+\.\d+\.\d+(?:-rc\d+)?))" class="folder "\s*>.*?<abbr title="(?P<timestamp>[^"]+)"',
    re.S,
)
NET_SF_FILES_PATTERN = re.compile(r"net\.sf\.files = (\{.*?\});", re.S)
DECODE_PATTERN = re.compile(
    r"^(?P<utc>\d{6})\s+(?P<snr>-?\d+)\s+(?P<dt>-?\d+(?:\.\d+)?)\s+(?P<freq>\d+)\s+~\s+(?P<message>.+?)\s*$"
)


@dataclass(frozen=True)
class Paths:
    root: Path
    config: Path
    artifacts: Path
    cache: Path
    discovery: Path
    samples: Path
    releases: Path
    results: Path
    reports: Path
    temp: Path


def default_paths(root: Path | None = None) -> Paths:
    resolved_root = (root or Path.cwd()).resolve()
    artifacts = resolved_root / "artifacts"
    return Paths(
        root=resolved_root,
        config=resolved_root / "config" / "sources.json",
        artifacts=artifacts,
        cache=artifacts / "cache",
        discovery=artifacts / "discovery",
        samples=artifacts / "samples",
        releases=artifacts / "releases",
        results=artifacts / "results",
        reports=artifacts / "reports",
        temp=resolved_root / ".tmp",
    )


def ensure_directories(paths: Paths) -> None:
    for path in (
        paths.artifacts,
        paths.cache,
        paths.discovery,
        paths.samples,
        paths.releases,
        paths.results,
        paths.reports,
        paths.temp,
    ):
        path.mkdir(parents=True, exist_ok=True)


def load_config(paths: Paths) -> dict[str, Any]:
    return json.loads(paths.config.read_text())


def fetch_bytes(url: str, method: str = "GET") -> bytes:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json, text/plain, */*",
            "User-Agent": USER_AGENT,
        },
        method=method,
    )
    with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        return response.read()


def fetch_json(url: str) -> Any:
    return json.loads(fetch_bytes(url).decode("utf-8"))


def fetch_text(url: str) -> str:
    return fetch_bytes(url).decode("utf-8", errors="replace")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text())


def version_key(version: str) -> tuple[int, int, int, int, int]:
    match = VERSION_PATTERN.fullmatch(version)
    if not match:
        raise ValueError(f"Unsupported version format: {version}")
    major, minor, patch = match.group("core").split(".")
    tag = match.group("tag")
    tag_number = int(match.group("tag_number") or 0)
    stage_rank = 0 if tag == "rc" else 1
    return int(major), int(minor), int(patch), stage_rank, tag_number


def version_parts(version: str) -> tuple[int, int, int, str | None, int]:
    match = VERSION_PATTERN.fullmatch(version)
    if not match:
        raise ValueError(f"Unsupported version format: {version}")
    major, minor, patch = match.group("core").split(".")
    return (
        int(major),
        int(minor),
        int(patch),
        match.group("tag"),
        int(match.group("tag_number") or 0),
    )


def should_include_release(
    version: str,
    minimum_version: str,
    include_prerelease_major_min: int | None = None,
) -> bool:
    if version_key(version) < version_key(minimum_version):
        return False
    major, _, _, tag, _ = version_parts(version)
    if tag is None:
        return True
    if include_prerelease_major_min is None:
        return False
    return major >= include_prerelease_major_min


def normalize_message(message: str) -> str:
    return re.sub(r"\s+", " ", message.strip().upper())


def split_message_suffix(raw_message: str) -> tuple[str, str | None]:
    collapsed = raw_message.rstrip()
    parts = re.split(r"\s{2,}", collapsed, maxsplit=1)
    message = parts[0]
    suffix = parts[1].strip() if len(parts) > 1 else None
    return message, suffix


def parse_decode_lines(text: str) -> list[dict[str, Any]]:
    decodes: list[dict[str, Any]] = []
    for line in text.splitlines():
        match = DECODE_PATTERN.match(line)
        if not match:
            continue
        message_text, suffix = split_message_suffix(match.group("message"))
        message = normalize_message(message_text)
        decodes.append(
            {
                "utc": match.group("utc"),
                "snr": int(match.group("snr")),
                "dt": float(match.group("dt")),
                "freq_hz": int(match.group("freq")),
                "message": message,
                "annotation": suffix,
            }
        )
    return decodes


def parse_truth_file(path: Path) -> list[dict[str, Any]]:
    decodes: list[dict[str, Any]] = []
    for line in path.read_text().splitlines():
        match = DECODE_PATTERN.match(line)
        if not match:
            continue
        message_text, suffix = split_message_suffix(match.group("message"))
        message = normalize_message(message_text)
        decodes.append(
            {
                "utc": match.group("utc"),
                "snr": int(match.group("snr")),
                "dt": float(match.group("dt")),
                "freq_hz": int(match.group("freq")),
                "message": message,
                "annotation": suffix,
            }
        )
    return decodes


def discover_releases(paths: Paths, verify: bool = False) -> dict[str, Any]:
    config = load_config(paths)
    source = config["release_source"]
    minimum_version = source["minimum_version"]
    include_prerelease_major_min = source.get("include_prerelease_major_min")
    html_text = fetch_text(source["index_url"])
    release_rows = sorted(
        {
            match.group("release"): match.group("timestamp")
            for match in ROOT_RELEASE_ROW_PATTERN.finditer(html_text)
            if should_include_release(
                match.group("version"),
                minimum_version,
                include_prerelease_major_min=include_prerelease_major_min,
            )
        }.items(),
        key=lambda item: version_key(item[0].removeprefix("wsjtx-")),
    )
    releases: list[dict[str, Any]] = []
    for release_id, released_at in release_rows:
        version = release_id.removeprefix("wsjtx-")
        release_page = f"{source['index_url'].rstrip('/')}/{release_id}/"
        release_files = discover_release_files(release_page)
        mac_artifact = select_macos_artifact(release_files)
        entry = {
            "version": version,
            "release_id": release_id,
            "release_page": release_page,
            "released_at": released_at,
            "platform": "macos",
            "artifact_name": mac_artifact["name"] if mac_artifact else None,
            "download_url": mac_artifact["download_url"] if mac_artifact else None,
            "macos_available": mac_artifact is not None,
        }
        if verify and entry["download_url"]:
            entry["download_verified"] = verify_download(entry["download_url"])
        releases.append(entry)
    payload = {
        "generated_at": utc_now(),
        "source": source["index_url"],
        "minimum_version": minimum_version,
        "include_prerelease_major_min": include_prerelease_major_min,
        "releases": releases,
    }
    write_json(paths.discovery / "releases.json", payload)
    return payload


def verify_download(url: str) -> bool:
    try:
        fetch_bytes(url, method="HEAD")
        return True
    except Exception:
        return False


def discover_release_files(release_page: str) -> dict[str, dict[str, Any]]:
    html_text = fetch_text(release_page)
    match = NET_SF_FILES_PATTERN.search(html_text)
    if not match:
        raise RuntimeError(f"Unable to parse SourceForge file listing from {release_page}")
    return json.loads(match.group(1))


def select_macos_artifact(release_files: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    candidates = [
        file_info
        for file_info in release_files.values()
        if file_info.get("downloadable")
        and file_info.get("name", "").lower().endswith(".dmg")
        and "darwin" in file_info.get("name", "").lower()
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda item: item.get("name", ""))
    return candidates[0]


def discover_datasets(paths: Paths) -> dict[str, Any]:
    config = load_config(paths)
    datasets: list[dict[str, Any]] = []
    for dataset in config["datasets"]:
        listing = fetch_json(dataset["listing_url"])
        by_stem: dict[str, dict[str, Any]] = defaultdict(dict)
        for entry in listing:
            name = entry["name"]
            stem, suffix = os.path.splitext(name)
            if suffix.lower() not in {".wav", ".txt"}:
                continue
            by_stem[stem][suffix.lower()] = entry
        samples: list[dict[str, Any]] = []
        for stem, files in sorted(by_stem.items()):
            wav_file = files.get(".wav")
            if not wav_file:
                continue
            sample = {
                "id": stem,
                "wav_url": wav_file["download_url"],
                "wav_size": wav_file["size"],
            }
            text_file = files.get(".txt")
            if dataset["pair_text_files"] and text_file:
                sample["truth_url"] = text_file["download_url"]
                sample["truth_size"] = text_file["size"]
            samples.append(sample)
        datasets.append(
            {
                "id": dataset["id"],
                "label": dataset["label"],
                "kind": dataset["kind"],
                "description": dataset["description"],
                "samples": samples,
            }
        )
    payload = {
        "generated_at": utc_now(),
        "datasets": datasets,
    }
    write_json(paths.discovery / "datasets.json", payload)
    return payload


def load_or_discover_releases(paths: Paths, verify: bool = False) -> dict[str, Any]:
    release_path = paths.discovery / "releases.json"
    if release_path.exists():
        return json.loads(release_path.read_text())
    return discover_releases(paths, verify=verify)


def load_or_discover_datasets(paths: Paths) -> dict[str, Any]:
    dataset_path = paths.discovery / "datasets.json"
    if dataset_path.exists():
        return json.loads(dataset_path.read_text())
    return discover_datasets(paths)


def sync_samples(
    paths: Paths,
    dataset_filter: set[str] | None = None,
    sample_limit: int | None = None,
) -> dict[str, Any]:
    datasets_payload = load_or_discover_datasets(paths)
    synced: list[dict[str, Any]] = []
    for dataset in datasets_payload["datasets"]:
        if dataset_filter and dataset["id"] not in dataset_filter:
            continue
        dataset_dir = paths.samples / dataset["id"]
        dataset_dir.mkdir(parents=True, exist_ok=True)
        count = 0
        for sample in dataset["samples"]:
            if sample_limit is not None and count >= sample_limit:
                break
            sample_dir = dataset_dir / sample["id"]
            sample_dir.mkdir(parents=True, exist_ok=True)
            download(sample["wav_url"], sample_dir / f"{sample['id']}.wav")
            if "truth_url" in sample:
                download(sample["truth_url"], sample_dir / f"{sample['id']}.txt")
            count += 1
        synced.append(
            {
                "dataset_id": dataset["id"],
                "sample_count": count,
                "path": str(dataset_dir),
            }
        )
    payload = {
        "generated_at": utc_now(),
        "datasets": synced,
    }
    write_json(paths.discovery / "synced_samples.json", payload)
    return payload


def download(url: str, destination: Path) -> None:
    if destination.exists():
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    tmp_destination = destination.with_suffix(destination.suffix + ".part")
    with urllib.request.urlopen(
        urllib.request.Request(url, headers={"User-Agent": USER_AGENT}),
        timeout=TIMEOUT_SECONDS,
    ) as response, tmp_destination.open("wb") as handle:
        shutil.copyfileobj(response, handle)
    tmp_destination.replace(destination)


def sync_releases(paths: Paths, version_filter: set[str] | None = None) -> dict[str, Any]:
    releases_payload = load_or_discover_releases(paths)
    synced: list[dict[str, Any]] = []
    for release in releases_payload["releases"]:
        version = release["version"]
        if version_filter and version not in version_filter:
            continue
        if not release.get("download_url"):
            synced.append(
                {
                    "version": version,
                    "available": False,
                    "reason": "no_macos_artifact",
                }
            )
            continue
        app_path = ensure_release_app(paths, release)
        metadata = describe_release_app(app_path)
        synced.append(
            {
                "version": version,
                "app_path": str(app_path),
                "available": True,
                "artifact_name": release.get("artifact_name"),
                "jt9_arches": metadata["jt9_arches"],
                "host_arch": host_arch(),
                "execution_mode": execution_mode(metadata["jt9_arches"], host_arch()),
            }
        )
    payload = {
        "generated_at": utc_now(),
        "releases": synced,
    }
    write_json(paths.discovery / "synced_releases.json", payload)
    return payload


def ensure_release_app(paths: Paths, release: dict[str, Any]) -> Path:
    version = release["version"]
    release_dir = paths.releases / version
    app_path = release_dir / "wsjtx.app"
    if app_path.exists():
        return app_path

    release_dir.mkdir(parents=True, exist_ok=True)
    artifact_name = release.get("artifact_name") or f"wsjtx-{version}-Darwin.dmg"
    dmg_path = release_dir / artifact_name
    download(release["download_url"], dmg_path)

    attach = subprocess.run(
        ["hdiutil", "attach", "-plist", "-nobrowse", "-readonly", str(dmg_path)],
        input=("Y\n" * 8).encode("utf-8"),
        capture_output=True,
        check=True,
    )
    mount_point = parse_mount_point(attach.stdout)
    if not mount_point:
        raise RuntimeError(f"Unable to determine mount point for {dmg_path}")

    source_app = Path(mount_point) / "wsjtx.app"
    if not source_app.exists():
        subprocess.run(["hdiutil", "detach", mount_point], check=False)
        raise RuntimeError(f"Missing wsjtx.app inside {mount_point}")

    try:
        shutil.copytree(source_app, app_path)
    finally:
        subprocess.run(["hdiutil", "detach", mount_point], check=False)
    write_json(release_dir / "metadata.json", describe_release_app(app_path))
    return app_path


def describe_release_app(app_path: Path) -> dict[str, Any]:
    jt9_path = app_path / "Contents" / "MacOS" / "jt9"
    arches = inspect_binary_arches(jt9_path)
    payload = {
        "app_path": str(app_path),
        "jt9_path": str(jt9_path),
        "jt9_arches": arches,
    }
    metadata_path = app_path.parent / "metadata.json"
    write_json(metadata_path, payload)
    return payload


def parse_mount_point(plist_bytes: bytes) -> str | None:
    payload = plistlib.loads(extract_plist_bytes(plist_bytes))
    for entity in payload.get("system-entities", []):
        mount_point = entity.get("mount-point")
        if mount_point:
            return mount_point
    return None


def extract_plist_bytes(raw_bytes: bytes) -> bytes:
    xml_start = raw_bytes.find(b"<?xml")
    if xml_start != -1:
        return raw_bytes[xml_start:]
    binary_start = raw_bytes.find(b"bplist00")
    if binary_start != -1:
        return raw_bytes[binary_start:]
    return raw_bytes


def inspect_binary_arches(binary_path: Path) -> list[str]:
    if not binary_path.exists():
        return []
    try:
        completed = subprocess.run(
            ["lipo", "-archs", str(binary_path)],
            capture_output=True,
            text=True,
            check=True,
        )
        arches = completed.stdout.strip().split()
        if arches:
            return arches
    except Exception:
        pass
    completed = subprocess.run(
        ["file", str(binary_path)],
        capture_output=True,
        text=True,
        check=True,
    )
    output = completed.stdout
    detected: list[str] = []
    for arch in ("arm64", "x86_64"):
        if arch in output:
            detected.append(arch)
    return detected or ["unknown"]


def host_arch() -> str:
    return platform.machine()


def execution_mode(binary_arches: list[str], current_host_arch: str) -> str:
    if current_host_arch in binary_arches:
        return "native"
    if current_host_arch == "arm64" and "x86_64" in binary_arches:
        return "rosetta"
    return "incompatible"


def profile_sort_key(profile_id: str, configured_profiles: list[dict[str, Any]]) -> int:
    for index, profile in enumerate(configured_profiles):
        if profile["id"] == profile_id:
            return index
    return len(configured_profiles)


def run_benchmarks(
    paths: Paths,
    versions: list[str] | None = None,
    datasets: list[str] | None = None,
    sample_limit: int | None = None,
    profiles: list[str] | None = None,
    force: bool = False,
    jobs: int | None = None,
) -> dict[str, Any]:
    config = load_config(paths)
    release_payload = load_or_discover_releases(paths)
    dataset_payload = load_or_discover_datasets(paths)

    version_filter = set(versions or [])
    dataset_filter = set(datasets or [])
    profile_filter = set(profiles or [])

    sync_releases(paths, version_filter or None)
    sync_samples(paths, dataset_filter or None, sample_limit)

    selected_releases = [
        release
        for release in release_payload["releases"]
        if (not version_filter or release["version"] in version_filter)
        and release.get("macos_available", True)
    ]
    release_metadata = {
        release["version"]: describe_release_app(paths.releases / release["version"] / "wsjtx.app")
        for release in selected_releases
    }
    selected_datasets = [
        dataset
        for dataset in dataset_payload["datasets"]
        if not dataset_filter or dataset["id"] in dataset_filter
    ]
    selected_profiles = [
        profile
        for profile in config["profiles"]
        if not profile_filter or profile["id"] in profile_filter
    ]
    worker_count = max(1, jobs or os.cpu_count() or 1)
    total_jobs = sum(
        len(dataset["samples"][:sample_limit] if sample_limit is not None else dataset["samples"])
        for dataset in selected_datasets
    ) * len(selected_profiles) * len(selected_releases)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = paths.results / run_id
    raw_dir = run_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_cache_dir = paths.cache / "raw"
    raw_cache_dir.mkdir(parents=True, exist_ok=True)

    job_specs: list[dict[str, Any]] = []
    for release in selected_releases:
        app_path = paths.releases / release["version"] / "wsjtx.app"
        jt9_path = app_path / "Contents" / "MacOS" / "jt9"
        for profile in selected_profiles:
            for dataset in selected_datasets:
                sample_entries = dataset["samples"]
                if sample_limit is not None:
                    sample_entries = sample_entries[:sample_limit]
                for sample in sample_entries:
                    sample_dir = paths.samples / dataset["id"] / sample["id"]
                    job_specs.append(
                        {
                            "release": release,
                            "profile": profile,
                            "dataset": dataset,
                            "sample": sample,
                            "sample_dir": sample_dir,
                            "app_path": app_path,
                            "jt9_path": jt9_path,
                            "run_id": run_id,
                        }
                    )

    runs: list[dict[str, Any]] = []
    completed_jobs = 0
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        pending: dict[Future[dict[str, Any]], dict[str, Any]] = {}
        job_iter = iter(job_specs)

        while len(pending) < worker_count:
            try:
                spec = next(job_iter)
            except StopIteration:
                break
            future = executor.submit(
                run_decode_job,
                paths=paths,
                raw_dir=raw_dir,
                raw_cache_dir=raw_cache_dir,
                release_metadata=release_metadata,
                job_spec=spec,
                force=force,
            )
            pending[future] = spec

        while pending:
            completed, _ = wait(pending.keys(), return_when=FIRST_COMPLETED)
            for future in completed:
                spec = pending.pop(future)
                run = future.result()
                completed_jobs += 1
                print(
                    f"[{completed_jobs}/{total_jobs}] {spec['release']['version']} {spec['profile']['id']} {spec['dataset']['id']} {spec['sample']['id']}",
                    flush=True,
                )
                runs.append(run)
                try:
                    next_spec = next(job_iter)
                except StopIteration:
                    continue
                next_future = executor.submit(
                    run_decode_job,
                    paths=paths,
                    raw_dir=raw_dir,
                    raw_cache_dir=raw_cache_dir,
                    release_metadata=release_metadata,
                    job_spec=next_spec,
                    force=force,
                )
                pending[next_future] = next_spec

    runs.sort(
        key=lambda item: (
            version_key(item["release_version"]),
            profile_sort_key(item["profile_id"], selected_profiles),
            item["dataset_id"],
            item["sample_id"],
        )
    )

    payload = {
        "generated_at": utc_now(),
        "run_id": run_id,
        "host_arch": host_arch(),
        "profiles": selected_profiles,
        "jobs": worker_count,
        "releases": [
            {
                "version": release["version"],
                "jt9_arches": release_metadata[release["version"]]["jt9_arches"],
                "execution_mode": execution_mode(
                    release_metadata[release["version"]]["jt9_arches"],
                    host_arch(),
                ),
            }
            for release in selected_releases
        ],
        "datasets": [
            {
                "id": dataset["id"],
                "label": dataset["label"],
                "kind": dataset["kind"],
            }
            for dataset in selected_datasets
        ],
        "runs": runs,
    }
    results_json = run_dir / "results.json"
    write_json(results_json, payload)
    write_summary_csv(run_dir / "summary.csv", summarize_runs(runs))
    latest_dir = paths.results / "latest"
    if latest_dir.exists() or latest_dir.is_symlink():
        if latest_dir.is_symlink() or latest_dir.is_file():
            latest_dir.unlink()
        else:
            shutil.rmtree(latest_dir)
    shutil.copytree(run_dir, latest_dir)
    return payload


def invoke_decoder(
    jt9_path: Path,
    app_path: Path,
    sample_path: Path,
    depth: int,
    work_root: Path,
) -> dict[str, Any]:
    work_root.mkdir(parents=True, exist_ok=True)
    run_temp = Path(tempfile.mkdtemp(prefix="jt9-", dir=work_root))
    stdout_path = run_temp / "stdout.txt"
    stderr_path = run_temp / "stderr.txt"
    macos_dir = app_path / "Contents" / "MacOS"
    frameworks_dir = app_path / "Contents" / "Frameworks"
    env = os.environ.copy()
    env["DYLD_LIBRARY_PATH"] = ":".join(
        str(path) for path in (macos_dir, frameworks_dir) if path.exists()
    )
    command = [
        str(jt9_path),
        "-8",
        "-d",
        str(depth),
        "-e",
        str(macos_dir),
        "-a",
        str(run_temp),
        "-t",
        str(run_temp),
        str(sample_path),
    ]
    started_at = time.monotonic()
    with stdout_path.open("w") as stdout_handle, stderr_path.open("w") as stderr_handle:
        process = subprocess.Popen(
            command,
            stdout=stdout_handle,
            stderr=stderr_handle,
            text=True,
            env=env,
            cwd=str(run_temp),
        )
        _, status, rusage = os.wait4(process.pid, 0)
    elapsed = time.monotonic() - started_at
    exit_code = os.waitstatus_to_exitcode(status)
    if exit_code != 0:
        raise subprocess.CalledProcessError(exit_code, command)
    cpu_user_seconds = rusage.ru_utime
    cpu_system_seconds = rusage.ru_stime
    cpu_seconds = cpu_user_seconds + cpu_system_seconds
    stdout = stdout_path.read_text()
    stderr = stderr_path.read_text()
    if stderr.strip():
        stdout = stdout + ("\n" if stdout and not stdout.endswith("\n") else "")
        stdout += f"# stderr ({elapsed:.3f}s)\n{stderr}"
    return {
        "stdout": stdout,
        "wall_seconds": elapsed,
        "cpu_user_seconds": cpu_user_seconds,
        "cpu_system_seconds": cpu_system_seconds,
        "cpu_seconds": cpu_seconds,
    }


def run_decode_job(
    paths: Paths,
    raw_dir: Path,
    raw_cache_dir: Path,
    release_metadata: dict[str, dict[str, Any]],
    job_spec: dict[str, Any],
    force: bool,
) -> dict[str, Any]:
    release = job_spec["release"]
    profile = job_spec["profile"]
    dataset = job_spec["dataset"]
    sample = job_spec["sample"]
    sample_dir = job_spec["sample_dir"]
    wav_path = sample_dir / f"{sample['id']}.wav"
    truth_path = sample_dir / f"{sample['id']}.txt"
    raw_output_path = (
        raw_dir
        / release["version"]
        / profile["id"]
        / dataset["id"]
        / f"{sample['id']}.txt"
    )
    cache_output_path = (
        raw_cache_dir
        / release["version"]
        / profile["id"]
        / dataset["id"]
        / f"{sample['id']}.txt"
    )
    cache_metrics_path = cache_output_path.with_suffix(".json")

    if cache_output_path.exists() and not force:
        stdout = cache_output_path.read_text()
        timing = read_json(cache_metrics_path) if cache_metrics_path.exists() else {}
    else:
        result = invoke_decoder(
            jt9_path=job_spec["jt9_path"],
            app_path=job_spec["app_path"],
            sample_path=wav_path,
            depth=profile["depth"],
            work_root=paths.temp / "runs" / job_spec["run_id"],
        )
        stdout = result["stdout"]
        timing = {
            "wall_seconds": result["wall_seconds"],
            "cpu_user_seconds": result["cpu_user_seconds"],
            "cpu_system_seconds": result["cpu_system_seconds"],
            "cpu_seconds": result["cpu_seconds"],
        }
        cache_output_path.parent.mkdir(parents=True, exist_ok=True)
        cache_output_path.write_text(stdout)
        write_json(cache_metrics_path, timing)

    raw_output_path.parent.mkdir(parents=True, exist_ok=True)
    raw_output_path.write_text(stdout)
    decodes = parse_decode_lines(stdout)
    truth = parse_truth_file(truth_path) if truth_path.exists() else []
    metrics = compare_decodes(decodes, truth) if truth else None
    return {
        "release_version": release["version"],
        "host_arch": host_arch(),
        "jt9_arches": release_metadata[release["version"]]["jt9_arches"],
        "execution_mode": execution_mode(
            release_metadata[release["version"]]["jt9_arches"],
            host_arch(),
        ),
        "profile_id": profile["id"],
        "profile_label": profile["label"],
        "dataset_id": dataset["id"],
        "dataset_label": dataset["label"],
        "dataset_kind": dataset["kind"],
        "sample_id": sample["id"],
        "raw_output_path": str(raw_output_path),
        "decode_count": len(decodes),
        "truth_count": len(truth),
        "wall_seconds": timing.get("wall_seconds"),
        "cpu_user_seconds": timing.get("cpu_user_seconds"),
        "cpu_system_seconds": timing.get("cpu_system_seconds"),
        "cpu_seconds": timing.get("cpu_seconds"),
        "decodes": decodes,
        "truth": truth,
        "metrics": metrics,
    }


def compare_decodes(
    decodes: list[dict[str, Any]],
    truth: list[dict[str, Any]],
) -> dict[str, Any]:
    predicted = {entry["message"] for entry in decodes}
    expected = {entry["message"] for entry in truth}
    true_positive = sorted(predicted & expected)
    false_positive = sorted(predicted - expected)
    false_negative = sorted(expected - predicted)
    precision = len(true_positive) / len(predicted) if predicted else 0.0
    recall = len(true_positive) / len(expected) if expected else 0.0
    f1 = (
        2 * precision * recall / (precision + recall)
        if (precision + recall) > 0
        else 0.0
    )
    return {
        "matching_rule": "unique-message",
        "tp": len(true_positive),
        "fp": len(false_positive),
        "fn": len(false_negative),
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "true_positive_messages": true_positive,
        "false_positive_messages": false_positive,
        "false_negative_messages": false_negative,
    }


def summarize_runs(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for run in runs:
        key = (run["release_version"], run["profile_id"], run["dataset_id"])
        entry = grouped.setdefault(
            key,
            {
                "release_version": run["release_version"],
                "profile_id": run["profile_id"],
                "profile_label": run["profile_label"],
                "dataset_id": run["dataset_id"],
                "dataset_label": run["dataset_label"],
                "dataset_kind": run["dataset_kind"],
                "samples": 0,
                "decode_count": 0,
                "truth_count": 0,
                "wall_seconds": 0.0,
                "cpu_user_seconds": 0.0,
                "cpu_system_seconds": 0.0,
                "cpu_seconds": 0.0,
                "tp": 0,
                "fp": 0,
                "fn": 0,
            },
        )
        entry["samples"] += 1
        entry["decode_count"] += run["decode_count"]
        entry["truth_count"] += run["truth_count"]
        entry["wall_seconds"] += run.get("wall_seconds") or 0.0
        entry["cpu_user_seconds"] += run.get("cpu_user_seconds") or 0.0
        entry["cpu_system_seconds"] += run.get("cpu_system_seconds") or 0.0
        entry["cpu_seconds"] += run.get("cpu_seconds") or 0.0
        if run["metrics"]:
            entry["tp"] += run["metrics"]["tp"]
            entry["fp"] += run["metrics"]["fp"]
            entry["fn"] += run["metrics"]["fn"]

    summary = sorted(grouped.values(), key=lambda item: (version_key(item["release_version"]), item["profile_id"], item["dataset_id"]))
    for entry in summary:
        entry["avg_wall_seconds"] = entry["wall_seconds"] / entry["samples"] if entry["samples"] else None
        entry["avg_cpu_user_seconds"] = entry["cpu_user_seconds"] / entry["samples"] if entry["samples"] else None
        entry["avg_cpu_system_seconds"] = entry["cpu_system_seconds"] / entry["samples"] if entry["samples"] else None
        entry["avg_cpu_seconds"] = entry["cpu_seconds"] / entry["samples"] if entry["samples"] else None
        if entry["dataset_kind"] == "scored":
            predicted = entry["tp"] + entry["fp"]
            expected = entry["tp"] + entry["fn"]
            entry["precision"] = entry["tp"] / predicted if predicted else 0.0
            entry["recall"] = entry["tp"] / expected if expected else 0.0
            precision = entry["precision"]
            recall = entry["recall"]
            entry["f1"] = (
                2 * precision * recall / (precision + recall)
                if (precision + recall) > 0
                else 0.0
            )
        else:
            entry["precision"] = None
            entry["recall"] = None
            entry["f1"] = None
    return summary


def write_summary_csv(path: Path, summary_rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "release_version",
                "profile_id",
                "dataset_id",
                "dataset_kind",
                "samples",
                "decode_count",
                "truth_count",
                "wall_seconds",
                "cpu_user_seconds",
                "cpu_system_seconds",
                "cpu_seconds",
                "avg_wall_seconds",
                "avg_cpu_user_seconds",
                "avg_cpu_system_seconds",
                "avg_cpu_seconds",
                "tp",
                "fp",
                "fn",
                "precision",
                "recall",
                "f1",
            ],
        )
        writer.writeheader()
        for row in summary_rows:
            writer.writerow(
                {
                    key: row.get(key)
                    for key in writer.fieldnames
                }
            )


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def latest_results_path(paths: Paths) -> Path:
    results_path = paths.results / "latest" / "results.json"
    if not results_path.exists():
        raise FileNotFoundError("No benchmark results found. Run the benchmark first.")
    return results_path


def generate_report(paths: Paths, results_path: Path | None = None) -> Path:
    from .report import render_report

    results_file = results_path or latest_results_path(paths)
    payload = json.loads(results_file.read_text())
    report_dir = paths.reports / payload["run_id"]
    report_dir.mkdir(parents=True, exist_ok=True)
    html_path = report_dir / "index.html"
    html_path.write_text(render_report(payload))
    latest_dir = paths.reports / "latest"
    if latest_dir.exists() or latest_dir.is_symlink():
        if latest_dir.is_symlink() or latest_dir.is_file():
            latest_dir.unlink()
        else:
            shutil.rmtree(latest_dir)
    shutil.copytree(report_dir, latest_dir)
    return html_path


def invoke_local_decoder(binary_path: Path, sample_path: Path) -> dict[str, Any]:
    started_at = time.monotonic()
    completed = subprocess.Popen(
        [str(binary_path), str(sample_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    _, status, rusage = os.wait4(completed.pid, 0)
    stdout, stderr = completed.communicate()
    elapsed = time.monotonic() - started_at
    exit_code = os.waitstatus_to_exitcode(status)
    if exit_code != 0:
        raise subprocess.CalledProcessError(exit_code, [str(binary_path), str(sample_path)], stdout, stderr)
    if stderr.strip():
        stdout = stdout + ("\n" if stdout and not stdout.endswith("\n") else "") + stderr
    return {
        "stdout": stdout,
        "wall_seconds": elapsed,
        "cpu_user_seconds": rusage.ru_utime,
        "cpu_system_seconds": rusage.ru_stime,
        "cpu_seconds": rusage.ru_utime + rusage.ru_stime,
    }


def summarize_decoder_runs(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for run in runs:
        entry = grouped.setdefault(
            run["dataset_id"],
            {
                "dataset_id": run["dataset_id"],
                "dataset_label": run["dataset_label"],
                "dataset_kind": run["dataset_kind"],
                "samples": 0,
                "decode_count": 0,
                "truth_count": 0,
                "wall_seconds": 0.0,
                "cpu_user_seconds": 0.0,
                "cpu_system_seconds": 0.0,
                "cpu_seconds": 0.0,
                "tp": 0,
                "fp": 0,
                "fn": 0,
            },
        )
        entry["samples"] += 1
        entry["decode_count"] += run["decode_count"]
        entry["truth_count"] += run["truth_count"]
        entry["wall_seconds"] += run.get("wall_seconds") or 0.0
        entry["cpu_user_seconds"] += run.get("cpu_user_seconds") or 0.0
        entry["cpu_system_seconds"] += run.get("cpu_system_seconds") or 0.0
        entry["cpu_seconds"] += run.get("cpu_seconds") or 0.0
        if run["metrics"]:
            entry["tp"] += run["metrics"]["tp"]
            entry["fp"] += run["metrics"]["fp"]
            entry["fn"] += run["metrics"]["fn"]
    summary = sorted(grouped.values(), key=lambda item: item["dataset_id"])
    for entry in summary:
        entry["avg_wall_seconds"] = entry["wall_seconds"] / entry["samples"] if entry["samples"] else None
        entry["avg_cpu_user_seconds"] = entry["cpu_user_seconds"] / entry["samples"] if entry["samples"] else None
        entry["avg_cpu_system_seconds"] = entry["cpu_system_seconds"] / entry["samples"] if entry["samples"] else None
        entry["avg_cpu_seconds"] = entry["cpu_seconds"] / entry["samples"] if entry["samples"] else None
        if entry["dataset_kind"] == "scored":
            predicted = entry["tp"] + entry["fp"]
            expected = entry["tp"] + entry["fn"]
            entry["precision"] = entry["tp"] / predicted if predicted else 0.0
            entry["recall"] = entry["tp"] / expected if expected else 0.0
            precision = entry["precision"]
            recall = entry["recall"]
            entry["f1"] = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
        else:
            entry["precision"] = None
            entry["recall"] = None
            entry["f1"] = None
    return summary


def write_decoder_summary_csv(path: Path, summary_rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "dataset_id",
                "dataset_kind",
                "samples",
                "decode_count",
                "truth_count",
                "wall_seconds",
                "cpu_user_seconds",
                "cpu_system_seconds",
                "cpu_seconds",
                "avg_wall_seconds",
                "avg_cpu_user_seconds",
                "avg_cpu_system_seconds",
                "avg_cpu_seconds",
                "tp",
                "fp",
                "fn",
                "precision",
                "recall",
                "f1",
            ],
        )
        writer.writeheader()
        for row in summary_rows:
            writer.writerow({key: row.get(key) for key in writer.fieldnames})


def run_rust_benchmark(
    paths: Paths,
    binary_path: Path,
    datasets: list[str] | None = None,
    sample_limit: int | None = None,
) -> dict[str, Any]:
    if not binary_path.exists():
        raise FileNotFoundError(f"Missing decoder binary: {binary_path}")

    dataset_payload = load_or_discover_datasets(paths)
    dataset_filter = set(datasets or [])
    sync_samples(paths, dataset_filter or None, sample_limit)

    selected_datasets = [
        dataset
        for dataset in dataset_payload["datasets"]
        if not dataset_filter or dataset["id"] in dataset_filter
    ]
    run_id = f"rust-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    run_dir = paths.results / run_id
    raw_dir = run_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    runs: list[dict[str, Any]] = []
    total_jobs = sum(
        len(dataset["samples"][:sample_limit] if sample_limit is not None else dataset["samples"])
        for dataset in selected_datasets
    )
    completed_jobs = 0
    for dataset in selected_datasets:
        sample_entries = dataset["samples"][:sample_limit] if sample_limit is not None else dataset["samples"]
        for sample in sample_entries:
            sample_dir = paths.samples / dataset["id"] / sample["id"]
            wav_path = sample_dir / f"{sample['id']}.wav"
            truth_path = sample_dir / f"{sample['id']}.txt"
            result = invoke_local_decoder(binary_path, wav_path)
            stdout = result["stdout"]
            raw_output_path = raw_dir / dataset["id"] / f"{sample['id']}.txt"
            raw_output_path.parent.mkdir(parents=True, exist_ok=True)
            raw_output_path.write_text(stdout)
            decodes = parse_decode_lines(stdout)
            truth = parse_truth_file(truth_path) if truth_path.exists() else []
            metrics = compare_decodes(decodes, truth) if truth else None
            runs.append(
                {
                    "decoder_id": "rust-ft8",
                    "decoder_label": "Rust FT8 Decoder",
                    "binary_path": str(binary_path),
                    "dataset_id": dataset["id"],
                    "dataset_label": dataset["label"],
                    "dataset_kind": dataset["kind"],
                    "sample_id": sample["id"],
                    "raw_output_path": str(raw_output_path),
                    "decode_count": len(decodes),
                    "truth_count": len(truth),
                    "wall_seconds": result["wall_seconds"],
                    "cpu_user_seconds": result["cpu_user_seconds"],
                    "cpu_system_seconds": result["cpu_system_seconds"],
                    "cpu_seconds": result["cpu_seconds"],
                    "decodes": decodes,
                    "truth": truth,
                    "metrics": metrics,
                }
            )
            completed_jobs += 1
            print(f"[{completed_jobs}/{total_jobs}] rust-ft8 {dataset['id']} {sample['id']}", flush=True)

    summary_rows = summarize_decoder_runs(runs)
    payload = {
        "generated_at": utc_now(),
        "run_id": run_id,
        "decoder_id": "rust-ft8",
        "decoder_label": "Rust FT8 Decoder",
        "binary_path": str(binary_path),
        "datasets": [
            {"id": dataset["id"], "label": dataset["label"], "kind": dataset["kind"]}
            for dataset in selected_datasets
        ],
        "runs": runs,
        "summary": summary_rows,
    }
    write_json(run_dir / "results.json", payload)
    write_decoder_summary_csv(run_dir / "summary.csv", summary_rows)
    return payload
