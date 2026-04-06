#!/usr/bin/env python3

from __future__ import annotations

import argparse
import concurrent.futures
import json
import math
import os
import random
import re
import shlex
import subprocess
import tempfile
import wave
from array import array
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

from mode_reference import DEFAULT_FT2_REF_GEN

DECODE_PATTERN = re.compile(
    r"^(?:(?P<utc>(?:\d{6}(?:_\d{9})?|\*{6}))\s+)?(?P<snr>-?\d+)\s+(?P<dt>-?\d+(?:\.\d+)?)\s+(?P<freq>\d+)\s+(?:~|\+|RX)\s+(?P<message>.+?)(?:\s+-?\d+\s+-?\d+\s+-?\d+)?\s*$",
    re.IGNORECASE,
)


@dataclass
class SpecCase:
    id: str
    mode: str
    first: str
    second: str
    info: str
    acknowledge: bool
    freq_hz: float
    start_seconds: float
    total_seconds: float
    expected_message: str
    wav_path: str


def write_manifest(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, indent=2) + "\n")
    return path


def parse_decode_records(text: str) -> list[dict]:
    records: list[dict] = []
    for line in text.splitlines():
        match = DECODE_PATTERN.match(line.strip())
        if not match:
            continue
        records.append(
            {
                "utc": match.group("utc") or "",
                "snr_db": int(match.group("snr")),
                "dt_seconds": float(match.group("dt")),
                "freq_hz": float(match.group("freq")),
                "message": re.sub(r"\s+", " ", match.group("message").strip().upper()),
            }
        )
    records.sort(key=lambda record: (record["freq_hz"], record["message"], record["dt_seconds"]))
    return records


def parse_decode_lines(text: str) -> list[str]:
    return sorted({record["message"] for record in parse_decode_records(text)})


def truth_messages_for_case(case: dict) -> list[str]:
    if "expected_messages" in case:
        return sorted(set(case.get("expected_messages", [])))
    expected_message = case.get("expected_message")
    if expected_message:
        return [re.sub(r"\s+", " ", expected_message.strip().upper())]
    return []


def command_from_template(template: str, **replacements: str) -> list[str]:
    rendered = template
    for key, value in replacements.items():
        rendered = rendered.replace("{" + key + "}", value)
    return shlex.split(rendered)


def default_stock_reference_template(profile: str) -> str:
    script = Path(__file__).resolve().with_name("run_stock_decode.py")
    return f"python3 {shlex.quote(str(script))} {{wav}} {{mode}} --profile {shlex.quote(profile)}"


def render_expected_message(first: str, second: str, info: str, acknowledge: bool) -> str:
    info = info.strip().upper()
    if acknowledge and info.startswith("-"):
        trailing = f"R{info}"
    else:
        trailing = info
    return " ".join(part for part in [first.strip().upper(), second.strip().upper(), trailing] if part)


def builtin_messages() -> list[tuple[str, str, str, bool]]:
    return [
        ("CQ", "K1ABC", "", False),
        ("CQ", "K1ABC", "FN31", False),
        ("W1XYZ", "K1ABC", "FN31", False),
        ("W1XYZ", "K1ABC", "-07", False),
        ("W1XYZ", "K1ABC", "-07", True),
        ("W1XYZ", "K1ABC", "RRR", False),
        ("W1XYZ", "K1ABC", "RR73", False),
        ("W1XYZ", "K1ABC", "73", False),
    ]


def default_total_seconds(mode: str) -> float:
    if mode == "ft4":
        return 7.5
    if mode == "ft2":
        return 2.5
    return 15.0


def load_wav_i16(path: Path) -> tuple[int, list[int]]:
    with wave.open(str(path), "rb") as reader:
        if reader.getnchannels() != 1 or reader.getsampwidth() != 2:
            raise ValueError(f"unsupported wav format: {path}")
        frames = reader.readframes(reader.getnframes())
        samples = array("h")
        samples.frombytes(frames)
        return reader.getframerate(), list(samples)


def write_wav_i16(path: Path, sample_rate_hz: int, samples: list[int]) -> None:
    with wave.open(str(path), "wb") as writer:
        writer.setnchannels(1)
        writer.setsampwidth(2)
        writer.setframerate(sample_rate_hz)
        writer.writeframes(array("h", samples).tobytes())


def build_padded_waveform(raw_samples: list[int], sample_rate_hz: int, start_seconds: float, total_seconds: float) -> list[int]:
    total_samples = round(total_seconds * sample_rate_hz)
    start_sample = round(start_seconds * sample_rate_hz)
    padded = [0] * total_samples
    end_sample = min(total_samples, start_sample + len(raw_samples))
    copy_len = max(0, end_sample - start_sample)
    padded[start_sample:end_sample] = raw_samples[:copy_len]
    return padded


def generate_reference_samples(
    generator: Path,
    message: str,
    freq_hz: float,
    start_seconds: float,
    total_seconds: float,
) -> tuple[int, list[int]]:
    with tempfile.TemporaryDirectory(prefix="mode-parity-ref-") as tmpdir:
        raw_path = Path(tmpdir) / "raw.wav"
        subprocess.run(
            [str(generator), str(raw_path), message, str(freq_hz)],
            check=True,
            capture_output=True,
            text=True,
        )
        sample_rate_hz, raw_samples = load_wav_i16(raw_path)
    return sample_rate_hz, build_padded_waveform(raw_samples, sample_rate_hz, start_seconds, total_seconds)


def generate_rust_samples(
    decoder_binary: Path,
    mode: str,
    first: str,
    second: str,
    info: str,
    acknowledge: bool,
    freq_hz: float,
    start_seconds: float,
    total_seconds: float,
) -> tuple[int, list[int]]:
    with tempfile.TemporaryDirectory(prefix="mode-parity-rust-") as tmpdir:
        wav_path = Path(tmpdir) / "raw.wav"
        command = [
            str(decoder_binary),
            "generate-standard",
            str(wav_path),
            "--mode",
            mode,
            "--first",
            first,
            "--second",
            second,
            f"--info={info}",
            "--freq-hz",
            str(freq_hz),
            "--start-seconds",
            str(start_seconds),
            "--total-seconds",
            str(total_seconds),
        ]
        if acknowledge:
            command.append("--acknowledge")
        subprocess.run(command, check=True)
        return load_wav_i16(wav_path)


def write_generated_waveform(
    *,
    generator_source: str,
    decoder_binary: Path,
    reference_generators: dict[str, Path | None],
    mode: str,
    first: str,
    second: str,
    info: str,
    acknowledge: bool,
    freq_hz: float,
    start_seconds: float,
    total_seconds: float,
    output_path: Path,
) -> None:
    sample_rate_hz, samples, _ = generate_case_waveform(
        generator_source=generator_source,
        decoder_binary=decoder_binary,
        reference_generators=reference_generators,
        mode=mode,
        first=first,
        second=second,
        info=info,
        acknowledge=acknowledge,
        freq_hz=freq_hz,
        start_seconds=start_seconds,
        total_seconds=total_seconds,
    )
    write_wav_i16(output_path, sample_rate_hz, samples)


def generate_case_waveform(
    *,
    generator_source: str,
    decoder_binary: Path,
    reference_generators: dict[str, Path | None],
    mode: str,
    first: str,
    second: str,
    info: str,
    acknowledge: bool,
    freq_hz: float,
    start_seconds: float,
    total_seconds: float,
) -> tuple[int, list[int], str]:
    rendered = render_expected_message(first, second, info, acknowledge)
    if generator_source == "reference":
        generator = reference_generators[mode]
        if generator is None:
            raise SystemExit(f"missing reference generator for mode {mode}")
        sample_rate_hz, samples = generate_reference_samples(
            generator, rendered, freq_hz, start_seconds, total_seconds
        )
    else:
        sample_rate_hz, samples = generate_rust_samples(
            decoder_binary,
            mode,
            first,
            second,
            info,
            acknowledge,
            freq_hz,
            start_seconds,
            total_seconds,
        )
    return sample_rate_hz, samples, rendered


def build_spec_corpus(args: argparse.Namespace) -> int:
    output_root = Path(args.output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    decoder_binary = Path(args.decoder_binary).resolve()
    reference_generators = {
        "ft4": Path(args.reference_ft4_gen).resolve() if args.reference_ft4_gen else None,
        "ft2": Path(args.reference_ft2_gen).resolve() if args.reference_ft2_gen else None,
    }
    modes = args.modes
    freqs = [float(value) for value in args.freq_hz]
    starts = [float(value) for value in args.start_seconds]

    cases: list[SpecCase] = []
    for mode in modes:
        mode_dir = output_root / mode
        mode_dir.mkdir(parents=True, exist_ok=True)
        for index, (first, second, info, acknowledge) in enumerate(builtin_messages()):
            freq_hz = freqs[index % len(freqs)]
            start_seconds = starts[index % len(starts)]
            total_seconds = args.total_seconds or default_total_seconds(mode)
            case_id = f"{mode}-{index:03d}"
            wav_path = mode_dir / f"{case_id}.wav"
            rendered = render_expected_message(first, second, info, acknowledge)
            write_generated_waveform(
                generator_source=args.generator_source,
                decoder_binary=decoder_binary,
                reference_generators=reference_generators,
                mode=mode,
                first=first,
                second=second,
                info=info,
                acknowledge=acknowledge,
                freq_hz=freq_hz,
                start_seconds=start_seconds,
                total_seconds=total_seconds,
                output_path=wav_path,
            )
            cases.append(
                SpecCase(
                    id=case_id,
                    mode=mode,
                    first=first,
                    second=second,
                    info=info,
                    acknowledge=acknowledge,
                    freq_hz=freq_hz,
                    start_seconds=start_seconds,
                    total_seconds=total_seconds,
                    expected_message=rendered,
                    wav_path=str(wav_path),
                )
            )

    manifest_path = output_root / "manifest.json"
    write_manifest(
        manifest_path,
        {
            "kind": "spec",
            "generator": (
                "reference"
                if args.generator_source == "reference"
                else str(decoder_binary)
            ),
            "cases": [asdict(case) for case in cases],
        },
    )
    print(manifest_path)
    return 0


def build_synth_corpus(args: argparse.Namespace) -> int:
    output_root = Path(args.output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    decoder_binary = Path(args.decoder_binary).resolve()
    reference_generators = {
        "ft4": Path(args.reference_ft4_gen).resolve() if args.reference_ft4_gen else None,
        "ft2": Path(args.reference_ft2_gen).resolve() if args.reference_ft2_gen else None,
    }
    rng = random.Random(args.seed)
    cases = []

    for mode in args.modes:
        mode_dir = output_root / mode
        mode_dir.mkdir(parents=True, exist_ok=True)
        total_seconds = args.total_seconds or default_total_seconds(mode)
        sample_rate_hz = 12_000
        total_samples = round(total_seconds * sample_rate_hz)
        if args.single_count_per_mode is not None or args.mixed_count_per_mode is not None:
            single_count = args.single_count_per_mode or 0
            mixed_count = args.mixed_count_per_mode or 0
        else:
            single_count = 0
            mixed_count = args.count_per_mode
        for cohort, cohort_count in (("single", single_count), ("mixed", mixed_count)):
            for index in range(cohort_count):
                signal_count = 1 if cohort == "single" else rng.randint(2, max(2, args.max_signals))
                mixed = [0.0] * total_samples
                expected = []
                signals = []
                for signal_index in range(signal_count):
                    first, second, info, acknowledge = rng.choice(builtin_messages())
                    freq_hz = rng.choice([650.0, 900.0, 1150.0, 1400.0, 1650.0, 1900.0])
                    start_seconds = rng.choice([0.0, 0.05, 0.1, 0.2, 0.35])
                    _, samples, rendered = generate_case_waveform(
                        generator_source=args.generator_source,
                        decoder_binary=decoder_binary,
                        reference_generators=reference_generators,
                        mode=mode,
                        first=first,
                        second=second,
                        info=info,
                        acknowledge=acknowledge,
                        freq_hz=freq_hz,
                        start_seconds=start_seconds,
                        total_seconds=total_seconds,
                    )
                    expected.append(rendered)
                    signals.append(
                        {
                            "first": first,
                            "second": second,
                            "info": info,
                            "acknowledge": acknowledge,
                            "freq_hz": freq_hz,
                            "start_seconds": start_seconds,
                        }
                    )
                    gain = 10 ** (rng.uniform(args.signal_db_min, args.signal_db_max) / 20.0)
                    for i, sample in enumerate(samples):
                        mixed[i] += gain * (sample / 32768.0)

                noise_sigma = 10 ** (args.noise_dbfs / 20.0)
                for i in range(total_samples):
                    mixed[i] += rng.gauss(0.0, noise_sigma)

                peak = max(1.0, max(abs(value) for value in mixed) / 0.8)
                pcm = [max(-32767, min(32767, round((value / peak) * 32767.0))) for value in mixed]
                case_id = f"{mode}-{cohort}-{index:05d}"
                wav_path = mode_dir / f"{case_id}.wav"
                write_wav_i16(wav_path, sample_rate_hz, pcm)
                cases.append(
                    {
                        "id": case_id,
                        "mode": mode,
                        "cohort": cohort,
                        "wav_path": str(wav_path),
                        "expected_messages": sorted(set(expected)),
                        "signals": signals,
                        "noise_dbfs": args.noise_dbfs,
                    }
                )

    manifest_path = output_root / "manifest.json"
    write_manifest(
        manifest_path,
        {
            "kind": "synthetic",
            "generator": args.generator_source,
            "seed": args.seed,
            "cases": cases,
        },
    )
    print(manifest_path)
    return 0


def build_replay_manifest(args: argparse.Namespace) -> int:
    output_root = Path(args.output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    cases = []
    for index, sample_spec in enumerate(args.sample):
        mode, raw_path = sample_spec.split(":", 1)
        wav_path = Path(raw_path).resolve()
        if not wav_path.exists():
            raise SystemExit(f"missing replay sample: {wav_path}")
        reference_command = command_from_template(args.reference_cmd, wav=str(wav_path), mode=mode)
        reference_output = subprocess.run(
            reference_command, check=True, capture_output=True, text=True
        )
        reference_records = parse_decode_records(reference_output.stdout)
        cases.append(
            {
                "id": f"{mode}-replay-{index:03d}",
                "mode": mode,
                "cohort": "replay",
                "wav_path": str(wav_path),
                "expected_messages": [record["message"] for record in reference_records],
                "expected_records": reference_records,
                "reference_stdout": reference_output.stdout,
            }
        )
    manifest_path = output_root / "manifest.json"
    write_manifest(
        manifest_path,
        {
            "kind": "replay",
            "reference_cmd": args.reference_cmd,
            "cases": cases,
        },
    )
    print(manifest_path)
    return 0


def compare_case(case: dict, rust_template: str, reference_template: str | None) -> dict:
    wav = case["wav_path"]
    rust_command = command_from_template(rust_template, wav=wav, mode=case["mode"])
    rust_output = subprocess.run(rust_command, check=True, capture_output=True, text=True)
    rust_messages = parse_decode_lines(rust_output.stdout)
    truth_messages = truth_messages_for_case(case)
    if reference_template is not None:
        reference_command = command_from_template(reference_template, wav=wav, mode=case["mode"])
        reference_output = subprocess.run(
            reference_command, check=True, capture_output=True, text=True
        )
        reference_messages = parse_decode_lines(reference_output.stdout)
    else:
        reference_messages = truth_messages
    rust_only_messages = sorted(set(rust_messages) - set(reference_messages))
    stock_only_messages = sorted(set(reference_messages) - set(rust_messages))
    stock_missed_truth_messages = sorted(set(truth_messages) - set(reference_messages))
    rust_only_truth_messages = sorted(set(rust_messages) & set(truth_messages) - set(reference_messages))
    return {
        "id": case["id"],
        "mode": case["mode"],
        "cohort": case.get("cohort"),
        "wav_path": wav,
        "truth_messages": truth_messages,
        "rust_messages": rust_messages,
        "reference_messages": reference_messages,
        "rust_only_messages": rust_only_messages,
        "stock_only_messages": stock_only_messages,
        "stock_missed_truth_messages": stock_missed_truth_messages,
        "rust_only_truth_messages": rust_only_truth_messages,
        "match": rust_messages == reference_messages,
    }


def compare_corpus(args: argparse.Namespace) -> int:
    manifest = json.loads(Path(args.manifest).read_text())
    cases = manifest["cases"]
    reference_template = args.reference_cmd
    if args.use_stock_reference:
        reference_template = default_stock_reference_template(args.profile)
    results = []
    mismatches = 0
    jobs = args.jobs or os.cpu_count() or 1
    with concurrent.futures.ThreadPoolExecutor(max_workers=jobs) as executor:
        futures = [
            executor.submit(compare_case, case, args.rust_cmd, reference_template)
            for case in cases
        ]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            mismatches += int(not result["match"])
            results.append(result)
    results.sort(key=lambda item: item["id"])

    payload = {
        "manifest": str(Path(args.manifest).resolve()),
        "profile": args.profile,
        "reference_cmd": reference_template,
        "case_count": len(results),
        "mismatch_count": mismatches,
        "results": results,
    }
    if args.output:
        Path(args.output).write_text(json.dumps(payload, indent=2) + "\n")
    print(json.dumps({"case_count": len(results), "mismatch_count": mismatches}, indent=2))
    return 0 if mismatches == 0 else 1


def run_json_command(command: list[str]) -> dict:
    completed = subprocess.run(command, check=True, capture_output=True, text=True)
    return json.loads(completed.stdout)


def run_decoder_report(
    decoder_binary: Path,
    wav_path: str,
    mode: str,
    profile: str,
    *,
    max_candidates: int,
    search_passes: int,
    no_subtraction: bool,
) -> dict:
    command = [
        str(decoder_binary),
        "decode",
        wav_path,
        "--mode",
        mode,
        "--profile",
        profile,
        "--max-candidates",
        str(max_candidates),
        "--search-passes",
        str(search_passes),
        "--json",
    ]
    if no_subtraction:
        command.append("--no-subtraction")
    return run_json_command(command)


def run_decoder_debug(
    decoder_binary: Path,
    wav_path: str,
    mode: str,
    dt_seconds: float,
    freq_hz: float,
    message: str | None,
) -> dict | None:
    if message is not None:
        standard_command = [
            str(decoder_binary),
            "debug-standard-candidate",
            wav_path,
            "--mode",
            mode,
            f"--dt-seconds={dt_seconds}",
            f"--freq-hz={freq_hz}",
            "--message",
            message,
        ]
        completed = subprocess.run(
            standard_command,
            capture_output=True,
            text=True,
        )
        if completed.returncode == 0:
            return json.loads(completed.stdout)

    generic_command = [
        str(decoder_binary),
        "debug-candidate",
        wav_path,
        "--mode",
        mode,
        f"--dt-seconds={dt_seconds}",
        f"--freq-hz={freq_hz}",
    ]
    completed = subprocess.run(generic_command, capture_output=True, text=True)
    if completed.returncode != 0:
        return None
    return json.loads(completed.stdout)


def reference_records_for_case(case: dict, reference_template: str) -> list[dict]:
    wav = case["wav_path"]
    command = command_from_template(reference_template, wav=wav, mode=case["mode"])
    completed = subprocess.run(command, check=True, capture_output=True, text=True)
    return parse_decode_records(completed.stdout)


def lookup_case(manifest: dict, case_id: str) -> dict:
    for case in manifest["cases"]:
        if case["id"] == case_id:
            return case
    raise KeyError(f"missing case {case_id}")


def nearest_top_candidate(report: dict, dt_seconds: float, freq_hz: float) -> dict | None:
    candidates = report.get("diagnostics", {}).get("top_candidates", [])
    if not candidates:
        return None
    return min(
        candidates,
        key=lambda candidate: (
            abs(candidate["freq_hz"] - freq_hz),
            abs(candidate["dt_seconds"] - dt_seconds),
            -candidate["score"],
        ),
    )


def has_nearby_top_candidate(
    report: dict,
    dt_seconds: float,
    freq_hz: float,
    *,
    dt_tolerance: float = 0.08,
    freq_tolerance: float = 7.5,
) -> bool:
    candidate = nearest_top_candidate(report, dt_seconds, freq_hz)
    if candidate is None:
        return False
    return (
        abs(candidate["dt_seconds"] - dt_seconds) <= dt_tolerance
        and abs(candidate["freq_hz"] - freq_hz) <= freq_tolerance
    )


def message_in_decode_report(report: dict, message: str) -> bool:
    return any(decode["text"].strip().upper() == message for decode in report.get("decodes", []))


def closest_decode(report: dict, dt_seconds: float, freq_hz: float) -> dict | None:
    decodes = report.get("decodes", [])
    if not decodes:
        return None
    return min(
        decodes,
        key=lambda decode: (
            abs(decode["dt_seconds"] - dt_seconds),
            abs(decode["freq_hz"] - freq_hz),
            decode["candidate_score"],
        ),
    )


def classify_stock_only_message(
    *,
    stock_record: dict,
    baseline_report: dict,
    raised_report: dict,
    extra_pass_report: dict,
    no_subtraction_report: dict,
    stock_debug: dict | None,
) -> str:
    message = stock_record["message"]
    if not has_nearby_top_candidate(raised_report, stock_record["dt_seconds"], stock_record["freq_hz"]):
        return "search/admission"
    if message_in_decode_report(no_subtraction_report, message) or message_in_decode_report(
        extra_pass_report, message
    ):
        return "subtraction/pass scheduling"
    if stock_debug is None:
        return "search/admission"

    if any(pass_info.get("decoded_text") == message for pass_info in stock_debug.get("passes", [])):
        return "subtraction/pass scheduling"

    refined_dt = stock_debug.get("refined_dt_seconds", stock_record["dt_seconds"])
    refined_freq = stock_debug.get("refined_freq_hz", stock_record["freq_hz"])
    if abs(refined_dt - stock_record["dt_seconds"]) > 0.03 or abs(refined_freq - stock_record["freq_hz"]) > 4.5:
        return "refine drift"
    return "metric/LLR extraction"


def classify_rust_only_message(message: str, truth_messages: list[str]) -> tuple[str, str]:
    if message in truth_messages:
        return "acceptance/gating", "stock miss on truth-labeled signal"
    return "acceptance/gating", "rust false positive vs stock"


def triage_mismatches(args: argparse.Namespace) -> int:
    compare_payload = json.loads(Path(args.compare).read_text())
    manifest = json.loads(Path(compare_payload["manifest"]).read_text())
    decoder_binary = Path(args.decoder_binary).resolve()
    output_root = Path(args.output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    reference_template = args.reference_cmd or compare_payload.get("reference_cmd")
    if args.use_stock_reference or not reference_template:
        reference_template = default_stock_reference_template(args.profile)

    bundle_count = 0
    mismatch_count = 0
    for result in compare_payload["results"]:
        if result["match"]:
            continue
        mismatch_count += 1
        case = lookup_case(manifest, result["id"])
        mode = case["mode"]
        wav_path = case["wav_path"]
        truth_messages = truth_messages_for_case(case)
        stock_records = reference_records_for_case(case, reference_template)
        stock_records_by_message: dict[str, list[dict]] = {}
        for record in stock_records:
            stock_records_by_message.setdefault(record["message"], []).append(record)

        baseline_report = run_decoder_report(
            decoder_binary,
            wav_path,
            mode,
            args.profile,
            max_candidates=args.max_candidates,
            search_passes=args.search_passes,
            no_subtraction=False,
        )
        raised_report = run_decoder_report(
            decoder_binary,
            wav_path,
            mode,
            args.profile,
            max_candidates=args.raised_max_candidates,
            search_passes=args.search_passes,
            no_subtraction=False,
        )
        extra_pass_report = run_decoder_report(
            decoder_binary,
            wav_path,
            mode,
            args.profile,
            max_candidates=args.raised_max_candidates,
            search_passes=args.extra_search_passes,
            no_subtraction=False,
        )
        no_subtraction_report = run_decoder_report(
            decoder_binary,
            wav_path,
            mode,
            args.profile,
            max_candidates=args.raised_max_candidates,
            search_passes=args.search_passes,
            no_subtraction=True,
        )

        bundle_dir = output_root / result["id"]
        bundle_dir.mkdir(parents=True, exist_ok=True)
        bundle = {
            "id": result["id"],
            "mode": mode,
            "cohort": case.get("cohort"),
            "wav_path": wav_path,
            "truth_messages": truth_messages,
            "stock_messages": [record["message"] for record in stock_records],
            "rust_messages": [decode["text"].strip().upper() for decode in baseline_report.get("decodes", [])],
            "stock_only": [],
            "rust_only": [],
            "baseline_report": baseline_report,
            "raised_report": raised_report,
            "extra_pass_report": extra_pass_report,
            "no_subtraction_report": no_subtraction_report,
        }

        for message in sorted(set(result.get("stock_only_messages", []))):
            for ordinal, stock_record in enumerate(stock_records_by_message.get(message, [])):
                stock_debug = run_decoder_debug(
                    decoder_binary,
                    wav_path,
                    mode,
                    stock_record["dt_seconds"],
                    stock_record["freq_hz"],
                    message,
                )
                best_rust_decode = closest_decode(
                    baseline_report, stock_record["dt_seconds"], stock_record["freq_hz"]
                )
                best_rust_candidate = nearest_top_candidate(
                    raised_report, stock_record["dt_seconds"], stock_record["freq_hz"]
                )
                best_rust_debug = None
                if best_rust_decode is not None:
                    best_rust_debug = run_decoder_debug(
                        decoder_binary,
                        wav_path,
                        mode,
                        best_rust_decode["dt_seconds"],
                        best_rust_decode["freq_hz"],
                        best_rust_decode["text"],
                    )
                elif best_rust_candidate is not None:
                    best_rust_debug = run_decoder_debug(
                        decoder_binary,
                        wav_path,
                        mode,
                        best_rust_candidate["dt_seconds"],
                        best_rust_candidate["freq_hz"],
                        None,
                    )
                stage = classify_stock_only_message(
                    stock_record=stock_record,
                    baseline_report=baseline_report,
                    raised_report=raised_report,
                    extra_pass_report=extra_pass_report,
                    no_subtraction_report=no_subtraction_report,
                    stock_debug=stock_debug,
                )
                artifact = {
                    "message": message,
                    "ordinal": ordinal,
                    "classification": stage,
                    "stock_record": stock_record,
                    "closest_baseline_decode": best_rust_decode,
                    "closest_raised_top_candidate": best_rust_candidate,
                    "stock_coordinate_debug": stock_debug,
                    "best_rust_coordinate_debug": best_rust_debug,
                }
                bundle["stock_only"].append(artifact)
                bundle_count += 1

        for message in sorted(set(result.get("rust_only_messages", []))):
            stage, issue_kind = classify_rust_only_message(message, truth_messages)
            rust_decode = next(
                (
                    decode
                    for decode in baseline_report.get("decodes", [])
                    if decode["text"].strip().upper() == message
                ),
                None,
            )
            rust_debug = None
            if rust_decode is not None:
                rust_debug = run_decoder_debug(
                    decoder_binary,
                    wav_path,
                    mode,
                    rust_decode["dt_seconds"],
                    rust_decode["freq_hz"],
                    message,
                )
            bundle["rust_only"].append(
                {
                    "message": message,
                    "classification": stage,
                    "issue_kind": issue_kind,
                    "rust_decode": rust_decode,
                    "rust_coordinate_debug": rust_debug,
                }
            )
            bundle_count += 1

        write_manifest(bundle_dir / "triage.json", bundle)

    summary = {
        "compare": str(Path(args.compare).resolve()),
        "manifest": compare_payload["manifest"],
        "profile": args.profile,
        "reference_cmd": reference_template,
        "mismatch_cases": mismatch_count,
        "mismatch_bundles": bundle_count,
        "output_root": str(output_root),
    }
    write_manifest(output_root / "summary.json", summary)
    print(json.dumps(summary, indent=2))
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Mode parity corpus tooling")
    subparsers = parser.add_subparsers(dest="command", required=True)

    build_spec = subparsers.add_parser("build-spec", help="Generate a pinned spec corpus")
    build_spec.add_argument(
        "--decoder-binary",
        default="decoder/target/debug/ft8-decoder",
        help="Path to the local generator binary.",
    )
    build_spec.add_argument(
        "--generator-source",
        choices=["rust", "reference"],
        default="rust",
        help="Whether to generate WAVs with the local Rust encoder or reference helpers.",
    )
    build_spec.add_argument("--reference-ft4-gen", help="Path to the transient FT4 reference generator.")
    build_spec.add_argument("--reference-ft2-gen", help="Path to the transient FT2 reference generator.")
    build_spec.add_argument("--output-root", default="artifacts/mode-parity/spec")
    build_spec.add_argument("--modes", nargs="+", default=["ft4", "ft2"])
    build_spec.add_argument("--freq-hz", nargs="+", default=["650", "900", "1150", "1400"])
    build_spec.add_argument("--start-seconds", nargs="+", default=["0.0", "0.1", "0.25", "0.5"])
    build_spec.add_argument("--total-seconds", type=float)

    compare = subparsers.add_parser("compare", help="Compare corpus decode sets")
    compare.add_argument("--manifest", required=True)
    compare.add_argument(
        "--rust-cmd",
        required=True,
        help="Shell-style command template with {wav} and optional {mode}.",
    )
    compare.add_argument(
        "--reference-cmd",
        help="Shell-style command template with {wav} and optional {mode}. Omit to use frozen expected_messages in the manifest.",
    )
    compare.add_argument(
        "--use-stock-reference",
        action="store_true",
        help="Use the canonical stock reference entrypoint for the requested profile.",
    )
    compare.add_argument(
        "--profile",
        default="medium",
        choices=["medium", "deepest"],
        help="Reference profile when --use-stock-reference is enabled.",
    )
    compare.add_argument("--jobs", type=int, help="Parallel decode jobs. Defaults to the host CPU count.")
    compare.add_argument("--output", help="Optional JSON output path.")

    build_replay = subparsers.add_parser("build-replay", help="Freeze stock outputs for sample WAVs")
    build_replay.add_argument("--output-root", default="artifacts/mode-parity/replay")
    build_replay.add_argument(
        "--sample",
        action="append",
        required=True,
        help="Replay sample in MODE:/absolute/path.wav form. May be repeated.",
    )
    build_replay.add_argument(
        "--reference-cmd",
        required=True,
        help="Shell-style command template with {wav} and optional {mode}.",
    )

    build_synth = subparsers.add_parser("build-synth", help="Generate a deterministic synthetic corpus")
    build_synth.add_argument(
        "--decoder-binary",
        default="decoder/target/debug/ft8-decoder",
        help="Path to the local generator binary.",
    )
    build_synth.add_argument(
        "--generator-source",
        choices=["rust", "reference"],
        default="reference",
        help="Whether to generate source signals with the local Rust encoder or reference helpers.",
    )
    build_synth.add_argument("--reference-ft4-gen", help="Path to the transient FT4 reference generator.")
    build_synth.add_argument(
        "--reference-ft2-gen",
        default=str(DEFAULT_FT2_REF_GEN),
        help="Path to the transient FT2 reference generator.",
    )
    build_synth.add_argument("--output-root", default="artifacts/mode-parity/synthetic")
    build_synth.add_argument("--modes", nargs="+", default=["ft4", "ft2"])
    build_synth.add_argument("--count-per-mode", type=int, default=32)
    build_synth.add_argument(
        "--single-count-per-mode",
        type=int,
        help="If set, emit exactly this many single-signal cases per mode.",
    )
    build_synth.add_argument(
        "--mixed-count-per-mode",
        type=int,
        help="If set, emit exactly this many mixed-signal cases per mode.",
    )
    build_synth.add_argument("--seed", type=int, default=12345)
    build_synth.add_argument("--max-signals", type=int, default=3)
    build_synth.add_argument("--signal-db-min", type=float, default=-9.0)
    build_synth.add_argument("--signal-db-max", type=float, default=0.0)
    build_synth.add_argument("--noise-dbfs", type=float, default=-28.0)
    build_synth.add_argument("--total-seconds", type=float)

    triage = subparsers.add_parser("triage", help="Emit one mismatch triage bundle per failing case")
    triage.add_argument("--compare", required=True, help="JSON emitted by the compare subcommand.")
    triage.add_argument(
        "--decoder-binary",
        default="decoder/target/release/ft8-decoder",
        help="Path to the local Rust decoder binary.",
    )
    triage.add_argument(
        "--output-root",
        required=True,
        help="Directory that will receive one triage bundle per mismatching case.",
    )
    triage.add_argument(
        "--reference-cmd",
        help="Shell-style command template with {wav} and optional {mode}. Defaults to the canonical stock entrypoint.",
    )
    triage.add_argument(
        "--use-stock-reference",
        action="store_true",
        help="Ignore any frozen reference output and rerun the canonical stock decoder for triage.",
    )
    triage.add_argument(
        "--profile",
        default="medium",
        choices=["medium", "deepest"],
        help="Decode profile to use for both stock and Rust triage reruns.",
    )
    triage.add_argument("--max-candidates", type=int, default=600)
    triage.add_argument("--raised-max-candidates", type=int, default=4000)
    triage.add_argument("--search-passes", type=int, default=3)
    triage.add_argument("--extra-search-passes", type=int, default=6)

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "build-spec":
        return build_spec_corpus(args)
    if args.command == "compare":
        return compare_corpus(args)
    if args.command == "build-replay":
        return build_replay_manifest(args)
    if args.command == "build-synth":
        return build_synth_corpus(args)
    if args.command == "triage":
        return triage_mismatches(args)
    raise AssertionError(f"unsupported command {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
