#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import math
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


DECODE_PATTERN = re.compile(
    r"^(?P<utc>\d{6})\s+(?P<snr>-?\d+)\s+(?P<dt>-?\d+(?:\.\d+)?)\s+(?P<freq>\d+)\s+(?:~|\+)\s+(?P<message>.+?)\s*$"
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


def parse_decode_lines(text: str) -> list[str]:
    messages: list[str] = []
    for line in text.splitlines():
        match = DECODE_PATTERN.match(line.strip())
        if not match:
            continue
        message = re.sub(r"\s+", " ", match.group("message").strip().upper())
        messages.append(message)
    return sorted(set(messages))


def command_from_template(template: str, **replacements: str) -> list[str]:
    rendered = template
    for key, value in replacements.items():
        rendered = rendered.replace("{" + key + "}", value)
    return shlex.split(rendered)


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
    write_wav_i16(output_path, sample_rate_hz, samples)


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
    manifest_path.write_text(
        json.dumps(
            {
                "kind": "spec",
                "generator": (
                    "reference"
                    if args.generator_source == "reference"
                    else str(decoder_binary)
                ),
                "cases": [asdict(case) for case in cases],
            },
            indent=2,
        )
        + "\n"
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
        for index in range(args.count_per_mode):
            signal_count = rng.randint(1, args.max_signals)
            mixed = [0.0] * total_samples
            expected = []
            signals = []
            for signal_index in range(signal_count):
                first, second, info, acknowledge = rng.choice(builtin_messages())
                freq_hz = rng.choice([650.0, 900.0, 1150.0, 1400.0, 1650.0, 1900.0])
                start_seconds = rng.choice([0.0, 0.05, 0.1, 0.2, 0.35])
                rendered = render_expected_message(first, second, info, acknowledge)
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
                if args.generator_source == "reference":
                    generator = reference_generators[mode]
                    if generator is None:
                        raise SystemExit(f"missing reference generator for mode {mode}")
                    _, samples = generate_reference_samples(
                        generator, rendered, freq_hz, start_seconds, total_seconds
                    )
                else:
                    _, samples = generate_rust_samples(
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
                gain = 10 ** (rng.uniform(args.signal_db_min, args.signal_db_max) / 20.0)
                for i, sample in enumerate(samples):
                    mixed[i] += gain * (sample / 32768.0)

            noise_sigma = 10 ** (args.noise_dbfs / 20.0)
            for i in range(total_samples):
                mixed[i] += rng.gauss(0.0, noise_sigma)

            peak = max(1.0, max(abs(value) for value in mixed) / 0.8)
            pcm = [max(-32767, min(32767, round((value / peak) * 32767.0))) for value in mixed]
            case_id = f"{mode}-mix-{index:04d}"
            wav_path = mode_dir / f"{case_id}.wav"
            write_wav_i16(wav_path, sample_rate_hz, pcm)
            cases.append(
                {
                    "id": case_id,
                    "mode": mode,
                    "wav_path": str(wav_path),
                    "expected_messages": sorted(set(expected)),
                    "signals": signals,
                    "noise_dbfs": args.noise_dbfs,
                }
            )

    manifest_path = output_root / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "kind": "synthetic",
                "generator": args.generator_source,
                "seed": args.seed,
                "cases": cases,
            },
            indent=2,
        )
        + "\n"
    )
    print(manifest_path)
    return 0


def compare_corpus(args: argparse.Namespace) -> int:
    manifest = json.loads(Path(args.manifest).read_text())
    cases = manifest["cases"]
    results = []
    mismatches = 0
    for case in cases:
        wav = case["wav_path"]
        rust_command = command_from_template(args.rust_cmd, wav=wav, mode=case["mode"])
        reference_command = command_from_template(args.reference_cmd, wav=wav, mode=case["mode"])
        rust_output = subprocess.run(rust_command, check=True, capture_output=True, text=True)
        reference_output = subprocess.run(
            reference_command, check=True, capture_output=True, text=True
        )
        rust_messages = parse_decode_lines(rust_output.stdout)
        reference_messages = parse_decode_lines(reference_output.stdout)
        mismatch = rust_messages != reference_messages
        mismatches += int(mismatch)
        results.append(
            {
                "id": case["id"],
                "mode": case["mode"],
                "wav_path": wav,
                "rust_messages": rust_messages,
                "reference_messages": reference_messages,
                "match": not mismatch,
            }
        )

    payload = {
        "manifest": str(Path(args.manifest).resolve()),
        "case_count": len(results),
        "mismatch_count": mismatches,
        "results": results,
    }
    if args.output:
        Path(args.output).write_text(json.dumps(payload, indent=2) + "\n")
    print(json.dumps({"case_count": len(results), "mismatch_count": mismatches}, indent=2))
    return 0 if mismatches == 0 else 1


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
        required=True,
        help="Shell-style command template with {wav} and optional {mode}.",
    )
    compare.add_argument("--output", help="Optional JSON output path.")

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
    build_synth.add_argument("--reference-ft2-gen", help="Path to the transient FT2 reference generator.")
    build_synth.add_argument("--output-root", default="artifacts/mode-parity/synthetic")
    build_synth.add_argument("--modes", nargs="+", default=["ft4", "ft2"])
    build_synth.add_argument("--count-per-mode", type=int, default=32)
    build_synth.add_argument("--seed", type=int, default=12345)
    build_synth.add_argument("--max-signals", type=int, default=3)
    build_synth.add_argument("--signal-db-min", type=float, default=-9.0)
    build_synth.add_argument("--signal-db-max", type=float, default=0.0)
    build_synth.add_argument("--noise-dbfs", type=float, default=-28.0)
    build_synth.add_argument("--total-seconds", type=float)

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "build-spec":
        return build_spec_corpus(args)
    if args.command == "compare":
        return compare_corpus(args)
    if args.command == "build-synth":
        return build_synth_corpus(args)
    raise AssertionError(f"unsupported command {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
