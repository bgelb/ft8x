#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
from pathlib import Path


def run_json(command: list[str], cwd: Path) -> dict:
    completed = subprocess.run(command, cwd=cwd, check=True, capture_output=True, text=True)
    return json.loads(completed.stdout)


def norm_message(text: str) -> str:
    return " ".join(text.strip().upper().split())


def first_decoded_variant(fixed_payload: dict) -> tuple[dict, dict] | tuple[None, None]:
    for variant in fixed_payload.get("variants", []):
        for decode_pass in variant.get("passes", []):
            subtract_event = decode_pass.get("subtract_event")
            if subtract_event is None:
                continue
            return variant, decode_pass
    return None, None


def subtract_bits(cwd: Path, wav: Path, out_wav: Path, bits77: str, freq_hz: float, dt_internal: float) -> None:
    helper = Path("/private/tmp/mode-refs-test/ft4/ft4-stock-subtract-bits")
    subprocess.run(
        [
            str(helper),
            str(wav),
            str(out_wav),
            bits77,
            str(freq_hz),
            str(dt_internal),
        ],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run FT4 stock component pipeline: search, fixed decode, subtract, repeat.")
    parser.add_argument("wav")
    parser.add_argument("--profile", choices=["medium", "deepest"], default="medium")
    parser.add_argument("--search-passes", type=int, default=3)
    parser.add_argument("--max-candidates", type=int, default=16)
    args = parser.parse_args()

    cwd = Path(__file__).resolve().parent.parent
    input_wav = Path(args.wav).resolve()
    passes = []
    final_messages: list[str] = []
    seen_messages: set[str] = set()

    with tempfile.TemporaryDirectory(prefix="ft4-stock-component-") as tmpdir:
        current_wav = Path(tmpdir) / "pass0.wav"
        current_wav.write_bytes(input_wav.read_bytes())

        for pass_index in range(1, args.search_passes + 1):
            search_payload = run_json(
                ["python3", "scripts/run_stock_ft4_search.py", str(current_wav)],
                cwd,
            )
            pass_entry = {
                "pass_index": pass_index,
                "search": search_payload,
                "candidate_results": [],
                "accepted_messages": [],
            }
            candidates = search_payload.get("candidates", [])[: args.max_candidates]
            if not candidates:
                passes.append(pass_entry)
                break

            changed = False
            for candidate in candidates:
                fixed_payload = run_json(
                    [
                        "python3",
                        "scripts/run_stock_ft4_fixed.py",
                        str(current_wav),
                        str(candidate["freq_hz"]),
                        "--profile",
                        args.profile,
                    ],
                    cwd,
                )
                variant, decode_pass = first_decoded_variant(fixed_payload)
                candidate_entry = {
                    "candidate_index": candidate["candidate_index"],
                    "coarse_freq_hz": candidate["freq_hz"],
                    "coarse_score": candidate["score"],
                    "fixed": fixed_payload,
                }
                if variant is None or decode_pass is None:
                    pass_entry["candidate_results"].append(candidate_entry)
                    continue
                subtract_event = decode_pass["subtract_event"]
                message = norm_message(decode_pass["decoded"])
                subtract_bits(
                    cwd,
                    current_wav,
                    Path(tmpdir) / f"pass{pass_index}-cand{candidate['candidate_index']}.wav",
                    subtract_event["message_bits"],
                    subtract_event["freq_exact_hz"],
                    subtract_event["dt_internal_seconds"],
                )
                current_wav = Path(tmpdir) / f"pass{pass_index}-cand{candidate['candidate_index']}.wav"
                candidate_entry["accepted"] = {
                    "message": message,
                    "segment": variant["segment"],
                    "dt_internal_seconds": subtract_event["dt_internal_seconds"],
                    "freq_exact_hz": subtract_event["freq_exact_hz"],
                    "message_bits": subtract_event["message_bits"],
                }
                if message not in seen_messages:
                    seen_messages.add(message)
                    final_messages.append(message)
                    pass_entry["accepted_messages"].append(message)
                changed = True
                pass_entry["candidate_results"].append(candidate_entry)
            if changed:
                post_search = run_json(
                    ["python3", "scripts/run_stock_ft4_search.py", str(current_wav)],
                    cwd,
                )
                pass_entry["post_residual_signature"] = post_search.get("residual_signature")
            else:
                pass_entry["post_residual_signature"] = search_payload.get("residual_signature")
            passes.append(pass_entry)
            if not pass_entry["accepted_messages"]:
                break
            if not changed:
                break

    print(
        json.dumps(
            {
                "wav": str(input_wav),
                "profile": args.profile,
                "passes": passes,
                "final_messages": final_messages,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
