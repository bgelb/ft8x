#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import subprocess

from mode_reference import locate_ft4_stock_decode174


def parse_floats(payload: str) -> list[float]:
    payload = payload.strip()
    if not payload:
        return []
    return [float(value) for value in payload.split()]


def parse_output(stdout: str) -> dict:
    result: dict[str, object] = {"raw_stdout": stdout, "osd_results": []}
    current_osd: dict[str, object] | None = None
    for raw_line in stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key in {"bp_iter", "ntype", "nharderror", "saved_count"}:
            result[key] = int(value)
        elif key == "dmin":
            result[key] = float(value)
        elif key == "decoded":
            result["decoded"] = " ".join(value.upper().split())
        elif key == "message_bits":
            result["message_bits"] = value
        elif key.startswith("zsave"):
            result[key] = parse_floats(value)
        elif key == "osd_index":
            # Stock helper emits `osd_index=N nharderror=X`, `osd_index=N dmin=...`, `osd_index=N decoded=...`
            parts = value.split()
            index = int(parts[0])
            if current_osd is None or current_osd.get("index") != index:
                current_osd = {"index": index}
                result["osd_results"].append(current_osd)
            if len(parts) >= 2 and parts[1].startswith("nharderror"):
                current_osd["nharderror"] = int(parts[1].split("=", 1)[1])
            elif len(parts) >= 2 and parts[1].startswith("dmin"):
                current_osd["dmin"] = float(parts[1].split("=", 1)[1])
            elif len(parts) >= 2 and parts[1].startswith("decoded"):
                current_osd["decoded"] = " ".join(parts[1].split("=", 1)[1].upper().split())
        elif key == "osd_bits":
            index_text, bits = value.split(maxsplit=1)
            index = int(index_text)
            if current_osd is None or current_osd.get("index") != index:
                current_osd = {"index": index}
                result["osd_results"].append(current_osd)
            current_osd["message_bits"] = bits.strip()
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Run stock FT4 decode174 probe")
    parser.add_argument("wav")
    parser.add_argument("--freq-hz", type=float, required=True)
    parser.add_argument("--dt-seconds", type=float, required=True)
    parser.add_argument("--llr-set", type=int, choices=[1, 2, 3], required=True)
    parser.add_argument("--max-osd", type=int, default=2)
    parser.add_argument("--norder", type=int, default=2)
    parser.add_argument("--helper", help="Override FT4 stock decode174 helper path.")
    args = parser.parse_args()

    helper = locate_ft4_stock_decode174(args.helper)
    completed = subprocess.run(
        [
            str(helper),
            args.wav,
            str(args.freq_hz),
            str(args.dt_seconds),
            str(args.llr_set),
            str(args.max_osd),
            str(args.norder),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    print(json.dumps(parse_output(completed.stdout)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
