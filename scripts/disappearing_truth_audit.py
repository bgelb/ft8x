from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


CALL_RE = re.compile(r"^[A-Z0-9/]{3,}$")
GRID_RE = re.compile(r"^[A-R]{2}\d{2}(?:[A-X]{2})?$")
REPORT_RE = re.compile(r"^[R+-]?\d{2}$|^RR73$|^RRR$|^73$")
VERSION_RE = re.compile(r"^(?P<core>\d+\.\d+\.\d+)(?:-(?P<tag>rc)(?P<tag_number>\d+))?$")
AMATEUR_CALL_RE = re.compile(r"^(?:[A-Z0-9]{1,4}/)?[A-Z0-9]{0,2}\d[A-Z0-9]{1,6}(?:/[A-Z0-9]{1,4})?$")

LOOKS_REAL = "looks_real"
AMBIGUOUS = "ambiguous"
LIKELY_STALE_LABEL = "likely_stale_label"

GRID_COUNTRY_MATCH = "match"
GRID_COUNTRY_MISMATCH = "mismatch"
GRID_COUNTRY_UNKNOWN = "unknown"

CALLSIGN_ENTITY_RULES: list[tuple[str, str]] = [
    ("EA8", "Spain"),
    ("IW9", "Italy"),
    ("IZ8", "Italy"),
    ("IU2", "Italy"),
    ("JH1", "Japan"),
    ("NT6", "United States"),
    ("NA4", "United States"),
    ("WG5", "United States"),
    ("2E0", "United Kingdom"),
    ("DO1", "Germany"),
    ("DO2", "Germany"),
    ("DO4", "Germany"),
    ("DO6", "Germany"),
    ("ES3", "Estonia"),
    ("F4", "France"),
    ("G4", "United Kingdom"),
    ("G8", "United Kingdom"),
    ("K6", "United States"),
    ("M0", "United Kingdom"),
    ("N2", "United States"),
    ("OE4", "Austria"),
    ("ON7", "Belgium"),
    ("R2", "Russia"),
    ("R7", "Russia"),
    ("RA1", "Russia"),
    ("RW6", "Russia"),
    ("S57", "Slovenia"),
    ("SV2", "Greece"),
    ("UA6", "Russia"),
    ("UA9", "Russia"),
    ("UT9", "Ukraine"),
    ("W3", "United States"),
    ("W4", "United States"),
    ("YO9", "Romania"),
]

ENTITY_EQUIVALENTS: dict[str, set[str]] = {
    "Austria": {"Austria"},
    "Belgium": {"Belgium"},
    "Estonia": {"Estonia"},
    "France": {"France"},
    "Germany": {"Germany"},
    "Greece": {"Greece"},
    "Italy": {"Italy"},
    "Japan": {"Japan"},
    "Romania": {"Romania"},
    "Russia": {"Russia"},
    "Slovenia": {"Slovenia"},
    "Spain": {"Spain", "Canary Islands"},
    "Ukraine": {"Ukraine"},
    "United Kingdom": {"United Kingdom"},
    "United States": {"United States"},
    "Canary Islands": {"Spain", "Canary Islands"},
}


@dataclass
class Candidate:
    scope: str
    sample_id: str
    message: str
    first_version: str
    last_version: str
    hit_count: int
    missing_after_count: int
    pattern: str
    ever_decoded_profiles: list[str]
    pattern_by_profile: dict[str, str]
    truth_freq_hz: int
    truth_dt: float
    truth_snr: int
    nearest_truth_message: str
    nearest_truth_delta_hz: float
    sender_callsign: str | None
    local_sender_conflicts: list[str]
    exact_message_elsewhere: list[str]
    exact_message_nearby: list[str]
    pair_elsewhere: list[str]
    pair_nearby: list[str]
    callsign_context_elsewhere: dict[str, list[str]]
    callsign_context_nearby: dict[str, list[str]]
    callsign_legality: str
    grid_owner_callsign: str | None
    grid_locator: str | None
    callsign_entity: str | None
    grid_entity: str | None
    grid_country_consistency: str
    classification: str
    suspicion_score: int
    reasoning: str


def version_key(version: str) -> tuple[int, int, int, int, int]:
    match = VERSION_RE.fullmatch(version)
    if not match:
        raise ValueError(f"Unsupported version format: {version}")
    major, minor, patch = match.group("core").split(".")
    tag = match.group("tag")
    tag_number = int(match.group("tag_number") or 0)
    stage_rank = 0 if tag == "rc" else 1
    return int(major), int(minor), int(patch), stage_rank, tag_number


def extract_callsigns(message: str) -> list[str]:
    callsigns: list[str] = []
    for token in message.split():
        if token in {"CQ", "DX"} or GRID_RE.match(token) or REPORT_RE.match(token):
            continue
        if CALL_RE.match(token):
            callsigns.append(token)
    return callsigns


def callsign_legal(callsign: str) -> bool:
    return bool(AMATEUR_CALL_RE.fullmatch(callsign))


def determine_grid_owner(message: str) -> tuple[str | None, str | None]:
    tokens = message.split()
    grid = tokens[-1] if tokens and GRID_RE.match(tokens[-1]) and not REPORT_RE.match(tokens[-1]) else None
    if not grid:
        return None, None
    if tokens[0] == "CQ":
        for token in tokens[1:-1]:
            if token in {"DX"} or REPORT_RE.match(token):
                continue
            if CALL_RE.match(token):
                return token, grid
    callsigns = [token for token in tokens[:-1] if CALL_RE.match(token) and token not in {"CQ", "DX"}]
    if len(callsigns) >= 2:
        return callsigns[1], grid
    return None, grid


def determine_sender_callsign(message: str) -> str | None:
    tokens = message.split()
    if not tokens:
        return None
    if tokens[0] == "CQ":
        for token in tokens[1:]:
            if token == "DX" or GRID_RE.match(token) or REPORT_RE.match(token):
                continue
            if CALL_RE.match(token):
                return token
        return None
    callsigns = extract_callsigns(message)
    if len(callsigns) >= 2:
        return callsigns[1]
    return callsigns[0] if callsigns else None


def callsign_entity(callsign: str | None) -> str | None:
    if not callsign:
        return None
    for prefix, entity in sorted(CALLSIGN_ENTITY_RULES, key=lambda item: len(item[0]), reverse=True):
        if callsign.startswith(prefix):
            return entity
    return None


def compare_entities(callsign_country: str | None, grid_country: str | None) -> str:
    if not callsign_country or not grid_country:
        return GRID_COUNTRY_UNKNOWN
    left = ENTITY_EQUIVALENTS.get(callsign_country, {callsign_country})
    right = ENTITY_EQUIVALENTS.get(grid_country, {grid_country})
    return GRID_COUNTRY_MATCH if left & right else GRID_COUNTRY_MISMATCH


def load_grid_country_cache(path: Path) -> dict[str, str | None]:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def truth_entry_key(entry: dict[str, Any]) -> tuple[str, int, str]:
    return entry["message"], int(entry["freq_hz"]), str(entry["utc"])


def nearby_sample_ids(sample_id: str, sample_ids: set[str]) -> set[str]:
    match = re.fullmatch(r"websdr_test(\d+)", sample_id)
    if match:
        value = int(match.group(1))
        return {
            candidate
            for delta in (-2, -1, 1, 2)
            if (candidate := f"websdr_test{value + delta}") in sample_ids
        }

    match = re.fullmatch(r"(\d{6})_(\d{6})", sample_id)
    if match:
        date_part, time_part = match.groups()
        current = datetime.strptime(date_part + time_part, "%y%m%d%H%M%S")
        neighbors: set[str] = set()
        for candidate in sample_ids:
            other = re.fullmatch(r"(\d{6})_(\d{6})", candidate)
            if not other:
                continue
            other_dt = datetime.strptime("".join(other.groups()), "%y%m%d%H%M%S")
            delta = abs((other_dt - current).total_seconds())
            if 0 < delta <= 30:
                neighbors.add(candidate)
        return neighbors

    return set()


def pick_results_path(root: Path) -> Path:
    best: tuple[int, int, str, Path] | None = None
    for candidate in root.glob("artifacts/results/*/results.json"):
        try:
            payload = json.loads(candidate.read_text())
        except Exception:
            continue
        scored_runs = [run for run in payload.get("runs", []) if run.get("metrics") and run.get("dataset_id") == "kgoba-ft8-lib"]
        release_count = len({run["release_version"] for run in scored_runs})
        if release_count < 2:
            continue
        score = (release_count, len(scored_runs), payload.get("generated_at", ""), candidate)
        if best is None or score > best:
            best = score
    if best is None:
        raise SystemExit("No multi-version scored results.json found under artifacts/results/")
    return best[-1]


def classify_candidate(
    callsigns: list[str],
    callsign_legality: str,
    grid_country_consistency: str,
    sender_callsign: str | None,
    local_sender_conflicts: list[str],
    nearest_truth_delta_hz: float,
    exact_message_elsewhere: list[str],
    exact_message_nearby: list[str],
    pair_elsewhere: list[str],
    pair_nearby: list[str],
    callsign_context_elsewhere: dict[str, list[str]],
    callsign_context_nearby: dict[str, list[str]],
) -> tuple[str, str]:
    nearby_support = bool(exact_message_nearby or pair_nearby or callsign_context_nearby)
    strong_support = bool(exact_message_elsewhere or pair_elsewhere or exact_message_nearby or pair_nearby)
    soft_support = len(callsign_context_elsewhere) >= 1 or len(callsign_context_nearby) >= 1

    if callsign_legality != "all_legal":
        return (
            LIKELY_STALE_LABEL,
            "The message contains a token that does not look like a legal amateur callsign, which makes an inherited bad label more plausible.",
        )
    if grid_country_consistency == GRID_COUNTRY_MISMATCH:
        return (
            LIKELY_STALE_LABEL,
            "The grid square does not line up with the callsign's expected entity, which is a strong sign that the label is not real.",
        )
    if sender_callsign and local_sender_conflicts:
        return (
            LIKELY_STALE_LABEL,
            f"The sender `{sender_callsign}` also appears in another truth line in the same 15-second slot ({'; '.join(local_sender_conflicts)}), which should be impossible for a real FT8 transmission.",
        )
    if nearest_truth_delta_hz <= 3 and not strong_support and not nearby_support:
        return (
            LIKELY_STALE_LABEL,
            f"The line sits only {nearest_truth_delta_hz:.0f} Hz from another truth label and lacks cross-sample corroboration, which looks more like an early decoder collision than a real signal.",
        )
    if strong_support:
        pieces = []
        if exact_message_nearby:
            pieces.append(f"the exact message repeats in nearby sample(s) {', '.join(exact_message_nearby)}")
        if exact_message_elsewhere:
            pieces.append(f"the exact message recurs in {', '.join(exact_message_elsewhere)}")
        if pair_nearby:
            pieces.append(f"the same station pair shows up in nearby sample(s) {', '.join(pair_nearby)}")
        if pair_elsewhere:
            pieces.append(f"the station pair recurs in {', '.join(pair_elsewhere)}")
        return (
            LOOKS_REAL,
            " and ".join(pieces).capitalize() + ", which is strong QSO continuity evidence that the label is real.",
        )
    if soft_support and grid_country_consistency != GRID_COUNTRY_MISMATCH:
        sources = []
        if callsign_context_nearby:
            nearby_details = ", ".join(
                f"{callsign} in {', '.join(samples)}"
                for callsign, samples in sorted(callsign_context_nearby.items())
            )
            sources.append(f"nearby samples ({nearby_details})")
        if callsign_context_elsewhere:
            corpus_details = ", ".join(
                f"{callsign} in {', '.join(samples)}"
                for callsign, samples in sorted(callsign_context_elsewhere.items())
            )
            sources.append(f"other samples ({corpus_details})")
        details = ", ".join(
            f"{callsign} in {', '.join(samples)}"
            for callsign, samples in sorted(callsign_context_nearby.items())
        )
        return (
            LOOKS_REAL,
            f"The exact line does not recur, but its callsign context does via {' and '.join(sources)}, which makes the label look plausible rather than random decoder output.",
        )
    return (
        AMBIGUOUS,
        "The disappearance pattern is real, but local context is too thin to tell whether this is a stale label or a genuine regression.",
    )


def suspicion_score(
    classification: str,
    hit_count: int,
    missing_after_count: int,
    nearest_truth_delta_hz: float,
    sender_callsign: str | None,
    local_sender_conflicts: list[str],
    exact_message_nearby: list[str],
    pair_nearby: list[str],
    callsign_context_nearby: dict[str, list[str]],
    exact_message_elsewhere: list[str],
    pair_elsewhere: list[str],
    callsign_context_elsewhere: dict[str, list[str]],
    grid_country_consistency: str,
    callsign_legality: str,
) -> int:
    score = 0
    if classification == LIKELY_STALE_LABEL:
        score += 8
    elif classification == AMBIGUOUS:
        score += 3

    if not exact_message_nearby and not pair_nearby and not callsign_context_nearby:
        score += 4
    if not exact_message_elsewhere and not pair_elsewhere and not callsign_context_elsewhere:
        score += 2
    if sender_callsign and local_sender_conflicts:
        score += 5
    if grid_country_consistency == GRID_COUNTRY_MISMATCH:
        score += 5
    if callsign_legality != "all_legal":
        score += 3
    if nearest_truth_delta_hz <= 3:
        score += 5
    elif nearest_truth_delta_hz <= 10:
        score += 3
    elif nearest_truth_delta_hz <= 20:
        score += 1
    if hit_count <= 2:
        score += 2
    elif hit_count >= 10:
        score -= 2
    if missing_after_count >= 10:
        score += 1
    return score


def build_candidates(root: Path, results_path: Path, dataset_id: str) -> tuple[list[Candidate], dict[str, Any]]:
    payload = json.loads(results_path.read_text())
    runs = [run for run in payload["runs"] if run.get("dataset_id") == dataset_id and run.get("metrics")]
    versions = sorted({run["release_version"] for run in runs}, key=version_key)
    version_index = {version: idx for idx, version in enumerate(versions)}
    profiles = sorted({run["profile_id"] for run in runs})

    truth_by_sample: dict[str, list[dict[str, Any]]] = {}
    truth_message_samples: dict[str, set[str]] = defaultdict(set)
    truth_pair_samples: dict[frozenset[str], set[str]] = defaultdict(set)
    truth_callsign_samples: dict[str, set[str]] = defaultdict(set)
    per_profile_presence: dict[tuple[str, str, str], list[bool]] = defaultdict(lambda: [False] * len(versions))

    for run in runs:
        sample_id = run["sample_id"]
        truth_by_sample.setdefault(sample_id, run["truth"])
        decoded_messages = {decode["message"] for decode in run["decodes"]}
        for entry in run["truth"]:
            message = entry["message"]
            per_profile_presence[(run["profile_id"], sample_id, message)][version_index[run["release_version"]]] = message in decoded_messages
        for entry in run["truth"]:
            truth_message_samples[entry["message"]].add(sample_id)
            callsigns = extract_callsigns(entry["message"])
            for callsign in callsigns:
                truth_callsign_samples[callsign].add(sample_id)
            if len(callsigns) == 2:
                truth_pair_samples[frozenset(callsigns)].add(sample_id)

    all_profile_presence: dict[tuple[str, str], list[bool]] = defaultdict(lambda: [False] * len(versions))
    decoded_profiles_by_message: dict[tuple[str, str], set[str]] = defaultdict(set)
    for (profile_id, sample_id, message), pattern in per_profile_presence.items():
        for idx, present in enumerate(pattern):
            all_profile_presence[(sample_id, message)][idx] = all_profile_presence[(sample_id, message)][idx] or present
        if any(pattern):
            decoded_profiles_by_message[(sample_id, message)].add(profile_id)

    grid_cache = load_grid_country_cache(root / "artifacts" / "cache" / "grid_country_cache.json")
    sample_ids = set(truth_by_sample)
    nearby_map = {sample_id: nearby_sample_ids(sample_id, sample_ids) for sample_id in sample_ids}
    candidates: list[Candidate] = []

    def maybe_add_candidate(scope: str, sample_id: str, message: str, pattern: list[bool], patterns_by_profile: dict[str, str]) -> None:
        if not any(pattern) or pattern[-1]:
            return
        last_true = max(idx for idx, present in enumerate(pattern) if present)
        hit_count = sum(pattern[: last_true + 1])
        missing_after_count = len(pattern) - 1 - last_true
        if hit_count < 2 or missing_after_count < 2:
            return

        truth_entries = truth_by_sample[sample_id]
        entry = next(item for item in truth_entries if item["message"] == message)
        callsigns = extract_callsigns(message)
        sender_callsign = determine_sender_callsign(message)
        grid_owner, grid_locator = determine_grid_owner(message)
        owner_entity = callsign_entity(grid_owner)
        grid_entity = grid_cache.get(grid_locator) if grid_locator else None
        grid_consistency = compare_entities(owner_entity, grid_entity)

        other_truth = [item for item in truth_entries if truth_entry_key(item) != truth_entry_key(entry)]
        nearest_truth = min(
            other_truth,
            key=lambda item: abs(item["freq_hz"] - entry["freq_hz"]),
            default={"message": "", "freq_hz": entry["freq_hz"]},
        )
        local_sender_conflicts = []
        if sender_callsign:
            local_sender_conflicts = sorted(
                item["message"]
                for item in other_truth
                if determine_sender_callsign(item["message"]) == sender_callsign
            )

        exact_message_elsewhere = sorted(truth_message_samples[message] - {sample_id})
        exact_message_nearby = sorted(set(exact_message_elsewhere) & nearby_map[sample_id])
        pair_elsewhere = sorted(truth_pair_samples[frozenset(callsigns)] - {sample_id}) if len(callsigns) == 2 else []
        pair_nearby = sorted(set(pair_elsewhere) & nearby_map[sample_id])
        callsign_context_elsewhere = {
            callsign: sorted(truth_callsign_samples[callsign] - {sample_id})
            for callsign in callsigns
            if truth_callsign_samples[callsign] - {sample_id}
        }
        callsign_context_nearby = {
            callsign: sorted((truth_callsign_samples[callsign] - {sample_id}) & nearby_map[sample_id])
            for callsign in callsigns
            if (truth_callsign_samples[callsign] - {sample_id}) & nearby_map[sample_id]
        }
        legality = "all_legal" if callsigns and all(callsign_legal(callsign) for callsign in callsigns) else "unknown_or_illegal"
        classification, reasoning = classify_candidate(
            callsigns=callsigns,
            callsign_legality=legality,
            grid_country_consistency=grid_consistency,
            sender_callsign=sender_callsign,
            local_sender_conflicts=local_sender_conflicts,
            nearest_truth_delta_hz=abs(nearest_truth["freq_hz"] - entry["freq_hz"]),
            exact_message_elsewhere=exact_message_elsewhere,
            exact_message_nearby=exact_message_nearby,
            pair_elsewhere=pair_elsewhere,
            pair_nearby=pair_nearby,
            callsign_context_elsewhere=callsign_context_elsewhere,
            callsign_context_nearby=callsign_context_nearby,
        )
        score = suspicion_score(
            classification=classification,
            hit_count=hit_count,
            missing_after_count=missing_after_count,
            nearest_truth_delta_hz=abs(nearest_truth["freq_hz"] - entry["freq_hz"]),
            sender_callsign=sender_callsign,
            local_sender_conflicts=local_sender_conflicts,
            exact_message_nearby=exact_message_nearby,
            pair_nearby=pair_nearby,
            callsign_context_nearby=callsign_context_nearby,
            exact_message_elsewhere=exact_message_elsewhere,
            pair_elsewhere=pair_elsewhere,
            callsign_context_elsewhere=callsign_context_elsewhere,
            grid_country_consistency=grid_consistency,
            callsign_legality=legality,
        )
        candidates.append(
            Candidate(
                scope=scope,
                sample_id=sample_id,
                message=message,
                first_version=versions[min(idx for idx, present in enumerate(pattern) if present)],
                last_version=versions[last_true],
                hit_count=hit_count,
                missing_after_count=missing_after_count,
                pattern="".join("Y" if present else "." for present in pattern),
                ever_decoded_profiles=sorted(decoded_profiles_by_message[(sample_id, message)]),
                pattern_by_profile=patterns_by_profile,
                truth_freq_hz=int(entry["freq_hz"]),
                truth_dt=float(entry["dt"]),
                truth_snr=int(entry["snr"]),
                nearest_truth_message=nearest_truth["message"],
                nearest_truth_delta_hz=abs(nearest_truth["freq_hz"] - entry["freq_hz"]),
                sender_callsign=sender_callsign,
                local_sender_conflicts=local_sender_conflicts,
                exact_message_elsewhere=exact_message_elsewhere,
                exact_message_nearby=exact_message_nearby,
                pair_elsewhere=pair_elsewhere,
                pair_nearby=pair_nearby,
                callsign_context_elsewhere=callsign_context_elsewhere,
                callsign_context_nearby=callsign_context_nearby,
                callsign_legality=legality,
                grid_owner_callsign=grid_owner,
                grid_locator=grid_locator,
                callsign_entity=owner_entity,
                grid_entity=grid_entity,
                grid_country_consistency=grid_consistency,
                classification=classification,
                suspicion_score=score,
                reasoning=reasoning,
            )
        )

    for (sample_id, message), pattern in sorted(all_profile_presence.items()):
        maybe_add_candidate(
            "all-profiles",
            sample_id,
            message,
            pattern,
            {
                profile: "".join("Y" if present else "." for present in per_profile_presence[(profile, sample_id, message)])
                for profile in profiles
            },
        )

    for (profile_id, sample_id, message), pattern in sorted(per_profile_presence.items()):
        maybe_add_candidate(
            profile_id,
            sample_id,
            message,
            pattern,
            {profile_id: "".join("Y" if present else "." for present in pattern)},
        )

    candidates.sort(
        key=lambda item: (
            item.scope != "all-profiles",
            item.classification != LIKELY_STALE_LABEL,
            item.classification != AMBIGUOUS,
            -item.suspicion_score,
            -item.missing_after_count,
            -item.hit_count,
            version_key(item.last_version),
            item.sample_id,
            item.message,
        )
    )
    return candidates, {"versions": versions, "profiles": profiles, "results_path": str(results_path)}


def write_csv(path: Path, candidates: list[Candidate]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "scope",
                "sample_id",
                "message",
                "classification",
                "first_version",
                "last_version",
                "hit_count",
                "missing_after_count",
                "pattern",
                "ever_decoded_profiles",
                "pattern_by_profile",
                "truth_freq_hz",
                "truth_dt",
                "truth_snr",
                "nearest_truth_delta_hz",
                "nearest_truth_message",
                "sender_callsign",
                "local_sender_conflicts",
                "exact_message_elsewhere",
                "exact_message_nearby",
                "pair_elsewhere",
                "pair_nearby",
                "callsign_context_elsewhere",
                "callsign_context_nearby",
                "callsign_legality",
                "grid_owner_callsign",
                "grid_locator",
                "callsign_entity",
                "grid_entity",
                "grid_country_consistency",
                "suspicion_score",
                "reasoning",
            ]
        )
        for item in candidates:
            writer.writerow(
                [
                    item.scope,
                    item.sample_id,
                    item.message,
                    item.classification,
                    item.first_version,
                    item.last_version,
                    item.hit_count,
                    item.missing_after_count,
                    item.pattern,
                    ",".join(item.ever_decoded_profiles),
                    json.dumps(item.pattern_by_profile, sort_keys=True),
                    item.truth_freq_hz,
                    f"{item.truth_dt:.1f}",
                    item.truth_snr,
                    f"{item.nearest_truth_delta_hz:.1f}",
                    item.nearest_truth_message,
                    item.sender_callsign,
                    json.dumps(item.local_sender_conflicts),
                    ",".join(item.exact_message_elsewhere),
                    ",".join(item.exact_message_nearby),
                    ",".join(item.pair_elsewhere),
                    ",".join(item.pair_nearby),
                    json.dumps(item.callsign_context_elsewhere, sort_keys=True),
                    json.dumps(item.callsign_context_nearby, sort_keys=True),
                    item.callsign_legality,
                    item.grid_owner_callsign,
                    item.grid_locator,
                    item.callsign_entity,
                    item.grid_entity,
                    item.grid_country_consistency,
                    item.suspicion_score,
                    item.reasoning,
                ]
            )


def write_markdown(path: Path, candidates: list[Candidate], metadata: dict[str, Any]) -> None:
    by_scope: dict[str, list[Candidate]] = defaultdict(list)
    by_classification: dict[str, int] = defaultdict(int)
    for item in candidates:
        by_scope[item.scope].append(item)
        by_classification[item.classification] += 1

    lines = [
        "# Disappearing Truth Label Audit",
        "",
        f"- Results file: `{metadata['results_path']}`",
        f"- Release span: `{metadata['versions'][0]}` -> `{metadata['versions'][-1]}`",
        f"- Profiles inspected: {', '.join(metadata['profiles'])}",
        f"- Candidates: {len(candidates)}",
        f"- Looks real: {by_classification[LOOKS_REAL]}",
        f"- Ambiguous: {by_classification[AMBIGUOUS]}",
        f"- Likely stale labels: {by_classification[LIKELY_STALE_LABEL]}",
        "",
        "A candidate here is a truth label that is decoded for a while and then never appears again in the chosen release span.",
        "",
    ]

    scope_order = ["all-profiles"] + [scope for scope in metadata["profiles"] if scope in by_scope]
    for scope in scope_order:
        items = by_scope.get(scope)
        if not items:
            continue
        lines.extend([f"## {scope}", ""])
        for item in items:
            profile_patterns = ", ".join(f"{name}: `{pattern}`" for name, pattern in sorted(item.pattern_by_profile.items()))
            context_bits = []
            if item.exact_message_elsewhere:
                context_bits.append(f"exact message elsewhere: {', '.join(item.exact_message_elsewhere)}")
            if item.exact_message_nearby:
                context_bits.append(f"exact message nearby: {', '.join(item.exact_message_nearby)}")
            if item.pair_elsewhere:
                context_bits.append(f"station pair elsewhere: {', '.join(item.pair_elsewhere)}")
            if item.pair_nearby:
                context_bits.append(f"station pair nearby: {', '.join(item.pair_nearby)}")
            if item.callsign_context_nearby:
                context_bits.append(
                    "callsign context nearby: "
                    + ", ".join(
                        f"{callsign} in {', '.join(samples)}"
                        for callsign, samples in sorted(item.callsign_context_nearby.items())
                    )
                )
            if item.callsign_context_elsewhere:
                context_bits.append(
                    "callsign context elsewhere: "
                    + ", ".join(
                        f"{callsign} in {', '.join(samples)}"
                        for callsign, samples in sorted(item.callsign_context_elsewhere.items())
                    )
                )
            if item.grid_locator:
                context_bits.append(
                    f"grid check: {item.grid_country_consistency} ({item.grid_owner_callsign or '?'} -> {item.callsign_entity or 'unknown'}, {item.grid_locator} -> {item.grid_entity or 'unknown'})"
                )
            if item.sender_callsign and item.local_sender_conflicts:
                context_bits.append(
                    f"same-slot sender conflict: {item.sender_callsign} also sends {'; '.join(item.local_sender_conflicts)}"
                )
            lines.append(
                f"- `{item.sample_id}` / `{item.message}` [{item.classification}]. "
                f"Seen {item.hit_count} times from `{item.first_version}` through `{item.last_version}`, then absent for {item.missing_after_count} releases. "
                f"Pattern `{item.pattern}`. Profiles: {', '.join(item.ever_decoded_profiles)}. "
                f"Suspicion score {item.suspicion_score}. "
                f"Truth line at {item.truth_freq_hz} Hz, nearest other truth {item.nearest_truth_delta_hz:.0f} Hz away (`{item.nearest_truth_message}`). "
                f"{item.reasoning} "
                f"Profile patterns: {profile_patterns}."
            )
            if context_bits:
                lines.append(f"  Context: {'; '.join(context_bits)}.")
        lines.append("")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines))


def write_suspicious_shortlist(path: Path, candidates: list[Candidate]) -> None:
    shortlisted = [
        item
        for item in candidates
        if item.suspicion_score >= 8 and item.scope in {"all-profiles", "quick", "medium", "deepest"}
    ]
    shortlisted.sort(
        key=lambda item: (
            -item.suspicion_score,
            item.scope != "all-profiles",
            item.classification != LIKELY_STALE_LABEL,
            -item.missing_after_count,
            -item.hit_count,
            item.sample_id,
            item.message,
        )
    )

    lines = [
        "# Suspicious Disappearing Labels",
        "",
        "These are the disappearing truth labels with the weakest corroboration and strongest signs of being stale labels.",
        "",
    ]
    if not shortlisted:
        lines.append("No candidates crossed the suspicion threshold.")
    else:
        for item in shortlisted:
            lines.append(
                f"- `{item.scope}` / `{item.sample_id}` / `{item.message}` [{item.classification}] "
                f"score={item.suspicion_score}, seen {item.hit_count} times through `{item.last_version}`, "
                f"then absent for {item.missing_after_count} releases. "
                f"Nearby corroboration: exact={','.join(item.exact_message_nearby) or '-'}, "
                f"pair={','.join(item.pair_nearby) or '-'}, calls={json.dumps(item.callsign_context_nearby, sort_keys=True) if item.callsign_context_nearby else '-'}. "
                f"Nearest truth delta {item.nearest_truth_delta_hz:.0f} Hz. {item.reasoning}"
            )

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines))


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit truth labels that vanish in later WSJT-X releases.")
    parser.add_argument("--results", type=Path, help="Path to a multi-version results.json file")
    parser.add_argument("--dataset", default="kgoba-ft8-lib", help="Scored dataset id to inspect")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    results_path = args.results.resolve() if args.results else pick_results_path(root)
    candidates, metadata = build_candidates(root, results_path, args.dataset)

    write_csv(root / "artifacts" / "analysis" / "disappearing_truth_audit_latest.csv", candidates)
    write_markdown(root / "artifacts" / "analysis" / "disappearing_truth_audit_latest.md", candidates, metadata)
    write_suspicious_shortlist(root / "artifacts" / "analysis" / "disappearing_truth_suspicious_latest.md", candidates)

    print(f"wrote {len(candidates)} candidates from {results_path}")


if __name__ == "__main__":
    main()
