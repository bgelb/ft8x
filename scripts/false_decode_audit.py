from __future__ import annotations

import csv
import json
import re
import statistics
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path


CALL_RE = re.compile(r"^[A-Z0-9/<>]{3,}$")
GRID_RE = re.compile(r"^[A-R]{2}\d{2}(?:[A-X]{2})?$")
REPORT_RE = re.compile(r"^[R+-]?\d{2}$|^RR73$|^RRR$|^73$")

LIKELY_MISDECODE = "likely_misdecode"
LIKELY_MISSED_LABEL = "likely_missed_label"
LIKELY_ARTIFACT = "likely_artifact"
AMBIGUOUS = "ambiguous"
RELABEL_READY = "ready"
RELABEL_CANDIDATE = "candidate"
RELABEL_HOLD = "hold"

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
    ("S57", "Slovenia"),
    ("SV2", "Greece"),
    ("UA6", "Russia"),
    ("UA9", "Russia"),
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
    "Hungary": {"Hungary"},
    "Italy": {"Italy"},
    "Japan": {"Japan"},
    "Romania": {"Romania"},
    "Russia": {"Russia"},
    "Slovenia": {"Slovenia"},
    "Spain": {"Spain", "Canary Islands"},
    "United Kingdom": {"United Kingdom"},
    "United States": {"United States"},
    "Canary Islands": {"Spain", "Canary Islands"},
}

GRID_ENTITY_OVERRIDES = {
    "IL38": "Canary Islands",
}

OVERRIDES: dict[tuple[str, str], tuple[str, str]] = {
    ("191111_110215", "ZS6S UA6LJX KN97"): (
        LIKELY_MISSED_LABEL,
        "Stable across 15 releases and all three depths at one frequency. It does not share callsigns with any labeled line in this sample, so this looks more like a missed label than a collision.",
    ),
    ("websdr_test10", "CQ M0SAS IO82"): (
        LIKELY_MISSED_LABEL,
        "This exact CQ is labeled in websdr_test12 and it is stable across all 20 releases here, so the most likely explanation is that this sample is under-labeled.",
    ),
    ("websdr_test10", "YO9HP K6DRY CM98"): (
        LIKELY_MISSED_LABEL,
        "This exact message is labeled in websdr_test12 and is stable across 15 releases in this sample. That points to a missed label rather than junk.",
    ),
    ("websdr_test10", "PY1NMG LU1CFU -04"): (
        AMBIGUOUS,
        "The decode is stable across 15 releases, which argues for a real signal, but it sits only 12 Hz away from a labeled line. I would not call this junk confidently, but I also would not auto-promote it to truth without looking at the waterfall.",
    ),
    ("websdr_test11", "CQ YO9HP KN35"): (
        AMBIGUOUS,
        "This one is stable, but it is only 4 Hz away from a labeled CQ at nearly the same DT. That makes it plausible either as a real overlapping signal or as a stable misdecode.",
    ),
    ("websdr_test11", "PY5HT IW9CTR RR73"): (
        LIKELY_MISSED_LABEL,
        "The same station pair appears as a labeled QSO in websdr_test12. Despite low support in this sample, it looks like a plausible real QSO turn rather than random text.",
    ),
    ("websdr_test11", "VE6BTC W1JGM R-22"): (
        AMBIGUOUS,
        "This decode is stable across 15 releases but it is not corroborated by another truth sample, and it is close enough to nearby labels that I would keep it as uncertain.",
    ),
    ("websdr_test11", "Z81D W3GQ EM95"): (
        AMBIGUOUS,
        "Only 3.0.0-rc1 finds this, but the same station pair appears in websdr_test13. That makes it plausible, though the support is still thin.",
    ),
    ("websdr_test12", "CQ EA8SD IL38"): (
        LIKELY_MISSED_LABEL,
        "This CQ is stable across 17 releases and two depths, and it is not a callsign collision with any labeled line in the same sample. It looks more like an omitted label than a bad decode.",
    ),
    ("websdr_test13", "LZ3CQ K8JDC R-08"): (
        LIKELY_MISSED_LABEL,
        "The exact same message also appears as an unmatched decode in websdr_test11. Cross-sample repetition makes this look real even though only four releases decode it here.",
    ),
    ("websdr_test2", "CQ G4IJC JO02"): (
        LIKELY_MISDECODE,
        "This sits at the exact frequency of a labeled signal in the same sample, which is much stronger evidence for a collision than for an unlabeled extra CQ.",
    ),
    ("websdr_test2", "UA9SIX GM0LIR R-07"): (
        AMBIGUOUS,
        "Only deepest mode decodes this, and it is not corroborated elsewhere. The message is well-formed, but the support is not strong enough to call it a missed label confidently.",
    ),
    ("websdr_test4", "IT9EJP IU2KAJ JN45"): (
        LIKELY_MISDECODE,
        "This is only 1 Hz away from a labeled line in the same sample. That is classic collision territory.",
    ),
    ("websdr_test5", "CQ DO6AZ JO50"): (
        LIKELY_MISSED_LABEL,
        "This exact CQ is labeled in websdr_test7 and is stable across 15 releases here. That is strong evidence for an omitted label.",
    ),
    ("websdr_test5", "EA8PP JA6VQA -16"): (
        AMBIGUOUS,
        "Only one run finds this and the same sample already has two other EA8PP transmissions, so this could be a bad split of the same signal family.",
    ),
    ("websdr_test5", "EA8PP JH0INP PM96"): (
        LIKELY_MISDECODE,
        "The same sample already has two labeled EA8PP messages at other frequencies, so a third simultaneous EA8PP transmission is implausible. This looks like a misdecode.",
    ),
    ("websdr_test6", "RA1CP OM7JG R+03"): (
        LIKELY_MISSED_LABEL,
        "The same station pair is labeled in websdr_test4 and continues in websdr_test7 as OM7JG RA1CP RR73. That QSO continuity makes this look real.",
    ),
    ("websdr_test7", "CQ DO2HC JO50"): (
        LIKELY_MISSED_LABEL,
        "This decode is stable across 15 releases and sits apart from the labeled lines enough to look like a missed extra CQ rather than a collision.",
    ),
    ("websdr_test7", "OM7JG RA1CP RR73"): (
        LIKELY_MISSED_LABEL,
        "This matches the same RA1CP/OM7JG QSO that shows up labeled in websdr_test4 and as R+03 in websdr_test6. The sequence is coherent.",
    ),
    ("websdr_test9", "K6DRY YO9HP -15"): (
        LIKELY_MISSED_LABEL,
        "This station pair also appears in websdr_test13 and the reverse-direction message is labeled in websdr_test12, which makes this look like a real missed QSO turn.",
    ),
}

READY_RELABEL_OVERRIDES: set[tuple[str, str]] = {
    ("191111_110615", "WB2QJ ES3AT KO18"),
    ("191111_110630", "CQ M0NPT IO92"),
    ("websdr_test10", "CQ M0SAS IO82"),
    ("websdr_test10", "YO9HP K6DRY CM98"),
    ("websdr_test12", "CT7AIX WG5D EM62"),
    ("websdr_test12", "K1GUY NA4RR EM61"),
    ("websdr_test13", "CQ 2E0PKK IO90"),
    ("websdr_test13", "CQ N2BJ EN61"),
    ("websdr_test4", "CQ DX DO4TP JO31"),
    ("websdr_test4", "CQ HF19NY"),
    ("websdr_test5", "CQ DO6AZ JO50"),
    ("websdr_test6", "DL8FBD LZ2KV -16"),
    ("websdr_test6", "RA1CP OM7JG R+03"),
    ("websdr_test7", "CQ DO1RPK JO32"),
    ("websdr_test7", "OM7JG RA1CP RR73"),
    ("websdr_test9", "K6DRY YO9HP -15"),
}


@dataclass
class Cluster:
    sample_id: str
    message: str
    decodes: list[dict]
    truth: list[dict]
    truth_elsewhere: list[str]
    callsigns_in_truth_elsewhere: dict[str, list[str]]
    shared_callsigns: list[str]
    grid_owner_callsign: str | None
    grid_locator: str | None
    callsign_entity: str | None
    grid_entity: str | None
    grid_country_consistency: str
    nearest_truth_message: str
    nearest_truth_freq_hz: int
    nearest_truth_delta_hz: float
    versions: list[str]
    profiles: list[str]
    median_freq_hz: float
    median_dt: float
    median_snr: float
    classification: str
    relabel_confidence: str
    reasoning: str


def extract_callsigns(message: str) -> list[str]:
    callsigns: list[str] = []
    for token in message.split():
        if token in {"CQ", "DX"} or GRID_RE.match(token) or REPORT_RE.match(token):
            continue
        if CALL_RE.match(token):
            callsigns.append(token)
    return callsigns


def old_style_message(decode: dict) -> str:
    annotation = decode.get("annotation")
    if annotation:
        return f"{decode['message']} {annotation.upper()}"
    return decode["message"]


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


def normalize_entity(entity: str | None) -> str | None:
    if entity is None:
        return None
    if entity in ENTITY_EQUIVALENTS:
        return entity
    return entity


def callsign_entity(callsign: str | None) -> str | None:
    if not callsign:
        return None
    for prefix, entity in sorted(CALLSIGN_ENTITY_RULES, key=lambda item: len(item[0]), reverse=True):
        if callsign.startswith(prefix):
            return entity
    return None


def maidenhead_center(locator: str) -> tuple[float, float]:
    locator = locator.strip().upper()
    lon = (ord(locator[0]) - ord("A")) * 20 - 180
    lat = (ord(locator[1]) - ord("A")) * 10 - 90
    lon += int(locator[2]) * 2
    lat += int(locator[3])
    if len(locator) >= 6:
        lon += (ord(locator[4]) - ord("A")) * (5 / 60)
        lat += (ord(locator[5]) - ord("A")) * (2.5 / 60)
        lon += (5 / 60) / 2
        lat += (2.5 / 60) / 2
    else:
        lon += 1
        lat += 0.5
    return lat, lon


def load_grid_country_cache(path: Path) -> dict[str, str | None]:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def write_grid_country_cache(path: Path, cache: dict[str, str | None]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache, indent=2, sort_keys=True) + "\n")


def geocode_grid_entities(grids: set[str], cache_path: Path) -> dict[str, str | None]:
    cache = load_grid_country_cache(cache_path)
    missing = sorted(grid for grid in grids if grid not in cache and grid not in GRID_ENTITY_OVERRIDES)
    for grid in missing:
        lat, lon = maidenhead_center(grid)
        params = urllib.parse.urlencode(
            {
                "lat": lat,
                "lon": lon,
                "format": "jsonv2",
                "zoom": 3,
                "accept-language": "en",
            }
        )
        request = urllib.request.Request(
            "https://nominatim.openstreetmap.org/reverse?" + params,
            headers={"User-Agent": "ft8-regr/0.1"},
        )
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = json.load(response)
        cache[grid] = payload.get("address", {}).get("country")
        time.sleep(1)
    for grid, entity in GRID_ENTITY_OVERRIDES.items():
        cache[grid] = entity
    write_grid_country_cache(cache_path, cache)
    return {grid: cache.get(grid) for grid in grids}


def compare_entities(callsign_country: str | None, grid_country: str | None) -> str:
    if not callsign_country or not grid_country:
        return GRID_COUNTRY_UNKNOWN
    left = ENTITY_EQUIVALENTS.get(callsign_country, {callsign_country})
    right = ENTITY_EQUIVALENTS.get(grid_country, {grid_country})
    return GRID_COUNTRY_MATCH if left & right else GRID_COUNTRY_MISMATCH


def classify_cluster(cluster: Cluster) -> tuple[str, str]:
    key = (cluster.sample_id, cluster.message)
    if key in OVERRIDES:
        return OVERRIDES[key]

    versions = len(cluster.versions)
    profiles = len(cluster.profiles)
    partial = "<" in cluster.message or ">" in cluster.message or "?" in cluster.message

    if partial:
        return (
            LIKELY_ARTIFACT,
            "The message contains placeholder characters, which are low-confidence decoder output. I would treat this as junk rather than a missing label.",
        )

    if cluster.truth_elsewhere:
        return (
            LIKELY_MISSED_LABEL,
            f"This exact message is labeled in {', '.join(cluster.truth_elsewhere)}, which is strong evidence that the signal itself is real and this sample is under-labeled.",
        )

    if cluster.shared_callsigns:
        return (
            LIKELY_MISDECODE,
            f"The message reuses callsign(s) {', '.join(cluster.shared_callsigns)} that already appear in labeled lines in the same 15-second slot. A station cannot send multiple different FT8 payloads in one slot, so this is more likely a misdecode.",
        )

    if cluster.nearest_truth_delta_hz <= 5:
        return (
            LIKELY_MISDECODE,
            f"The decode lands only {cluster.nearest_truth_delta_hz:.0f} Hz away from a labeled signal (`{cluster.nearest_truth_message}`), which strongly suggests a collision rather than an extra unlabeled transmission.",
        )

    if cluster.callsigns_in_truth_elsewhere:
        details = ", ".join(
            f"{callsign} in {', '.join(samples)}"
            for callsign, samples in sorted(cluster.callsigns_in_truth_elsewhere.items())
        )
        return (
            LIKELY_MISSED_LABEL,
            f"The exact message is not labeled elsewhere, but its callsign(s) do appear in other labeled samples ({details}). That makes the message locally plausible as a missed label.",
        )

    if versions >= 10 and profiles >= 2 and cluster.nearest_truth_delta_hz >= 40:
        return (
            LIKELY_MISSED_LABEL,
            f"The decode is stable across {versions} releases and {profiles} depths, and the nearest labeled line is {cluster.nearest_truth_delta_hz:.0f} Hz away. That pattern looks more like a consistently missed real signal than random junk.",
        )

    return (
        AMBIGUOUS,
        "The message is well-formed, but the support is mixed and the frequency separation from labeled lines is not decisive. I would leave this as uncertain without waterfall inspection.",
    )


def relabel_confidence(cluster: Cluster) -> str:
    if cluster.classification != LIKELY_MISSED_LABEL:
        return RELABEL_HOLD
    key = (cluster.sample_id, cluster.message)
    if key in READY_RELABEL_OVERRIDES:
        return RELABEL_READY
    if cluster.truth_elsewhere and cluster.grid_country_consistency != GRID_COUNTRY_MISMATCH:
        return RELABEL_READY
    if (
        cluster.grid_country_consistency == GRID_COUNTRY_MATCH
        and len(cluster.versions) >= 15
        and cluster.nearest_truth_delta_hz >= 60
    ):
        return RELABEL_READY
    return RELABEL_CANDIDATE


def build_clusters(
    root: Path,
    results_path: Path,
) -> tuple[list[Cluster], list[tuple[str, str]]]:
    payload = json.loads(results_path.read_text())
    truth_by_sample: dict[str, list[dict]] = {}
    truth_samples_by_message: dict[str, set[str]] = defaultdict(set)
    truth_samples_by_callsign: dict[str, set[str]] = defaultdict(set)
    raw_clusters: dict[tuple[str, str], list[dict]] = defaultdict(list)
    old_style_false_clusters: set[tuple[str, str]] = set()

    for run in payload["runs"]:
        if not run["metrics"]:
            continue
        sample_id = run["sample_id"]
        truth = run["truth"]
        truth_by_sample.setdefault(sample_id, truth)
        truth_messages = {entry["message"] for entry in truth}
        for entry in truth:
            truth_samples_by_message[entry["message"]].add(sample_id)
            for callsign in extract_callsigns(entry["message"]):
                truth_samples_by_callsign[callsign].add(sample_id)

        seen: set[str] = set()
        old_seen: set[str] = set()
        for decode in run["decodes"]:
            if decode["message"] not in truth_messages and decode["message"] not in seen:
                raw_clusters[(sample_id, decode["message"])].append(
                    {
                        **decode,
                        "release_version": run["release_version"],
                        "profile_id": run["profile_id"],
                    }
                )
                seen.add(decode["message"])

            old_message = old_style_message(decode)
            if old_message not in truth_messages and old_message not in old_seen:
                old_style_false_clusters.add((sample_id, old_message))
                old_seen.add(old_message)

    current_cluster_keys = set(raw_clusters)
    annotation_only_false_clusters = sorted(old_style_false_clusters - current_cluster_keys)
    grid_pairs = {
        determine_grid_owner(message)
        for (_, message) in raw_clusters
    }
    grid_entities = geocode_grid_entities(
        {grid for callsign, grid in grid_pairs if callsign and grid},
        root / "artifacts" / "cache" / "grid_country_cache.json",
    )

    clusters: list[Cluster] = []
    for (sample_id, message), decodes in sorted(raw_clusters.items()):
        truth = truth_by_sample[sample_id]
        median_freq_hz = statistics.median(decode["freq_hz"] for decode in decodes)
        nearest_truth = min(
            truth,
            key=lambda entry: abs(entry["freq_hz"] - median_freq_hz),
        )
        shared_callsigns = sorted(
            set(extract_callsigns(message))
            & {
                callsign
                for entry in truth
                for callsign in extract_callsigns(entry["message"])
            }
        )
        grid_owner_callsign, grid_locator = determine_grid_owner(message)
        owner_entity = callsign_entity(grid_owner_callsign)
        grid_entity = grid_entities.get(grid_locator) if grid_locator else None
        cluster = Cluster(
            sample_id=sample_id,
            message=message,
            decodes=decodes,
            truth=truth,
            truth_elsewhere=sorted(truth_samples_by_message[message] - {sample_id}),
            callsigns_in_truth_elsewhere={
                callsign: sorted(truth_samples_by_callsign[callsign] - {sample_id})
                for callsign in extract_callsigns(message)
                if truth_samples_by_callsign[callsign] - {sample_id}
            },
            shared_callsigns=shared_callsigns,
            grid_owner_callsign=grid_owner_callsign,
            grid_locator=grid_locator,
            callsign_entity=owner_entity,
            grid_entity=grid_entity,
            grid_country_consistency=compare_entities(owner_entity, grid_entity),
            nearest_truth_message=nearest_truth["message"],
            nearest_truth_freq_hz=nearest_truth["freq_hz"],
            nearest_truth_delta_hz=abs(nearest_truth["freq_hz"] - median_freq_hz),
            versions=sorted({decode["release_version"] for decode in decodes}),
            profiles=sorted({decode["profile_id"] for decode in decodes}),
            median_freq_hz=median_freq_hz,
            median_dt=statistics.median(decode["dt"] for decode in decodes),
            median_snr=statistics.median(decode["snr"] for decode in decodes),
            classification="",
            relabel_confidence=RELABEL_HOLD,
            reasoning="",
        )
        cluster.classification, cluster.reasoning = classify_cluster(cluster)
        cluster.relabel_confidence = relabel_confidence(cluster)
        clusters.append(cluster)

    return clusters, annotation_only_false_clusters


def write_csv(path: Path, clusters: list[Cluster]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "sample_id",
                "message",
                "classification",
                "versions",
                "profiles",
                "median_freq_hz",
                "median_dt",
                "median_snr",
                "nearest_truth_delta_hz",
                "nearest_truth_message",
                "truth_elsewhere",
                "callsigns_in_truth_elsewhere",
                "shared_callsigns",
                "grid_owner_callsign",
                "grid_locator",
                "callsign_entity",
                "grid_entity",
                "grid_country_consistency",
                "relabel_confidence",
                "reasoning",
            ]
        )
        for cluster in clusters:
            writer.writerow(
                [
                    cluster.sample_id,
                    cluster.message,
                    cluster.classification,
                    len(cluster.versions),
                    ",".join(cluster.profiles),
                    f"{cluster.median_freq_hz:.1f}",
                    f"{cluster.median_dt:.1f}",
                    f"{cluster.median_snr:.1f}",
                    f"{cluster.nearest_truth_delta_hz:.1f}",
                    cluster.nearest_truth_message,
                    ",".join(cluster.truth_elsewhere),
                    ",".join(
                        f"{callsign}:{'|'.join(samples)}"
                        for callsign, samples in sorted(cluster.callsigns_in_truth_elsewhere.items())
                    ),
                    ",".join(cluster.shared_callsigns),
                    cluster.grid_owner_callsign,
                    cluster.grid_locator,
                    cluster.callsign_entity,
                    cluster.grid_entity,
                    cluster.grid_country_consistency,
                    cluster.relabel_confidence,
                    cluster.reasoning,
                ]
            )


def write_markdown(
    path: Path,
    clusters: list[Cluster],
) -> None:
    counts: dict[str, int] = defaultdict(int)
    consistency_counts: dict[str, int] = defaultdict(int)
    relabel_counts: dict[str, int] = defaultdict(int)
    for cluster in clusters:
        counts[cluster.classification] += 1
        consistency_counts[cluster.grid_country_consistency] += 1
        relabel_counts[cluster.relabel_confidence] += 1

    lines = [
        "# False Decode Audit",
        "",
        "Residual unmatched decodes on the scored corpus at the current state of the labeled dataset.",
        "",
        "## Summary",
        "",
        f"- Current unmatched decode clusters on scored samples: {len(clusters)}",
        f"- Likely missed labels: {counts[LIKELY_MISSED_LABEL]}",
        f"- Likely decoder misdecodes: {counts[LIKELY_MISDECODE]}",
        f"- Likely low-confidence artifacts: {counts[LIKELY_ARTIFACT]}",
        f"- Ambiguous: {counts[AMBIGUOUS]}",
        f"- Grid/callsign country matches: {consistency_counts[GRID_COUNTRY_MATCH]}",
        f"- Grid/callsign country mismatches: {consistency_counts[GRID_COUNTRY_MISMATCH]}",
        f"- Grid/callsign country unknown: {consistency_counts[GRID_COUNTRY_UNKNOWN]}",
        f"- Relabel ready now: {relabel_counts[RELABEL_READY]}",
        f"- Relabel candidates needing more review: {relabel_counts[RELABEL_CANDIDATE]}",
    ]

    lines.extend(
        [
            "",
            "## Per-Message Assessment",
            "",
            "Each item includes the evidence used for the classification: release/depth stability, nearest labeled line in the same sample, and whether the exact message is labeled elsewhere in the corpus.",
            "",
        ]
    )

    order = [
        LIKELY_MISSED_LABEL,
        LIKELY_MISDECODE,
        LIKELY_ARTIFACT,
        AMBIGUOUS,
    ]
    for classification in order:
        matching = [cluster for cluster in clusters if cluster.classification == classification]
        if not matching:
            continue
        lines.append(f"### {classification}")
        lines.append("")
        for cluster in matching:
            evidence = [
                f"{len(cluster.versions)} releases",
                f"profiles: {', '.join(cluster.profiles)}",
                f"median freq {cluster.median_freq_hz:.0f} Hz",
                f"nearest truth {cluster.nearest_truth_delta_hz:.0f} Hz away (`{cluster.nearest_truth_message}`)",
            ]
            if cluster.truth_elsewhere:
                evidence.append(f"truth elsewhere: {', '.join(cluster.truth_elsewhere)}")
            if cluster.callsigns_in_truth_elsewhere:
                evidence.append(
                    "callsigns seen in truth elsewhere: "
                    + ", ".join(
                        f"{callsign} ({', '.join(samples)})"
                        for callsign, samples in sorted(cluster.callsigns_in_truth_elsewhere.items())
                    )
                )
            if cluster.shared_callsigns:
                evidence.append(f"shared callsigns with local truth: {', '.join(cluster.shared_callsigns)}")
            if cluster.grid_locator:
                evidence.append(
                    f"grid consistency: {cluster.grid_country_consistency} "
                    f"({cluster.grid_owner_callsign} -> {cluster.callsign_entity or 'unknown'}, "
                    f"{cluster.grid_locator} -> {cluster.grid_entity or 'unknown'})"
                )
            if cluster.classification == LIKELY_MISSED_LABEL:
                evidence.append(f"relabel confidence: {cluster.relabel_confidence}")
            lines.append(
                f"- `{cluster.sample_id}` / `{cluster.message}`: {cluster.reasoning} "
                f"Evidence: {', '.join(evidence)}."
            )
        lines.append("")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    results_path = root / "artifacts" / "results" / "latest" / "results.json"
    clusters, annotation_only_false_clusters = build_clusters(root, results_path)
    write_csv(root / "artifacts" / "analysis" / "false_decode_audit_latest.csv", clusters)
    write_markdown(
        root / "artifacts" / "analysis" / "false_decode_audit_latest.md",
        clusters,
    )


if __name__ == "__main__":
    main()
