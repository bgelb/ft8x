from __future__ import annotations

import json
from collections import defaultdict
from typing import Any

from .core import summarize_runs, version_key

PROFILE_ORDER = {
    "quick": 0,
    "medium": 1,
    "deepest": 2,
}

PROFILE_COLORS = {
    "quick": "#0f6c5b",
    "medium": "#b56a2d",
    "deepest": "#7f3f98",
}


def pct(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value * 100:.1f}%"


def seconds(value: float | None) -> str:
    if value is None:
        return "-"
    if value >= 10:
        return f"{value:.1f}s"
    return f"{value:.3f}s"


def render_report(payload: dict[str, Any]) -> str:
    summary_rows = summarize_runs(payload["runs"])
    trend_sections = render_trend_sections(summary_rows)
    release_matrix = render_release_matrix(summary_rows)
    detail_sections = render_detail_sections(payload["runs"])
    raw_payload = json.dumps(payload)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>FT8 WSJT-X Regression Report</title>
  <style>
    :root {{
      --bg: #f5f0e8;
      --panel: #fffdf8;
      --ink: #1b2021;
      --muted: #5f665e;
      --accent: #0f6c5b;
      --accent-soft: #d7efe8;
      --bad: #aa3c30;
      --bad-soft: #f5d7d2;
      --good: #215f3d;
      --good-soft: #ddefe2;
      --line: #d8cfbf;
      --shadow: 0 12px 30px rgba(36, 33, 27, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Iowan Old Style", "Palatino Linotype", "Book Antiqua", serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(15, 108, 91, 0.12), transparent 38%),
        linear-gradient(180deg, #f8f3ec 0%, var(--bg) 100%);
    }}
    main {{
      max-width: 1200px;
      margin: 0 auto;
      padding: 32px 20px 64px;
    }}
    header {{
      background: var(--panel);
      border: 1px solid var(--line);
      box-shadow: var(--shadow);
      padding: 24px;
      border-radius: 20px;
    }}
    h1, h2, h3 {{
      font-family: "Avenir Next Condensed", "Franklin Gothic Medium", sans-serif;
      letter-spacing: 0.02em;
      margin: 0;
    }}
    h1 {{
      font-size: clamp(2rem, 4vw, 3.4rem);
      line-height: 0.95;
      margin-bottom: 12px;
    }}
    h2 {{
      font-size: 1.5rem;
      margin-bottom: 12px;
    }}
    p {{
      margin: 0;
      color: var(--muted);
      line-height: 1.5;
    }}
    .meta {{
      margin-top: 12px;
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      font-family: "Avenir Next", "Helvetica Neue", sans-serif;
      font-size: 0.95rem;
    }}
    .pill {{
      background: var(--accent-soft);
      color: var(--accent);
      border-radius: 999px;
      padding: 8px 12px;
    }}
    section {{
      margin-top: 24px;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 20px;
      box-shadow: var(--shadow);
      padding: 24px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-family: "Avenir Next", "Helvetica Neue", sans-serif;
      font-size: 0.95rem;
    }}
    th, td {{
      border-bottom: 1px solid var(--line);
      padding: 10px 8px;
      text-align: left;
      vertical-align: top;
    }}
    th {{
      color: var(--muted);
      font-size: 0.85rem;
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }}
    tr:last-child td {{
      border-bottom: none;
    }}
    .delta-up {{
      color: var(--good);
      background: var(--good-soft);
      border-radius: 999px;
      padding: 2px 8px;
    }}
    .delta-down {{
      color: var(--bad);
      background: var(--bad-soft);
      border-radius: 999px;
      padding: 2px 8px;
    }}
    .delta-flat {{
      color: var(--muted);
    }}
    .detail-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      gap: 16px;
    }}
    .trend-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      gap: 16px;
      margin-top: 16px;
    }}
    .detail-card {{
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 16px;
      background: linear-gradient(180deg, rgba(255,255,255,0.98), rgba(248,244,238,0.98));
    }}
    .detail-card h3 {{
      font-size: 1.15rem;
      margin-bottom: 8px;
    }}
    .detail-meta {{
      font-family: "Avenir Next", "Helvetica Neue", sans-serif;
      font-size: 0.9rem;
      color: var(--muted);
      margin-bottom: 10px;
    }}
    .summary-kv {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      font-family: "Avenir Next", "Helvetica Neue", sans-serif;
      font-size: 0.9rem;
      color: var(--muted);
      margin-bottom: 10px;
    }}
    .chart {{
      margin-top: 12px;
      border-radius: 12px;
      overflow: hidden;
      border: 1px solid var(--line);
      background: linear-gradient(180deg, rgba(15,108,91,0.04), rgba(15,108,91,0.0));
    }}
    .chart svg {{
      display: block;
      width: 100%;
      height: auto;
    }}
    .chart-caption {{
      margin-top: 8px;
      font-family: "Avenir Next", "Helvetica Neue", sans-serif;
      font-size: 0.85rem;
      color: var(--muted);
    }}
    .mono {{
      font-family: "SFMono-Regular", "Menlo", "Consolas", monospace;
      font-size: 0.9rem;
      white-space: pre-wrap;
      color: #2f342d;
      background: rgba(15, 108, 91, 0.05);
      border-radius: 12px;
      padding: 10px;
    }}
    .list-label {{
      margin-top: 10px;
      margin-bottom: 4px;
      font-family: "Avenir Next", "Helvetica Neue", sans-serif;
      font-size: 0.85rem;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }}
    details {{
      margin-top: 16px;
    }}
    summary {{
      cursor: pointer;
      font-family: "Avenir Next", "Helvetica Neue", sans-serif;
      color: var(--accent);
    }}
    @media (max-width: 720px) {{
      main {{ padding: 20px 12px 48px; }}
      section, header {{ padding: 18px; }}
      table {{ font-size: 0.88rem; }}
    }}
  </style>
</head>
<body>
  <main>
    <header>
      <h1>WSJT-X FT8 Historical Regression</h1>
      <p>Prototype benchmark for official macOS Intel WSJT-X releases using the command-line <span class="mono">jt9 -8</span> decoder across depth profiles and mixed scored/unscored FT8 sample sets.</p>
      <div class="meta">
        <span class="pill">Run: {payload["run_id"]}</span>
        <span class="pill">Generated: {payload["generated_at"]}</span>
        <span class="pill">Host: {payload.get("host_arch", "unknown")}</span>
        <span class="pill">Releases: {len(payload["releases"])}</span>
        <span class="pill">Profiles: {len(payload["profiles"])}</span>
        <span class="pill">Samples: {len(payload["runs"])}</span>
      </div>
    </header>

    <section>
      <h2>Overview</h2>
      <p>Scored datasets use unique-message matching against companion truth files. Unscored datasets only report decode volume. Each overview row is one release, with side-by-side depth columns for decode quality and average CPU time per sample.</p>
      {trend_sections}
      {release_matrix}
    </section>

    <section>
      <h2>Run Details</h2>
      <p>Each card shows a release/profile/dataset slice. Missing and unexpected messages are listed only for scored datasets.</p>
      {detail_sections}
    </section>

    <section>
      <h2>Embedded Data</h2>
      <p>The raw benchmark payload is embedded below so this page can be archived or inspected without a separate backend.</p>
      <details>
        <summary>Show JSON payload</summary>
        <div class="mono">{raw_payload}</div>
      </details>
    </section>
  </main>
</body>
</html>
"""


def render_release_matrix(summary_rows: list[dict[str, Any]]) -> str:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in summary_rows:
        grouped[(row["dataset_id"], row["dataset_label"])].append(row)
    sections = []
    for (_, dataset_label), rows in sorted(grouped.items()):
        sections.append(render_release_matrix_section(dataset_label, rows))
    return "".join(sections)


def render_release_matrix_section(dataset_label: str, rows: list[dict[str, Any]]) -> str:
    profiles = sorted(
        {row["profile_id"]: row["profile_label"] for row in rows}.items(),
        key=lambda item: PROFILE_ORDER.get(item[0], 99),
    )
    by_release: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in rows:
        by_release[row["release_version"]][row["profile_id"]] = row

    table_rows = []
    previous_scored_f1: dict[str, float | None] = {profile_id: None for profile_id, _ in profiles}
    for release_version in sorted(by_release, key=version_key):
        cells = [f"<td>{release_version}</td>"]
        release_rows = by_release[release_version]
        for profile_id, _ in profiles:
            row = release_rows.get(profile_id)
            if row is None:
                cells.append("<td>-</td>")
                continue
            if row["dataset_kind"] == "scored":
                previous_f1 = previous_scored_f1[profile_id]
                delta_markup = render_delta(row["f1"], previous_f1)
                previous_scored_f1[profile_id] = row["f1"]
                cells.append(
                    "<td>"
                    f"<strong>{pct(row['f1'])}</strong><br>"
                    f"R {pct(row['recall'])} | P {pct(row['precision'])}<br>"
                    f"CPU {seconds(row.get('avg_cpu_seconds'))}/sample<br>"
                    f"{delta_markup}"
                    "</td>"
                )
            else:
                cells.append(
                    "<td>"
                    f"<strong>{row['decode_count']}</strong><br>"
                    f"{row['samples']} samples<br>"
                    f"CPU {seconds(row.get('avg_cpu_seconds'))}/sample"
                    "</td>"
                )
        table_rows.append(f"<tr>{''.join(cells)}</tr>")

    header_cells = "".join(f"<th>{label}</th>" for _, label in profiles)
    metric_note = (
        "F1 with recall/precision, F1 delta vs previous release, and average CPU seconds per sample."
        if rows[0]["dataset_kind"] == "scored"
        else "Decode count and average CPU seconds per sample by depth."
    )
    return f"""
    <div class="detail-card" style="margin-top: 16px;">
      <h3>{dataset_label}</h3>
      <div class="detail-meta">{metric_note}</div>
      <table>
        <thead>
          <tr>
            <th>Release</th>
            {header_cells}
          </tr>
        </thead>
        <tbody>
          {''.join(table_rows)}
        </tbody>
      </table>
    </div>
    """


def render_delta(current: float | None, previous: float | None) -> str:
    if current is None or previous is None:
        return '<span class="delta-flat">-</span>'
    delta = current - previous
    if delta > 0:
        return f'<span class="delta-up">+{delta * 100:.1f} pts</span>'
    if delta < 0:
        return f'<span class="delta-down">{delta * 100:.1f} pts</span>'
    return '<span class="delta-flat">0.0 pts</span>'


def render_trend_sections(summary_rows: list[dict[str, Any]]) -> str:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in summary_rows:
        grouped[(row["dataset_id"], row["dataset_label"])].append(row)

    cards = []
    for (_, dataset_label), rows in sorted(grouped.items()):
        series: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            series[row["profile_id"]].append(row)
        ordered_series = {
            profile_id: sorted(profile_rows, key=lambda item: version_key(item["release_version"]))
            for profile_id, profile_rows in sorted(series.items(), key=lambda item: PROFILE_ORDER.get(item[0], 99))
        }
        ylabel = "Recall %" if rows[0]["dataset_kind"] == "scored" else "Decode count"
        legend = "".join(
            f"<span style='color: {PROFILE_COLORS.get(profile_id, '#0f6c5b')};'><strong>{profile_rows[0]['profile_label']}</strong></span>"
            for profile_id, profile_rows in ordered_series.items()
        )
        cards.append(
            f"""
            <article class="detail-card">
              <h3>{dataset_label}</h3>
              <div class="detail-meta">{ylabel} and average CPU time across all three decode depths</div>
              <div class="summary-kv">
                <span>Points: {len(next(iter(ordered_series.values())))}</span>
                <span>Start: {next(iter(ordered_series.values()))[0]["release_version"]}</span>
                <span>End: {next(iter(ordered_series.values()))[-1]["release_version"]}</span>
                <span>{legend}</span>
              </div>
              <div class="chart">{render_multi_line_chart(ordered_series, metric='quality')}</div>
              <div class="chart-caption">{ylabel}. Endpoints are labeled; intermediate releases are encoded in order from oldest to newest.</div>
              <div class="chart">{render_multi_line_chart(ordered_series, metric='cpu')}</div>
              <div class="chart-caption">Average CPU seconds per sample. This is process CPU time, so it ignores idle wall-clock waiting.</div>
            </article>
            """
        )
    return f"<div class='trend-grid'>{''.join(cards)}</div>"


def render_multi_line_chart(series: dict[str, list[dict[str, Any]]], metric: str) -> str:
    width = 640
    height = 220
    left = 54
    right = 18
    top = 18
    bottom = 34

    def value_of(row: dict[str, Any]) -> float:
        if metric == "cpu":
            return float(row.get("avg_cpu_seconds") or 0.0)
        if row["dataset_kind"] == "scored" and row["recall"] is not None:
            return row["recall"] * 100
        return float(row["decode_count"])

    all_values = [value_of(row) for rows in series.values() for row in rows]
    min_value = min(all_values) if all_values else 0.0
    max_value = max(all_values) if all_values else 1.0
    if max_value == min_value:
        max_value = min_value + 1.0

    x_count = max((len(rows) for rows in series.values()), default=1)

    def x(index: int) -> float:
        usable = width - left - right
        return left + usable * (index / max(x_count - 1, 1))

    def y(value: float) -> float:
        usable = height - top - bottom
        return top + usable * (1 - ((value - min_value) / (max_value - min_value)))

    lines = []
    first_series = next(iter(series.values()), [])
    first = first_series[0]["release_version"] if first_series else "-"
    last = first_series[-1]["release_version"] if first_series else "-"
    for profile_id, rows in series.items():
        values = [value_of(row) for row in rows]
        color = PROFILE_COLORS.get(profile_id, "#0f6c5b")
        points = " ".join(f"{x(i):.1f},{y(v):.1f}" for i, v in enumerate(values))
        dots = "".join(
            f"<circle cx='{x(i):.1f}' cy='{y(v):.1f}' r='4' fill='{color}' />"
            for i, v in enumerate(values)
        )
        lines.append(
            f"<polyline points='{points}' fill='none' stroke='{color}' stroke-width='3' stroke-linecap='round' stroke-linejoin='round' />"
            f"{dots}"
        )
    return f"""
    <svg viewBox="0 0 {width} {height}" role="img" aria-label="trend chart">
      <line x1="{left}" y1="{height-bottom}" x2="{width-right}" y2="{height-bottom}" stroke="#d8cfbf" stroke-width="1" />
      <line x1="{left}" y1="{top}" x2="{left}" y2="{height-bottom}" stroke="#d8cfbf" stroke-width="1" />
      <text x="{left}" y="{top-4}" font-size="12" fill="#5f665e">{max_value:.1f}</text>
      <text x="{left}" y="{height-bottom+16}" font-size="12" fill="#5f665e">{min_value:.1f}</text>
      {''.join(lines)}
      <text x="{x(0):.1f}" y="{height-8}" font-size="12" fill="#5f665e" text-anchor="start">{first}</text>
      <text x="{x(x_count-1):.1f}" y="{height-8}" font-size="12" fill="#5f665e" text-anchor="end">{last}</text>
    </svg>
    """


def render_detail_sections(runs: list[dict[str, Any]]) -> str:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for run in runs:
        key = (run["release_version"], run["profile_label"], run["dataset_label"])
        grouped[key].append(run)

    cards = []
    for (version, profile_label, dataset_label), entries in sorted(
        grouped.items(),
        key=lambda item: (version_key(item[0][0]), item[0][1], item[0][2]),
    ):
        execution_mode = entries[0].get("execution_mode", "unknown")
        jt9_arches = ", ".join(entries[0].get("jt9_arches", [])) or "unknown"
        sample_lines = []
        scored_entries = [entry for entry in entries if entry["metrics"]]
        total_fp = sum(entry["metrics"]["fp"] for entry in scored_entries) if scored_entries else 0
        total_fn = sum(entry["metrics"]["fn"] for entry in scored_entries) if scored_entries else 0
        total_tp = sum(entry["metrics"]["tp"] for entry in scored_entries) if scored_entries else 0
        total_cpu_seconds = sum(entry.get("cpu_seconds") or 0.0 for entry in entries)
        avg_cpu_seconds = total_cpu_seconds / len(entries) if entries else None
        total_wall_seconds = sum(entry.get("wall_seconds") or 0.0 for entry in entries)
        avg_wall_seconds = total_wall_seconds / len(entries) if entries else None
        for entry in sorted(entries, key=lambda item: item["sample_id"]):
            if entry["metrics"]:
                metrics = entry["metrics"]
                sample_lines.append(
                    f"<div class='list-label'>{entry['sample_id']}</div>"
                    f"<div class='mono'>TP {metrics['tp']} | FP {metrics['fp']} | FN {metrics['fn']}\n"
                    f"cpu: {seconds(entry.get('cpu_seconds'))} | wall: {seconds(entry.get('wall_seconds'))}\n"
                    f"missing: {', '.join(metrics['false_negative_messages']) or '-'}\n"
                    f"unexpected: {', '.join(metrics['false_positive_messages']) or '-'}</div>"
                )
            else:
                decoded = ", ".join(decode["message"] for decode in entry["decodes"][:8]) or "-"
                sample_lines.append(
                    f"<div class='list-label'>{entry['sample_id']}</div>"
                    f"<div class='mono'>decode count: {entry['decode_count']}\n"
                    f"cpu: {seconds(entry.get('cpu_seconds'))} | wall: {seconds(entry.get('wall_seconds'))}\n"
                    f"messages: {decoded}</div>"
                )
        cards.append(
            f"""
            <details class="detail-card">
              <summary>
                <h3>{version}</h3>
                <div class="detail-meta">{profile_label} | {dataset_label} | {len(entries)} samples | jt9: {jt9_arches} | mode: {execution_mode}</div>
              </summary>
              <div class="summary-kv">
                <span>TP: {total_tp}</span>
                <span>FP: {total_fp}</span>
                <span>FN: {total_fn}</span>
                <span>CPU total: {seconds(total_cpu_seconds)}</span>
                <span>CPU avg: {seconds(avg_cpu_seconds)}</span>
                <span>Wall avg: {seconds(avg_wall_seconds)}</span>
              </div>
              {''.join(sample_lines)}
            </details>
            """
        )
    return f"<div class='detail-grid'>{''.join(cards)}</div>"
