use axum::extract::State;
use axum::response::Html;
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, Local, Utc};
use clap::Parser;
use ft8_decoder::{
    AudioBuffer, CallModifier, DecodeOptions, DecodeProfile, DecodeStage, DecodedMessage,
    DecoderSession, DecoderState, StageDecodeReport, StructuredCallField, StructuredCallValue,
    StructuredInfoField, StructuredInfoValue, StructuredMessage,
};
use hound::{SampleFormat, WavSpec, WavWriter};
use rigctl::audio::{AudioDevice, AudioStreamConfig, SampleStream};
use rigctl::{Band, K3s, K3sConfig, Mode, detect_k3s_audio_device};
use rustfft::FftPlanner;
use rustfft::num_complex::Complex32;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const SLOT_SECONDS: u64 = 15;
const DECODER_SAMPLE_RATE_HZ: u32 = 12_000;
const WATERFALL_MAX_HZ: f32 = 4_000.0;
const WATERFALL_BUCKETS: usize = 400;
const WATERFALL_HISTORY_ROWS: usize = 180;
const WATERFALL_SAMPLES: usize = 4096;
const WATERFALL_UPDATE_MS: u64 = 200;
const WEB_BIND_DEFAULT: &str = "127.0.0.1:8000";
const BANDMAP_COLUMNS: usize = 15;
const BANDMAP_ROWS: usize = 4;
const BANDMAP_MAX_AGE_SLOTS: u64 = 10;
const DT_HISTORY_FRAMES: usize = 40;
const STATION_RETENTION: Duration = Duration::from_secs(60 * 60);

#[derive(Debug, Parser)]
#[command(name = "ft8rx")]
struct Cli {
    #[arg(long)]
    oneshot: bool,
    #[arg(long, default_value = WEB_BIND_DEFAULT)]
    web_bind: String,
    #[arg(long)]
    save_wav: Option<PathBuf>,
    #[arg(long)]
    save_raw_wav: Option<PathBuf>,
    #[arg(long)]
    device: Option<String>,
}

#[derive(Debug, thiserror::Error)]
enum AppError {
    #[error("rig error: {0}")]
    Rig(#[from] rigctl::Error),
    #[error("audio error: {0}")]
    Audio(#[from] rigctl::audio::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("wav error: {0}")]
    Wav(#[from] hound::Error),
    #[error("decoder error: {0}")]
    Decoder(String),
    #[error("system clock error")]
    Clock,
}

#[derive(Debug, Clone)]
struct DisplayState {
    rig: Option<RigSnapshot>,
    audio: AudioDevice,
    capture_rms_dbfs: f32,
    capture_latest_sample_time: Option<SystemTime>,
    capture_channel_rms_dbfs: Vec<f32>,
    capture_channel: usize,
    capture_recoveries: u64,
    decode_status: String,
    early41_wall_ms: Option<u128>,
    early47_wall_ms: Option<u128>,
    early47_tx_margin_ms: Option<i128>,
    full_wall_ms: Option<u128>,
    last_decode_wall_ms: Option<u128>,
    dropped_slots: u64,
    last_slot_start: Option<SystemTime>,
    early41_decodes: Vec<DecodedMessage>,
    early47_decodes: Vec<DecodedMessage>,
    full_decodes: Vec<DecodedMessage>,
}

#[derive(Debug, Clone)]
struct RigSnapshot {
    frequency_hz: u64,
    mode: Mode,
    band: Band,
    configured_power_w: Option<f32>,
    bar_graph: Option<u8>,
    transmitting: Option<bool>,
}

#[derive(Debug, Clone)]
struct CompositeDecodeRow {
    display: DecodedMessage,
    seen: &'static str,
}

type SharedWebSnapshot = Arc<Mutex<WebSnapshot>>;

#[derive(Debug, Clone, Default)]
struct BandMapStore {
    even: BTreeMap<String, BandMapEntry>,
    odd: BTreeMap<String, BandMapEntry>,
}

#[derive(Debug, Clone)]
struct BandMapEntry {
    callsign: String,
    detail: Option<String>,
    freq_hz: f32,
    last_seen_slot_index: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
struct WebSnapshot {
    time_utc: String,
    rig_frequency_hz: Option<u64>,
    rig_mode: String,
    rig_band: String,
    rig_power_w: Option<f32>,
    rig_bargraph: Option<u8>,
    rig_is_tx: Option<bool>,
    decode_status: String,
    audio_stats: WebAudioStats,
    decode_times: WebDecodeTimes,
    dt_stats: WebDtStats,
    current_slot: String,
    last_done_slot: Option<String>,
    decodes: Vec<WebDecodeRow>,
    waterfall: Vec<Vec<u8>>,
    bandmaps: WebBandMaps,
    stations: Vec<WebStationSummary>,
    station_logs: Vec<WebStationLog>,
}

#[derive(Debug, Clone, Default, Serialize)]
struct WebAudioStats {
    latest_sample: Option<String>,
    selected_channel: usize,
    overall_dbfs: f32,
    left_dbfs: f32,
    right_dbfs: f32,
    recoveries: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
struct WebDecodeTimes {
    early_seconds: Option<f32>,
    mid_seconds: Option<f32>,
    late_seconds: Option<f32>,
    tx_margin_seconds: Option<f32>,
}

#[derive(Debug, Clone, Default, Serialize)]
struct WebDtStats {
    current_mean_seconds: Option<f32>,
    current_median_seconds: Option<f32>,
    current_stddev_seconds: Option<f32>,
    current_count: usize,
    ten_minute_mean_seconds: Option<f32>,
    ten_minute_median_seconds: Option<f32>,
    ten_minute_count: usize,
}

#[derive(Debug, Clone, Serialize)]
struct WebDecodeRow {
    seen: String,
    utc: String,
    snr_db: i32,
    dt_seconds: f32,
    freq_hz: f32,
    kind: String,
    field1: String,
    field1_select_call: Option<String>,
    field2: String,
    field2_select_call: Option<String>,
    info: String,
    text: String,
}

#[derive(Debug, Clone, Default, Serialize)]
struct WebBandMaps {
    even: Vec<Vec<Vec<WebBandMapCall>>>,
    odd: Vec<Vec<Vec<WebBandMapCall>>>,
}

#[derive(Debug, Clone, Serialize)]
struct WebBandMapCall {
    callsign: String,
    detail: Option<String>,
    age_slots: u64,
}

#[derive(Debug, Clone, Serialize)]
struct WebStationSummary {
    callsign: String,
    last_heard_at: String,
    is_in_qso: bool,
    in_qso_since: Option<String>,
    qso_with: Option<String>,
    last_qso_ended_at: Option<String>,
    qso_history: Vec<WebCompletedQso>,
}

#[derive(Debug, Clone, Serialize)]
struct WebCompletedQso {
    started_at: String,
    ended_at: String,
    peer: String,
}

#[derive(Debug, Clone, Serialize)]
struct WebStationLog {
    timestamp: String,
    sender_call: String,
    display_peer: Option<String>,
    peer_before: Option<String>,
    peer_after: Option<String>,
    related_calls: Vec<String>,
    snr_db: i32,
    dt_seconds: f32,
    freq_hz: f32,
    kind: String,
    field1: String,
    field2: String,
    info: String,
    text: String,
}

#[derive(Debug, Clone)]
struct StationTracker {
    stations: BTreeMap<String, StationState>,
    logs: VecDeque<LoggedDecode>,
    hash12_resolutions: BTreeMap<u16, String>,
    hash22_resolutions: BTreeMap<u32, String>,
}

#[derive(Debug, Clone)]
struct StationState {
    last_heard_at: SystemTime,
    active_qso: Option<ActiveQso>,
    last_qso_ended_at: Option<SystemTime>,
    qso_history: Vec<CompletedQso>,
}

#[derive(Debug, Clone)]
struct ActiveQso {
    since: SystemTime,
    peer: PeerRef,
}

#[derive(Debug, Clone)]
struct CompletedQso {
    started_at: SystemTime,
    ended_at: SystemTime,
    peer: PeerRef,
}

#[derive(Debug, Clone)]
enum PeerRef {
    Callsign(String),
    Hash12(u16),
    Hash22(u32),
}

#[derive(Debug, Clone)]
struct LoggedDecode {
    received_at: SystemTime,
    sender_call: String,
    display_peer: Option<PeerRef>,
    peer_before: Option<PeerRef>,
    peer_after: Option<PeerRef>,
    related_calls: Vec<String>,
    snr_db: i32,
    dt_seconds: f32,
    freq_hz: f32,
    kind: String,
    field1: String,
    field2: String,
    info: String,
    text: String,
}

impl Default for StationTracker {
    fn default() -> Self {
        Self {
            stations: BTreeMap::new(),
            logs: VecDeque::new(),
            hash12_resolutions: BTreeMap::new(),
            hash22_resolutions: BTreeMap::new(),
        }
    }
}

#[derive(Debug)]
struct DecodeJob {
    slot_start: SystemTime,
    stage: DecodeStage,
    capture_end: SystemTime,
    samples: Vec<i16>,
    sample_rate_hz: u32,
    raw_path: Option<PathBuf>,
}

#[derive(Debug)]
enum DecodeEvent {
    Finished {
        slot_start: SystemTime,
        stage: DecodeStage,
        wall_ms: u128,
        result: Result<StageDecodeReport, AppError>,
    },
}

#[derive(Debug)]
struct DecodeSummary {
    final_decodes: Vec<DecodedMessage>,
}

#[derive(Debug, Clone, Copy, Default)]
struct SlotStageState {
    early41: bool,
    early47: bool,
    full: bool,
}

impl SlotStageState {
    fn is_handled(self, stage: DecodeStage) -> bool {
        match stage {
            DecodeStage::Early41 => self.early41,
            DecodeStage::Early47 => self.early47,
            DecodeStage::Full => self.full,
        }
    }

    fn mark_handled(&mut self, stage: DecodeStage) {
        match stage {
            DecodeStage::Early41 => self.early41 = true,
            DecodeStage::Early47 => self.early47 = true,
            DecodeStage::Full => self.full = true,
        }
    }

    fn next_due_stage(self, slot_start: SystemTime, latest_sample_time: Option<SystemTime>) -> Option<DecodeStage> {
        let latest_sample_time = latest_sample_time?;
        for stage in [DecodeStage::Early41, DecodeStage::Early47, DecodeStage::Full] {
            if self.is_handled(stage) {
                continue;
            }
            let ready_at = stage_capture_end(slot_start, stage).ok()?;
            if latest_sample_time >= ready_at {
                return Some(stage);
            }
            return None;
        }
        None
    }
}

#[derive(Debug, Clone, Copy)]
struct ActiveDecodeJob {
    slot_start: SystemTime,
    stage: DecodeStage,
}

const INDEX_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>ft8rx</title>
  <style>
    :root {
      --bg: #06111a;
      --panel: #0d1d29;
      --panel-2: #132838;
      --ink: #e7f2f7;
      --muted: #8fb0c0;
      --grid: #20394b;
      --accent: #6ad3ff;
      --good: #97f0a9;
      --warn: #ffd26a;
      --focus-station: #8fe9ff;
      --partner-station: #ffd26a;
      --font: "Iosevka Term", "SF Mono", "Menlo", monospace;
      --waterfall-width: 800px;
      --waterfall-shell-width: 840px;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background:
        radial-gradient(circle at top left, rgba(74, 144, 226, 0.16), transparent 30%),
        linear-gradient(180deg, #071019 0%, #06111a 100%);
      color: var(--ink);
      font-family: var(--font);
    }
    .page {
      width: 100%;
      margin: 0;
      padding: 18px;
    }
    .top-layout {
      display: grid;
      grid-template-columns: minmax(0, 1.6fr) minmax(320px, 0.9fr);
      gap: 18px;
      align-items: start;
    }
    .top-main {
      display: grid;
      gap: 18px;
      width: min(100%, var(--waterfall-shell-width));
    }
    .panel {
      background: rgba(13, 29, 41, 0.95);
      border: 1px solid rgba(143, 176, 192, 0.16);
      border-radius: 14px;
      padding: 14px;
      box-shadow: 0 14px 40px rgba(0, 0, 0, 0.24);
      margin-bottom: 16px;
    }
    .status-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px 18px;
    }
    .label {
      font-size: 11px;
      letter-spacing: 0.12em;
      text-transform: uppercase;
      color: var(--muted);
      margin-bottom: 4px;
    }
    .value {
      font-size: 20px;
      line-height: 1.2;
    }
    .value.small {
      font-size: 15px;
    }
    #waterfall {
      width: 100%;
      max-width: var(--waterfall-width);
      height: 110px;
      display: block;
      margin: 0 auto;
      image-rendering: pixelated;
      background: #02070c;
      border-radius: 10px;
      border: 1px solid rgba(143, 176, 192, 0.12);
    }
    .status-panel,
    .waterfall-panel {
      width: 100%;
      max-width: var(--waterfall-shell-width);
    }
    .maps {
      display: grid;
      grid-template-columns: 1fr;
      gap: 16px;
      margin-bottom: 16px;
    }
    .map-grid {
      display: grid;
      grid-template-columns: repeat(15, minmax(0, 1fr));
      gap: 6px;
      margin-top: 10px;
    }
    .cell {
      min-height: 74px;
      background: linear-gradient(180deg, rgba(19, 40, 56, 0.95), rgba(10, 23, 34, 0.95));
      border: 1px solid rgba(143, 176, 192, 0.12);
      border-radius: 8px;
      padding: 6px;
      overflow: hidden;
    }
    .cell-title {
      font-size: 10px;
      color: var(--muted);
      margin-bottom: 4px;
    }
    .call {
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      font-size: 12px;
      line-height: 1.3;
      color: var(--good);
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }
    th, td {
      padding: 8px 10px;
      border-bottom: 1px solid rgba(143, 176, 192, 0.09);
      text-align: left;
    }
    th {
      color: var(--muted);
      font-size: 12px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    td.num { text-align: right; }
    .seen-early { color: var(--good); }
    .seen-mid { color: var(--warn); }
    .seen-late { color: #ff9e80; }
    .meta-line {
      display: flex;
      justify-content: center;
      gap: 18px;
      flex-wrap: wrap;
      color: var(--muted);
      font-size: 13px;
      margin-top: 8px;
    }
    .pickable {
      color: inherit;
      cursor: pointer;
      text-decoration: none;
    }
    .detail-panel {
      min-height: 100%;
    }
    .detail-block {
      margin-top: 14px;
    }
    .detail-lines {
      color: var(--ink);
      font-size: 13px;
      line-height: 1.45;
      white-space: pre-wrap;
    }
    .history-list {
      max-height: 8.2em;
      overflow: auto;
      padding-right: 4px;
    }
    .detail-empty {
      color: var(--muted);
      font-size: 13px;
    }
    .activity-list {
      margin-top: 8px;
      display: grid;
      gap: 4px;
      max-height: 220px;
      overflow: auto;
      padding-right: 4px;
    }
    .activity-item {
      border: 1px solid rgba(143, 176, 192, 0.1);
      border-radius: 8px;
      padding: 4px 6px;
      background: rgba(19, 40, 56, 0.45);
      font-size: 12px;
      line-height: 1.15;
      display: grid;
      grid-template-columns: 64px 110px 78px 34px 46px 50px minmax(0, 1fr);
      gap: 6px;
      align-items: baseline;
    }
    .activity-head {
      position: sticky;
      top: 0;
      z-index: 1;
      border: 0;
      padding: 0 6px 2px;
      background: linear-gradient(180deg, rgba(13, 29, 41, 0.98), rgba(13, 29, 41, 0.92));
      font-size: 10px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    .activity-col {
      color: var(--muted);
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .activity-station {
      font-weight: 600;
      color: var(--focus-station);
    }
    .activity-partner {
      color: var(--partner-station);
    }
    .activity-msg {
      color: var(--ink);
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    @media (max-width: 1100px) {
      .status-grid { grid-template-columns: 1fr; }
      .top-layout { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="page">
    <div class="top-layout">
      <div class="top-main">
        <section class="panel status-panel">
          <div class="status-grid">
            <div><div class="label">UTC</div><div class="value" id="time"></div></div>
            <div><div class="label">Rig</div><div class="value small" id="rig"></div></div>
            <div><div class="label">Audio</div><div class="value small" id="audio"></div></div>
            <div><div class="label">Status</div><div class="value small" id="status"></div></div>
            <div><div class="label">Slot</div><div class="value small" id="slot"></div></div>
            <div><div class="label">Decode</div><div class="value small" id="times"></div></div>
            <div><div class="label">dT</div><div class="value small" id="dtstats"></div></div>
            <div><div class="label">Decodes</div><div class="value small" id="count"></div></div>
          </div>
        </section>
        <section class="panel waterfall-panel">
          <canvas id="waterfall" width="300" height="180"></canvas>
          <div class="meta-line">
            <span>Waterfall: 0-4000 Hz</span>
            <span>Rows: latest at top</span>
          </div>
        </section>
      </div>
      <section class="panel detail-panel">
        <div class="label">Station Detail</div>
        <div class="value" id="detail-call">No station selected</div>
        <div class="detail-block">
          <div class="label">Current State</div>
          <div class="detail-lines" id="detail-state">Click a callsign in the bandmap or decode table.</div>
        </div>
        <div class="detail-block">
          <div class="label">Recent QSOs</div>
          <div class="detail-lines history-list" id="detail-history"></div>
        </div>
        <div class="detail-block">
          <div class="label">Live Activity</div>
          <div class="activity-list" id="detail-logs">
            <div class="activity-item activity-head">
              <div class="activity-col">Time</div>
              <div class="activity-col">To</div>
              <div class="activity-col">De</div>
              <div class="activity-col">SNR</div>
              <div class="activity-col">dT</div>
              <div class="activity-col">Freq</div>
              <div class="activity-col">Msg</div>
            </div>
          </div>
        </div>
      </section>
    </div>
    <div class="maps">
      <section class="panel">
        <div class="label">Even Slots (:00 / :30)</div>
        <div id="even-map" class="map-grid"></div>
      </section>
      <section class="panel">
        <div class="label">Odd Slots (:15 / :45)</div>
        <div id="odd-map" class="map-grid"></div>
      </section>
    </div>
    <section class="panel">
      <div class="label">Recent Decodes</div>
      <table>
        <thead>
          <tr>
            <th>Seen</th>
            <th>UTC</th>
            <th>SNR</th>
            <th>dT</th>
            <th>Freq</th>
            <th>Kind</th>
            <th>Field 1</th>
            <th>Field 2</th>
            <th>Info</th>
            <th>Message</th>
          </tr>
        </thead>
        <tbody id="decodes"></tbody>
      </table>
    </section>
  </div>
  <script>
    const canvas = document.getElementById('waterfall');
    const ctx = canvas.getContext('2d');
    let selectedCall = null;
    let lastSnapshot = null;
    let autoFollowLogs = true;
    function fmtSec(value) {
      return value == null ? '-' : `${value.toFixed(2)}s`;
    }
    function pickCall(call) {
      if (!call) return;
      selectedCall = call;
      if (lastSnapshot) renderDetail(lastSnapshot);
    }
    function renderCallValue(value, call) {
      if (!call) return value;
      return `<span class="pickable" data-call="${call}">${value}</span>`;
    }
    function escapeHtml(value) {
      return String(value)
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
    }
    function renderParty(value, selected) {
      const text = value || '-';
      if (!selected) {
        return escapeHtml(text);
      }
      const cls = text === selected ? 'activity-station' : 'activity-partner';
      return `<span class="${cls}">${escapeHtml(text)}</span>`;
    }
    function renderWaterfall(rows) {
      if (!rows || rows.length === 0) {
        ctx.fillStyle = '#02070c';
        ctx.fillRect(0, 0, canvas.width, canvas.height);
        return;
      }
      const width = rows[0].length;
      const height = rows.length;
      canvas.width = width;
      canvas.height = height;
      const image = ctx.createImageData(width, height);
      function gradient(v) {
        const t = Math.max(0, Math.min(1, v / 255));
        if (t < 0.18) {
          const u = t / 0.18;
          return [4 + u * 10, 8 + u * 18, 18 + u * 42];
        }
        if (t < 0.42) {
          const u = (t - 0.18) / 0.24;
          return [14 + u * 14, 26 + u * 92, 60 + u * 120];
        }
        if (t < 0.68) {
          const u = (t - 0.42) / 0.26;
          return [28 + u * 152, 118 + u * 74, 180 - u * 76];
        }
        if (t < 0.86) {
          const u = (t - 0.68) / 0.18;
          return [180 + u * 50, 192 + u * 32, 104 - u * 48];
        }
        const u = (t - 0.86) / 0.14;
        return [230 + u * 25, 224 + u * 28, 56 + u * 120];
      }
      for (let y = 0; y < height; y++) {
        const row = rows[y];
        for (let x = 0; x < width; x++) {
          const value = row[x] || 0;
          const [r, g, b] = gradient(value);
          const i = (y * width + x) * 4;
          image.data[i + 0] = Math.round(r);
          image.data[i + 1] = Math.round(g);
          image.data[i + 2] = Math.round(b);
          image.data[i + 3] = 255;
        }
      }
      ctx.putImageData(image, 0, 0);
    }
    function renderBandMap(rootId, grid) {
      const root = document.getElementById(rootId);
      root.innerHTML = '';
      for (let row = 0; row < grid.length; row++) {
        for (let col = 0; col < grid[row].length; col++) {
          const startHz = col * 200 + row * 50;
          const cell = document.createElement('div');
          cell.className = 'cell';
          const title = document.createElement('div');
          title.className = 'cell-title';
          title.textContent = `${startHz}-${startHz + 49} Hz`;
          cell.appendChild(title);
          const entries = grid[row][col] || [];
          for (const entry of entries) {
            const line = document.createElement('div');
            line.className = 'call';
            const fade = Math.min(1, (entry.age_slots || 0) / 4);
            const lightness = 72 - fade * 34;
            const saturation = 88 - fade * 58;
            line.style.color = `hsl(135 ${saturation}% ${lightness}%)`;
            line.innerHTML = renderCallValue(
              entry.detail ? `${entry.callsign} ${entry.detail}` : entry.callsign,
              entry.callsign
            );
            cell.appendChild(line);
          }
          root.appendChild(cell);
        }
      }
    }
    function renderDecodes(rows) {
      const body = document.getElementById('decodes');
      body.innerHTML = '';
      if (!rows.length) {
        const tr = document.createElement('tr');
        const td = document.createElement('td');
        td.colSpan = 10;
        td.textContent = 'No decodes yet';
        tr.appendChild(td);
        body.appendChild(tr);
        return;
      }
      for (const row of rows) {
        const tr = document.createElement('tr');
        const seen = row.seen;
        tr.innerHTML = `
          <td class="seen-${seen}">${seen}</td>
          <td>${row.utc}</td>
          <td class="num">${row.snr_db}</td>
          <td class="num">${row.dt_seconds.toFixed(2)}</td>
          <td class="num">${Math.round(row.freq_hz)}</td>
          <td>${row.kind}</td>
          <td>${renderCallValue(row.field1, row.field1_select_call)}</td>
          <td>${renderCallValue(row.field2, row.field2_select_call)}</td>
          <td>${row.info}</td>
          <td>${row.text}</td>`;
        body.appendChild(tr);
      }
    }
    function renderDetail(data) {
      const title = document.getElementById('detail-call');
      const state = document.getElementById('detail-state');
      const history = document.getElementById('detail-history');
      const logs = document.getElementById('detail-logs');
      const stations = new Map((data.stations || []).map((entry) => [entry.callsign, entry]));
      if (!selectedCall || !stations.has(selectedCall)) {
        title.textContent = selectedCall ? `${selectedCall} not active in last 60m` : 'No station selected';
        state.textContent = selectedCall ? '' : 'Click a callsign in the bandmap or decode table.';
        history.textContent = '';
        logs.innerHTML = '';
        return;
      }
      const station = stations.get(selectedCall);
      title.textContent = station.callsign;
      const current = station.is_in_qso
        ? `In QSO since ${station.in_qso_since ?? '-'} with ${station.qso_with ?? '?'}`
        : `Idle${station.last_qso_ended_at ? `, last ended ${station.last_qso_ended_at}` : ''}`;
      state.textContent = `Last heard: ${station.last_heard_at}\n${current}`;
      history.textContent = station.qso_history.length
        ? [...station.qso_history]
            .reverse()
            .map((item) => `${item.started_at} -> ${item.ended_at}  ${item.peer}`)
            .join('\n')
        : 'No QSO history in last 60m.';
      const related = (data.station_logs || []).filter((item) => (item.related_calls || []).includes(selectedCall));
      logs.innerHTML = `
        <div class="activity-item activity-head">
          <div class="activity-col">Time</div>
          <div class="activity-col">To</div>
          <div class="activity-col">De</div>
          <div class="activity-col">SNR</div>
          <div class="activity-col">dT</div>
          <div class="activity-col">Freq</div>
          <div class="activity-col">Msg</div>
        </div>`;
      if (!related.length) {
        logs.innerHTML += '<div class="detail-empty">No related decodes in last 60m.</div>';
        return;
      }
      for (const item of related) {
        const div = document.createElement('div');
        div.className = 'activity-item';
        const peer = item.display_peer ?? '-';
        div.innerHTML = `
          <div class="activity-col">${item.timestamp}</div>
          <div class="activity-col">${renderParty(peer, selectedCall)}</div>
          <div class="activity-col">${renderParty(item.sender_call, selectedCall)}</div>
          <div class="activity-col">${item.snr_db}</div>
          <div class="activity-col">${item.dt_seconds.toFixed(2)}</div>
          <div class="activity-col">${Math.round(item.freq_hz)}</div>
          <div class="activity-msg">${escapeHtml(item.text)}</div>`;
        logs.appendChild(div);
      }
      if (autoFollowLogs) {
        logs.scrollTop = logs.scrollHeight;
      }
    }
    let refreshInFlight = { value: false };
    let refreshTimer = { value: null };
    function scheduleRefresh(delayMs) {
      if (refreshTimer.value != null) {
        clearTimeout(refreshTimer.value);
      }
      refreshTimer.value = setTimeout(() => refresh().catch(console.error), delayMs);
    }
    async function refresh() {
      if (refreshInFlight.value) {
        return;
      }
      refreshInFlight.value = true;
      try {
        const response = await fetch('/api/state', { cache: 'no-store' });
        const data = await response.json();
        lastSnapshot = data;
        document.getElementById('time').textContent = data.time_utc || '-';
        const freq = data.rig_frequency_hz == null ? 'unavailable' : `${(data.rig_frequency_hz / 1e6).toFixed(3)} MHz`;
      const rigDir = data.rig_is_tx == null ? '?' : (data.rig_is_tx ? 'TX' : 'RX');
      const rigPower = data.rig_power_w == null ? '-' : `${data.rig_power_w.toFixed(1)}W`;
      const rigBg = data.rig_bargraph == null ? '-' : data.rig_bargraph;
      document.getElementById('rig').textContent = `${freq}  ${data.rig_mode}  ${data.rig_band}  ${rigDir}  P=${rigPower}  BG=${rigBg}`;
      document.getElementById('audio').textContent =
        `latest=${data.audio_stats.latest_sample ?? '-'}  ch=${data.audio_stats.selected_channel}  L=${data.audio_stats.left_dbfs.toFixed(1)}  R=${data.audio_stats.right_dbfs.toFixed(1)}  all=${data.audio_stats.overall_dbfs.toFixed(1)} dBFS  rec=${data.audio_stats.recoveries}`;
      document.getElementById('status').textContent = data.decode_status || '-';
      document.getElementById('slot').textContent =
        `${data.current_slot}${data.last_done_slot ? `  last=${data.last_done_slot}` : ''}`;
      document.getElementById('times').textContent =
        `early=${fmtSec(data.decode_times.early_seconds)}  mid=${fmtSec(data.decode_times.mid_seconds)}  late=${fmtSec(data.decode_times.late_seconds)}  tx_margin=${fmtSec(data.decode_times.tx_margin_seconds)}`;
      document.getElementById('dtstats').textContent =
        `cur avg=${fmtSec(data.dt_stats.current_mean_seconds)}  med=${fmtSec(data.dt_stats.current_median_seconds)}  std=${fmtSec(data.dt_stats.current_stddev_seconds)}  n=${data.dt_stats.current_count}  10m avg=${fmtSec(data.dt_stats.ten_minute_mean_seconds)}  med=${fmtSec(data.dt_stats.ten_minute_median_seconds)}  n=${data.dt_stats.ten_minute_count}`;
      document.getElementById('count').textContent = `${data.decodes.length} visible`;
      renderWaterfall(data.waterfall);
      renderBandMap('even-map', data.bandmaps.even);
      renderBandMap('odd-map', data.bandmaps.odd);
      renderDecodes(data.decodes);
      renderDetail(data);
      document.querySelectorAll('[data-call]').forEach((node) => {
        node.addEventListener('click', (event) => {
          event.preventDefault();
          pickCall(node.dataset.call);
        });
      } finally {
        refreshInFlight.value = false;
        scheduleRefresh(250);
      }
    }
    refresh().catch(console.error);
    document.getElementById('detail-logs').addEventListener('scroll', (event) => {
      const node = event.currentTarget;
      const remaining = node.scrollHeight - node.clientHeight - node.scrollTop;
      autoFollowLogs = remaining < 12;
    });
  </script>
</body>
</html>
"#;

fn main() -> Result<(), AppError> {
    let cli = Cli::parse();
    if cli.oneshot {
        run_oneshot(cli)
    } else {
        run_continuous(cli)
    }
}

fn start_web_server(bind: &str, snapshot: SharedWebSnapshot) -> Result<(), AppError> {
    let addr: SocketAddr = bind.parse().map_err(|error| {
        AppError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid web bind address '{bind}': {error}"),
        ))
    })?;
    thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        runtime.block_on(async move {
            let app = Router::new()
                .route("/", get(index_handler))
                .route("/api/state", get(api_state_handler))
                .with_state(snapshot);
            match tokio::net::TcpListener::bind(addr).await {
                Ok(listener) => {
                    if let Err(error) = axum::serve(listener, app).await {
                        eprintln!("web server failed: {error}");
                    }
                }
                Err(error) => eprintln!("web bind failed on {addr}: {error}"),
            }
        });
    });
    Ok(())
}

async fn index_handler() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn api_state_handler(State(snapshot): State<SharedWebSnapshot>) -> Json<WebSnapshot> {
    Json(snapshot.lock().expect("web snapshot poisoned").clone())
}

fn run_continuous(cli: Cli) -> Result<(), AppError> {
    let audio = detect_k3s_audio_device(cli.device.as_deref())?;
    let capture = SampleStream::start(audio.clone(), AudioStreamConfig::default())?;
    let mut rig = K3s::connect(K3sConfig::default()).ok();
    let web_snapshot = Arc::new(Mutex::new(WebSnapshot::default()));
    start_web_server(&cli.web_bind, Arc::clone(&web_snapshot))?;
    let (job_tx, job_rx) = mpsc::sync_channel::<DecodeJob>(1);
    let (event_tx, event_rx) = mpsc::channel::<DecodeEvent>();
    thread::spawn(move || {
        let mut session_slot: Option<SystemTime> = None;
        let mut session = DecoderSession::new();
        let mut state = DecoderState::new();
        while let Ok(job) = job_rx.recv() {
            if session_slot != Some(job.slot_start) {
                session.reset();
                session_slot = Some(job.slot_start);
            }
            let result = decode_stage_from_samples(
                &mut session,
                &mut state,
                &job.samples,
                job.sample_rate_hz,
                job.stage,
                job.slot_start,
                job.raw_path.as_deref(),
            );
            let wall_ms = SystemTime::now()
                .duration_since(job.capture_end)
                .unwrap_or_default()
                .as_millis();
            let _ = event_tx.send(DecodeEvent::Finished {
                slot_start: job.slot_start,
                stage: job.stage,
                wall_ms,
                result,
            });
            if job.stage == DecodeStage::Full {
                session.reset();
                session_slot = None;
            }
        }
    });
    let stop = Arc::new(AtomicBool::new(false));
    let signal = Arc::clone(&stop);
    ctrlc::set_handler(move || {
        signal.store(true, Ordering::Relaxed);
    })
    .map_err(std::io::Error::other)?;

    let mut display = DisplayState {
        rig: read_rig_snapshot(&mut rig),
        audio,
        capture_rms_dbfs: -120.0,
        capture_latest_sample_time: None,
        capture_channel_rms_dbfs: vec![-120.0; capture.config().channels],
        capture_channel: 0,
        capture_recoveries: 0,
        decode_status: "Idle".to_string(),
        early41_wall_ms: None,
        early47_wall_ms: None,
        early47_tx_margin_ms: None,
        full_wall_ms: None,
        last_decode_wall_ms: None,
        dropped_slots: 0,
        last_slot_start: None,
        early41_decodes: Vec::new(),
        early47_decodes: Vec::new(),
        full_decodes: Vec::new(),
    };

    let mut next_slot = next_slot_boundary(SystemTime::now());
    let mut next_slot_stages = SlotStageState::default();
    let mut last_rig_poll = UNIX_EPOCH;
    let mut active_decode: Option<ActiveDecodeJob> = None;
    let mut waterfall_rows = seeded_waterfall_rows();
    let mut last_waterfall_update = UNIX_EPOCH;
    let mut last_waterfall_sample_time = UNIX_EPOCH;
    let mut bandmaps = BandMapStore::default();
    let mut dt_frame_history = VecDeque::<Vec<DecodedMessage>>::with_capacity(DT_HISTORY_FRAMES);
    let mut station_tracker = StationTracker::default();

    print!("\x1b[?25l");
    while !stop.load(Ordering::Relaxed) {
        let stats = capture.stats();
        display.capture_rms_dbfs = stats.last_chunk_rms_dbfs;
        display.capture_latest_sample_time = stats.latest_sample_time;
        display.capture_channel_rms_dbfs = stats.channel_rms_dbfs;
        display.capture_channel = stats.selected_channel;
        display.capture_recoveries = stats.recoveries;

        let now = SystemTime::now();
        if now.duration_since(last_rig_poll).unwrap_or_default() >= Duration::from_secs(2) {
            display.rig = read_rig_snapshot(&mut rig);
            last_rig_poll = now;
        }

        while let Ok(event) = event_rx.try_recv() {
            match event {
                DecodeEvent::Finished {
                    slot_start,
                    stage,
                    wall_ms,
                    result,
                } => {
                    active_decode = None;
                    match result {
                        Ok(update) => {
                            match stage {
                                DecodeStage::Early41 => {
                                    display.early41_wall_ms = Some(wall_ms);
                                    display.early41_decodes = update.report.decodes.clone();
                                }
                                DecodeStage::Early47 => {
                                    display.early47_wall_ms = Some(wall_ms);
                                    display.early47_tx_margin_ms = Some(
                                        tx_margin_after_stage_decode_ms(slot_start, stage, wall_ms)?,
                                    );
                                    display.early47_decodes = update.report.decodes.clone();
                                }
                                DecodeStage::Full => {
                                    display.last_slot_start = Some(slot_start);
                                    display.full_wall_ms = Some(wall_ms);
                                    display.last_decode_wall_ms = Some(wall_ms);
                                    display.full_decodes = update.report.decodes.clone();
                                    if dt_frame_history.len() == DT_HISTORY_FRAMES {
                                        dt_frame_history.pop_front();
                                    }
                                    dt_frame_history.push_back(display.full_decodes.clone());
                                    station_tracker.ingest_frame(slot_start, &display.full_decodes);
                                    update_bandmaps(&mut bandmaps, slot_start, &display.full_decodes);
                                }
                            }
                        }
                        Err(error) => {
                            display.decode_status =
                                format!("Last {} {} failed: {}", stage.as_str(), format_slot_time(slot_start), error);
                            if stage == DecodeStage::Full {
                                display.last_slot_start = Some(slot_start);
                                display.full_wall_ms = Some(wall_ms);
                                display.last_decode_wall_ms = Some(wall_ms);
                                display.early41_decodes.clear();
                                display.early47_decodes.clear();
                                display.full_decodes.clear();
                            }
                        }
                    }
                }
            }
        }

        while let Some(stage) = next_slot_stages.next_due_stage(next_slot, display.capture_latest_sample_time) {
            let slot_start = next_slot;
            let capture_end = stage_capture_end(slot_start, stage)?;
            let samples = match extract_stage_capture(&capture, slot_start, stage) {
                Ok(raw) => raw,
                Err(AppError::Audio(rigctl::audio::Error::WindowNotReady)) => {
                    break;
                }
                Err(error) => {
                    display.decode_status = format!(
                        "Capture error for {} {}: {}",
                        stage.as_str(),
                        format_slot_time(slot_start),
                        error
                    );
                    next_slot_stages.mark_handled(stage);
                    if stage == DecodeStage::Full {
                        display.last_slot_start = Some(slot_start);
                        display.early41_decodes.clear();
                        display.early47_decodes.clear();
                        display.full_decodes.clear();
                        next_slot += Duration::from_secs(SLOT_SECONDS);
                        next_slot_stages = SlotStageState::default();
                    }
                    continue;
                }
            };

            let raw_path = if stage == DecodeStage::Full {
                Some(
                    cli.save_raw_wav
                        .clone()
                        .unwrap_or_else(|| temp_path("ft8rx-raw.wav")),
                )
            } else {
                None
            };

            let send_result = job_tx.try_send(DecodeJob {
                slot_start,
                stage,
                capture_end,
                samples,
                sample_rate_hz: capture.config().sample_rate_hz,
                raw_path,
            });
            next_slot_stages.mark_handled(stage);
            match send_result {
                Ok(()) => {
                    if stage == DecodeStage::Early41 {
                        display.early41_wall_ms = None;
                        display.early47_wall_ms = None;
                        display.early47_tx_margin_ms = None;
                        display.full_wall_ms = None;
                        display.last_decode_wall_ms = None;
                        display.early41_decodes.clear();
                        display.early47_decodes.clear();
                        display.full_decodes.clear();
                    }
                    active_decode = Some(ActiveDecodeJob { slot_start, stage });
                }
                Err(mpsc::TrySendError::Full(_)) => {
                    if stage == DecodeStage::Full {
                        display.dropped_slots += 1;
                        display.decode_status = format!(
                            "capture=active decode=busy dropping={} drops={} next={}",
                            format_slot_time(slot_start),
                            display.dropped_slots,
                            format_slot_time(next_slot + Duration::from_secs(SLOT_SECONDS))
                        );
                        display.last_slot_start = Some(slot_start);
                        display.early41_decodes.clear();
                        display.early47_decodes.clear();
                        display.full_decodes.clear();
                    }
                }
                Err(mpsc::TrySendError::Disconnected(_)) => {
                    return Err(AppError::Io(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "decode worker disconnected",
                    )));
                }
            }

            if stage == DecodeStage::Full {
                next_slot += Duration::from_secs(SLOT_SECONDS);
                next_slot_stages = SlotStageState::default();
            }
        }

        if should_refresh_waterfall(
            now,
            last_waterfall_update,
            display.capture_latest_sample_time,
            last_waterfall_sample_time,
        ) {
            if let Some(latest_sample_time) = display.capture_latest_sample_time {
                if let Ok(row) = compute_latest_waterfall_row(&capture, latest_sample_time) {
                    push_waterfall_row(&mut waterfall_rows, row);
                    last_waterfall_update = now;
                    last_waterfall_sample_time = latest_sample_time;
                }
            }
        }

        display.decode_status = format_status(
            active_decode,
            next_slot,
            display.dropped_slots,
            capture.config().sample_rate_hz,
        );
        refresh_web_snapshot(
            &web_snapshot,
            &display,
            &waterfall_rows,
            &bandmaps,
            &dt_frame_history,
            &station_tracker,
        );
        render(&display);
        thread::sleep(Duration::from_millis(50));
    }

    print!("\x1b[?25h");
    Ok(())
}

fn refresh_web_snapshot(
    snapshot: &SharedWebSnapshot,
    display: &DisplayState,
    waterfall_rows: &VecDeque<Vec<u8>>,
    bandmaps: &BandMapStore,
    dt_frame_history: &VecDeque<Vec<DecodedMessage>>,
    station_tracker: &StationTracker,
) {
    let now = SystemTime::now();
    let current_slot = current_slot_boundary(now);
    let composite = composite_rows(display);
    let preferred_decodes = preferred_stage_decodes(display);
    let current_dt_stats = summarize_dt(preferred_decodes);
    let ten_minute_dt_stats = summarize_dt_history(dt_frame_history);
    let decodes = composite
        .into_iter()
        .map(|row| {
            let (kind, field1, field2, info) = decode_columns(&row.display);
            WebDecodeRow {
                seen: row.seen.to_string(),
                utc: row.display.utc,
                snr_db: row.display.snr_db,
                dt_seconds: row.display.dt_seconds,
                freq_hz: row.display.freq_hz,
                kind,
                field1,
                field1_select_call: semantic_first_call_display_call(&row.display.message),
                field2,
                field2_select_call: semantic_sender_call(&row.display.message),
                info,
                text: row.display.text,
            }
        })
        .collect::<Vec<_>>();
    let current_slot_index = slot_index(current_slot);
    let mut guard = snapshot.lock().expect("web snapshot poisoned");
    guard.time_utc = {
        let now_utc: DateTime<Utc> = now.into();
        now_utc.format("%Y-%m-%d %H:%M:%S UTC").to_string()
    };
    guard.rig_frequency_hz = display.rig.as_ref().map(|state| state.frequency_hz);
    guard.rig_mode = display
        .rig
        .as_ref()
        .map(|state| state.mode.to_string())
        .unwrap_or_else(|| "unavailable".to_string());
    guard.rig_band = display
        .rig
        .as_ref()
        .map(|state| state.band.to_string())
        .unwrap_or_else(|| "unavailable".to_string());
    guard.rig_power_w = display.rig.as_ref().and_then(|state| state.configured_power_w);
    guard.rig_bargraph = display.rig.as_ref().and_then(|state| state.bar_graph);
    guard.rig_is_tx = display.rig.as_ref().and_then(|state| state.transmitting);
    guard.decode_status = display.decode_status.clone();
    guard.audio_stats = WebAudioStats {
        latest_sample: display.capture_latest_sample_time.map(format_slot_time),
        selected_channel: display.capture_channel,
        overall_dbfs: display.capture_rms_dbfs,
        left_dbfs: display.capture_channel_rms_dbfs.first().copied().unwrap_or(-120.0),
        right_dbfs: display.capture_channel_rms_dbfs.get(1).copied().unwrap_or(-120.0),
        recoveries: display.capture_recoveries,
    };
    guard.decode_times = WebDecodeTimes {
        early_seconds: display.early41_wall_ms.map(ms_to_seconds),
        mid_seconds: display.early47_wall_ms.map(ms_to_seconds),
        late_seconds: display.full_wall_ms.map(ms_to_seconds),
        tx_margin_seconds: display.early47_tx_margin_ms.map(ms_to_signed_seconds),
    };
    guard.dt_stats = WebDtStats {
        current_mean_seconds: current_dt_stats.mean,
        current_median_seconds: current_dt_stats.median,
        current_stddev_seconds: current_dt_stats.stddev,
        current_count: current_dt_stats.count,
        ten_minute_mean_seconds: ten_minute_dt_stats.mean,
        ten_minute_median_seconds: ten_minute_dt_stats.median,
        ten_minute_count: ten_minute_dt_stats.count,
    };
    guard.current_slot = format_slot_time(current_slot);
    guard.last_done_slot = display.last_slot_start.map(format_slot_time);
    guard.decodes = decodes;
    guard.waterfall = waterfall_rows.iter().cloned().collect();
    guard.bandmaps = WebBandMaps {
        even: build_bandmap_grid(&bandmaps.even, current_slot_index),
        odd: build_bandmap_grid(&bandmaps.odd, current_slot_index),
    };
    guard.stations = station_tracker.web_station_summaries();
    guard.station_logs = station_tracker.web_logs();
}

fn run_oneshot(cli: Cli) -> Result<(), AppError> {
    let audio = detect_k3s_audio_device(cli.device.as_deref())?;
    let capture = SampleStream::start(audio.clone(), AudioStreamConfig::default())?;
    let target_slot = next_slot_boundary(SystemTime::now());

    println!("audio=\"{}\" spec={}", audio.name, audio.spec);
    println!("target_slot={}", format_slot_time(target_slot));

    let ready_at = slot_capture_end(target_slot, capture.config().sample_rate_hz)?;
    while SystemTime::now() < ready_at {
        let stats = capture.stats();
        let latest = stats
            .latest_sample_time
            .map(format_slot_time)
            .unwrap_or_else(|| "------".to_string());
        let left = stats.channel_rms_dbfs.first().copied().unwrap_or(-120.0);
        let right = stats.channel_rms_dbfs.get(1).copied().unwrap_or(-120.0);
        println!(
            "waiting latest_sample={} ch={} rms={:.1}dBFS left={:.1} right={:.1} recoveries={}",
            latest,
            stats.selected_channel,
            stats.last_chunk_rms_dbfs,
            left,
            right,
            stats.recoveries
        );
        thread::sleep(Duration::from_secs(1));
    }

    let summary = decode_slot_from_capture(
        &capture,
        target_slot,
        cli.save_raw_wav.as_deref().or(cli.save_wav.as_deref()),
    )?;
    println!("decodes={}", summary.final_decodes.len());
    for decode in summary.final_decodes {
        println!(
            "{} {:>4} {:+5.2} {:>6.0} {}",
            decode.utc, decode.snr_db, decode.dt_seconds, decode.freq_hz, decode.text
        );
    }
    Ok(())
}

fn read_rig_snapshot(rig: &mut Option<K3s>) -> Option<RigSnapshot> {
    let rig = rig.as_mut()?;
    Some(RigSnapshot {
        frequency_hz: rig.get_frequency_hz().ok()?,
        mode: rig.get_mode().ok()?,
        band: rig.get_band().ok()?,
        configured_power_w: rig.get_configured_power_w().ok(),
        bar_graph: rig.get_bar_graph().ok().map(|reading| reading.level),
        transmitting: rig.is_transmitting().ok(),
    })
}

fn extract_slot_capture(capture: &SampleStream, slot_start: SystemTime) -> Result<Vec<i16>, AppError> {
    Ok(capture.extract_window(
        slot_start,
        full_slot_sample_count(capture.config().sample_rate_hz),
    )?)
}

fn extract_stage_capture(
    capture: &SampleStream,
    slot_start: SystemTime,
    stage: DecodeStage,
) -> Result<Vec<i16>, AppError> {
    Ok(capture.extract_window(
        slot_start,
        stage_sample_count(capture.config().sample_rate_hz, stage),
    )?)
}

fn decode_slot_from_capture(
    capture: &SampleStream,
    slot_start: SystemTime,
    save_raw_wav: Option<&Path>,
) -> Result<DecodeSummary, AppError> {
    let samples = extract_slot_capture(capture, slot_start)?;
    let raw_path = save_raw_wav
        .map(Path::to_path_buf)
        .unwrap_or_else(|| temp_path("ft8rx-raw.wav"));
    decode_slot_from_samples_with_raw_path(
        &samples,
        capture.config().sample_rate_hz,
        &raw_path,
        save_raw_wav.is_some(),
        slot_start,
    )
}

fn decode_slot_from_samples_with_raw_path(
    samples: &[i16],
    sample_rate_hz: u32,
    raw_path: &Path,
    keep_raw: bool,
    slot_start: SystemTime,
) -> Result<DecodeSummary, AppError> {
    if keep_raw {
        write_mono_wav(raw_path, sample_rate_hz, samples)?;
    }
    let decodes = decode_slot_from_samples(samples, sample_rate_hz, slot_start)?;
    if !keep_raw && raw_path.exists() {
        let _ = std::fs::remove_file(raw_path);
    }
    Ok(decodes)
}

fn decode_stage_from_samples(
    session: &mut DecoderSession,
    state: &mut DecoderState,
    samples: &[i16],
    sample_rate_hz: u32,
    stage: DecodeStage,
    slot_start: SystemTime,
    raw_path: Option<&Path>,
) -> Result<StageDecodeReport, AppError> {
    if let Some(raw_path) = raw_path {
        write_mono_wav(raw_path, sample_rate_hz, samples)?;
    }
    let options = DecodeOptions {
        profile: DecodeProfile::Medium,
        min_freq_hz: 200.0,
        max_freq_hz: 3_500.0,
        ..DecodeOptions::default()
    };
    let audio = AudioBuffer {
        sample_rate_hz: DECODER_SAMPLE_RATE_HZ,
        samples: resample_linear_f32(
            &samples
                .iter()
                .map(|&sample| sample as f32 / i16::MAX as f32)
                .collect::<Vec<_>>(),
            sample_rate_hz,
            DECODER_SAMPLE_RATE_HZ,
        ),
    };
    let (mut update, next_state) = session
        .decode_stage_with_state(&audio, &options, stage, Some(state))
        .map_err(|error| AppError::Decoder(error.to_string()))?;
    *state = next_state;
    relabel_stage_update(&mut update, slot_start);
    Ok(update)
}

fn decode_slot_from_samples(
    samples: &[i16],
    sample_rate_hz: u32,
    slot_start: SystemTime,
) -> Result<DecodeSummary, AppError> {
    let options = DecodeOptions {
        profile: DecodeProfile::Medium,
        min_freq_hz: 200.0,
        max_freq_hz: 3_500.0,
        ..DecodeOptions::default()
    };
    let audio = AudioBuffer {
        sample_rate_hz: DECODER_SAMPLE_RATE_HZ,
        samples: resample_linear_f32(
            &samples
                .iter()
                .map(|&sample| sample as f32 / i16::MAX as f32)
                .collect::<Vec<_>>(),
            sample_rate_hz,
            DECODER_SAMPLE_RATE_HZ,
        ),
    };
    let mut session = DecoderSession::new();
    let updates = session
        .decode_available(&audio, &options)
        .map_err(|error| AppError::Decoder(error.to_string()))?;
    let mut final_decodes = Vec::new();
    for mut update in updates {
        relabel_stage_update(&mut update, slot_start);
        match update.stage {
            DecodeStage::Early41 => {}
            DecodeStage::Early47 => {
                final_decodes = update.report.decodes.clone();
            }
            DecodeStage::Full => {
                final_decodes = update.report.decodes.clone();
            }
        }
    }
    Ok(DecodeSummary {
        final_decodes,
    })
}

fn write_mono_wav(path: &Path, sample_rate_hz: u32, samples: &[i16]) -> Result<(), AppError> {
    let spec = WavSpec {
        channels: 1,
        sample_rate: sample_rate_hz,
        bits_per_sample: 16,
        sample_format: SampleFormat::Int,
    };
    let mut writer = WavWriter::create(path, spec)?;
    for &sample in samples {
        writer.write_sample(sample)?;
    }
    writer.finalize()?;
    Ok(())
}

fn slot_progress_bar(now: SystemTime) -> String {
    let slot_start = current_slot_boundary(now);
    let elapsed = now
        .duration_since(slot_start)
        .unwrap_or_default()
        .as_secs_f32();
    let progress = (elapsed / SLOT_SECONDS as f32).clamp(0.0, 1.0);
    let width: usize = 16;
    let filled = (progress * width as f32).round() as usize;
    format!(
        "[{}{}] {:>3}%",
        "#".repeat(filled),
        ".".repeat(width.saturating_sub(filled)),
        (progress * 100.0).round() as i32
    )
}

fn render(display: &DisplayState) {
    let now_local: DateTime<Local> = SystemTime::now().into();
    let now = SystemTime::now();
    let current_slot = current_slot_boundary(now);
    let rig_frequency = display
        .rig
        .as_ref()
        .map(|state| format!("{:.3} MHz", state.frequency_hz as f64 / 1_000_000.0))
        .unwrap_or_else(|| "unavailable".to_string());
    let rig_mode = display
        .rig
        .as_ref()
        .map(|state| state.mode.to_string())
        .unwrap_or_else(|| "unavailable".to_string());
    let rig_band = display
        .rig
        .as_ref()
        .map(|state| state.band.to_string())
        .unwrap_or_else(|| "unavailable".to_string());
    let rig_direction = display
        .rig
        .as_ref()
        .and_then(|state| state.transmitting)
        .map(|tx| if tx { "TX" } else { "RX" })
        .unwrap_or("?");
    let rig_power = display
        .rig
        .as_ref()
        .and_then(|state| state.configured_power_w)
        .map(|watts| format!("{watts:.1}W"))
        .unwrap_or_else(|| "-".to_string());
    let rig_bargraph = display
        .rig
        .as_ref()
        .and_then(|state| state.bar_graph)
        .map(|level| level.to_string())
        .unwrap_or_else(|| "-".to_string());
    let display_decodes = preferred_stage_decodes(display);
    let dt_stats = summarize_dt(display_decodes);
    let composite_rows = composite_rows(display);
    let left = display.capture_channel_rms_dbfs.first().copied().unwrap_or(-120.0);
    let right = display.capture_channel_rms_dbfs.get(1).copied().unwrap_or(-120.0);
    let latest_sample = display
        .capture_latest_sample_time
        .map(format_slot_time)
        .unwrap_or_else(|| "------".to_string());

    let mut output = String::new();
    let _ = writeln!(output, "\x1b[2J\x1b[HFT8RX    {}", now_local.format("%Y-%m-%d %H:%M:%S %Z"));
    let _ = writeln!(
        output,
        "Rig      {}  {}  {}  {}  P={}  BG={}",
        rig_frequency, rig_mode, rig_band, rig_direction, rig_power, rig_bargraph
    );
    let _ = writeln!(output, "Audio    {} ({})", display.audio.name, display.audio.spec);
    let _ = writeln!(
        output,
        "Chan     latest={} selected={} left={:.1} dBFS right={:.1} dBFS",
        latest_sample, display.capture_channel, left, right
    );
    let _ = writeln!(
        output,
        "Status   {}{}",
        display.decode_status,
        display
            .last_decode_wall_ms
            .map(|ms| format!(" last={:.2}s", ms as f32 / 1000.0))
            .unwrap_or_default()
    );
    let _ = writeln!(
        output,
        "DecodeT  early={} mid={} late={} tx_margin={}",
        format_wall_time(display.early41_wall_ms),
        format_wall_time(display.early47_wall_ms),
        format_wall_time(display.full_wall_ms),
        format_signed_wall_time(display.early47_tx_margin_ms)
    );
    if let Some(slot_start) = display.last_slot_start {
        let _ = writeln!(
            output,
            "Slot     {} {} last_done={}",
            format_slot_time(current_slot),
            slot_progress_bar(now),
            format_slot_time(slot_start)
        );
    } else {
        let _ = writeln!(
            output,
            "Slot     {} {}",
            format_slot_time(current_slot),
            slot_progress_bar(now)
        );
    }
    let _ = writeln!(
        output,
        "AudioLvl last={:.1} dBFS recoveries={}",
        display.capture_rms_dbfs, display.capture_recoveries
    );
    let _ = writeln!(
        output,
        "dT stats avg={:+.2}s med={:+.2}s stddev={:.2}s count={}",
        dt_stats.mean.unwrap_or(0.0),
        dt_stats.median.unwrap_or(0.0),
        dt_stats.stddev.unwrap_or(0.0),
        dt_stats.count
    );
    let _ = writeln!(output);
    let _ = writeln!(
        output,
        "Seen    UTC    SNR   dT(s)   Freq(Hz)  Message"
    );
    let _ = writeln!(
        output,
        "------  -----  ----  ------  --------  -------"
    );
    if composite_rows.is_empty() {
        let _ = writeln!(output, "no decodes yet");
    } else {
        for row in &composite_rows {
            let _ = writeln!(
                output,
                "{:<6}  {:<5}  {:>4}  {:+6.2}  {:>8.0}  {}",
                row.seen,
                row.display.utc,
                row.display.snr_db,
                row.display.dt_seconds,
                row.display.freq_hz,
                row.display.text
            );
        }
    }
    print!("{output}");
}

#[derive(Debug, Clone, Copy, Default)]
struct DtSummary {
    mean: Option<f32>,
    median: Option<f32>,
    stddev: Option<f32>,
    count: usize,
}

fn summarize_dt(decodes: &[DecodedMessage]) -> DtSummary {
    if decodes.is_empty() {
        return DtSummary::default();
    }
    let values: Vec<f32> = decodes.iter().map(|decode| decode.dt_seconds).collect();
    summarize_dt_values(&values)
}

fn summarize_dt_history(frame_history: &VecDeque<Vec<DecodedMessage>>) -> DtSummary {
    let values = frame_history
        .iter()
        .flat_map(|frame| frame.iter().map(|decode| decode.dt_seconds))
        .collect::<Vec<_>>();
    summarize_dt_values(&values)
}

fn summarize_dt_values(values: &[f32]) -> DtSummary {
    if values.is_empty() {
        return DtSummary::default();
    }
    let mean = values.iter().sum::<f32>() / values.len() as f32;
    let variance = values
        .iter()
        .map(|value| {
            let delta = *value - mean;
            delta * delta
        })
        .sum::<f32>()
        / values.len() as f32;
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.total_cmp(right));
    let median = if sorted.len() % 2 == 1 {
        sorted[sorted.len() / 2]
    } else {
        let upper = sorted.len() / 2;
        (sorted[upper - 1] + sorted[upper]) * 0.5
    };
    DtSummary {
        mean: Some(mean),
        median: Some(median),
        stddev: Some(variance.sqrt()),
        count: values.len(),
    }
}

fn format_wall_time(wall_ms: Option<u128>) -> String {
    wall_ms
        .map(|ms| format!("{:.2}s", ms as f32 / 1000.0))
        .unwrap_or_else(|| "-".to_string())
}

fn format_signed_wall_time(wall_ms: Option<i128>) -> String {
    wall_ms
        .map(|ms| format!("{:+.2}s", ms as f32 / 1000.0))
        .unwrap_or_else(|| "-".to_string())
}

fn ms_to_seconds(ms: u128) -> f32 {
    ms as f32 / 1000.0
}

fn ms_to_signed_seconds(ms: i128) -> f32 {
    ms as f32 / 1000.0
}

fn next_slot_boundary(now: SystemTime) -> SystemTime {
    current_slot_boundary(now) + Duration::from_secs(SLOT_SECONDS)
}

fn current_slot_boundary(now: SystemTime) -> SystemTime {
    let since_epoch = now.duration_since(UNIX_EPOCH).unwrap_or_default();
    let current = (since_epoch.as_secs() / SLOT_SECONDS) * SLOT_SECONDS;
    UNIX_EPOCH + Duration::from_secs(current)
}

fn slot_index(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() / SLOT_SECONDS
}

fn is_even_slot_family(time: SystemTime) -> bool {
    let second = time.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() % 60;
    matches!(second, 0 | 30)
}

fn format_slot_time(time: SystemTime) -> String {
    let utc: DateTime<Utc> = time.into();
    utc.format("%H%M%S").to_string()
}

fn temp_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("{}-{}", std::process::id(), name))
}

fn full_slot_sample_count(sample_rate_hz: u32) -> usize {
    SLOT_SECONDS as usize * sample_rate_hz as usize
}

fn samples_to_duration(sample_rate_hz: u32, sample_count: usize) -> Duration {
    Duration::from_secs_f64(sample_count as f64 / sample_rate_hz as f64)
}

fn stage_sample_count(sample_rate_hz: u32, stage: DecodeStage) -> usize {
    (((stage.required_samples() as u64 * sample_rate_hz as u64) + (DECODER_SAMPLE_RATE_HZ as u64 / 2))
        / DECODER_SAMPLE_RATE_HZ as u64) as usize
}

fn capture_window_duration(sample_rate_hz: u32) -> Duration {
    Duration::from_secs_f64(full_slot_sample_count(sample_rate_hz) as f64 / sample_rate_hz as f64)
}

fn slot_capture_end(slot_start: SystemTime, sample_rate_hz: u32) -> Result<SystemTime, AppError> {
    slot_start
        .checked_add(capture_window_duration(sample_rate_hz))
        .ok_or(AppError::Clock)
}

fn stage_capture_end(slot_start: SystemTime, stage: DecodeStage) -> Result<SystemTime, AppError> {
    slot_start
        .checked_add(Duration::from_secs_f64(
            stage.required_samples() as f64 / DECODER_SAMPLE_RATE_HZ as f64,
        ))
        .ok_or(AppError::Clock)
}

fn tx_margin_after_stage_decode_ms(
    slot_start: SystemTime,
    stage: DecodeStage,
    wall_ms: u128,
) -> Result<i128, AppError> {
    let tx_start = slot_start
        .checked_add(Duration::from_secs(SLOT_SECONDS))
        .ok_or(AppError::Clock)?;
    let capture_end = stage_capture_end(slot_start, stage)?;
    let capture_to_tx_ms = tx_start
        .duration_since(capture_end)
        .map_err(|_| AppError::Clock)?
        .as_millis() as i128;
    Ok(capture_to_tx_ms - wall_ms as i128)
}

fn format_status(
    active_decode: Option<ActiveDecodeJob>,
    next_slot: SystemTime,
    dropped_slots: u64,
    sample_rate_hz: u32,
) -> String {
    let now = SystemTime::now();
    let capture_active = match slot_capture_end(current_slot_boundary(now), sample_rate_hz) {
        Ok(capture_end) => now < capture_end,
        Err(_) => false,
    };
    let capture_state = if capture_active { "active" } else { "idle" };
    match active_decode {
        Some(active) => format!(
            "capture={} decode={} slot={} drops={} next={}",
            capture_state,
            active.stage.as_str(),
            format_slot_time(active.slot_start),
            dropped_slots,
            format_slot_time(next_slot)
        ),
        None => format!(
            "capture={} decode=idle drops={} next={}",
            capture_state,
            dropped_slots,
            format_slot_time(next_slot)
        ),
    }
}

fn relabel_stage_update(update: &mut StageDecodeReport, slot_start: SystemTime) {
    let slot_label = format_slot_time(slot_start);
    for decode in &mut update.report.decodes {
        decode.utc = slot_label.clone();
    }
    for decode in &mut update.new_decodes {
        decode.utc = slot_label.clone();
    }
    for decode in &mut update.updated_decodes {
        decode.utc = slot_label.clone();
    }
}

fn preferred_stage_decodes(display: &DisplayState) -> &[DecodedMessage] {
    if !display.full_decodes.is_empty() {
        &display.full_decodes
    } else if !display.early47_decodes.is_empty() {
        &display.early47_decodes
    } else {
        &display.early41_decodes
    }
}

fn composite_rows(display: &DisplayState) -> Vec<CompositeDecodeRow> {
    let mut rows = BTreeMap::<String, CompositeDecodeRow>::new();
    for decode in &display.early41_decodes {
        let key = decode.text.clone();
        rows.insert(
            key,
            CompositeDecodeRow {
                display: decode.clone(),
                seen: "early",
            },
        );
    }
    for decode in &display.early47_decodes {
        let entry = rows.entry(decode.text.clone()).or_insert_with(|| CompositeDecodeRow {
            display: decode.clone(),
            seen: "mid",
        });
        entry.display = decode.clone();
    }
    for decode in &display.full_decodes {
        let entry = rows.entry(decode.text.clone()).or_insert_with(|| CompositeDecodeRow {
            display: decode.clone(),
            seen: "late",
        });
        entry.display = decode.clone();
    }
    let mut rows: Vec<_> = rows.into_values().collect();
    rows.sort_by(|left, right| {
        left.display
            .freq_hz
            .total_cmp(&right.display.freq_hz)
            .then_with(|| left.display.text.cmp(&right.display.text))
    });
    rows
}

fn decode_columns(decode: &DecodedMessage) -> (String, String, String, String) {
    match &decode.message {
        StructuredMessage::Standard {
            first,
            second,
            acknowledge,
            info,
            ..
        } => (
            "std".to_string(),
            structured_call_text(first),
            structured_call_text(second),
            structured_info_text(*acknowledge, info),
        ),
        StructuredMessage::Nonstandard {
            hashed_call,
            plain_call,
            hashed_is_second,
            reply,
            cq,
            ..
        } => {
            let hashed = hashed_call_text(hashed_call);
            let (field1, field2) = if *cq {
                ("CQ".to_string(), plain_call.callsign.clone())
            } else if *hashed_is_second {
                (plain_call.callsign.clone(), hashed)
            } else {
                (hashed, plain_call.callsign.clone())
            };
            let info = if *cq {
                "CQ".to_string()
            } else {
                reply_word_text(*reply).to_string()
            };
            ("nonstd".to_string(), field1, field2, info)
        }
        StructuredMessage::FreeText { .. } => (
            "free".to_string(),
            String::new(),
            String::new(),
            String::new(),
        ),
        StructuredMessage::Unsupported { .. } => (
            "unsup".to_string(),
            String::new(),
            String::new(),
            String::new(),
        ),
    }
}

fn semantic_sender_call(message: &StructuredMessage) -> Option<String> {
    match message {
        StructuredMessage::Standard { second, .. } => structured_call_station_name(second),
        StructuredMessage::Nonstandard {
            hashed_call,
            plain_call,
            hashed_is_second,
            cq,
            ..
        } => {
            if *cq {
                Some(plain_call.callsign.clone())
            } else if *hashed_is_second {
                hashed_call.resolved_callsign.clone()
            } else {
                Some(plain_call.callsign.clone())
            }
        }
        StructuredMessage::FreeText { .. } | StructuredMessage::Unsupported { .. } => None,
    }
}

fn semantic_first_call_display_call(message: &StructuredMessage) -> Option<String> {
    match message {
        StructuredMessage::Standard { first, .. } => structured_call_station_name(first),
        StructuredMessage::Nonstandard {
            hashed_call,
            plain_call,
            hashed_is_second,
            cq,
            ..
        } => {
            if *cq {
                None
            } else if *hashed_is_second {
                Some(plain_call.callsign.clone())
            } else {
                hashed_call.resolved_callsign.clone()
            }
        }
        StructuredMessage::FreeText { .. } | StructuredMessage::Unsupported { .. } => None,
    }
}

fn structured_call_station_name(field: &StructuredCallField) -> Option<String> {
    match &field.value {
        StructuredCallValue::StandardCall { callsign } => Some(callsign.clone()),
        StructuredCallValue::Hash22 {
            resolved_callsign: Some(callsign),
            ..
        } => Some(callsign.clone()),
        StructuredCallValue::Token { .. } | StructuredCallValue::Hash22 { .. } => None,
    }
}

fn message_related_calls(message: &StructuredMessage) -> Vec<String> {
    let mut related = BTreeSet::<String>::new();
    match message {
        StructuredMessage::Standard { first, second, .. } => {
            if let Some(call) = structured_call_station_name(first) {
                related.insert(call);
            }
            if let Some(call) = structured_call_station_name(second) {
                related.insert(call);
            }
        }
        StructuredMessage::Nonstandard {
            hashed_call,
            plain_call,
            cq,
            ..
        } => {
            related.insert(plain_call.callsign.clone());
            if !*cq {
                if let Some(call) = &hashed_call.resolved_callsign {
                    related.insert(call.clone());
                }
            }
        }
        StructuredMessage::FreeText { .. } | StructuredMessage::Unsupported { .. } => {}
    }
    related.into_iter().collect()
}

fn bandmap_detail(message: &StructuredMessage) -> Option<String> {
    match message {
        StructuredMessage::Standard {
            first,
            second,
            acknowledge,
            info,
            ..
        } => {
            let mut parts = Vec::new();
            if let Some(token) = bandmap_token(first).or_else(|| bandmap_token(second)) {
                parts.push(token);
            }
            let info_text = structured_info_text(*acknowledge, info);
            if !info_text.is_empty() {
                parts.push(info_text);
            }
            (!parts.is_empty()).then(|| parts.join(" "))
        }
        StructuredMessage::Nonstandard { reply, cq, .. } => {
            let mut parts = Vec::new();
            if *cq {
                parts.push("CQ".to_string());
            }
            let reply_text = reply_word_text(*reply);
            if !reply_text.is_empty() {
                parts.push(reply_text.to_string());
            }
            (!parts.is_empty()).then(|| parts.join(" "))
        }
        StructuredMessage::FreeText { .. } | StructuredMessage::Unsupported { .. } => None,
    }
}

fn bandmap_token(field: &StructuredCallField) -> Option<String> {
    match &field.value {
        StructuredCallValue::Token { token } => token
            .split_whitespace()
            .next()
            .map(ToOwned::to_owned)
            .or_else(|| (!token.is_empty()).then(|| token.clone())),
        StructuredCallValue::StandardCall { .. } | StructuredCallValue::Hash22 { .. } => None,
    }
}

fn structured_call_text(field: &StructuredCallField) -> String {
    let base = match &field.value {
        StructuredCallValue::Token { token } => token.clone(),
        StructuredCallValue::StandardCall { callsign } => callsign.clone(),
        StructuredCallValue::Hash22 {
            resolved_callsign: Some(callsign),
            ..
        } => callsign.clone(),
        StructuredCallValue::Hash22 {
            resolved_callsign: None,
            ..
        } => "<...>".to_string(),
    };
    match field.modifier {
        Some(CallModifier::R) => format!("{base}/R"),
        Some(CallModifier::P) => format!("{base}/P"),
        None => base,
    }
}

fn hashed_call_text(field: &ft8_decoder::HashedCallField12) -> String {
    field
        .resolved_callsign
        .as_ref()
        .map(|callsign| format!("<{callsign}>"))
        .unwrap_or_else(|| "<...>".to_string())
}

fn structured_info_text(acknowledge: bool, info: &StructuredInfoField) -> String {
    match &info.value {
        StructuredInfoValue::Blank => {
            if acknowledge {
                "R".to_string()
            } else {
                String::new()
            }
        }
        StructuredInfoValue::Grid { locator } => {
            if acknowledge {
                format!("R {locator}")
            } else {
                locator.clone()
            }
        }
        StructuredInfoValue::SignalReport { db } => {
            let value = format!("{db:+03}");
            if acknowledge {
                format!("R{value}")
            } else {
                value
            }
        }
        StructuredInfoValue::Reply { word } => reply_word_text(*word).to_string(),
    }
}

fn reply_word_text(word: ft8_decoder::ReplyWord) -> &'static str {
    match word {
        ft8_decoder::ReplyWord::Blank => "",
        ft8_decoder::ReplyWord::Rrr => "RRR",
        ft8_decoder::ReplyWord::Rr73 => "RR73",
        ft8_decoder::ReplyWord::SeventyThree => "73",
    }
}

impl StationTracker {
    fn ingest_frame(&mut self, received_at: SystemTime, decodes: &[DecodedMessage]) {
        let mut grouped = BTreeMap::<String, Vec<&DecodedMessage>>::new();
        let mut passthrough = Vec::<&DecodedMessage>::new();
        for decode in decodes {
            self.observe_message_resolutions(&decode.message);
            if let Some(sender_call) = semantic_sender_call(&decode.message) {
                grouped.entry(sender_call).or_default().push(decode);
            } else {
                passthrough.push(decode);
            }
        }

        for decode in passthrough {
            self.ingest_decode(received_at, decode);
        }

        for sender_decodes in grouped.values() {
            let mut ordered = sender_decodes.clone();
            ordered.sort_by_key(|decode| {
                if message_ends_qso(&decode.message) {
                    1_u8
                } else {
                    0_u8
                }
            });
            for decode in ordered {
                self.ingest_decode(received_at, decode);
            }
        }
        self.prune(received_at);
    }

    fn ingest_decode(&mut self, received_at: SystemTime, decode: &DecodedMessage) {
        let Some(sender_call) = semantic_sender_call(&decode.message) else {
            return;
        };
        let first_peer = qso_peer_from_first_field(&decode.message, self);
        let existing_active = self
            .stations
            .get(&sender_call)
            .and_then(|state| state.active_qso.clone());
        let peer_before = existing_active.as_ref().map(|qso| qso.peer.clone());

        enum QsoTransition {
            None,
            End,
            Keep(PeerRef),
            Start(PeerRef),
            Replace { old: ActiveQso, new_peer: PeerRef },
        }

        let transition = if message_ends_qso(&decode.message) {
            QsoTransition::End
        } else if let Some(peer) = first_peer {
            match existing_active {
                Some(active) if self.same_peer_identity(&active.peer, &peer) => {
                    QsoTransition::Keep(self.prefer_peer_identity(active.peer, peer))
                }
                Some(active) => QsoTransition::Replace {
                    old: active,
                    new_peer: peer,
                },
                None => QsoTransition::Start(peer),
            }
        } else {
            QsoTransition::None
        };

        let entry = self
            .stations
            .entry(sender_call.clone())
            .or_insert_with(|| StationState {
                last_heard_at: received_at,
                active_qso: None,
                last_qso_ended_at: None,
                qso_history: Vec::new(),
            });
        entry.last_heard_at = received_at;

        match transition {
            QsoTransition::None => {}
            QsoTransition::End => {
                if let Some(active) = entry.active_qso.take() {
                    entry.qso_history.push(CompletedQso {
                        started_at: active.since,
                        ended_at: received_at,
                        peer: active.peer,
                    });
                    entry.last_qso_ended_at = Some(received_at);
                }
            }
            QsoTransition::Keep(peer) => {
                if let Some(active) = &mut entry.active_qso {
                    active.peer = peer;
                } else {
                    entry.active_qso = Some(ActiveQso {
                        since: received_at,
                        peer,
                    });
                }
            }
            QsoTransition::Start(peer) => {
                entry.active_qso = Some(ActiveQso {
                    since: received_at,
                    peer,
                });
            }
            QsoTransition::Replace { old, new_peer } => {
                entry.qso_history.push(CompletedQso {
                    started_at: old.since,
                    ended_at: received_at,
                    peer: old.peer,
                });
                entry.active_qso = Some(ActiveQso {
                    since: received_at,
                    peer: new_peer,
                });
            }
        }

        let peer_after = entry
            .active_qso
            .as_ref()
            .map(|active| active.peer.clone());
        let (kind, field1, field2, info) = decode_columns(decode);
        let display_peer = if message_is_cq(&decode.message) {
            None
        } else {
            peer_after.clone().or_else(|| peer_before.clone())
        };
        self.logs.push_back(LoggedDecode {
            received_at,
            sender_call,
            display_peer,
            peer_before,
            peer_after,
            related_calls: message_related_calls(&decode.message),
            snr_db: decode.snr_db,
            dt_seconds: decode.dt_seconds,
            freq_hz: decode.freq_hz,
            kind,
            field1,
            field2,
            info,
            text: decode.text.clone(),
        });
    }

    fn observe_message_resolutions(&mut self, message: &StructuredMessage) {
        match message {
            StructuredMessage::Standard { first, second, .. } => {
                self.observe_structured_call(first);
                self.observe_structured_call(second);
            }
            StructuredMessage::Nonstandard { hashed_call, .. } => {
                if let Some(callsign) = &hashed_call.resolved_callsign {
                    self.hash12_resolutions
                        .insert(hashed_call.raw, callsign.clone());
                }
            }
            StructuredMessage::FreeText { .. } | StructuredMessage::Unsupported { .. } => {}
        }
    }

    fn observe_structured_call(&mut self, field: &StructuredCallField) {
        if let StructuredCallValue::Hash22 {
            hash,
            resolved_callsign: Some(callsign),
        } = &field.value
        {
            self.hash22_resolutions.insert(*hash, callsign.clone());
        }
    }

    fn resolve_peer(&self, peer: PeerRef) -> PeerRef {
        match peer {
            PeerRef::Callsign(_) => peer,
            PeerRef::Hash12(raw) => self
                .hash12_resolutions
                .get(&raw)
                .cloned()
                .map(PeerRef::Callsign)
                .unwrap_or(PeerRef::Hash12(raw)),
            PeerRef::Hash22(raw) => self
                .hash22_resolutions
                .get(&raw)
                .cloned()
                .map(PeerRef::Callsign)
                .unwrap_or(PeerRef::Hash22(raw)),
        }
    }

    fn same_peer_identity(&self, left: &PeerRef, right: &PeerRef) -> bool {
        match (self.resolve_peer(left.clone()), self.resolve_peer(right.clone())) {
            (PeerRef::Callsign(left), PeerRef::Callsign(right)) => left == right,
            (PeerRef::Hash12(left), PeerRef::Hash12(right)) => left == right,
            (PeerRef::Hash22(left), PeerRef::Hash22(right)) => left == right,
            _ => false,
        }
    }

    fn prefer_peer_identity(&self, existing: PeerRef, observed: PeerRef) -> PeerRef {
        match (self.resolve_peer(existing.clone()), self.resolve_peer(observed.clone())) {
            (PeerRef::Callsign(_), PeerRef::Callsign(_)) => self.resolve_peer(observed),
            (PeerRef::Callsign(_), _) => self.resolve_peer(existing),
            (_, PeerRef::Callsign(_)) => self.resolve_peer(observed),
            _ => self.resolve_peer(observed),
        }
    }

    fn peer_display(&self, peer: &PeerRef) -> String {
        match self.resolve_peer(peer.clone()) {
            PeerRef::Callsign(call) => call,
            PeerRef::Hash12(raw) => format!("<h12:{raw:03X}>"),
            PeerRef::Hash22(raw) => format!("<h22:{raw:06X}>"),
        }
    }

    fn prune(&mut self, now: SystemTime) {
        while matches!(
            self.logs.front(),
            Some(entry) if now.duration_since(entry.received_at).unwrap_or_default() > STATION_RETENTION
        ) {
            self.logs.pop_front();
        }
        self.stations.retain(|_, state| {
            let keep = now.duration_since(state.last_heard_at).unwrap_or_default() <= STATION_RETENTION;
            if keep {
                state.qso_history.retain(|qso| {
                    now.duration_since(qso.ended_at).unwrap_or_default() <= STATION_RETENTION
                });
            }
            keep
        });
    }

    fn web_station_summaries(&self) -> Vec<WebStationSummary> {
        self.stations
            .iter()
            .map(|(callsign, state)| WebStationSummary {
                callsign: callsign.clone(),
                last_heard_at: format_time(state.last_heard_at),
                is_in_qso: state.active_qso.is_some(),
                in_qso_since: state.active_qso.as_ref().map(|qso| format_time(qso.since)),
                qso_with: state
                    .active_qso
                    .as_ref()
                    .map(|qso| self.peer_display(&qso.peer)),
                last_qso_ended_at: state.last_qso_ended_at.map(format_time),
                qso_history: state
                    .qso_history
                    .iter()
                    .map(|qso| WebCompletedQso {
                        started_at: format_time(qso.started_at),
                        ended_at: format_time(qso.ended_at),
                        peer: self.peer_display(&qso.peer),
                    })
                    .collect(),
            })
            .collect()
    }

    fn web_logs(&self) -> Vec<WebStationLog> {
        self.logs
            .iter()
            .map(|entry| WebStationLog {
                timestamp: format_time(entry.received_at),
                sender_call: entry.sender_call.clone(),
                display_peer: entry.display_peer.as_ref().map(|peer| self.peer_display(peer)),
                peer_before: entry.peer_before.as_ref().map(|peer| self.peer_display(peer)),
                peer_after: entry.peer_after.as_ref().map(|peer| self.peer_display(peer)),
                related_calls: entry.related_calls.clone(),
                snr_db: entry.snr_db,
                dt_seconds: entry.dt_seconds,
                freq_hz: entry.freq_hz,
                kind: entry.kind.clone(),
                field1: entry.field1.clone(),
                field2: entry.field2.clone(),
                info: entry.info.clone(),
                text: entry.text.clone(),
            })
            .collect()
    }
}

fn qso_peer_from_first_field(message: &StructuredMessage, tracker: &StationTracker) -> Option<PeerRef> {
    match message {
        StructuredMessage::Standard { first, .. } => match &first.value {
            StructuredCallValue::StandardCall { callsign } => Some(PeerRef::Callsign(callsign.clone())),
            StructuredCallValue::Hash22 { hash, .. } => Some(tracker.resolve_peer(PeerRef::Hash22(*hash))),
            StructuredCallValue::Token { .. } => None,
        },
        StructuredMessage::Nonstandard {
            hashed_call,
            plain_call,
            hashed_is_second,
            cq,
            ..
        } => {
            if *cq {
                None
            } else if *hashed_is_second {
                Some(PeerRef::Callsign(plain_call.callsign.clone()))
            } else {
                Some(tracker.resolve_peer(PeerRef::Hash12(hashed_call.raw)))
            }
        }
        StructuredMessage::FreeText { .. } | StructuredMessage::Unsupported { .. } => None,
    }
}

fn message_ends_qso(message: &StructuredMessage) -> bool {
    if message_is_cq(message) {
        return true;
    }
    match message {
        StructuredMessage::Standard { info, .. } => matches!(
            info.value,
            StructuredInfoValue::Reply {
                word: ft8_decoder::ReplyWord::Rr73 | ft8_decoder::ReplyWord::SeventyThree
            }
        ),
        StructuredMessage::Nonstandard { reply, .. } => matches!(
            reply,
            ft8_decoder::ReplyWord::Rr73 | ft8_decoder::ReplyWord::SeventyThree
        ),
        StructuredMessage::FreeText { .. } | StructuredMessage::Unsupported { .. } => false,
    }
}

fn message_is_cq(message: &StructuredMessage) -> bool {
    match message {
        StructuredMessage::Standard { first, .. } => matches!(
            &first.value,
            StructuredCallValue::Token { token } if token == "CQ" || token.starts_with("CQ ")
        ),
        StructuredMessage::Nonstandard { cq, .. } => *cq,
        StructuredMessage::FreeText { .. } | StructuredMessage::Unsupported { .. } => false,
    }
}

fn format_time(time: SystemTime) -> String {
    let utc: DateTime<Utc> = time.into();
    utc.format("%H:%M:%S").to_string()
}

fn resample_linear_f32(samples: &[f32], src_rate_hz: u32, dst_rate_hz: u32) -> Vec<f32> {
    if samples.is_empty() || src_rate_hz == dst_rate_hz {
        return samples.to_vec();
    }

    let output_len =
        ((samples.len() as u64 * dst_rate_hz as u64) + (src_rate_hz as u64 / 2)) / src_rate_hz as u64;
    let mut output = Vec::with_capacity(output_len as usize);
    let scale = src_rate_hz as f64 / dst_rate_hz as f64;
    for index in 0..output_len as usize {
        let position = index as f64 * scale;
        let left = position.floor() as usize;
        let right = (left + 1).min(samples.len().saturating_sub(1));
        let frac = (position - left as f64) as f32;
        output.push(samples[left] * (1.0 - frac) + samples[right] * frac);
    }
    output
}

fn should_refresh_waterfall(
    now: SystemTime,
    last_update: SystemTime,
    latest_sample_time: Option<SystemTime>,
    last_sample_time: SystemTime,
) -> bool {
    if now.duration_since(last_update).unwrap_or_default() < Duration::from_millis(WATERFALL_UPDATE_MS) {
        return false;
    }
    latest_sample_time.is_some_and(|latest| latest > last_sample_time)
}

fn compute_latest_waterfall_row(capture: &SampleStream, latest_sample_time: SystemTime) -> Result<Vec<u8>, AppError> {
    let sample_rate_hz = capture.config().sample_rate_hz;
    let start = latest_sample_time
        .checked_sub(samples_to_duration(sample_rate_hz, WATERFALL_SAMPLES))
        .ok_or(AppError::Clock)?;
    let samples = capture.extract_window(start, WATERFALL_SAMPLES)?;
    Ok(compute_waterfall_row(&samples, sample_rate_hz))
}

fn compute_waterfall_row(samples: &[i16], sample_rate_hz: u32) -> Vec<u8> {
    let mut planner = FftPlanner::<f32>::new();
    let fft = planner.plan_fft_forward(WATERFALL_SAMPLES);
    let mut buffer = vec![Complex32::new(0.0, 0.0); WATERFALL_SAMPLES];
    for (index, slot) in buffer.iter_mut().enumerate() {
        let window = 0.5 - 0.5 * ((2.0 * std::f32::consts::PI * index as f32) / WATERFALL_SAMPLES as f32).cos();
        let sample = samples.get(index).copied().unwrap_or_default() as f32 / i16::MAX as f32;
        *slot = Complex32::new(sample * window, 0.0);
    }
    fft.process(&mut buffer);

    let bin_hz = sample_rate_hz as f32 / WATERFALL_SAMPLES as f32;
    let bucket_hz = WATERFALL_MAX_HZ / WATERFALL_BUCKETS as f32;
    let mut bucket_db = vec![0.0f32; WATERFALL_BUCKETS];
    let mut min_db = f32::INFINITY;
    let mut max_db = f32::NEG_INFINITY;
    let mut sum_db = 0.0f32;
    let mut row = vec![0u8; WATERFALL_BUCKETS];
    for (bucket_index, db_slot) in bucket_db.iter_mut().enumerate() {
        let start_hz = bucket_index as f32 * bucket_hz;
        let end_hz = start_hz + bucket_hz;
        let start_bin = (start_hz / bin_hz).floor() as usize;
        let end_bin = ((end_hz / bin_hz).ceil() as usize).min(buffer.len() / 2);
        let mut peak = 1.0e-12f32;
        for bin in start_bin..end_bin.max(start_bin + 1) {
            peak = peak.max(buffer[bin].norm_sqr());
        }
        let db = 10.0 * peak.log10();
        *db_slot = db;
        min_db = min_db.min(db);
        max_db = max_db.max(db);
        sum_db += db;
    }

    let mean_db = sum_db / WATERFALL_BUCKETS as f32;
    let floor_db = min_db.max(mean_db - 10.0);
    let ceiling_db = max_db.min(mean_db + 26.0);
    let span_db = (ceiling_db - floor_db).max(8.0);
    for (bucket_index, cell) in row.iter_mut().enumerate() {
        let normalized = ((bucket_db[bucket_index] - floor_db) / span_db).clamp(0.0, 1.0);
        let curved = normalized.powf(0.72);
        *cell = (curved * 255.0).round() as u8;
    }
    row
}

fn push_waterfall_row(rows: &mut VecDeque<Vec<u8>>, row: Vec<u8>) {
    if rows.len() == WATERFALL_HISTORY_ROWS {
        rows.pop_back();
    }
    rows.push_front(row);
}

fn seeded_waterfall_rows() -> VecDeque<Vec<u8>> {
    let mut rows = VecDeque::with_capacity(WATERFALL_HISTORY_ROWS);
    for _ in 0..WATERFALL_HISTORY_ROWS {
        rows.push_back(vec![0u8; WATERFALL_BUCKETS]);
    }
    rows
}

fn update_bandmaps(store: &mut BandMapStore, slot_start: SystemTime, decodes: &[DecodedMessage]) {
    let slot_idx = slot_index(slot_start);
    prune_bandmap(&mut store.even, slot_idx);
    prune_bandmap(&mut store.odd, slot_idx);
    let map = if is_even_slot_family(slot_start) {
        &mut store.even
    } else {
        &mut store.odd
    };
    for decode in decodes {
        for (callsign, detail) in bandmap_calls_from_decode(decode) {
            map.insert(
                callsign.clone(),
                BandMapEntry {
                    callsign,
                    detail,
                    freq_hz: decode.freq_hz,
                    last_seen_slot_index: slot_idx,
                },
            );
        }
    }
}

fn prune_bandmap(map: &mut BTreeMap<String, BandMapEntry>, current_slot_index: u64) {
    map.retain(|_, entry| current_slot_index.saturating_sub(entry.last_seen_slot_index) < BANDMAP_MAX_AGE_SLOTS);
}

fn bandmap_calls_from_decode(decode: &DecodedMessage) -> Vec<(String, Option<String>)> {
    let detail = bandmap_detail(&decode.message);
    semantic_sender_call(&decode.message)
        .into_iter()
        .map(|call| (call, detail.clone()))
        .collect()
}

fn build_bandmap_grid(
    map: &BTreeMap<String, BandMapEntry>,
    current_slot_index: u64,
) -> Vec<Vec<Vec<WebBandMapCall>>> {
    let mut cells =
        vec![vec![Vec::<(f32, WebBandMapCall)>::new(); BANDMAP_COLUMNS]; BANDMAP_ROWS];
    for entry in map.values() {
        let age_slots = current_slot_index.saturating_sub(entry.last_seen_slot_index);
        if age_slots >= BANDMAP_MAX_AGE_SLOTS {
            continue;
        }
        if !(0.0..WATERFALL_MAX_HZ).contains(&entry.freq_hz) {
            continue;
        }
        let column = (entry.freq_hz / 200.0).floor() as usize;
        let row = ((entry.freq_hz % 200.0) / 50.0).floor() as usize;
        if row < BANDMAP_ROWS && column < BANDMAP_COLUMNS {
            cells[row][column].push((
                entry.freq_hz,
                WebBandMapCall {
                    callsign: entry.callsign.clone(),
                    detail: entry.detail.clone(),
                    age_slots,
                },
            ));
        }
    }

    cells
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|mut cell| {
                    cell.sort_by(|left, right| {
                        left.0
                            .total_cmp(&right.0)
                            .then_with(|| left.1.callsign.cmp(&right.1.callsign))
                    });
                    cell.into_iter().map(|(_, call)| call).collect()
                })
                .collect()
        })
        .collect()
}


#[cfg(test)]
mod tests {
    use super::*;

    fn standard_call(callsign: &str) -> StructuredCallField {
        StructuredCallField {
            raw: 0,
            value: StructuredCallValue::StandardCall {
                callsign: callsign.to_string(),
            },
            modifier: None,
        }
    }

    fn blank_info() -> StructuredInfoField {
        StructuredInfoField {
            raw: 0,
            value: StructuredInfoValue::Blank,
        }
    }

    fn token_call(token: &str) -> StructuredCallField {
        StructuredCallField {
            raw: 0,
            value: StructuredCallValue::Token {
                token: token.to_string(),
            },
            modifier: None,
        }
    }

    fn grid_info(locator: &str) -> StructuredInfoField {
        StructuredInfoField {
            raw: 0,
            value: StructuredInfoValue::Grid {
                locator: locator.to_string(),
            },
        }
    }

    fn directed_decode(from: &str, to: &str) -> DecodedMessage {
        let message = StructuredMessage::Standard {
            i3: 0,
            first: standard_call(to),
            second: standard_call(from),
            acknowledge: false,
            info: blank_info(),
        };
        DecodedMessage {
            utc: "00:00:00".to_string(),
            snr_db: -10,
            dt_seconds: 0.1,
            freq_hz: 1000.0,
            text: message.to_text(),
            candidate_score: 0.0,
            ldpc_iterations: 0,
            message,
        }
    }

    fn cq_decode(from: &str) -> DecodedMessage {
        let message = StructuredMessage::Standard {
            i3: 0,
            first: token_call("CQ"),
            second: standard_call(from),
            acknowledge: false,
            info: grid_info("FN20"),
        };
        DecodedMessage {
            utc: "00:00:00".to_string(),
            snr_db: -10,
            dt_seconds: 0.1,
            freq_hz: 1000.0,
            text: message.to_text(),
            candidate_score: 0.0,
            ldpc_iterations: 0,
            message,
        }
    }

    #[test]
    fn related_calls_ignore_previous_qso_peer() {
        let mut tracker = StationTracker::default();
        let now = UNIX_EPOCH + Duration::from_secs(1);

        tracker.ingest_decode(now, &directed_decode("B", "A"));
        tracker.ingest_decode(now + Duration::from_secs(15), &directed_decode("B", "C"));

        let logs = tracker.web_logs();
        assert_eq!(logs.len(), 2);
        assert_eq!(logs[0].related_calls, vec!["A".to_string(), "B".to_string()]);
        assert_eq!(logs[1].related_calls, vec!["B".to_string(), "C".to_string()]);
        assert_eq!(logs[1].peer_before.as_deref(), Some("A"));
        assert_eq!(logs[1].peer_after.as_deref(), Some("C"));
        assert!(!logs[1].related_calls.iter().any(|call| call == "A"));
    }

    #[test]
    fn cq_clears_display_peer_after_qso() {
        let mut tracker = StationTracker::default();
        let now = UNIX_EPOCH + Duration::from_secs(1);

        tracker.ingest_decode(now, &directed_decode("B", "A"));
        tracker.ingest_decode(now + Duration::from_secs(15), &cq_decode("B"));

        let logs = tracker.web_logs();
        assert_eq!(logs.len(), 2);
        assert_eq!(logs[1].peer_before.as_deref(), Some("A"));
        assert_eq!(logs[1].peer_after, None);
        assert_eq!(logs[1].display_peer, None);
    }
}
