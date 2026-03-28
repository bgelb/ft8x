use chrono::{DateTime, Local, Utc};
use clap::Parser;
use ft8_decoder::{
    AudioBuffer, DecodeOptions, DecodeProfile, DecodeStage, DecodedMessage, DecoderSession,
    StageDecodeReport,
};
use hound::{SampleFormat, WavSpec, WavWriter};
use rigctl::audio::{AudioDevice, AudioStreamConfig, SampleStream};
use rigctl::{K3s, K3sConfig, RigState, detect_k3s_audio_device};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const SLOT_SECONDS: u64 = 15;
const DECODER_SAMPLE_RATE_HZ: u32 = 12_000;

#[derive(Debug, Parser)]
#[command(name = "ft8rx")]
struct Cli {
    #[arg(long)]
    oneshot: bool,
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
    rig: Option<RigState>,
    audio: AudioDevice,
    capture_rms_dbfs: f32,
    capture_latest_sample_time: Option<SystemTime>,
    capture_channel_rms_dbfs: Vec<f32>,
    capture_channel: usize,
    capture_recoveries: u64,
    decode_status: String,
    last_decode_wall_ms: Option<u128>,
    dropped_slots: u64,
    last_slot_start: Option<SystemTime>,
    early41_decodes: Vec<DecodedMessage>,
    early47_decodes: Vec<DecodedMessage>,
    full_decodes: Vec<DecodedMessage>,
}

#[derive(Debug, Clone)]
struct CompositeDecodeRow {
    display: DecodedMessage,
    seen: &'static str,
    early41: Option<DecodedMessage>,
    early47: Option<DecodedMessage>,
    full: Option<DecodedMessage>,
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

fn main() -> Result<(), AppError> {
    let cli = Cli::parse();
    if cli.oneshot {
        run_oneshot(cli)
    } else {
        run_continuous(cli)
    }
}

fn run_continuous(cli: Cli) -> Result<(), AppError> {
    let audio = detect_k3s_audio_device(cli.device.as_deref())?;
    let capture = SampleStream::start(audio.clone(), AudioStreamConfig::default())?;
    let (job_tx, job_rx) = mpsc::sync_channel::<DecodeJob>(1);
    let (event_tx, event_rx) = mpsc::channel::<DecodeEvent>();
    thread::spawn(move || {
        let mut session_slot: Option<SystemTime> = None;
        let mut session = DecoderSession::new();
        while let Ok(job) = job_rx.recv() {
            if session_slot != Some(job.slot_start) {
                session.reset();
                session_slot = Some(job.slot_start);
            }
            let result = decode_stage_from_samples(
                &mut session,
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
        rig: read_rig_state().ok(),
        audio,
        capture_rms_dbfs: -120.0,
        capture_latest_sample_time: None,
        capture_channel_rms_dbfs: vec![-120.0; capture.config().channels],
        capture_channel: 0,
        capture_recoveries: 0,
        decode_status: "Idle".to_string(),
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
            display.rig = read_rig_state().ok();
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
                                    display.early41_decodes = update.report.decodes.clone();
                                }
                                DecodeStage::Early47 => {
                                    display.early47_decodes = update.report.decodes.clone();
                                }
                                DecodeStage::Full => {
                                    display.last_slot_start = Some(slot_start);
                                    display.last_decode_wall_ms = Some(wall_ms);
                                    display.full_decodes = update.report.decodes.clone();
                                }
                            }
                        }
                        Err(error) => {
                            display.decode_status =
                                format!("Last {} {} failed: {}", stage.as_str(), format_slot_time(slot_start), error);
                            if stage == DecodeStage::Full {
                                display.last_slot_start = Some(slot_start);
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

        display.decode_status = format_status(
            active_decode,
            next_slot,
            display.dropped_slots,
            capture.config().sample_rate_hz,
        );
        render(&display);
        thread::sleep(Duration::from_millis(50));
    }

    print!("\x1b[?25h");
    Ok(())
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

fn read_rig_state() -> Result<RigState, AppError> {
    let mut rig = K3s::connect(K3sConfig::default())?;
    Ok(rig.read_state()?)
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
    let mut update = session
        .decode_stage(&audio, &options, stage)
        .map_err(|error| AppError::Decoder(error.to_string()))?;
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
    let _ = writeln!(output, "Rig      {}  {}  {}", rig_frequency, rig_mode, rig_band);
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
        "dT stats avg={:+.2}s stddev={:.2}s count={}",
        dt_stats.0,
        dt_stats.1,
        display_decodes.len()
    );
    let _ = writeln!(output);
    let _ = writeln!(
        output,
        "Seen    UTC    SNR   dT(s)   Freq(Hz)  Early Margin          Mid Margin            Late Margin           Message"
    );
    let _ = writeln!(
        output,
        "------  -----  ----  ------  --------  --------------------  --------------------  --------------------  -------"
    );
    if composite_rows.is_empty() {
        let _ = writeln!(output, "no decodes yet");
    } else {
        for row in &composite_rows {
            let _ = writeln!(
                output,
                "{:<6}  {:<5}  {:>4}  {:+6.2}  {:>8.0}  {:<20}  {:<20}  {:<20}  {}",
                row.seen,
                row.display.utc,
                row.display.snr_db,
                row.display.dt_seconds,
                row.display.freq_hz,
                format_stage_metric(row.early41.as_ref(), None),
                format_stage_metric(row.early47.as_ref(), row.early41.as_ref()),
                format_stage_metric(row.full.as_ref(), row.early47.as_ref()),
                row.display.text
            );
        }
    }
    print!("{output}");
}

fn summarize_dt(decodes: &[DecodedMessage]) -> (f32, f32) {
    if decodes.is_empty() {
        return (0.0, 0.0);
    }
    let mean = decodes.iter().map(|decode| decode.dt_seconds).sum::<f32>() / decodes.len() as f32;
    let variance = decodes
        .iter()
        .map(|decode| {
            let delta = decode.dt_seconds - mean;
            delta * delta
        })
        .sum::<f32>()
        / decodes.len() as f32;
    (mean, variance.sqrt())
}

fn next_slot_boundary(now: SystemTime) -> SystemTime {
    current_slot_boundary(now) + Duration::from_secs(SLOT_SECONDS)
}

fn current_slot_boundary(now: SystemTime) -> SystemTime {
    let since_epoch = now.duration_since(UNIX_EPOCH).unwrap_or_default();
    let current = (since_epoch.as_secs() / SLOT_SECONDS) * SLOT_SECONDS;
    UNIX_EPOCH + Duration::from_secs(current)
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
                early41: Some(decode.clone()),
                early47: None,
                full: None,
            },
        );
    }
    for decode in &display.early47_decodes {
        let entry = rows.entry(decode.text.clone()).or_insert_with(|| CompositeDecodeRow {
            display: decode.clone(),
            seen: "mid",
            early41: None,
            early47: None,
            full: None,
        });
        entry.display = decode.clone();
        entry.early47 = Some(decode.clone());
    }
    for decode in &display.full_decodes {
        let entry = rows.entry(decode.text.clone()).or_insert_with(|| CompositeDecodeRow {
            display: decode.clone(),
            seen: "late",
            early41: None,
            early47: None,
            full: None,
        });
        entry.display = decode.clone();
        entry.full = Some(decode.clone());
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

fn format_stage_metric(
    decode: Option<&DecodedMessage>,
    prior_decode: Option<&DecodedMessage>,
) -> String {
    match decode {
        Some(decode) => {
            let margin = format!("{:.2}", decode.mean_abs_llr);
            let colored_margin = match prior_decode {
                Some(prior) if (prior.mean_abs_llr - decode.mean_abs_llr).abs() > f32::EPSILON =>
                {
                    colorize_metric(
                        &margin,
                        decode.mean_abs_llr.partial_cmp(&prior.mean_abs_llr),
                    )
                }
                _ => margin,
            };
            format!("{colored_margin} i{}", decode.ldpc_iterations)
        }
        None => "-".to_string(),
    }
}

fn colorize_metric(metric: &str, ordering: Option<std::cmp::Ordering>) -> String {
    let color = match ordering {
        Some(std::cmp::Ordering::Greater) => "\x1b[32m",
        Some(std::cmp::Ordering::Less) => "\x1b[31m",
        Some(std::cmp::Ordering::Equal) | None => "\x1b[33m",
    };
    format!("{color}{metric}\x1b[0m")
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
