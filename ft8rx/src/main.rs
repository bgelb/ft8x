use chrono::{DateTime, Local, Utc};
use clap::Parser;
use ft8_decoder::{DecodeOptions, DecodeProfile, DecodedMessage, decode_wav_file};
use hound::{SampleFormat, WavSpec, WavWriter};
use rigctl::audio::{AudioDevice, AudioStreamConfig, SampleStream};
use rigctl::{K3s, K3sConfig, RigState, detect_k3s_audio_device};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const SLOT_SECONDS: u64 = 15;

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
    last_decodes: Vec<DecodedMessage>,
}

#[derive(Debug)]
struct DecodeJob {
    slot_start: SystemTime,
    capture_end: SystemTime,
    samples: Vec<i16>,
    sample_rate_hz: u32,
    raw_path: PathBuf,
    keep_raw: bool,
}

#[derive(Debug)]
enum DecodeEvent {
    Finished {
        slot_start: SystemTime,
        wall_ms: u128,
        result: Result<Vec<DecodedMessage>, AppError>,
    },
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
    let (job_tx, job_rx) = mpsc::channel::<DecodeJob>();
    let (event_tx, event_rx) = mpsc::channel::<DecodeEvent>();
    thread::spawn(move || {
        while let Ok(job) = job_rx.recv() {
            let result = decode_slot_from_samples_with_raw_path(
                &job.samples,
                job.sample_rate_hz,
                &job.raw_path,
                job.keep_raw,
                job.slot_start,
            );
            let wall_ms = SystemTime::now()
                .duration_since(job.capture_end)
                .unwrap_or_default()
                .as_millis();
            let _ = event_tx.send(DecodeEvent::Finished {
                slot_start: job.slot_start,
                wall_ms,
                result,
            });
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
        last_decodes: Vec::new(),
    };

    let mut next_slot = next_slot_boundary(SystemTime::now());
    let mut last_rig_poll = UNIX_EPOCH;
    let mut active_decode_slot: Option<SystemTime> = None;

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
                    wall_ms,
                    result,
                } => {
                    active_decode_slot = None;
                    display.last_slot_start = Some(slot_start);
                    display.last_decode_wall_ms = Some(wall_ms);
                    match result {
                        Ok(decodes) => {
                            display.last_decodes = decodes;
                        }
                        Err(error) => {
                            display.decode_status =
                                format!("Last decode {} failed: {}", format_slot_time(slot_start), error);
                            display.last_decodes.clear();
                        }
                    }
                }
            }
        }

        while let Some(latest_slot) = latest_ready_capture_slot(display.capture_latest_sample_time) {
            if next_slot > latest_slot {
                break;
            }

            let slot_start = next_slot;
            let capture_end = slot_capture_end(slot_start, capture.config().sample_rate_hz)?;
            let samples = match extract_slot_capture(&capture, slot_start) {
                Ok(raw) => raw,
                Err(AppError::Audio(rigctl::audio::Error::WindowNotReady)) => {
                    break;
                }
                Err(error) => {
                    display.decode_status =
                        format!("Capture error for {}: {}", format_slot_time(slot_start), error);
                    display.last_slot_start = Some(slot_start);
                    display.last_decodes.clear();
                    next_slot += Duration::from_secs(SLOT_SECONDS);
                    continue;
                }
            };

            let raw_path = cli
                .save_raw_wav
                .clone()
                .unwrap_or_else(|| temp_path("ft8rx-raw.wav"));
            if active_decode_slot.is_some() {
                display.dropped_slots += 1;
                display.decode_status = format!(
                    "capture=idle decode=busy dropping={} drops={} next={}",
                    format_slot_time(slot_start),
                    display.dropped_slots,
                    format_slot_time(next_slot + Duration::from_secs(SLOT_SECONDS))
                );
                display.last_slot_start = Some(slot_start);
                display.last_decodes.clear();
                next_slot += Duration::from_secs(SLOT_SECONDS);
                continue;
            }
            active_decode_slot = Some(slot_start);
            let _ = job_tx.send(DecodeJob {
                slot_start,
                capture_end,
                samples,
                sample_rate_hz: capture.config().sample_rate_hz,
                raw_path,
                keep_raw: cli.save_raw_wav.is_some(),
            });

            next_slot += Duration::from_secs(SLOT_SECONDS);
        }

        display.decode_status = format_status(
            active_decode_slot,
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

    let decodes = decode_slot_from_capture(
        &capture,
        target_slot,
        cli.save_raw_wav.as_deref().or(cli.save_wav.as_deref()),
    )?;
    println!("decodes={}", decodes.len());
    for decode in decodes {
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

fn decode_slot_from_capture(
    capture: &SampleStream,
    slot_start: SystemTime,
    save_raw_wav: Option<&Path>,
) -> Result<Vec<DecodedMessage>, AppError> {
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
) -> Result<Vec<DecodedMessage>, AppError> {
    write_mono_wav(raw_path, sample_rate_hz, samples)?;
    let decodes = decode_slot_from_wav(raw_path, slot_start)?;
    if !keep_raw {
        let _ = std::fs::remove_file(raw_path);
    }
    Ok(decodes)
}

fn decode_slot_from_wav(path: &Path, slot_start: SystemTime) -> Result<Vec<DecodedMessage>, AppError> {
    let options = DecodeOptions {
        profile: DecodeProfile::Medium,
        min_freq_hz: 200.0,
        max_freq_hz: 3_500.0,
        ..DecodeOptions::default()
    };
    let mut best_decodes =
        decode_wav_file(path, &options).map_err(|error| AppError::Decoder(error.to_string()))?.decodes;
    let slot_label = format_slot_time(slot_start);
    for decode in &mut best_decodes {
        decode.utc = slot_label.clone();
    }
    Ok(best_decodes)
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
    let dt_stats = summarize_dt(&display.last_decodes);
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
        display.last_decodes.len()
    );
    let _ = writeln!(output);
    let _ = writeln!(output, "UTC    SNR   dT(s)   Freq(Hz)  Message");
    let _ = writeln!(output, "-----  ----  ------  --------  -------");
    if display.last_decodes.is_empty() {
        let _ = writeln!(output, "no decodes yet");
    } else {
        for decode in &display.last_decodes {
            let _ = writeln!(
                output,
                "{:<5}  {:>4}  {:+6.2}  {:>8.0}  {}",
                decode.utc, decode.snr_db, decode.dt_seconds, decode.freq_hz, decode.text
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

fn latest_ready_capture_slot(latest_sample_time: Option<SystemTime>) -> Option<SystemTime> {
    let latest_sample_time = latest_sample_time?;
    current_slot_boundary(latest_sample_time).checked_sub(Duration::from_secs(SLOT_SECONDS))
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

fn capture_window_duration(sample_rate_hz: u32) -> Duration {
    Duration::from_secs_f64(full_slot_sample_count(sample_rate_hz) as f64 / sample_rate_hz as f64)
}

fn slot_capture_end(slot_start: SystemTime, sample_rate_hz: u32) -> Result<SystemTime, AppError> {
    slot_start
        .checked_add(capture_window_duration(sample_rate_hz))
        .ok_or(AppError::Clock)
}

fn format_status(
    active_decode_slot: Option<SystemTime>,
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
    match active_decode_slot {
        Some(slot) => format!(
            "capture={} decode=active slot={} drops={} next={}",
            capture_state,
            format_slot_time(slot),
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
