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
const RAW_WINDOW_SECONDS: u64 = 17;
const PRE_ROLL_MS: i64 = 1_200;
const POST_ROLL_MS: i64 = 1_800;
const SLOT_OFFSET_MS: usize = 1_200;
const SLOT_SEARCH_OFFSETS_MS: [usize; 5] = [0, 600, 1_200, 1_800, 2_400];
const DECODE_PROGRESS_SECONDS: f32 = 8.0;

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
    capture_channel_rms_dbfs: Vec<f32>,
    capture_channel: usize,
    capture_recoveries: u64,
    status: String,
    last_slot_start: Option<SystemTime>,
    last_decodes: Vec<DecodedMessage>,
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
        capture_channel_rms_dbfs: vec![-120.0; capture.config().channels],
        capture_channel: 0,
        capture_recoveries: 0,
        status: "Capturing audio".to_string(),
        last_slot_start: None,
        last_decodes: Vec::new(),
    };

    let mut next_slot = next_slot_boundary(SystemTime::now());
    let mut last_rig_poll = UNIX_EPOCH;

    print!("\x1b[?25l");
    while !stop.load(Ordering::Relaxed) {
        let stats = capture.stats();
        display.capture_rms_dbfs = stats.last_chunk_rms_dbfs;
        display.capture_channel_rms_dbfs = stats.channel_rms_dbfs;
        display.capture_channel = stats.selected_channel;
        display.capture_recoveries = stats.recoveries;

        let now = SystemTime::now();
        if now.duration_since(last_rig_poll).unwrap_or_default() >= Duration::from_secs(2) {
            display.rig = read_rig_state().ok();
            last_rig_poll = now;
        }

        while let Some(latest_slot) = latest_decodable_slot_start(SystemTime::now()) {
            if next_slot > latest_slot {
                break;
            }

            let raw = match extract_raw_window(&capture, next_slot) {
                Ok(raw) => raw,
                Err(AppError::Audio(rigctl::audio::Error::WindowNotReady)) => {
                    display.status = format!("Waiting for slot {}", format_slot_time(next_slot));
                    break;
                }
                Err(error) => {
                    display.status = format!("Decode error for {}: {}", format_slot_time(next_slot), error);
                    display.last_slot_start = Some(next_slot);
                    display.last_decodes.clear();
                    next_slot += Duration::from_secs(SLOT_SECONDS);
                    continue;
                }
            };

            let raw_path = cli
                .save_raw_wav
                .clone()
                .unwrap_or_else(|| temp_path("ft8rx-raw.wav"));
            let slot_path = cli
                .save_wav
                .clone()
                .unwrap_or_else(|| temp_path("ft8rx-slot.wav"));
            let keep_raw = cli.save_raw_wav.is_some();
            let keep_slot = cli.save_wav.is_some();
            let sample_rate_hz = capture.config().sample_rate_hz;
            let slot_start = next_slot;

            let (tx, rx) = mpsc::channel();
            thread::spawn(move || {
                let result = decode_slot_from_raw_with_paths(
                    raw,
                    sample_rate_hz,
                    &raw_path,
                    keep_raw,
                    &slot_path,
                    keep_slot,
                    slot_start,
                );
                let _ = tx.send(result);
            });

            let decode_started = SystemTime::now();
            loop {
                display.status = format!(
                    "Decoding slot {} {}",
                    format_slot_time(next_slot),
                    decode_progress_bar(decode_started)
                );
                render(&display);

                match rx.recv_timeout(Duration::from_millis(120)) {
                    Ok(Ok(decodes)) => {
                        display.last_slot_start = Some(next_slot);
                        display.last_decodes = decodes;
                        display.status = format!("Decoded slot {}", format_slot_time(next_slot));
                        break;
                    }
                    Ok(Err(error)) => {
                        display.status = format!("Decode error for {}: {}", format_slot_time(next_slot), error);
                        display.last_slot_start = Some(next_slot);
                        display.last_decodes.clear();
                        break;
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        display.status =
                            format!("Decode error for {}: decoder worker exited", format_slot_time(next_slot));
                        display.last_slot_start = Some(next_slot);
                        display.last_decodes.clear();
                        break;
                    }
                }

                if stop.load(Ordering::Relaxed) {
                    break;
                }
            }

            next_slot += Duration::from_secs(SLOT_SECONDS);
        }

        if display.last_slot_start.is_none() {
            display.status = format!(
                "Waiting for slot {}",
                format_slot_time(next_slot_boundary(SystemTime::now()))
            );
        }
        render(&display);
        thread::sleep(Duration::from_millis(200));
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

    let ready_at = shift_time(target_slot, SLOT_SECONDS as i64 * 1_000 + POST_ROLL_MS)?;
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
        cli.save_raw_wav.as_deref(),
        cli.save_wav.as_deref(),
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

fn extract_raw_window(capture: &SampleStream, slot_start: SystemTime) -> Result<Vec<i16>, AppError> {
    let raw_start = shift_time(slot_start, -PRE_ROLL_MS)?;
    let raw_sample_count = (RAW_WINDOW_SECONDS as usize) * capture.config().sample_rate_hz as usize;
    Ok(capture.extract_window(raw_start, raw_sample_count)?)
}

fn decode_slot_from_capture(
    capture: &SampleStream,
    slot_start: SystemTime,
    save_raw_wav: Option<&Path>,
    save_slot_wav: Option<&Path>,
) -> Result<Vec<DecodedMessage>, AppError> {
    let raw = extract_raw_window(capture, slot_start)?;
    let raw_path = save_raw_wav
        .map(Path::to_path_buf)
        .unwrap_or_else(|| temp_path("ft8rx-raw.wav"));
    let slot_path = save_slot_wav
        .map(Path::to_path_buf)
        .unwrap_or_else(|| temp_path("ft8rx-slot.wav"));
    decode_slot_from_raw_with_paths(
        raw,
        capture.config().sample_rate_hz,
        &raw_path,
        save_raw_wav.is_some(),
        &slot_path,
        save_slot_wav.is_some(),
        slot_start,
    )
}

fn decode_slot_from_raw_with_paths(
    raw: Vec<i16>,
    sample_rate_hz: u32,
    raw_path: &Path,
    keep_raw: bool,
    slot_path: &Path,
    keep_slot: bool,
    slot_start: SystemTime,
) -> Result<Vec<DecodedMessage>, AppError> {
    write_mono_wav(raw_path, sample_rate_hz, &raw)?;
    let decodes = decode_slot_from_raw(&raw, sample_rate_hz, slot_path, slot_start)?;
    if !keep_raw {
        let _ = std::fs::remove_file(raw_path);
    }
    if !keep_slot {
        let _ = std::fs::remove_file(slot_path);
    }
    Ok(decodes)
}

fn decode_slot_from_raw(
    raw: &[i16],
    sample_rate_hz: u32,
    slot_path: &Path,
    slot_start: SystemTime,
) -> Result<Vec<DecodedMessage>, AppError> {
    let options = DecodeOptions {
        profile: DecodeProfile::Quick,
        min_freq_hz: 200.0,
        max_freq_hz: 3_500.0,
        ..DecodeOptions::default()
    };
    let slot_len = SLOT_SECONDS as usize * sample_rate_hz as usize;
    let mut best_decodes = Vec::new();
    let mut best_offset = SLOT_OFFSET_MS;
    let mut best_rms = f32::NEG_INFINITY;

    for offset_ms in SLOT_SEARCH_OFFSETS_MS {
        let start = offset_ms * sample_rate_hz as usize / 1_000;
        if start + slot_len > raw.len() {
            continue;
        }
        let slice = &raw[start..start + slot_len];
        write_mono_wav(slot_path, sample_rate_hz, slice)?;
        let rms = slice_rms_dbfs(slice);
        let report =
            decode_wav_file(slot_path, &options).map_err(|error| AppError::Decoder(error.to_string()))?;
        if report.decodes.len() > best_decodes.len()
            || (report.decodes.len() == best_decodes.len() && rms > best_rms)
        {
            best_decodes = report.decodes;
            best_offset = offset_ms;
            best_rms = rms;
        }
    }

    let start = best_offset * sample_rate_hz as usize / 1_000;
    let slice = &raw[start..start + slot_len];
    write_mono_wav(slot_path, sample_rate_hz, slice)?;

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

fn slice_rms_dbfs(samples: &[i16]) -> f32 {
    if samples.is_empty() {
        return -120.0;
    }
    let rms = (samples
        .iter()
        .map(|&sample| {
            let value = sample as f32 / i16::MAX as f32;
            value * value
        })
        .sum::<f32>()
        / samples.len() as f32)
        .sqrt();
    20.0 * rms.max(1e-9).log10()
}

fn decode_progress_bar(started_at: SystemTime) -> String {
    let elapsed = SystemTime::now()
        .duration_since(started_at)
        .unwrap_or_default()
        .as_secs_f32();
    let progress = (elapsed / DECODE_PROGRESS_SECONDS).clamp(0.0, 1.0);
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

    let mut output = String::new();
    let _ = writeln!(output, "\x1b[2J\x1b[HFT8RX    {}", now_local.format("%Y-%m-%d %H:%M:%S %Z"));
    let _ = writeln!(output, "Rig      {}  {}  {}", rig_frequency, rig_mode, rig_band);
    let _ = writeln!(output, "Audio    {} ({})", display.audio.name, display.audio.spec);
    let _ = writeln!(
        output,
        "Capture  last={:.1} dBFS recoveries={}",
        display.capture_rms_dbfs, display.capture_recoveries
    );
    let _ = writeln!(
        output,
        "Chan     selected={} left={:.1} dBFS right={:.1} dBFS",
        display.capture_channel, left, right
    );
    let _ = writeln!(output, "Status   {}", display.status);
    if let Some(slot_start) = display.last_slot_start {
        let _ = writeln!(output, "Slot     {}", format_slot_time(slot_start));
    } else {
        let _ = writeln!(
            output,
            "Slot     waiting for {}",
            format_slot_time(next_slot_boundary(SystemTime::now()))
        );
    }
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
    let since_epoch = now.duration_since(UNIX_EPOCH).unwrap_or_default();
    let next = ((since_epoch.as_secs() / SLOT_SECONDS) + 1) * SLOT_SECONDS;
    UNIX_EPOCH + Duration::from_secs(next)
}

fn latest_decodable_slot_start(now: SystemTime) -> Option<SystemTime> {
    let since_epoch = now.duration_since(UNIX_EPOCH).ok()?;
    let current_boundary_secs = (since_epoch.as_secs() / SLOT_SECONDS) * SLOT_SECONDS;
    let current_boundary = UNIX_EPOCH + Duration::from_secs(current_boundary_secs);
    let elapsed_since_boundary = now.duration_since(current_boundary).ok()?;
    let completed_boundary = if elapsed_since_boundary >= Duration::from_millis(POST_ROLL_MS as u64) {
        current_boundary
    } else {
        current_boundary.checked_sub(Duration::from_secs(SLOT_SECONDS))?
    };
    completed_boundary.checked_sub(Duration::from_secs(SLOT_SECONDS))
}

fn format_slot_time(time: SystemTime) -> String {
    let utc: DateTime<Utc> = time.into();
    utc.format("%H%M%S").to_string()
}

fn temp_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("{}-{}", std::process::id(), name))
}

fn shift_time(time: SystemTime, millis: i64) -> Result<SystemTime, AppError> {
    if millis >= 0 {
        time.checked_add(Duration::from_millis(millis as u64)).ok_or(AppError::Clock)
    } else {
        time.checked_sub(Duration::from_millis((-millis) as u64)).ok_or(AppError::Clock)
    }
}
