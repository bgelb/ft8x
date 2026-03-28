use alsa::device_name::HintIter;
use alsa::Direction;
use chrono::{DateTime, Local, Utc};
use clap::Parser;
use ft8_decoder::{DecodeOptions, DecodeProfile, DecodedMessage, decode_wav_file};
use hound::{SampleFormat, WavSpec, WavWriter};
use rigctl::{K3s, K3sConfig, RigState};
use std::fmt::Write as _;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const SLOT_SECONDS: u64 = 15;
const CAPTURE_RATE_HZ: u32 = 48_000;
const CAPTURE_CHANNELS: usize = 2;
const CAPTURE_FRAMES_PER_READ: usize = 1_024;
const RAW_WINDOW_SECONDS: u64 = 17;
const PRE_ROLL_MS: i64 = 1_200;
const POST_ROLL_MS: i64 = 1_800;
const SLOT_OFFSET_MS: usize = 1_200;
const SLOT_SEARCH_OFFSETS_MS: [usize; 5] = [0, 600, 1_200, 1_800, 2_400];
const RING_SECONDS: usize = 120;

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
    #[error("alsa error: {0}")]
    Alsa(#[from] alsa::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("wav error: {0}")]
    Wav(#[from] hound::Error),
    #[error("decoder error: {0}")]
    Decoder(String),
    #[error("audio capture did not initialize")]
    CaptureInitTimeout,
    #[error("audio capture failed: {0}")]
    CaptureInit(String),
    #[error("audio buffer not ready for requested slot")]
    SlotNotReady,
    #[error("system clock error")]
    Clock,
}

#[derive(Debug, Clone)]
struct AudioDeviceInfo {
    name: String,
    spec: String,
}

#[derive(Debug, Clone)]
struct CaptureStats {
    latest_sample_time: Option<SystemTime>,
    last_chunk_rms_dbfs: f32,
    left_rms_dbfs: f32,
    right_rms_dbfs: f32,
    selected_channel: usize,
    recoveries: u64,
}

#[derive(Debug, Clone)]
struct DisplayState {
    rig: Option<RigState>,
    audio_name: String,
    audio_spec: String,
    capture_rms_dbfs: f32,
    capture_left_rms_dbfs: f32,
    capture_right_rms_dbfs: f32,
    capture_channel: usize,
    capture_recoveries: u64,
    status: String,
    last_slot_start: Option<SystemTime>,
    last_decodes: Vec<DecodedMessage>,
}

#[derive(Debug)]
struct AudioRing {
    samples: Vec<i16>,
    write_pos: usize,
    len: usize,
    total_samples: u64,
    latest_sample_time: Option<SystemTime>,
    last_chunk_rms_dbfs: f32,
    left_rms_dbfs: f32,
    right_rms_dbfs: f32,
    selected_channel: usize,
    recoveries: u64,
}

impl AudioRing {
    fn new(capacity: usize) -> Self {
        Self {
            samples: vec![0; capacity],
            write_pos: 0,
            len: 0,
            total_samples: 0,
            latest_sample_time: None,
            last_chunk_rms_dbfs: -120.0,
            left_rms_dbfs: -120.0,
            right_rms_dbfs: -120.0,
            selected_channel: 0,
            recoveries: 0,
        }
    }

    fn push_mono_samples(
        &mut self,
        mono: &[i16],
        chunk_end_time: SystemTime,
        left_rms_dbfs: f32,
        right_rms_dbfs: f32,
        selected_channel: usize,
    ) {
        if mono.is_empty() {
            return;
        }
        if mono.len() >= self.samples.len() {
            let start = mono.len() - self.samples.len();
            self.samples.copy_from_slice(&mono[start..]);
            self.write_pos = 0;
            self.len = self.samples.len();
            self.total_samples += mono.len() as u64;
            self.latest_sample_time = Some(chunk_end_time);
            self.last_chunk_rms_dbfs = slice_rms_dbfs(mono);
            self.left_rms_dbfs = left_rms_dbfs;
            self.right_rms_dbfs = right_rms_dbfs;
            self.selected_channel = selected_channel;
            return;
        }
        for &sample in mono {
            self.samples[self.write_pos] = sample;
            self.write_pos = (self.write_pos + 1) % self.samples.len();
        }
        self.len = (self.len + mono.len()).min(self.samples.len());
        self.total_samples += mono.len() as u64;
        self.latest_sample_time = Some(chunk_end_time);
        self.last_chunk_rms_dbfs = slice_rms_dbfs(mono);
        self.left_rms_dbfs = left_rms_dbfs;
        self.right_rms_dbfs = right_rms_dbfs;
        self.selected_channel = selected_channel;
    }

    fn stats(&self) -> CaptureStats {
        CaptureStats {
            latest_sample_time: self.latest_sample_time,
            last_chunk_rms_dbfs: self.last_chunk_rms_dbfs,
            left_rms_dbfs: self.left_rms_dbfs,
            right_rms_dbfs: self.right_rms_dbfs,
            selected_channel: self.selected_channel,
            recoveries: self.recoveries,
        }
    }

    fn extract_window(&self, start_time: SystemTime, sample_count: usize) -> Result<Vec<i16>, AppError> {
        let latest_time = self.latest_sample_time.ok_or(AppError::SlotNotReady)?;
        let end_time = start_time + samples_to_duration(sample_count as u64);
        if latest_time < end_time {
            return Err(AppError::SlotNotReady);
        }

        let samples_after_window =
            duration_to_samples(latest_time.duration_since(end_time).map_err(|_| AppError::Clock)?);
        let end_index = self.total_samples.checked_sub(samples_after_window).ok_or(AppError::SlotNotReady)?;
        let start_index = end_index.checked_sub(sample_count as u64).ok_or(AppError::SlotNotReady)?;
        let earliest_index = self.total_samples.saturating_sub(self.len as u64);
        if start_index < earliest_index {
            return Err(AppError::SlotNotReady);
        }

        let mut out = Vec::with_capacity(sample_count);
        for absolute_index in start_index..end_index {
            let offset_from_oldest = (absolute_index - earliest_index) as usize;
            let physical_index = if self.len == self.samples.len() {
                (self.write_pos + offset_from_oldest) % self.samples.len()
            } else {
                offset_from_oldest
            };
            out.push(self.samples[physical_index]);
        }
        Ok(out)
    }
}

struct AudioCapture {
    ring: Arc<Mutex<AudioRing>>,
    stop: Arc<AtomicBool>,
    child: Option<Child>,
    join: Option<thread::JoinHandle<()>>,
}

impl AudioCapture {
    fn start(spec: &str) -> Result<Self, AppError> {
        let ring = Arc::new(Mutex::new(AudioRing::new(RING_SECONDS * CAPTURE_RATE_HZ as usize)));
        let stop = Arc::new(AtomicBool::new(false));
        let (tx, rx) = mpsc::channel();
        let thread_ring = Arc::clone(&ring);
        let thread_stop = Arc::clone(&stop);
        let mut child = Command::new("arecord")
            .arg("-D")
            .arg(spec)
            .arg("-q")
            .arg("-t")
            .arg("raw")
            .arg("-f")
            .arg("S16_LE")
            .arg("-r")
            .arg(CAPTURE_RATE_HZ.to_string())
            .arg("-c")
            .arg(CAPTURE_CHANNELS.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| AppError::CaptureInit("arecord stdout unavailable".to_string()))?;
        let join = thread::spawn(move || {
            let result = run_capture_loop(stdout, thread_ring, thread_stop, tx);
            if let Err(error) = result {
                eprintln!("capture thread failed: {error}");
            }
        });

        match rx.recv_timeout(Duration::from_secs(5)) {
            Ok(Ok(())) => Ok(Self {
                ring,
                stop,
                child: Some(child),
                join: Some(join),
            }),
            Ok(Err(error)) => Err(AppError::CaptureInit(error)),
            Err(_) => Err(AppError::CaptureInitTimeout),
        }
    }

    fn stats(&self) -> CaptureStats {
        self.ring.lock().expect("audio ring poisoned").stats()
    }

    fn extract_window(&self, start_time: SystemTime, sample_count: usize) -> Result<Vec<i16>, AppError> {
        self.ring.lock().expect("audio ring poisoned").extract_window(start_time, sample_count)
    }
}

impl Drop for AudioCapture {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
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
    let audio = detect_audio_device(cli.device.as_deref())?;
    let capture = AudioCapture::start(&audio.spec)?;
    let stop = Arc::new(AtomicBool::new(false));
    let signal = Arc::clone(&stop);
    ctrlc::set_handler(move || {
        signal.store(true, Ordering::Relaxed);
    })
    .map_err(std::io::Error::other)?;

    let mut display = DisplayState {
        rig: read_rig_state().ok(),
        audio_name: audio.name.clone(),
        audio_spec: audio.spec.clone(),
        capture_rms_dbfs: -120.0,
        capture_left_rms_dbfs: -120.0,
        capture_right_rms_dbfs: -120.0,
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
        display.capture_left_rms_dbfs = stats.left_rms_dbfs;
        display.capture_right_rms_dbfs = stats.right_rms_dbfs;
        display.capture_channel = stats.selected_channel;
        display.capture_recoveries = stats.recoveries;
        let now = SystemTime::now();
        let should_poll_rig = now.duration_since(last_rig_poll).unwrap_or_default() >= Duration::from_secs(2);
        if should_poll_rig {
            display.rig = read_rig_state().ok();
            last_rig_poll = now;
        }

        while let Some(latest_slot) = latest_decodable_slot_start(SystemTime::now()) {
            if next_slot > latest_slot {
                break;
            }

            display.status = format!("Decoding slot {}", format_slot_time(next_slot));
            render(&display);
            match decode_slot_from_capture(
                &capture,
                next_slot,
                cli.save_raw_wav.as_deref(),
                cli.save_wav.as_deref(),
            ) {
                Ok(decodes) => {
                    display.last_slot_start = Some(next_slot);
                    display.last_decodes = decodes;
                    display.status = format!("Decoded slot {}", format_slot_time(next_slot));
                }
                Err(AppError::SlotNotReady) => {
                    display.status = format!("Waiting for slot {}", format_slot_time(next_slot));
                    break;
                }
                Err(error) => {
                    display.status = format!("Decode error for {}: {}", format_slot_time(next_slot), error);
                    display.last_slot_start = Some(next_slot);
                    display.last_decodes.clear();
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
    let audio = detect_audio_device(cli.device.as_deref())?;
    let capture = AudioCapture::start(&audio.spec)?;
    let target_slot = next_slot_boundary(SystemTime::now());

    println!(
        "audio=\"{}\" spec={}",
        audio.name, audio.spec,
    );
    println!("target_slot={}", format_slot_time(target_slot));

    let ready_at = shift_time(target_slot, SLOT_SECONDS as i64 * 1_000 + POST_ROLL_MS)?;
    while SystemTime::now() < ready_at {
        let stats = capture.stats();
        let latest = stats
            .latest_sample_time
            .map(format_slot_time)
            .unwrap_or_else(|| "------".to_string());
        println!(
            "waiting latest_sample={} ch={} rms={:.1}dBFS left={:.1} right={:.1} recoveries={}",
            latest,
            stats.selected_channel,
            stats.last_chunk_rms_dbfs,
            stats.left_rms_dbfs,
            stats.right_rms_dbfs,
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

fn detect_audio_device(override_spec: Option<&str>) -> Result<AudioDeviceInfo, AppError> {
    if let Some(spec) = override_spec {
        return Ok(AudioDeviceInfo {
            name: spec.to_string(),
            spec: spec.to_string(),
        });
    }

    let hints = HintIter::new_str(None, "pcm")?;
    let mut best: Option<AudioDeviceInfo> = None;
    for hint in hints {
        let Some(name) = hint.name else {
            continue;
        };
        let desc = hint.desc.unwrap_or_default();
        let is_capture = hint.direction.is_none() || hint.direction == Some(Direction::Capture);
        let looks_like_codec = name.contains("CARD=CODEC") || desc.contains("USB Audio CODEC");
        let looks_like_device0 = name.contains("DEV=0") || name == "default";
        let looks_like_pcm = name.starts_with("plughw:") || name.starts_with("hw:");
        if is_capture && looks_like_codec && looks_like_device0 && looks_like_pcm {
            let label = desc.lines().next().unwrap_or("USB Audio CODEC").trim().to_string();
            let info = AudioDeviceInfo { name: label, spec: name };
            if info.spec.starts_with("plughw:") {
                return Ok(info);
            }
            best = Some(info);
        }
    }

    best.ok_or_else(|| AppError::CaptureInit("USB Audio CODEC capture device not found".to_string()))
}

fn read_rig_state() -> Result<RigState, AppError> {
    let mut rig = K3s::connect(K3sConfig::default())?;
    Ok(rig.read_state()?)
}

fn run_capture_loop(
    mut stdout: ChildStdout,
    ring: Arc<Mutex<AudioRing>>,
    stop: Arc<AtomicBool>,
    init: mpsc::Sender<Result<(), String>>,
) -> Result<(), AppError> {
    let _ = init.send(Ok(()));

    let mut bytes = vec![0u8; CAPTURE_FRAMES_PER_READ * CAPTURE_CHANNELS * std::mem::size_of::<i16>()];
    while !stop.load(Ordering::Relaxed) {
        if let Err(error) = stdout.read_exact(&mut bytes) {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            let message = format!("audio stream read failed: {error}");
            let _ = init.send(Err(message.clone()));
            return Err(AppError::CaptureInit(message));
        }
        let interleaved: Vec<i16> = bytes
            .chunks_exact(2)
            .map(|pair| i16::from_le_bytes([pair[0], pair[1]]))
            .collect();
        let left: Vec<i16> = interleaved
            .chunks_exact(CAPTURE_CHANNELS)
            .map(|frame| frame[0])
            .collect();
        let right: Vec<i16> = interleaved
            .chunks_exact(CAPTURE_CHANNELS)
            .map(|frame| frame[1])
            .collect();
        let left_rms_dbfs = slice_rms_dbfs(&left);
        let right_rms_dbfs = slice_rms_dbfs(&right);
        let (mono, selected_channel) = if right_rms_dbfs > left_rms_dbfs {
            (right, 1)
        } else {
            (left, 0)
        };
        ring.lock()
            .expect("audio ring poisoned")
            .push_mono_samples(
                &mono,
                SystemTime::now(),
                left_rms_dbfs,
                right_rms_dbfs,
                selected_channel,
            );
    }
    Ok(())
}

fn decode_slot_from_capture(
    capture: &AudioCapture,
    slot_start: SystemTime,
    save_raw_wav: Option<&Path>,
    save_slot_wav: Option<&Path>,
) -> Result<Vec<DecodedMessage>, AppError> {
    let raw_start = shift_time(slot_start, -PRE_ROLL_MS)?;
    let raw_sample_count = (RAW_WINDOW_SECONDS as usize) * CAPTURE_RATE_HZ as usize;
    let raw = capture.extract_window(raw_start, raw_sample_count)?;

    let raw_path = save_raw_wav
        .map(Path::to_path_buf)
        .unwrap_or_else(|| temp_path("ft8rx-raw.wav"));
    let slot_path = save_slot_wav
        .map(Path::to_path_buf)
        .unwrap_or_else(|| temp_path("ft8rx-slot.wav"));
    write_mono_wav(&raw_path, &raw)?;
    let decodes = decode_slot_from_raw(&raw, &slot_path, slot_start)?;
    if save_raw_wav.is_none() {
        let _ = std::fs::remove_file(&raw_path);
    }
    if save_slot_wav.is_none() {
        let _ = std::fs::remove_file(&slot_path);
    }
    Ok(decodes)
}

fn decode_slot_from_raw(
    raw: &[i16],
    slot_path: &Path,
    slot_start: SystemTime,
) -> Result<Vec<DecodedMessage>, AppError> {
    let options = DecodeOptions {
        profile: DecodeProfile::Quick,
        min_freq_hz: 200.0,
        max_freq_hz: 3_500.0,
        ..DecodeOptions::default()
    };
    let slot_len = SLOT_SECONDS as usize * CAPTURE_RATE_HZ as usize;
    let mut best_decodes = Vec::new();
    let mut best_offset = SLOT_OFFSET_MS;
    let mut best_rms = f32::NEG_INFINITY;

    for offset_ms in SLOT_SEARCH_OFFSETS_MS {
        let start = offset_ms * CAPTURE_RATE_HZ as usize / 1_000;
        if start + slot_len > raw.len() {
            continue;
        }
        let slice = &raw[start..start + slot_len];
        write_mono_wav(slot_path, slice)?;
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

    let start = best_offset * CAPTURE_RATE_HZ as usize / 1_000;
    let slice = &raw[start..start + slot_len];
    write_mono_wav(slot_path, slice)?;

    let slot_label = format_slot_time(slot_start);
    for decode in &mut best_decodes {
        decode.utc = slot_label.clone();
    }
    Ok(best_decodes)
}

fn write_mono_wav(path: &Path, samples: &[i16]) -> Result<(), AppError> {
    let spec = WavSpec {
        channels: 1,
        sample_rate: CAPTURE_RATE_HZ,
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

    let mut output = String::new();
    let _ = writeln!(output, "\x1b[2J\x1b[HFT8RX    {}", now_local.format("%Y-%m-%d %H:%M:%S %Z"));
    let _ = writeln!(output, "Rig      {}  {}  {}", rig_frequency, rig_mode, rig_band);
    let _ = writeln!(output, "Audio    {} ({})", display.audio_name, display.audio_spec);
    let _ = writeln!(
        output,
        "Capture  last={:.1} dBFS recoveries={}",
        display.capture_rms_dbfs, display.capture_recoveries
    );
    let _ = writeln!(
        output,
        "Chan     selected={} left={:.1} dBFS right={:.1} dBFS",
        display.capture_channel, display.capture_left_rms_dbfs, display.capture_right_rms_dbfs
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

fn duration_to_samples(duration: Duration) -> u64 {
    ((duration.as_nanos() * CAPTURE_RATE_HZ as u128 + 500_000_000) / 1_000_000_000) as u64
}

fn samples_to_duration(samples: u64) -> Duration {
    Duration::from_nanos(((samples as u128) * 1_000_000_000 / CAPTURE_RATE_HZ as u128) as u64)
}

fn shift_time(time: SystemTime, millis: i64) -> Result<SystemTime, AppError> {
    if millis >= 0 {
        time.checked_add(Duration::from_millis(millis as u64)).ok_or(AppError::Clock)
    } else {
        time.checked_sub(Duration::from_millis((-millis) as u64)).ok_or(AppError::Clock)
    }
}
