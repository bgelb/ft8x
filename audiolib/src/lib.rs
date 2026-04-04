use std::f32::consts::TAU;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, SystemTime};

const DEFAULT_CAPTURE_CHANNELS: usize = 2;
const DEFAULT_FRAMES_PER_READ: usize = 1_024;
const DEFAULT_RING_SECONDS: usize = 120;
const DEFAULT_PLAYBACK_FRAMES_PER_WRITE: usize = 1_024;
const DEFAULT_PLAYBACK_BUFFER_FRAMES: usize = DEFAULT_PLAYBACK_FRAMES_PER_WRITE * 4;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[cfg(target_os = "linux")]
    #[error("alsa error: {0}")]
    Alsa(#[from] alsa::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("audio capture did not initialize")]
    CaptureInitTimeout,
    #[error("audio capture failed: {0}")]
    CaptureInit(String),
    #[error("audio buffer not ready for requested window")]
    WindowNotReady,
    #[error("system clock error")]
    Clock,
    #[error("unsupported platform")]
    UnsupportedPlatform,
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AudioDevice {
    pub name: String,
    pub spec: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AudioStreamConfig {
    pub sample_rate_hz: u32,
    pub channels: usize,
    pub frames_per_read: usize,
    pub ring_seconds: usize,
}

impl Default for AudioStreamConfig {
    fn default() -> Self {
        Self {
            sample_rate_hz: 48_000,
            channels: DEFAULT_CAPTURE_CHANNELS,
            frames_per_read: DEFAULT_FRAMES_PER_READ,
            ring_seconds: DEFAULT_RING_SECONDS,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CaptureStats {
    pub latest_sample_time: Option<SystemTime>,
    pub last_chunk_rms_dbfs: f32,
    pub channel_rms_dbfs: Vec<f32>,
    pub selected_channel: usize,
    pub recoveries: u64,
}

#[derive(Debug)]
struct AudioRing {
    samples: Vec<i16>,
    write_pos: usize,
    len: usize,
    total_samples: u64,
    latest_sample_time: Option<SystemTime>,
    last_chunk_rms_dbfs: f32,
    channel_rms_dbfs: Vec<f32>,
    selected_channel: usize,
    recoveries: u64,
}

impl AudioRing {
    fn new(capacity: usize, channel_count: usize) -> Self {
        Self {
            samples: vec![0; capacity],
            write_pos: 0,
            len: 0,
            total_samples: 0,
            latest_sample_time: None,
            last_chunk_rms_dbfs: -120.0,
            channel_rms_dbfs: vec![-120.0; channel_count],
            selected_channel: 0,
            recoveries: 0,
        }
    }

    fn push_mono_samples(
        &mut self,
        mono: &[i16],
        chunk_end_time: SystemTime,
        channel_rms_dbfs: Vec<f32>,
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
        } else {
            for &sample in mono {
                self.samples[self.write_pos] = sample;
                self.write_pos = (self.write_pos + 1) % self.samples.len();
            }
            self.len = (self.len + mono.len()).min(self.samples.len());
        }
        self.total_samples += mono.len() as u64;
        self.latest_sample_time = Some(chunk_end_time);
        self.last_chunk_rms_dbfs = slice_rms_dbfs(mono);
        self.channel_rms_dbfs = channel_rms_dbfs;
        self.selected_channel = selected_channel;
    }

    fn stats(&self) -> CaptureStats {
        CaptureStats {
            latest_sample_time: self.latest_sample_time,
            last_chunk_rms_dbfs: self.last_chunk_rms_dbfs,
            channel_rms_dbfs: self.channel_rms_dbfs.clone(),
            selected_channel: self.selected_channel,
            recoveries: self.recoveries,
        }
    }

    fn mark_recovery(&mut self) {
        self.recoveries += 1;
    }

    fn extract_window(&self, sample_rate_hz: u32, start_time: SystemTime, sample_count: usize) -> Result<Vec<i16>> {
        let latest_time = self.latest_sample_time.ok_or(Error::WindowNotReady)?;
        let end_time = start_time + samples_to_duration(sample_rate_hz, sample_count as u64);
        if latest_time < end_time {
            return Err(Error::WindowNotReady);
        }

        let samples_after_window =
            duration_to_samples(sample_rate_hz, latest_time.duration_since(end_time).map_err(|_| Error::Clock)?);
        let end_index = self.total_samples.checked_sub(samples_after_window).ok_or(Error::WindowNotReady)?;
        let start_index = end_index.checked_sub(sample_count as u64).ok_or(Error::WindowNotReady)?;
        let earliest_index = self.total_samples.saturating_sub(self.len as u64);
        if start_index < earliest_index {
            return Err(Error::WindowNotReady);
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

pub struct SampleStream {
    config: AudioStreamConfig,
    device: AudioDevice,
    ring: Arc<Mutex<AudioRing>>,
    stop: Arc<AtomicBool>,
    join: Option<thread::JoinHandle<()>>,
}

impl SampleStream {
    pub fn start(device: AudioDevice, config: AudioStreamConfig) -> Result<Self> {
        #[cfg(not(target_os = "linux"))]
        {
            let _ = device;
            let _ = config;
            return Err(Error::UnsupportedPlatform);
        }

        #[cfg(target_os = "linux")]
        {
            let ring = Arc::new(Mutex::new(AudioRing::new(
                config.ring_seconds * config.sample_rate_hz as usize,
                config.channels,
            )));
            let stop = Arc::new(AtomicBool::new(false));
            let (tx, rx) = mpsc::channel();
            let thread_ring = Arc::clone(&ring);
            let thread_stop = Arc::clone(&stop);
            let device_spec = device.spec.clone();
            let thread_config = config.clone();
            let join = thread::spawn(move || {
                let result = run_capture_loop(device_spec, thread_ring, thread_stop, thread_config, tx);
                if let Err(error) = result {
                    eprintln!("audio capture thread failed: {error}");
                }
            });

            match rx.recv_timeout(Duration::from_secs(5)) {
                Ok(Ok(())) => Ok(Self {
                    config,
                    device,
                    ring,
                    stop,
                    join: Some(join),
                }),
                Ok(Err(error)) => Err(Error::CaptureInit(error)),
                Err(_) => Err(Error::CaptureInitTimeout),
            }
        }
    }

    pub fn device(&self) -> &AudioDevice {
        &self.device
    }

    pub fn config(&self) -> &AudioStreamConfig {
        &self.config
    }

    pub fn stats(&self) -> CaptureStats {
        self.ring.lock().expect("audio ring poisoned").stats()
    }

    pub fn extract_window(&self, start_time: SystemTime, sample_count: usize) -> Result<Vec<i16>> {
        self.ring
            .lock()
            .expect("audio ring poisoned")
            .extract_window(self.config.sample_rate_hz, start_time, sample_count)
    }
}

impl Drop for SampleStream {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

#[cfg(target_os = "linux")]
pub fn list_input_devices() -> Result<Vec<AudioDevice>> {
    use alsa::Direction;
    use alsa::device_name::HintIter;

    let mut devices = Vec::new();
    for hint in HintIter::new_str(None, "pcm")? {
        let Some(spec) = hint.name else {
            continue;
        };
        let is_capture = hint.direction.is_none() || hint.direction == Some(Direction::Capture);
        if !is_capture {
            continue;
        }
        let description = hint.desc;
        let name = description
            .as_deref()
            .and_then(|desc| desc.lines().next())
            .unwrap_or(&spec)
            .trim()
            .to_string();
        devices.push(AudioDevice {
            name,
            spec,
            description,
        });
    }
    Ok(devices)
}

#[cfg(not(target_os = "linux"))]
pub fn list_input_devices() -> Result<Vec<AudioDevice>> {
    Err(Error::UnsupportedPlatform)
}

#[cfg(target_os = "linux")]
pub fn list_output_devices() -> Result<Vec<AudioDevice>> {
    use alsa::Direction;
    use alsa::device_name::HintIter;

    let mut devices = Vec::new();
    for hint in HintIter::new_str(None, "pcm")? {
        let Some(spec) = hint.name else {
            continue;
        };
        let is_playback = hint.direction.is_none() || hint.direction == Some(Direction::Playback);
        if !is_playback {
            continue;
        }
        let description = hint.desc;
        let name = description
            .as_deref()
            .and_then(|desc| desc.lines().next())
            .unwrap_or(&spec)
            .trim()
            .to_string();
        devices.push(AudioDevice {
            name,
            spec,
            description,
        });
    }
    Ok(devices)
}

#[cfg(not(target_os = "linux"))]
pub fn list_output_devices() -> Result<Vec<AudioDevice>> {
    Err(Error::UnsupportedPlatform)
}

#[cfg(target_os = "linux")]
pub fn play_tone(
    device: &AudioDevice,
    sample_rate_hz: u32,
    channels: usize,
    frequency_hz: f32,
    duration: Duration,
    amplitude: f32,
) -> Result<()> {
    let channel_count = channels.max(1);
    let frame_count =
        ((duration.as_secs_f64() * sample_rate_hz as f64).round() as usize).max(sample_rate_hz as usize / 10);
    let gain = amplitude.clamp(0.0, 1.0);
    let mut samples = Vec::with_capacity(frame_count);
    for index in 0..frame_count {
        let phase = TAU * frequency_hz * index as f32 / sample_rate_hz as f32;
        samples.push(phase.sin() * gain);
    }
    play_mono_samples(device, sample_rate_hz, channel_count, &samples)
}

#[cfg(not(target_os = "linux"))]
pub fn play_tone(
    _device: &AudioDevice,
    _sample_rate_hz: u32,
    _channels: usize,
    _frequency_hz: f32,
    _duration: Duration,
    _amplitude: f32,
) -> Result<()> {
    Err(Error::UnsupportedPlatform)
}

#[cfg(target_os = "linux")]
pub fn play_mono_samples(
    device: &AudioDevice,
    sample_rate_hz: u32,
    channels: usize,
    samples: &[f32],
) -> Result<()> {
    play_mono_samples_until(device, sample_rate_hz, channels, samples, None)
}

#[cfg(not(target_os = "linux"))]
pub fn play_mono_samples(
    _device: &AudioDevice,
    _sample_rate_hz: u32,
    _channels: usize,
    _samples: &[f32],
) -> Result<()> {
    Err(Error::UnsupportedPlatform)
}

#[cfg(target_os = "linux")]
pub fn play_mono_samples_until(
    device: &AudioDevice,
    sample_rate_hz: u32,
    channels: usize,
    samples: &[f32],
    cancel: Option<&AtomicBool>,
) -> Result<()> {
    let channel_count = channels.max(1);
    let interleaved = interleave_mono_samples_i16(samples, channel_count);
    play_interleaved_samples_i16_until(
        device,
        sample_rate_hz,
        channel_count,
        &interleaved,
        cancel,
    )
}

#[cfg(not(target_os = "linux"))]
pub fn play_mono_samples_until(
    _device: &AudioDevice,
    _sample_rate_hz: u32,
    _channels: usize,
    _samples: &[f32],
    _cancel: Option<&AtomicBool>,
) -> Result<()> {
    Err(Error::UnsupportedPlatform)
}

#[cfg(target_os = "linux")]
pub fn play_interleaved_samples_i16_until(
    device: &AudioDevice,
    sample_rate_hz: u32,
    channels: usize,
    samples: &[i16],
    cancel: Option<&AtomicBool>,
) -> Result<()> {
    use alsa::ValueOr;
    use alsa::pcm::{Access, Format, HwParams, PCM, State};

    if samples.is_empty() {
        return Ok(());
    }

    let pcm = PCM::new(&device.spec, alsa::Direction::Playback, false)?;
    let hwp = HwParams::any(&pcm)?;
    hwp.set_channels(channels as u32)?;
    hwp.set_rate(sample_rate_hz, ValueOr::Nearest)?;
    hwp.set_format(Format::s16())?;
    hwp.set_access(Access::RWInterleaved)?;
    let period_frames = DEFAULT_PLAYBACK_FRAMES_PER_WRITE as alsa::pcm::Frames;
    let buffer_frames = DEFAULT_PLAYBACK_BUFFER_FRAMES as alsa::pcm::Frames;
    let _ = hwp.set_period_size_near(period_frames, ValueOr::Nearest)?;
    let _ = hwp.set_buffer_size_near(buffer_frames)?;
    pcm.hw_params(&hwp)?;
    let swp = pcm.sw_params_current()?;
    swp.set_start_threshold(1)?;
    swp.set_avail_min(period_frames)?;
    pcm.sw_params(&swp)?;
    pcm.prepare()?;

    let io = pcm.io_i16()?;
    let mut offset = 0usize;
    while offset < samples.len() {
        if cancel.map(|flag| flag.load(Ordering::Relaxed)).unwrap_or(false) {
            pcm.drop()?;
            return Ok(());
        }
        let remaining_frames = (samples.len() - offset) / channels;
        let frames_to_write = remaining_frames.min(DEFAULT_PLAYBACK_FRAMES_PER_WRITE);
        let end = offset + frames_to_write * channels;
        match io.writei(&samples[offset..end]) {
            Ok(written_frames) => {
                if written_frames == 0 {
                    continue;
                }
                offset += written_frames * channels;
            }
            Err(error) => {
                if pcm.state() == State::XRun {
                    pcm.prepare()?;
                    continue;
                }
                return Err(Error::Alsa(error));
            }
        }
    }

    if cancel.map(|flag| flag.load(Ordering::Relaxed)).unwrap_or(false) {
        pcm.drop()?;
        return Ok(());
    }
    if let Err(error) = pcm.drain() {
        if pcm.state() != State::XRun {
            return Err(Error::Alsa(error));
        }
        pcm.prepare()?;
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn play_interleaved_samples_i16_until(
    _device: &AudioDevice,
    _sample_rate_hz: u32,
    _channels: usize,
    _samples: &[i16],
    _cancel: Option<&AtomicBool>,
) -> Result<()> {
    Err(Error::UnsupportedPlatform)
}

fn interleave_mono_samples_i16(samples: &[f32], channels: usize) -> Vec<i16> {
    let channel_count = channels.max(1);
    let mut interleaved = Vec::with_capacity(samples.len() * channel_count);
    for &sample in samples {
        let clamped = sample.clamp(-1.0, 1.0);
        let pcm = (clamped * i16::MAX as f32).round() as i16;
        for _ in 0..channel_count {
            interleaved.push(pcm);
        }
    }
    interleaved
}

#[cfg(target_os = "linux")]
fn run_capture_loop(
    device_spec: String,
    ring: Arc<Mutex<AudioRing>>,
    stop: Arc<AtomicBool>,
    config: AudioStreamConfig,
    init: mpsc::Sender<std::result::Result<(), String>>,
) -> Result<()> {
    use alsa::ValueOr;
    use alsa::pcm::{Access, Format, HwParams, PCM, State};

    let pcm = match PCM::new(&device_spec, alsa::Direction::Capture, false) {
        Ok(pcm) => pcm,
        Err(error) => {
            let message = format!("alsa capture open failed: {error}");
            let _ = init.send(Err(message.clone()));
            return Err(Error::CaptureInit(message));
        }
    };
    let hwp = match HwParams::any(&pcm) {
        Ok(params) => params,
        Err(error) => {
            let message = format!("alsa capture params failed: {error}");
            let _ = init.send(Err(message.clone()));
            return Err(Error::CaptureInit(message));
        }
    };
    for result in [
        hwp.set_channels(config.channels as u32),
        hwp.set_rate(config.sample_rate_hz, ValueOr::Nearest),
        hwp.set_format(Format::s16()),
        hwp.set_access(Access::RWInterleaved),
    ] {
        if let Err(error) = result {
            let message = format!("alsa capture setup failed: {error}");
            let _ = init.send(Err(message.clone()));
            return Err(Error::CaptureInit(message));
        }
    }
    if let Err(error) = pcm.hw_params(&hwp).and_then(|_| pcm.prepare()) {
        let message = format!("alsa capture prepare failed: {error}");
        let _ = init.send(Err(message.clone()));
        return Err(Error::CaptureInit(message));
    }
    let io = match pcm.io_i16() {
        Ok(io) => io,
        Err(error) => {
            let message = format!("alsa capture io init failed: {error}");
            let _ = init.send(Err(message.clone()));
            return Err(Error::CaptureInit(message));
        }
    };
    let _ = init.send(Ok(()));

    let mut interleaved = vec![0i16; config.frames_per_read * config.channels];
    while !stop.load(Ordering::Relaxed) {
        let frames_read = match io.readi(&mut interleaved) {
            Ok(frames) => frames,
            Err(error) => {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                if pcm.state() == State::XRun {
                    pcm.prepare()?;
                    ring.lock().expect("audio ring poisoned").mark_recovery();
                    continue;
                }
                let message = format!("audio capture read failed: {error}");
                let _ = init.send(Err(message.clone()));
                return Err(Error::CaptureInit(message));
            }
        };
        let samples = &interleaved[..frames_read * config.channels];
        let per_channel: Vec<Vec<i16>> = (0..config.channels)
            .map(|channel| {
                samples
                    .chunks_exact(config.channels)
                    .map(|frame| frame[channel])
                    .collect()
            })
            .collect();
        let channel_rms_dbfs: Vec<f32> = per_channel.iter().map(|samples| slice_rms_dbfs(samples)).collect();
        let selected_channel = channel_rms_dbfs
            .iter()
            .enumerate()
            .max_by(|left, right| left.1.total_cmp(right.1))
            .map(|(index, _)| index)
            .unwrap_or(0);

        ring.lock()
            .expect("audio ring poisoned")
            .push_mono_samples(
                &per_channel[selected_channel],
                SystemTime::now(),
                channel_rms_dbfs,
                selected_channel,
            );
    }
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

fn duration_to_samples(sample_rate_hz: u32, duration: Duration) -> u64 {
    ((duration.as_nanos() * sample_rate_hz as u128 + 500_000_000) / 1_000_000_000) as u64
}

fn samples_to_duration(sample_rate_hz: u32, samples: u64) -> Duration {
    Duration::from_nanos(((samples as u128) * 1_000_000_000 / sample_rate_hz as u128) as u64)
}
