use clap::Parser;
use rigctl::audio::{AudioStreamConfig, SampleStream, play_tone};
use rigctl::{
    K3s, K3sConfig, TxMeterMode, detect_k3s_audio_device, detect_k3s_audio_output_device,
};
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Exercise K3S RX/TX metering and audio tone drive"
)]
struct Cli {
    #[arg(long)]
    port: Option<PathBuf>,
    #[arg(long, default_value_t = rigctl::K3S_BAUD_RATE)]
    baud: u32,
    #[arg(long, default_value_t = 500)]
    timeout_ms: u64,
    #[arg(long)]
    input_device: Option<String>,
    #[arg(long)]
    output_device: Option<String>,
    #[arg(long, default_value_t = 1_000.0)]
    tone_hz: f32,
    #[arg(long, default_value_t = 5.0)]
    tone_seconds: f32,
    #[arg(long, default_value_t = 0.12)]
    tone_level: f32,
    #[arg(long, default_value_t = 48_000)]
    sample_rate_hz: u32,
    #[arg(long, default_value_t = 2)]
    playback_channels: usize,
    #[arg(long, default_value_t = 200)]
    poll_ms: u64,
    #[arg(long)]
    set_power_w: Option<f32>,
}

#[derive(Debug, Default, Clone)]
struct MeterSummary {
    samples: usize,
    tx_true_count: usize,
    bg_sum: u64,
    bg_max: u8,
    po_sum_w: f32,
    po_max_w: f32,
    audio_sum_dbfs: f32,
    audio_min_dbfs: f32,
    rx_signal_sum: u64,
    rx_signal_max: u16,
    swr_sum: f32,
    swr_max: f32,
}

impl MeterSummary {
    fn new() -> Self {
        Self {
            audio_min_dbfs: f32::INFINITY,
            ..Self::default()
        }
    }

    fn avg_bg(&self) -> Option<f32> {
        (self.samples > 0).then(|| self.bg_sum as f32 / self.samples as f32)
    }

    fn avg_audio_dbfs(&self) -> Option<f32> {
        (self.samples > 0).then(|| self.audio_sum_dbfs / self.samples as f32)
    }

    fn avg_rx_signal(&self) -> Option<f32> {
        (self.samples > 0).then(|| self.rx_signal_sum as f32 / self.samples as f32)
    }

    fn avg_swr(&self) -> Option<f32> {
        (self.samples > 0).then(|| self.swr_sum / self.samples as f32)
    }

    fn avg_po_w(&self) -> Option<f32> {
        (self.samples > 0).then(|| self.po_sum_w / self.samples as f32)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let mut config = K3sConfig::default();
    config.baud_rate = cli.baud;
    config.timeout = Duration::from_millis(cli.timeout_ms);
    if let Some(port) = cli.port {
        config.port_path = port;
    }

    let input_device = detect_k3s_audio_device(cli.input_device.as_deref())?;
    let output_device = detect_k3s_audio_output_device(cli.output_device.as_deref())?;
    let capture = SampleStream::start(
        input_device.clone(),
        AudioStreamConfig {
            sample_rate_hz: cli.sample_rate_hz,
            ..AudioStreamConfig::default()
        },
    )?;

    let mut rig = K3s::connect(config)?;
    if let Some(set_power_w) = cli.set_power_w {
        rig.set_configured_power_w(set_power_w)?;
        thread::sleep(Duration::from_millis(300));
    }
    let rig_state = rig.read_state()?;
    let configured_power_w = rig.get_configured_power_w()?;
    let original_meter_mode = rig.get_tx_meter_mode().unwrap_or(TxMeterMode::Rf);
    let originally_transmitting = rig.is_transmitting().unwrap_or(false);
    if originally_transmitting {
        rig.enter_rx()?;
        thread::sleep(Duration::from_millis(300));
    }

    println!("input_device={} ({})", input_device.name, input_device.spec);
    println!(
        "output_device={} ({})",
        output_device.name, output_device.spec
    );
    println!(
        "rig={} Hz  mode={}  band={}  antenna={}",
        rig_state.frequency_hz, rig_state.mode, rig_state.band, rig_state.antenna
    );
    println!("configured_power_w={configured_power_w:.1}");

    let poll = Duration::from_millis(cli.poll_ms);
    let rx_baseline = sample_receive(&mut rig, &capture, Duration::from_secs(2), poll)?;

    rig.set_tx_meter_mode(TxMeterMode::Rf)?;
    rig.enter_tx()?;
    thread::sleep(Duration::from_millis(400));
    let tx_silent = sample_transmit(&mut rig, &capture, Duration::from_secs(2), poll)?;

    let tone_duration = Duration::from_secs_f32(cli.tone_seconds);
    let tone_output = output_device.clone();
    let tone_hz = cli.tone_hz;
    let tone_level = cli.tone_level;
    let channels = cli.playback_channels;
    let sample_rate_hz = cli.sample_rate_hz;
    let tone_thread = thread::spawn(move || {
        play_tone(
            &tone_output,
            sample_rate_hz,
            channels,
            tone_hz,
            tone_duration,
            tone_level,
        )
    });
    let tx_tone = sample_transmit(&mut rig, &capture, tone_duration, poll)?;
    let tone_result = tone_thread.join().map_err(|_| "tone thread panicked")?;

    let cleanup_result = rig
        .enter_rx()
        .and_then(|_| rig.set_tx_meter_mode(original_meter_mode));
    tone_result?;
    cleanup_result?;

    println!();
    println!("RX baseline");
    print_summary(&rx_baseline, true);
    println!();
    println!("TX keyed, no audio");
    print_summary(&tx_silent, false);
    println!();
    println!(
        "TX keyed, {} Hz tone for {:.1}s",
        cli.tone_hz as u32, cli.tone_seconds
    );
    print_summary(&tx_tone, false);
    println!();
    println!("Interpretation");
    println!(
        "- audio_drop_db={:.1}",
        rx_baseline.avg_audio_dbfs().unwrap_or(-120.0)
            - tx_silent.avg_audio_dbfs().unwrap_or(-120.0)
    );
    println!(
        "- rf_meter_gain={} -> {} bars",
        tx_silent.bg_max, tx_tone.bg_max
    );
    println!(
        "- CAT on the K3S exposes requested TX power via PC ({} W) and RF meter bars via BG/TM0 (0-12 bars), not a direct watt reading.",
        configured_power_w
    );

    Ok(())
}

fn sample_receive(
    rig: &mut K3s,
    capture: &SampleStream,
    duration: Duration,
    poll: Duration,
) -> Result<MeterSummary, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + duration;
    let mut summary = MeterSummary::new();
    while Instant::now() < deadline {
        let signal = rig.get_signal_level()?;
        let audio = capture.stats();
        summary.samples += 1;
        summary.bg_sum += signal.bar_graph.unwrap_or_default() as u64;
        summary.bg_max = summary.bg_max.max(signal.bar_graph.unwrap_or_default());
        summary.audio_sum_dbfs += audio.last_chunk_rms_dbfs;
        summary.audio_min_dbfs = summary.audio_min_dbfs.min(audio.last_chunk_rms_dbfs);
        summary.rx_signal_sum += signal.coarse as u64;
        summary.rx_signal_max = summary.rx_signal_max.max(signal.coarse);
        thread::sleep(poll);
    }
    Ok(summary)
}

fn sample_transmit(
    rig: &mut K3s,
    capture: &SampleStream,
    duration: Duration,
    poll: Duration,
) -> Result<MeterSummary, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + duration;
    let mut summary = MeterSummary::new();
    while Instant::now() < deadline {
        let tx = rig.is_transmitting()?;
        let bg = rig.get_bar_graph()?;
        let po_w = rig.get_tx_output_power_w().unwrap_or(0.0);
        let swr = rig.get_last_tx_swr().unwrap_or(0.0);
        let audio = capture.stats();
        summary.samples += 1;
        summary.tx_true_count += usize::from(tx);
        summary.bg_sum += bg.level as u64;
        summary.bg_max = summary.bg_max.max(bg.level);
        summary.po_sum_w += po_w;
        summary.po_max_w = summary.po_max_w.max(po_w);
        summary.swr_sum += swr;
        summary.swr_max = summary.swr_max.max(swr);
        summary.audio_sum_dbfs += audio.last_chunk_rms_dbfs;
        summary.audio_min_dbfs = summary.audio_min_dbfs.min(audio.last_chunk_rms_dbfs);
        thread::sleep(poll);
    }
    Ok(summary)
}

fn print_summary(summary: &MeterSummary, include_rx_signal: bool) {
    if include_rx_signal {
        println!(
            "  signal_coarse_avg={:.2} signal_coarse_max={}",
            summary.avg_rx_signal().unwrap_or_default(),
            summary.rx_signal_max
        );
    } else {
        println!(
            "  transmitting_samples={}/{}",
            summary.tx_true_count, summary.samples
        );
    }
    println!(
        "  power_avg_w={:.2} power_max_w={:.2}",
        summary.avg_po_w().unwrap_or_default(),
        summary.po_max_w
    );
    if !include_rx_signal {
        println!(
            "  bargraph_avg={:.2} bargraph_max={}",
            summary.avg_bg().unwrap_or_default(),
            summary.bg_max
        );
        println!(
            "  swr_avg={:.2} swr_max={:.2}",
            summary.avg_swr().unwrap_or_default(),
            summary.swr_max
        );
    }
    println!(
        "  audio_avg_dbfs={:.1} audio_min_dbfs={:.1}",
        summary.avg_audio_dbfs().unwrap_or(-120.0),
        if summary.audio_min_dbfs.is_finite() {
            summary.audio_min_dbfs
        } else {
            -120.0
        }
    );
}
