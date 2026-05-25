use clap::Parser;
use rigctl::audio::{AudioStreamConfig, SampleStream, play_tone};
use rigctl::{
    Rig, RigConnectionConfig, RigKind, RigPowerRequest, RigPowerState, RigSnapshot, TxMeterMode,
    detect_audio_device_for_rig, detect_audio_output_device_for_rig, resolve_rig_kind,
};
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(author, version, about = "Exercise rig RX/TX audio and telemetry")]
struct Cli {
    #[arg(long)]
    rig: Option<RigKind>,
    #[arg(long)]
    port: Option<PathBuf>,
    #[arg(long, default_value_t = 500)]
    timeout_ms: u64,
    #[arg(long)]
    input_device: Option<String>,
    #[arg(long)]
    output_device: Option<String>,
    #[arg(long)]
    no_capture: bool,
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
    #[arg(long)]
    set_power_setting: Option<String>,
}

#[derive(Debug, Default, Clone)]
struct MeterSummary {
    samples: usize,
    tx_true_count: usize,
    audio_sum_dbfs: f32,
    audio_min_dbfs: f32,
    audio_max_dbfs: f32,
    rx_s_sum: f32,
    rx_s_max: f32,
    tx_power_sum_w: f32,
    tx_power_max_w: f32,
    tx_swr_sum: f32,
    tx_swr_max: f32,
}

impl MeterSummary {
    fn new() -> Self {
        Self {
            audio_min_dbfs: f32::INFINITY,
            audio_max_dbfs: f32::NEG_INFINITY,
            ..Self::default()
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let rig_kind = resolve_rig_kind(cli.rig)?;
    let output_device = detect_audio_output_device_for_rig(rig_kind, cli.output_device.as_deref())?;
    let input_and_capture = if cli.no_capture {
        None
    } else {
        let input_device = detect_audio_device_for_rig(rig_kind, cli.input_device.as_deref())?;
        let capture = SampleStream::start(
            input_device.clone(),
            AudioStreamConfig {
                sample_rate_hz: cli.sample_rate_hz,
                ..AudioStreamConfig::default()
            },
        )?;
        Some((input_device, capture))
    };
    let mut rig = Rig::connect(RigConnectionConfig {
        kind: rig_kind,
        port_path: cli.port,
        timeout: Duration::from_millis(cli.timeout_ms),
    })?;

    if let Rig::K3s(k3) = &mut rig {
        let _ = k3.set_tx_meter_mode(TxMeterMode::Rf);
    }
    if let Some(power_w) = cli.set_power_w {
        rig.apply_power_request(&RigPowerRequest::ContinuousWatts(power_w))?;
    }
    if let Some(setting_id) = cli.set_power_setting.as_deref() {
        rig.apply_power_request(&RigPowerRequest::SettingId(setting_id.to_string()))?;
    }

    let snapshot = rig.read_snapshot()?;
    println!("rig_kind={}", rig_kind);
    if let Some((input_device, _)) = &input_and_capture {
        println!("input_device={} ({})", input_device.name, input_device.spec);
    } else {
        println!("input_device=(disabled)");
    }
    println!(
        "output_device={} ({})",
        output_device.name, output_device.spec
    );
    print_snapshot(&snapshot);
    print_power_state(&snapshot.power);

    if cli.no_capture {
        let tone_duration = Duration::from_secs_f32(cli.tone_seconds);
        rig.enter_tx()?;
        thread::sleep(Duration::from_millis(300));
        let tone_result = play_tone(
            &output_device,
            cli.sample_rate_hz,
            cli.playback_channels,
            cli.tone_hz,
            tone_duration,
            cli.tone_level,
        );
        rig.enter_rx()?;
        tone_result?;
        println!();
        println!(
            "TX keyed, {} Hz tone for {:.1}s; capture disabled",
            cli.tone_hz as u32, cli.tone_seconds
        );
        return Ok(());
    }

    let Some((_, capture)) = input_and_capture else {
        unreachable!("capture is only absent when --no-capture returned early");
    };
    let poll = Duration::from_millis(cli.poll_ms);
    let rx_baseline = sample_window(&mut rig, &capture, Duration::from_secs(2), poll)?;
    rig.enter_tx()?;
    thread::sleep(Duration::from_millis(300));
    let tx_silent = sample_window(&mut rig, &capture, Duration::from_secs(2), poll)?;

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
    let tx_tone = sample_window(&mut rig, &capture, tone_duration, poll)?;
    let tone_result = tone_thread.join().map_err(|_| "tone thread panicked")?;
    rig.enter_rx()?;
    tone_result?;

    println!();
    println!("RX baseline");
    print_summary(&rx_baseline);
    println!();
    println!("TX keyed, no audio");
    print_summary(&tx_silent);
    println!();
    println!(
        "TX keyed, {} Hz tone for {:.1}s",
        cli.tone_hz as u32, cli.tone_seconds
    );
    print_summary(&tx_tone);
    Ok(())
}

fn sample_window(
    rig: &mut Rig,
    capture: &SampleStream,
    duration: Duration,
    poll: Duration,
) -> Result<MeterSummary, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + duration;
    let mut summary = MeterSummary::new();
    while Instant::now() < deadline {
        let snapshot = rig.read_snapshot()?;
        let audio = capture.stats();
        summary.samples += 1;
        if snapshot.transmitting {
            summary.tx_true_count += 1;
        }
        summary.audio_sum_dbfs += audio.last_chunk_rms_dbfs;
        summary.audio_min_dbfs = summary.audio_min_dbfs.min(audio.last_chunk_rms_dbfs);
        summary.audio_max_dbfs = summary.audio_max_dbfs.max(audio.last_chunk_rms_dbfs);
        if let Some(value) = snapshot.telemetry.rx_s_meter {
            summary.rx_s_sum += value;
            summary.rx_s_max = summary.rx_s_max.max(value);
        }
        if let Some(value) = snapshot.telemetry.tx_forward_power_w {
            summary.tx_power_sum_w += value;
            summary.tx_power_max_w = summary.tx_power_max_w.max(value);
        }
        if let Some(value) = snapshot.telemetry.tx_swr {
            summary.tx_swr_sum += value;
            summary.tx_swr_max = summary.tx_swr_max.max(value);
        }
        thread::sleep(poll);
    }
    Ok(summary)
}

fn print_snapshot(snapshot: &RigSnapshot) {
    println!(
        "state={} Hz mode={} band={} tx={} smeter={} fwd={}W swr={} alc={} bg={}",
        snapshot.frequency_hz,
        snapshot.mode,
        snapshot.band,
        snapshot.transmitting,
        snapshot
            .telemetry
            .rx_s_meter
            .map(|value| format!("{value:.1}"))
            .unwrap_or_else(|| "N/A".to_string()),
        snapshot
            .telemetry
            .tx_forward_power_w
            .map(|value| format!("{value:.1}"))
            .unwrap_or_else(|| "N/A".to_string()),
        snapshot
            .telemetry
            .tx_swr
            .map(|value| format!("{value:.1}"))
            .unwrap_or_else(|| "N/A".to_string()),
        snapshot
            .telemetry
            .tx_alc
            .map(|value| format!("{value:.1}"))
            .unwrap_or_else(|| "N/A".to_string()),
        snapshot
            .telemetry
            .bar_graph
            .map(|value| value.to_string())
            .unwrap_or_else(|| "N/A".to_string()),
    );
}

fn print_power_state(power: &RigPowerState) {
    match power {
        RigPowerState::Continuous {
            current_watts,
            min_watts,
            max_watts,
        } => {
            println!(
                "power=continuous current={}W range={min_watts:.1}-{max_watts:.1}W",
                current_watts
                    .map(|value| format!("{value:.1}"))
                    .unwrap_or_else(|| "N/A".to_string())
            );
        }
        RigPowerState::Discrete {
            current_id,
            current_label,
            settings,
            can_set,
        } => {
            println!(
                "power=discrete current={} ({}) settable={}",
                current_label.as_deref().unwrap_or("N/A"),
                current_id.as_deref().unwrap_or("-"),
                can_set
            );
            println!(
                "power_settings={}",
                settings
                    .iter()
                    .map(|setting| setting.label.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
    }
}

fn print_summary(summary: &MeterSummary) {
    let count = summary.samples.max(1) as f32;
    println!(
        "samples={} tx_samples={} audio_avg={:.1}dBFS audio_min={:.1} audio_max={:.1}",
        summary.samples,
        summary.tx_true_count,
        summary.audio_sum_dbfs / count,
        summary.audio_min_dbfs,
        summary.audio_max_dbfs
    );
    println!(
        "rx_s_avg={} rx_s_max={} fwd_avg={}W fwd_max={}W swr_avg={} swr_max={}",
        optional_average(summary.rx_s_sum, summary.samples),
        optional_peak(summary.rx_s_max),
        optional_average(summary.tx_power_sum_w, summary.samples),
        optional_peak(summary.tx_power_max_w),
        optional_average(summary.tx_swr_sum, summary.samples),
        optional_peak(summary.tx_swr_max),
    );
}

fn optional_average(sum: f32, count: usize) -> String {
    if count == 0 || sum == 0.0 {
        "N/A".to_string()
    } else {
        format!("{:.1}", sum / count as f32)
    }
}

fn optional_peak(value: f32) -> String {
    if value == 0.0 {
        "N/A".to_string()
    } else {
        format!("{value:.1}")
    }
}
