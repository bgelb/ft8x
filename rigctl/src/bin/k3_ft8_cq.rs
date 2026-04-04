use clap::Parser;
use ft8_decoder::{
    GridReport, WaveformOptions, encode_standard_message, synthesize_rectangular_waveform,
};
use rigctl::audio::play_mono_samples;
use rigctl::{K3S_BAUD_RATE, K3s, K3sConfig, TxMeterMode, detect_k3s_audio_output_device};
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Transmit one FT8 CQ at the next slot boundary"
)]
struct Cli {
    #[arg(long)]
    callsign: String,
    #[arg(long)]
    port: Option<PathBuf>,
    #[arg(long, default_value_t = K3S_BAUD_RATE)]
    baud: u32,
    #[arg(long, default_value_t = 500)]
    timeout_ms: u64,
    #[arg(long)]
    output_device: Option<String>,
    #[arg(long, default_value_t = 1_000.0)]
    base_freq_hz: f32,
    #[arg(long, default_value_t = 2)]
    playback_channels: usize,
    #[arg(long, default_value_t = 0.12)]
    drive_level: f32,
    #[arg(long)]
    power_w: Option<f32>,
    #[arg(long, default_value_t = 250)]
    poll_ms: u64,
}

struct TxGuard<'a> {
    rig: &'a mut K3s,
    active: bool,
}

impl<'a> TxGuard<'a> {
    fn enter(rig: &'a mut K3s) -> rigctl::Result<Self> {
        rig.enter_tx()?;
        Ok(Self { rig, active: true })
    }

    fn rig(&mut self) -> &mut K3s {
        self.rig
    }
}

impl Drop for TxGuard<'_> {
    fn drop(&mut self) {
        if self.active {
            let _ = self.rig.enter_rx();
        }
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

    let output_device = detect_k3s_audio_output_device(cli.output_device.as_deref())?;
    let mut rig = K3s::connect(config)?;
    if let Some(power_w) = cli.power_w {
        rig.set_configured_power_w(power_w)?;
        thread::sleep(Duration::from_millis(300));
    }
    let state = rig.read_state()?;
    let configured_power_w = rig.get_configured_power_w()?;
    let original_meter_mode = rig.get_tx_meter_mode().unwrap_or(TxMeterMode::Rf);
    let tx_wave = synthesize_ft8_cq(&cli.callsign, cli.base_freq_hz, cli.drive_level)?;
    let slot_start = next_ft8_slot(SystemTime::now());
    let pre_key = Duration::from_millis(150);
    let key_time = slot_start.checked_sub(pre_key).unwrap_or(slot_start);

    println!(
        "output_device={} ({})",
        output_device.name, output_device.spec
    );
    println!(
        "rig={} Hz mode={} band={} configured_power_w={configured_power_w:.1}",
        state.frequency_hz, state.mode, state.band
    );
    println!(
        "message=CQ {} base_freq_hz={:.1} drive_level={:.2}",
        cli.callsign.trim().to_uppercase(),
        cli.base_freq_hz,
        cli.drive_level
    );
    println!(
        "slot_start_utc={} key_time_utc={}",
        format_system_time(slot_start),
        format_system_time(key_time)
    );

    sleep_until(key_time);
    rig.set_tx_meter_mode(TxMeterMode::Rf)?;
    let mut tx = TxGuard::enter(&mut rig)?;
    thread::sleep(
        slot_start
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO),
    );
    println!("transmitting at {}", format_system_time(SystemTime::now()));
    let output_device_for_thread = output_device.clone();
    let sample_rate_hz = tx_wave.sample_rate_hz;
    let channels = cli.playback_channels;
    let samples = tx_wave.samples.clone();
    let poll = Duration::from_millis(cli.poll_ms);
    let playback_thread = thread::spawn(move || {
        play_mono_samples(
            &output_device_for_thread,
            sample_rate_hz,
            channels,
            &samples,
        )
    });
    loop {
        if playback_thread.is_finished() {
            break;
        }
        let tx_state = tx.rig().is_transmitting().unwrap_or(false);
        let bg = tx.rig().get_bar_graph().ok();
        let swr = tx.rig().get_last_tx_swr().ok();
        if let Some(bg) = bg {
            println!(
                "meter utc={} tx={} bg={}{} swr={:.1}",
                format_system_time(SystemTime::now()),
                tx_state,
                bg.level,
                if bg.receiving { "R" } else { "T" },
                swr.unwrap_or(0.0)
            );
        }
        thread::sleep(poll);
    }
    let playback_result = playback_thread
        .join()
        .map_err(|_| "playback thread panicked")?;
    playback_result?;
    thread::sleep(Duration::from_millis(100));
    let tx_state = tx.rig().is_transmitting().unwrap_or(false);
    println!(
        "playback complete at {} tx_state={tx_state}",
        format_system_time(SystemTime::now())
    );
    drop(tx);
    rig.set_tx_meter_mode(original_meter_mode)?;
    println!(
        "returned to RX at {}",
        format_system_time(SystemTime::now())
    );

    Ok(())
}

fn synthesize_ft8_cq(
    callsign: &str,
    base_freq_hz: f32,
    amplitude: f32,
) -> Result<ft8_decoder::AudioBuffer, Box<dyn std::error::Error>> {
    let frame = encode_standard_message(
        "CQ",
        &callsign.trim().to_uppercase(),
        false,
        &GridReport::Blank,
    )?;
    let waveform = synthesize_rectangular_waveform(
        &frame,
        &WaveformOptions {
            base_freq_hz,
            start_seconds: 0.5,
            total_seconds: 15.0,
            amplitude,
        },
    )?;
    Ok(waveform)
}

fn next_ft8_slot(now: SystemTime) -> SystemTime {
    let since_epoch = now.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
    let seconds = since_epoch.as_secs();
    let next_seconds = ((seconds / 15) + 1) * 15;
    UNIX_EPOCH + Duration::from_secs(next_seconds)
}

fn sleep_until(target: SystemTime) {
    loop {
        let now = SystemTime::now();
        match target.duration_since(now) {
            Ok(remaining) if remaining > Duration::from_millis(5) => {
                thread::sleep(remaining.min(Duration::from_millis(200)));
            }
            _ => break,
        }
    }
}

fn format_system_time(time: SystemTime) -> String {
    let since_epoch = time.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
    let seconds = since_epoch.as_secs();
    let millis = since_epoch.subsec_millis();
    format!("{seconds}.{millis:03}Z")
}
