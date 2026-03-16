use std::path::PathBuf;

use clap::{ArgAction, Parser};

use ft8_decoder::{DecodeOptions, decode_wav_file};

#[derive(Debug, Parser)]
#[command(name = "ft8-decoder")]
#[command(about = "From-scratch FT8 decoder library CLI")]
struct Cli {
    #[arg(value_name = "WAV")]
    wav: PathBuf,

    #[arg(long)]
    json: bool,

    #[arg(long, default_value_t = 200.0)]
    min_freq_hz: f32,

    #[arg(long, default_value_t = 3000.0)]
    max_freq_hz: f32,

    #[arg(long, default_value_t = 48)]
    max_candidates: usize,

    #[arg(long, default_value_t = 32)]
    max_successes: usize,

    #[arg(long, action = ArgAction::SetTrue)]
    pretty: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let options = DecodeOptions {
        min_freq_hz: cli.min_freq_hz,
        max_freq_hz: cli.max_freq_hz,
        max_candidates: cli.max_candidates,
        max_successes: cli.max_successes,
        ..DecodeOptions::default()
    };

    let report = decode_wav_file(&cli.wav, &options)?;
    if cli.json {
        if cli.pretty {
            println!("{}", serde_json::to_string_pretty(&report)?);
        } else {
            println!("{}", serde_json::to_string(&report)?);
        }
        return Ok(());
    }

    for decode in report.decodes {
        println!(
            "{} {:>4} {:>5.2} {:>4.0} ~ {}",
            decode.utc, decode.snr_db, decode.dt_seconds, decode.freq_hz, decode.text
        );
    }
    Ok(())
}
