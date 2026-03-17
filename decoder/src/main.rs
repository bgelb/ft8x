use std::path::PathBuf;

use clap::{ArgAction, Parser, Subcommand};

use ft8_decoder::{
    DecodeOptions, GridReport, WaveformOptions, debug_candidate_wav_file, decode_wav_file,
    parse_standard_info, write_rectangular_standard_wav,
};

#[derive(Debug, Parser)]
#[command(name = "ft8-decoder")]
#[command(about = "From-scratch FT8 decoder library CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Decode {
        #[arg(value_name = "WAV")]
        wav: PathBuf,

        #[arg(long)]
        json: bool,

        #[arg(long, default_value_t = 200.0)]
        min_freq_hz: f32,

        #[arg(long, default_value_t = 3000.0)]
        max_freq_hz: f32,

        #[arg(long, default_value_t = 600)]
        max_candidates: usize,

        #[arg(long, default_value_t = 200)]
        max_successes: usize,

        #[arg(long, action = ArgAction::SetTrue)]
        pretty: bool,
    },
    DebugCandidate {
        #[arg(value_name = "WAV")]
        wav: PathBuf,

        #[arg(long)]
        dt_seconds: f32,

        #[arg(long)]
        freq_hz: f32,

        #[arg(long, action = ArgAction::SetTrue)]
        pretty: bool,
    },
    GenerateStandard {
        #[arg(value_name = "OUTPUT_WAV")]
        output_wav: PathBuf,

        #[arg(long)]
        first: String,

        #[arg(long)]
        second: String,

        #[arg(long)]
        info: String,

        #[arg(long, default_value_t = false)]
        acknowledge: bool,

        #[arg(long, default_value_t = 1_000.0)]
        freq_hz: f32,

        #[arg(long, default_value_t = 0.5)]
        start_seconds: f32,

        #[arg(long, default_value_t = 15.0)]
        total_seconds: f32,

        #[arg(long, default_value_t = 0.8)]
        amplitude: f32,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Command::Decode {
            wav,
            json,
            min_freq_hz,
            max_freq_hz,
            max_candidates,
            max_successes,
            pretty,
        } => {
            let options = DecodeOptions {
                min_freq_hz,
                max_freq_hz,
                max_candidates,
                max_successes,
                ..DecodeOptions::default()
            };

            let report = decode_wav_file(&wav, &options)?;
            if json {
                if pretty {
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
        }
        Command::DebugCandidate {
            wav,
            dt_seconds,
            freq_hz,
            pretty,
        } => {
            let report = debug_candidate_wav_file(&wav, dt_seconds, freq_hz)?;
            if pretty {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!("{}", serde_json::to_string(&report)?);
            }
        }
        Command::GenerateStandard {
            output_wav,
            first,
            second,
            info,
            acknowledge,
            freq_hz,
            start_seconds,
            total_seconds,
            amplitude,
        } => {
            let info = parse_standard_info(&info)?;
            let options = WaveformOptions {
                base_freq_hz: freq_hz,
                start_seconds,
                total_seconds,
                amplitude,
            };
            let frame = write_rectangular_standard_wav(
                &output_wav,
                &first,
                &second,
                acknowledge,
                &info,
                &options,
            )?;
            let rendered_info = match info {
                GridReport::Grid(grid) => grid,
                GridReport::Signal(report) => format!("{report:+03}"),
                GridReport::Reply(reply) => format!("{reply:?}"),
                GridReport::Blank => String::new(),
            };
            println!(
                "wrote {} symbols={} message=\"{} {} {}\"",
                output_wav.display(),
                frame.channel_symbols.len(),
                first,
                second,
                rendered_info.trim()
            );
        }
    }
    Ok(())
}
