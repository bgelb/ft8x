use std::path::PathBuf;

use clap::{ArgAction, Parser, Subcommand};

use ft8_decoder::{
    DecodeOptions, DecodeProfile, GridReport, Mode, WaveformOptions,
    debug_candidate_truth_wav_file, debug_candidate_wav_file, decode_wav_file,
    encode_standard_message, encode_standard_message_for_mode, parse_standard_info,
    write_rectangular_standard_wav,
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

        #[arg(long, default_value_t = 4000.0)]
        max_freq_hz: f32,

        #[arg(long, default_value_t = 600)]
        max_candidates: usize,

        #[arg(long, default_value_t = 200)]
        max_successes: usize,

        #[arg(long, default_value_t = 3)]
        search_passes: usize,

        #[arg(long, default_value = "medium")]
        profile: String,

        #[arg(long, default_value = "ft8")]
        mode: String,

        #[arg(long, action = ArgAction::SetTrue)]
        pretty: bool,
    },
    DebugCandidate {
        #[arg(value_name = "WAV")]
        wav: PathBuf,

        #[arg(long, default_value = "ft8")]
        mode: String,

        #[arg(long)]
        dt_seconds: f32,

        #[arg(long)]
        freq_hz: f32,

        #[arg(long, action = ArgAction::SetTrue)]
        pretty: bool,
    },
    DebugStandardCandidate {
        #[arg(value_name = "WAV")]
        wav: PathBuf,

        #[arg(long, default_value = "ft8")]
        mode: String,

        #[arg(long)]
        dt_seconds: f32,

        #[arg(long)]
        freq_hz: f32,

        #[arg(long)]
        message: String,

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

        #[arg(long, default_value = "ft8")]
        mode: String,
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
            search_passes,
            profile,
            mode,
            pretty,
        } => {
            let profile = profile.parse::<DecodeProfile>().map_err(|message| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
            })?;
            let mode = mode.parse::<Mode>().map_err(|message| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
            })?;
            let options = DecodeOptions {
                mode,
                profile,
                min_freq_hz,
                max_freq_hz,
                max_candidates,
                max_successes,
                search_passes,
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
            mode,
            dt_seconds,
            freq_hz,
            pretty,
        } => {
            let mode = mode.parse::<Mode>().map_err(|message| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
            })?;
            let report = debug_candidate_wav_file(&wav, mode, dt_seconds, freq_hz)?;
            if pretty {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!("{}", serde_json::to_string(&report)?);
            }
        }
        Command::DebugStandardCandidate {
            wav,
            mode,
            dt_seconds,
            freq_hz,
            message,
            pretty,
        } => {
            let mode = mode.parse::<Mode>().map_err(|message| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
            })?;
            let (first, second, acknowledge, info) = parse_rendered_standard_message(&message)
                .map_err(|message| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
                })?;
            let frame = encode_standard_message_for_mode(mode, &first, &second, acknowledge, &info)
                .map_err(|error| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, error.to_string())
                })?;
            let report = debug_candidate_truth_wav_file(
                &wav,
                mode,
                dt_seconds,
                freq_hz,
                &frame.codeword_bits,
            )?;
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
            mode,
        } => {
            let info = parse_standard_info(&info)?;
            let mode = mode.parse::<Mode>().map_err(|message| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
            })?;
            let options = WaveformOptions {
                mode,
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

fn parse_rendered_standard_message(
    message: &str,
) -> Result<(String, String, bool, GridReport), String> {
    let tokens: Vec<&str> = message.split_whitespace().collect();
    if tokens.len() < 2 {
        return Err("standard message must contain at least two fields".to_string());
    }

    let mut matches = Vec::<(String, String, bool, GridReport)>::new();
    for (remaining_len, acknowledge, info) in tail_candidates(&tokens) {
        let call_tokens = &tokens[..remaining_len];
        if !(2..=4).contains(&call_tokens.len()) {
            continue;
        }
        for first_len in 1..=2 {
            if first_len >= call_tokens.len() {
                continue;
            }
            let second_len = call_tokens.len() - first_len;
            if !(1..=2).contains(&second_len) {
                continue;
            }
            if !is_valid_call_field(&call_tokens[..first_len])
                || !is_valid_call_field(&call_tokens[first_len..])
            {
                continue;
            }
            let first = call_tokens[..first_len].join(" ");
            let second = call_tokens[first_len..].join(" ");
            if encode_standard_message(&first, &second, acknowledge, &info).is_ok()
                && !matches.iter().any(|candidate| {
                    candidate.0 == first
                        && candidate.1 == second
                        && candidate.2 == acknowledge
                        && same_grid_report(&candidate.3, &info)
                })
            {
                matches.push((first, second, acknowledge, info.clone()));
            }
        }
    }

    match matches.len() {
        1 => Ok(matches.pop().expect("single match")),
        0 => Err(format!(
            "could not parse rendered standard message: {message:?}"
        )),
        _ => Err(format!("ambiguous rendered standard message: {message:?}")),
    }
}

fn tail_candidates(tokens: &[&str]) -> Vec<(usize, bool, GridReport)> {
    let mut candidates = vec![(tokens.len(), false, GridReport::Blank)];

    if tokens.len() >= 3 {
        if let Some((acknowledge, info)) = parse_trailing_info_token(tokens[tokens.len() - 1]) {
            candidates.push((tokens.len() - 1, acknowledge, info));
        }
    }

    if tokens.len() >= 4 && tokens[tokens.len() - 2].eq_ignore_ascii_case("R") {
        if let Ok(info) = parse_standard_info(tokens[tokens.len() - 1]) {
            candidates.push((tokens.len() - 2, true, info));
        }
    }

    candidates
}

fn parse_trailing_info_token(token: &str) -> Option<(bool, GridReport)> {
    if token.eq_ignore_ascii_case("R") {
        return Some((true, GridReport::Blank));
    }
    if !token.eq_ignore_ascii_case("RRR") && !token.eq_ignore_ascii_case("RR73") {
        if let Some(rest) = token.strip_prefix('R').or_else(|| token.strip_prefix('r')) {
            if !rest.is_empty() {
                if let Ok(info) = parse_standard_info(rest) {
                    return Some((true, info));
                }
            }
        }
    }
    parse_standard_info(token).ok().map(|info| (false, info))
}

fn is_valid_call_field(tokens: &[&str]) -> bool {
    match tokens {
        [single] => !single.is_empty(),
        [first, second] => first.eq_ignore_ascii_case("CQ") && !second.is_empty(),
        _ => false,
    }
}

fn same_grid_report(left: &GridReport, right: &GridReport) -> bool {
    match (left, right) {
        (GridReport::Grid(left), GridReport::Grid(right)) => left == right,
        (GridReport::Signal(left), GridReport::Signal(right)) => left == right,
        (GridReport::Blank, GridReport::Blank) => true,
        (GridReport::Reply(left), GridReport::Reply(right)) => {
            std::mem::discriminant(left) == std::mem::discriminant(right)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_signal_report_with_ack() {
        let parsed = parse_rendered_standard_message("KE0EE N1RDN R-18").expect("parsed");
        assert_eq!(parsed.0, "KE0EE");
        assert_eq!(parsed.1, "N1RDN");
        assert!(parsed.2);
        assert!(matches!(parsed.3, GridReport::Signal(-18)));
    }

    #[test]
    fn parses_grid_report_with_ack() {
        let parsed = parse_rendered_standard_message("K1GUY NA4RR R EM61").expect("parsed");
        assert_eq!(parsed.0, "K1GUY");
        assert_eq!(parsed.1, "NA4RR");
        assert!(parsed.2);
        assert!(matches!(parsed.3, GridReport::Grid(ref grid) if grid == "EM61"));
    }

    #[test]
    fn parses_cq_token_variant() {
        let parsed = parse_rendered_standard_message("CQ DX R6WA LN32").expect("parsed");
        assert_eq!(parsed.0, "CQ DX");
        assert_eq!(parsed.1, "R6WA");
        assert!(!parsed.2);
        assert!(matches!(parsed.3, GridReport::Grid(ref grid) if grid == "LN32"));
    }
}
