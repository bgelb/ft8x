use std::path::{Path, PathBuf};

use ft8_decoder::{
    AudioBuffer, DecodeOptions, GridReport, Mode, WaveformOptions, decode_pcm,
    encode_standard_message_for_mode, parse_standard_info, synthesize_rectangular_waveform,
    write_wav,
};
use serde::Deserialize;

const SNR_MANIFEST: &str = "data/snr_regressions.json";

#[derive(Debug, Deserialize)]
struct SnrManifest {
    cases: Vec<SnrCase>,
}

#[derive(Debug, Clone, Deserialize)]
struct SnrCase {
    id: String,
    mode: String,
    first: String,
    second: String,
    info: String,
    expected_text: String,
    base_freq_hz: f32,
    signal_gain: f32,
    noise_sigma: f32,
    noise_seed: u64,
    expected_stock_snr_db: i32,
}

#[test]
fn synthesized_snr_regressions_track_stock_within_one_db() {
    let manifest = load_manifest();
    let mut failures = Vec::new();

    for case in &manifest.cases {
        let mode = parse_mode(&case.mode);
        let audio = synthesize_case(case, mode);
        let report = decode_pcm(&audio, &DecodeOptions::for_mode(mode))
            .unwrap_or_else(|error| panic!("{}: decode failed: {error}", case.id));
        let expected_text = case.expected_text.trim().to_ascii_uppercase();
        let actual = report
            .decodes
            .iter()
            .find(|decode| decode.text.trim().to_ascii_uppercase() == expected_text);
        let Some(actual) = actual else {
            failures.push(format!(
                "{}: missing {:?}; got {:?}",
                case.id,
                case.expected_text,
                report
                    .decodes
                    .iter()
                    .map(|decode| decode.text.clone())
                    .collect::<Vec<_>>()
            ));
            continue;
        };
        let delta = (actual.snr_db - case.expected_stock_snr_db).abs();
        if delta > 1 {
            failures.push(format!(
                "{}: expected stock SNR {}, got {}",
                case.id, case.expected_stock_snr_db, actual.snr_db
            ));
        }
    }

    if !failures.is_empty() {
        panic!("{} SNR regression cases failed:\n{}", SNR_MANIFEST, failures.join("\n"));
    }
}

#[test]
#[ignore = "local helper to refresh stock-reference WAVs"]
fn write_snr_regression_reference_wavs() {
    let manifest = load_manifest();
    let output_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("snr-regression-wavs");
    std::fs::create_dir_all(&output_root).expect("create output dir");

    for case in &manifest.cases {
        let mode = parse_mode(&case.mode);
        let audio = synthesize_case(case, mode);
        let output = output_root.join(format!("{}.wav", case.id));
        write_wav(&output, &audio).unwrap_or_else(|error| {
            panic!("{}: failed to write {}: {error}", case.id, output.display())
        });
    }
}

fn load_manifest() -> SnrManifest {
    let path = manifest_path();
    serde_json::from_slice(&std::fs::read(&path).expect("read SNR manifest"))
        .unwrap_or_else(|error| panic!("failed to parse {}: {error}", path.display()))
}

fn manifest_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(SNR_MANIFEST)
}

fn synthesize_case(case: &SnrCase, mode: Mode) -> AudioBuffer {
    let info: GridReport = parse_standard_info(&case.info)
        .unwrap_or_else(|error| panic!("{}: invalid info {:?}: {error}", case.id, case.info));
    let frame = encode_standard_message_for_mode(
        mode,
        &case.first,
        &case.second,
        false,
        &info,
    )
    .unwrap_or_else(|error| panic!("{}: encode failed: {error}", case.id));

    let mut waveform = WaveformOptions::for_mode(mode);
    waveform.base_freq_hz = case.base_freq_hz;
    waveform.amplitude *= case.signal_gain;
    let mut audio = synthesize_rectangular_waveform(&frame, &waveform)
        .unwrap_or_else(|error| panic!("{}: synthesize failed: {error}", case.id));
    add_deterministic_noise(&mut audio, case.noise_sigma, case.noise_seed);
    audio
}

fn parse_mode(mode: &str) -> Mode {
    mode.parse::<Mode>()
        .unwrap_or_else(|error| panic!("invalid mode {mode:?}: {error}"))
}

fn add_deterministic_noise(audio: &mut AudioBuffer, sigma: f32, seed: u64) {
    if sigma <= 0.0 {
        return;
    }
    let mut state = seed;
    for sample in &mut audio.samples {
        *sample = (*sample + sigma * gaussianish_unit(&mut state)).clamp(-1.0, 1.0);
    }
}

fn gaussianish_unit(state: &mut u64) -> f32 {
    let mut sum = 0.0f32;
    for _ in 0..12 {
        *state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1);
        let uniform = ((*state >> 32) as f32) / 4294967296.0;
        sum += uniform;
    }
    sum - 6.0
}
