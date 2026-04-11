use std::collections::BTreeSet;
use std::path::Path;

use ft8_decoder::{DecodeOptions, DecodeProfile, Mode, WaveformOptions, decode_wav_file};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct DistilledManifest {
    mode: String,
    profile: String,
    cases: Vec<DistilledCase>,
}

#[derive(Debug, Deserialize)]
struct DistilledCase {
    id: String,
    mode: String,
    expected_messages: Vec<String>,
    wav_file: String,
}

#[test]
fn decode_options_for_mode_follow_mode_spec_defaults() {
    for mode in [Mode::Ft8, Mode::Ft4, Mode::Ft2] {
        let options = DecodeOptions::for_mode(mode);
        let waveform = WaveformOptions::for_mode(mode);
        let spec = mode.spec();
        assert_eq!(options.mode, mode);
        assert_eq!(options.profile, DecodeProfile::Medium);
        assert_eq!(options.target_freq_hz, spec.search.nfqso_hz);
        assert_eq!(options.tx_freq_hz, spec.search.nfqso_hz);
        assert_eq!(waveform.mode, mode);
        assert_eq!(waveform.base_freq_hz, spec.default_frequency_hz());
        assert_eq!(waveform.start_seconds, 0.0);
        assert_eq!(waveform.total_seconds, spec.frame_seconds());
        assert_eq!(waveform.amplitude, spec.default_amplitude());
    }
}

#[test]
fn ft4_mixed_medium_distilled_matches_expected_messages() {
    assert_manifest_matches("data/distilled/ft4-mixed-medium/manifest.json");
}

#[test]
fn ft4_mixed_deepest_distilled_matches_expected_messages() {
    assert_manifest_matches("data/distilled/ft4-mixed-deepest/manifest.json");
}

#[test]
fn ft2_mixed_medium_distilled_matches_expected_messages() {
    assert_manifest_matches("data/distilled/ft2-mixed-medium/manifest.json");
}

#[test]
fn ft2_mixed_deepest_distilled_matches_expected_messages() {
    assert_manifest_matches("data/distilled/ft2-mixed-deepest/manifest.json");
}

fn assert_manifest_matches(manifest_relative_path: &str) {
    let manifest_path = Path::new(env!("CARGO_MANIFEST_DIR")).join(manifest_relative_path);
    let manifest: DistilledManifest =
        serde_json::from_slice(&std::fs::read(&manifest_path).expect("read manifest"))
            .expect("parse manifest");
    let mode = parse_mode(&manifest.mode);
    let profile = parse_profile(&manifest.profile);
    let root = manifest_path.parent().expect("manifest parent");
    let mut failures = Vec::new();

    for case in &manifest.cases {
        let wav_path = root.join(&case.wav_file);
        let options = DecodeOptions {
            mode,
            profile,
            ..DecodeOptions::for_mode(mode)
        };
        let report = decode_wav_file(&wav_path, &options).unwrap_or_else(|error| {
            panic!(
                "failed to decode {} from {}: {error}",
                case.id,
                wav_path.display()
            )
        });
        let actual = normalize_messages(report.decodes.into_iter().map(|decode| decode.text));
        let expected = normalize_messages(case.expected_messages.iter().cloned());
        if case.mode != manifest.mode {
            failures.push(format!(
                "{}: manifest mode {} disagrees with case mode {}",
                case.id, manifest.mode, case.mode
            ));
        }
        if actual != expected {
            failures.push(format!(
                "{}: expected {:?}, got {:?} ({})",
                case.id,
                expected,
                actual,
                wav_path.display()
            ));
        }
    }

    if !failures.is_empty() {
        panic!(
            "{} regression cases failed:\n{}",
            manifest_relative_path,
            failures.join("\n")
        );
    }
}

fn normalize_messages(messages: impl IntoIterator<Item = String>) -> BTreeSet<String> {
    messages
        .into_iter()
        .map(|message| message.trim().to_ascii_uppercase())
        .collect()
}

fn parse_mode(mode: &str) -> Mode {
    mode.parse::<Mode>()
        .unwrap_or_else(|error| panic!("invalid mode {mode:?}: {error}"))
}

fn parse_profile(profile: &str) -> DecodeProfile {
    profile
        .parse::<DecodeProfile>()
        .unwrap_or_else(|error| panic!("invalid profile {profile:?}: {error}"))
}
