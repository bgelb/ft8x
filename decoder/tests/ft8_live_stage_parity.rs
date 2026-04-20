use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use ft8_decoder::{
    AudioBuffer, DecodeOptions, DecodeProfile, DecodeStage, DecodedMessage, DecoderSession,
    DecoderState, Mode,
};

#[test]
#[ignore = "local corpus check; requires artifacts/samples"]
fn deepest_live_stage_prep_matches_canonical_final_texts_on_local_samples() {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("repo root");
    let samples_root = repo_root.join("artifacts/samples");
    let mut wavs = Vec::new();
    collect_wavs(&samples_root, &mut wavs);
    assert!(
        !wavs.is_empty(),
        "no WAV samples found under {}",
        samples_root.display()
    );

    let mut failures = Vec::new();
    for wav in wavs {
        let audio = load_wav(&wav).unwrap_or_else(|error| {
            panic!("failed to load {}: {error}", wav.display());
        });
        let options = DecodeOptions {
            profile: DecodeProfile::Deepest,
            ..DecodeOptions::for_mode(Mode::Ft8)
        };
        let canonical = decode_canonical(&audio, &options);
        let live = decode_live_style_deepest(&audio, &options);
        if canonical != live {
            failures.push(format!(
                "{}\n  canonical-only={:?}\n  live-only={:?}",
                wav.display(),
                canonical.difference(&live).collect::<Vec<_>>(),
                live.difference(&canonical).collect::<Vec<_>>()
            ));
        }
    }

    if !failures.is_empty() {
        panic!(
            "Deepest live-stage path changed final decode texts:\n{}",
            failures.join("\n")
        );
    }
}

fn decode_canonical(audio: &AudioBuffer, options: &DecodeOptions) -> BTreeSet<String> {
    let mut session = DecoderSession::new();
    let mut state = DecoderState::new();
    for stage in DecodeStage::ordered() {
        let (update, next_state) = session
            .decode_stage_with_state(audio, options, stage, Some(&state))
            .unwrap_or_else(|error| panic!("canonical {} failed: {error}", stage.as_str()));
        state = next_state;
        if stage == DecodeStage::Full {
            return decode_texts(&update.report.decodes);
        }
    }
    BTreeSet::new()
}

fn decode_live_style_deepest(audio: &AudioBuffer, options: &DecodeOptions) -> BTreeSet<String> {
    let mut session = DecoderSession::new();
    let mut state = DecoderState::new();
    let (update, next_state) = session
        .decode_stage_with_state(audio, options, DecodeStage::Early41, Some(&state))
        .expect("live early41");
    drop(update);
    state = next_state;

    let (update, next_state) = session
        .decode_stage_with_state(audio, options, DecodeStage::Early47, Some(&state))
        .expect("live early47 prep");
    drop(update);
    state = next_state;

    let (update, _) = session
        .decode_stage_with_state(audio, options, DecodeStage::Full, Some(&state))
        .expect("live full");
    decode_texts(&update.report.decodes)
}

fn decode_texts(decodes: &[DecodedMessage]) -> BTreeSet<String> {
    decodes
        .iter()
        .map(|decode| decode.text.trim().to_ascii_uppercase())
        .collect()
}

fn collect_wavs(root: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_wavs(&path, out);
        } else if path
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("wav"))
        {
            out.push(path);
        }
    }
    out.sort();
}

fn load_wav(path: &Path) -> Result<AudioBuffer, hound::Error> {
    let mut reader = hound::WavReader::open(path)?;
    let spec = reader.spec();
    let channels = spec.channels as usize;
    let interleaved = match (spec.sample_format, spec.bits_per_sample) {
        (hound::SampleFormat::Int, 16) => reader
            .samples::<i16>()
            .map(|sample| sample.map(|value| value as f32 / i16::MAX as f32))
            .collect::<Result<Vec<_>, _>>()?,
        (hound::SampleFormat::Float, 32) => {
            reader.samples::<f32>().collect::<Result<Vec<_>, _>>()?
        }
        _ => Vec::new(),
    };
    let samples = if channels <= 1 {
        interleaved
    } else {
        interleaved
            .chunks_exact(channels)
            .map(|chunk| chunk.iter().copied().sum::<f32>() / channels as f32)
            .collect()
    };
    let (sample_rate_hz, samples) = if spec.sample_rate == 12_000 {
        (spec.sample_rate, samples)
    } else {
        (12_000, resample_linear(&samples, spec.sample_rate, 12_000))
    };
    Ok(AudioBuffer {
        sample_rate_hz,
        samples,
    })
}

fn resample_linear(samples: &[f32], src_rate_hz: u32, dst_rate_hz: u32) -> Vec<f32> {
    if samples.is_empty() || src_rate_hz == dst_rate_hz {
        return samples.to_vec();
    }
    let output_len = ((samples.len() as u64 * dst_rate_hz as u64) + (src_rate_hz as u64 / 2))
        / src_rate_hz as u64;
    let mut output = Vec::with_capacity(output_len as usize);
    let scale = src_rate_hz as f64 / dst_rate_hz as f64;
    for index in 0..output_len as usize {
        let position = index as f64 * scale;
        let left = position.floor() as usize;
        let right = (left + 1).min(samples.len().saturating_sub(1));
        let frac = (position - left as f64) as f32;
        output.push(samples[left] * (1.0 - frac) + samples[right] * frac);
    }
    output
}
