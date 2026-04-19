use std::time::Instant;

use ft8_decoder::{
    AudioBuffer, DecodeOptions, DecodeProfile, DecodeStage, DecoderSession, DecoderState, Mode,
    WaveformOptions, encode_standard_message_for_mode, parse_standard_info,
    synthesize_rectangular_waveform,
};

const DEFAULT_MAX_EARLY47_MS: u128 = 4_000;

#[test]
#[ignore = "synthetic performance workload; run explicitly with --ignored --nocapture"]
fn busy_ft8_early47_and_full_timing() {
    let audio = synthesize_busy_ft8_slot(96);
    let options = DecodeOptions {
        profile: DecodeProfile::Deepest,
        min_freq_hz: 200.0,
        max_freq_hz: 3_500.0,
        ..DecodeOptions::for_mode(Mode::Ft8)
    };
    let mut session = DecoderSession::new();
    let mut state = DecoderState::new();

    let early41_started = Instant::now();
    let (early41, next_state) = session
        .decode_stage_with_state(&audio, &options, DecodeStage::Early41, Some(&state))
        .expect("early41 decode");
    state = next_state;
    let early41_ms = early41_started.elapsed().as_millis();

    let early47_started = Instant::now();
    let (early47, next_state) = session
        .decode_stage_with_state(&audio, &options, DecodeStage::Early47, Some(&state))
        .expect("early47 decode");
    state = next_state;
    let early47_ms = early47_started.elapsed().as_millis();

    let full_started = Instant::now();
    let (full, _) = session
        .decode_stage_with_state(&audio, &options, DecodeStage::Full, Some(&state))
        .expect("full decode");
    let full_ms = full_started.elapsed().as_millis();

    println!(
        "busy_ft8 signals=96 early41={}ms decodes={} examined={} ldpc={} early47={}ms decodes={} examined={} ldpc={} full={}ms decodes={} examined={} ldpc={}",
        early41_ms,
        early41.report.decodes.len(),
        early41.report.diagnostics.examined_candidates,
        early41.report.diagnostics.ldpc_codewords,
        early47_ms,
        early47.report.decodes.len(),
        early47.report.diagnostics.examined_candidates,
        early47.report.diagnostics.ldpc_codewords,
        full_ms,
        full.report.decodes.len(),
        full.report.diagnostics.examined_candidates,
        full.report.diagnostics.ldpc_codewords,
    );

    let max_early47_ms = std::env::var("BUSY_FT8_MAX_EARLY47_MS")
        .ok()
        .and_then(|value| value.parse::<u128>().ok())
        .unwrap_or(DEFAULT_MAX_EARLY47_MS);
    assert!(
        early47_ms <= max_early47_ms,
        "busy FT8 early47 regression: {early47_ms}ms > {max_early47_ms}ms"
    );
}

fn synthesize_busy_ft8_slot(signal_count: usize) -> AudioBuffer {
    let spec = Mode::Ft8.spec();
    let mut mixed = AudioBuffer {
        sample_rate_hz: spec.geometry.sample_rate_hz,
        samples: vec![
            0.0;
            (spec.default_total_seconds() * spec.geometry.sample_rate_hz as f32).round()
                as usize
        ],
    };
    let info = parse_standard_info("FN31").expect("grid");

    for index in 0..signal_count {
        let frame =
            encode_standard_message_for_mode(Mode::Ft8, "CQ", &standard_call(index), false, &info)
                .expect("encode busy signal");
        let mut waveform = WaveformOptions::for_mode(Mode::Ft8);
        waveform.total_seconds = spec.default_total_seconds();
        waveform.base_freq_hz = 230.0 + (index % 96) as f32 * 34.0;
        waveform.start_seconds = (index % 7) as f32 * 0.032;
        waveform.amplitude = 0.0045 + (index % 5) as f32 * 0.00025;
        let signal =
            synthesize_rectangular_waveform(&frame, &waveform).expect("synthesize busy signal");
        for (slot, sample) in mixed.samples.iter_mut().zip(signal.samples.iter().copied()) {
            *slot += sample;
        }
    }
    add_deterministic_noise(&mut mixed, 0.0007, 0x5eed_2047);
    mixed
}

fn standard_call(index: usize) -> String {
    let digit = index % 10;
    let a = ((index / 10) % 26) as u8;
    let b = ((index / (10 * 26)) % 26) as u8;
    let c = ((index / (10 * 26 * 26)) % 26) as u8;
    format!(
        "K{}{}{}{}",
        digit,
        (b'A' + a) as char,
        (b'A' + b) as char,
        (b'A' + c) as char
    )
}

fn add_deterministic_noise(audio: &mut AudioBuffer, sigma: f32, seed: u64) {
    let mut state = seed;
    for sample in &mut audio.samples {
        let u1 = next_unit_f32(&mut state).max(1.0e-7);
        let u2 = next_unit_f32(&mut state);
        let radius = (-2.0 * u1.ln()).sqrt();
        let phase = 2.0 * std::f32::consts::PI * u2;
        *sample += sigma * radius * phase.cos();
    }
}

fn next_unit_f32(state: &mut u64) -> f32 {
    *state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
    ((*state >> 40) as f32) / ((1u64 << 24) as f32)
}
