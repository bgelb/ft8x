use std::time::Instant;

use ft8_decoder::{
    AudioBuffer, DecodeOptions, DecodeProfile, DecodeStage, DecoderSession, DecoderState, Mode,
    WaveformOptions, encode_standard_message_for_mode, parse_standard_info,
    synthesize_rectangular_waveform, write_wav,
};

const DEFAULT_MAX_EARLY47_MS: u128 = 4_000;
const DEFAULT_MAX_MEDIUM_LIVE_FULL_MS: u128 = 950;
const DEFAULT_MAX_DEEPEST_LIVE_FULL_MS: u128 = 2_000;
const FT8_EARLY47_TO_FULL_BUDGET_MS: u128 = 864;
const FT8_EARLY47_TO_PRE_KEY_BUDGET_MS: u128 = 1_814;

#[test]
#[ignore = "synthetic performance workload; run explicitly with --ignored --nocapture"]
fn busy_ft8_early47_and_full_timing() {
    let audio = synthesize_busy_ft8_slot(96);
    if let Ok(path) = std::env::var("BUSY_FT8_WRITE_WAV") {
        if let Some(parent) = std::path::Path::new(&path).parent() {
            std::fs::create_dir_all(parent).expect("create busy FT8 WAV directory");
        }
        write_wav(path, &audio).expect("write busy FT8 WAV");
    }
    measure_busy_profile(&audio, DecodeProfile::Medium);
    measure_busy_profile(&audio, DecodeProfile::Deepest);
}

fn measure_busy_profile(audio: &AudioBuffer, profile: DecodeProfile) {
    let options = DecodeOptions {
        profile,
        min_freq_hz: 200.0,
        max_freq_hz: 3_500.0,
        ..DecodeOptions::for_mode(Mode::Ft8)
    };
    let mut session = DecoderSession::new();
    let mut state = DecoderState::new();

    let early41_started = Instant::now();
    let (early41, next_state) = session
        .decode_stage_with_state(audio, &options, DecodeStage::Early41, Some(&state))
        .expect("early41 decode");
    state = next_state;
    let early41_ms = early41_started.elapsed().as_millis();

    let early47_started = Instant::now();
    let (early47, next_state) = session
        .decode_stage_with_state(audio, &options, DecodeStage::Early47, Some(&state))
        .expect("early47 decode");
    state = next_state;
    let early47_ms = early47_started.elapsed().as_millis();

    let full_started = Instant::now();
    let (full, _) = session
        .decode_stage_with_state(audio, &options, DecodeStage::Full, Some(&state))
        .expect("full decode");
    let full_ms = full_started.elapsed().as_millis();

    let mut live_session = DecoderSession::new();
    let mut live_state = DecoderState::new();
    let (_, next_state) = live_session
        .decode_stage_with_state(audio, &options, DecodeStage::Early41, Some(&live_state))
        .expect("live early41 decode");
    live_state = next_state;
    let live_full_started = Instant::now();
    let (live_full, _) = live_session
        .decode_stage_with_state(audio, &options, DecodeStage::Full, Some(&live_state))
        .expect("live full decode without early47");
    let live_full_ms = live_full_started.elapsed().as_millis();

    let mut reset_full_session = DecoderSession::new();
    let reset_full_started = Instant::now();
    let (reset_full, _) = reset_full_session
        .decode_stage_with_state(audio, &options, DecodeStage::Full, Some(&live_state))
        .expect("live full decode after stage reset");
    let reset_full_ms = reset_full_started.elapsed().as_millis();

    let mut raw_full_session = DecoderSession::new();
    let raw_full_started = Instant::now();
    let (raw_full, _) = raw_full_session
        .decode_stage_with_state(audio, &options, DecodeStage::Full, None)
        .expect("raw full decode");
    let raw_full_ms = raw_full_started.elapsed().as_millis();

    let scheduled_full_ms = match profile {
        DecodeProfile::Medium | DecodeProfile::Deepest => full_ms,
        DecodeProfile::Quick => reset_full_ms,
    };
    let scheduled_early47_ms = match profile {
        DecodeProfile::Medium | DecodeProfile::Deepest => early47_ms,
        DecodeProfile::Quick => 0,
    };

    println!(
        "busy_ft8 profile={} signals=96 early41={}ms decodes={} examined={} ldpc={} early47={}ms decodes={} examined={} ldpc={} full={}ms decodes={} examined={} ldpc={} live_full_no_early47={}ms decodes={} examined={} ldpc={} live_full_reset={}ms decodes={} examined={} ldpc={} raw_full={}ms decodes={} examined={} ldpc={} scheduled_early47={}ms scheduled_full={}ms",
        profile.as_str(),
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
        live_full_ms,
        live_full.report.decodes.len(),
        live_full.report.diagnostics.examined_candidates,
        live_full.report.diagnostics.ldpc_codewords,
        reset_full_ms,
        reset_full.report.decodes.len(),
        reset_full.report.diagnostics.examined_candidates,
        reset_full.report.diagnostics.ldpc_codewords,
        raw_full_ms,
        raw_full.report.decodes.len(),
        raw_full.report.diagnostics.examined_candidates,
        raw_full.report.diagnostics.ldpc_codewords,
        scheduled_early47_ms,
        scheduled_full_ms,
    );

    let max_early47_ms = std::env::var("BUSY_FT8_MAX_EARLY47_MS")
        .ok()
        .and_then(|value| value.parse::<u128>().ok())
        .unwrap_or(DEFAULT_MAX_EARLY47_MS);
    assert!(
        early47_ms <= max_early47_ms,
        "busy FT8 early47 regression: {early47_ms}ms > {max_early47_ms}ms"
    );
    if profile == DecodeProfile::Medium {
        assert!(
            scheduled_early47_ms <= FT8_EARLY47_TO_FULL_BUDGET_MS,
            "busy FT8 medium early47 residual prep misses full-ready budget: {scheduled_early47_ms}ms > {FT8_EARLY47_TO_FULL_BUDGET_MS}ms"
        );
    }
    if profile == DecodeProfile::Deepest {
        let scheduled_to_pre_key_ms = scheduled_early47_ms + scheduled_full_ms;
        assert!(
            scheduled_to_pre_key_ms <= FT8_EARLY47_TO_PRE_KEY_BUDGET_MS,
            "busy FT8 deepest live path misses pre-key budget: {scheduled_to_pre_key_ms}ms > {FT8_EARLY47_TO_PRE_KEY_BUDGET_MS}ms"
        );
    }
    let max_live_full_ms = max_live_full_ms_for_profile(profile);
    assert!(
        scheduled_full_ms <= max_live_full_ms,
        "busy FT8 scheduled full regression: {scheduled_full_ms}ms > {max_live_full_ms}ms"
    );
}

fn max_live_full_ms_for_profile(profile: DecodeProfile) -> u128 {
    let profile_key = profile.as_str().to_ascii_uppercase();
    std::env::var(format!("BUSY_FT8_MAX_{profile_key}_LIVE_FULL_MS"))
        .or_else(|_| std::env::var("BUSY_FT8_MAX_LIVE_FULL_MS"))
        .ok()
        .and_then(|value| value.parse::<u128>().ok())
        .unwrap_or(match profile {
            DecodeProfile::Quick => DEFAULT_MAX_MEDIUM_LIVE_FULL_MS,
            DecodeProfile::Medium => DEFAULT_MAX_MEDIUM_LIVE_FULL_MS,
            DecodeProfile::Deepest => DEFAULT_MAX_DEEPEST_LIVE_FULL_MS,
        })
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
