use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, OnceLock};

use num_complex::Complex32;
use rayon::prelude::*;
use realfft::{RealFftPlanner, RealToComplex};
use rustfft::{Fft, FftPlanner};
use serde::Serialize;

use crate::encode::{channel_symbols_from_codeword_bits, synthesize_channel_reference};
use crate::ldpc::ParityMatrix;
use crate::message::{DecodedPayload, GridReport, HashResolver, Payload, unpack_message};
use crate::protocol::{
    FT8_COSTAS, FT8_MESSAGE_SYMBOLS, FT8_SAMPLE_RATE, FT8_SYMBOL_SAMPLES, FT8_TONE_SPACING_HZ,
    HOP_SAMPLES, HOPS_PER_SYMBOL, all_costas_positions,
};
use crate::wave::{AudioBuffer, DecoderError, load_wav};

#[derive(Debug, Clone, Serialize)]
pub struct DecodeOptions {
    pub min_freq_hz: f32,
    pub max_freq_hz: f32,
    pub max_candidates: usize,
    pub max_successes: usize,
    pub search_passes: usize,
}

impl Default for DecodeOptions {
    fn default() -> Self {
        Self {
            min_freq_hz: 200.0,
            max_freq_hz: 4_000.0,
            max_candidates: 600,
            max_successes: 200,
            search_passes: 3,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DecodeCandidate {
    pub start_seconds: f32,
    pub dt_seconds: f32,
    pub freq_hz: f32,
    pub score: f32,
}

#[derive(Debug, Clone, Serialize)]
pub struct DecodedMessage {
    pub utc: String,
    pub snr_db: i32,
    pub dt_seconds: f32,
    pub freq_hz: f32,
    pub text: String,
    pub candidate_score: f32,
    pub ldpc_iterations: usize,
    pub payload: DecodedPayload,
}

#[derive(Debug, Clone, Serialize)]
pub struct DecodeDiagnostics {
    pub frame_count: usize,
    pub usable_bins: usize,
    pub examined_candidates: usize,
    pub accepted_candidates: usize,
    pub ldpc_codewords: usize,
    pub parsed_payloads: usize,
    pub top_candidates: Vec<DecodeCandidate>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DecodeReport {
    pub sample_rate_hz: u32,
    pub duration_seconds: f32,
    pub decodes: Vec<DecodedMessage>,
    pub diagnostics: DecodeDiagnostics,
}

#[derive(Debug, Clone, Serialize)]
pub struct CandidatePassDebug {
    pub pass_name: String,
    pub mean_abs_llr: f32,
    pub max_abs_llr: f32,
    pub decoded_text: Option<String>,
    pub ldpc_iterations: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CandidateDebugReport {
    pub coarse_start_seconds: f32,
    pub coarse_dt_seconds: f32,
    pub coarse_freq_hz: f32,
    pub refined_start_seconds: f32,
    pub refined_dt_seconds: f32,
    pub refined_freq_hz: f32,
    pub sync_score: f32,
    pub snr_db: i32,
    pub passes: Vec<CandidatePassDebug>,
}

#[derive(Debug, Clone)]
struct SuccessfulDecode {
    payload: Payload,
    codeword_bits: Vec<u8>,
    candidate: DecodeCandidate,
    ldpc_iterations: usize,
    snr_db: i32,
}

#[derive(Debug)]
struct RefinedCandidate {
    start_seconds: f32,
    freq_hz: f32,
    sync_score: f32,
    llr_sets: [Vec<f32>; 4],
    snr_db: i32,
}

#[derive(Default)]
struct DecodeCounters {
    ldpc_codewords: usize,
    parsed_payloads: usize,
}

struct SearchResult {
    successes: Vec<SuccessfulDecode>,
    residual_audio: AudioBuffer,
    frame_count: usize,
    usable_bins: usize,
    top_candidates: Vec<DecodeCandidate>,
    counters: DecodeCounters,
}

#[derive(Debug, Clone, Copy)]
struct SearchGrid {
    frame_count: usize,
    usable_bins: usize,
    min_bin: usize,
}

#[derive(Debug)]
struct Spectrogram {
    bins: Vec<f32>,
    frame_count: usize,
    usable_bins: usize,
    min_bin: usize,
}

#[derive(Debug)]
struct LongSpectrum {
    bins: Vec<Complex32>,
}

struct LongSpectrumPlan {
    forward: Arc<dyn RealToComplex<f32>>,
}

struct Sync8Plan {
    forward: Arc<dyn RealToComplex<f32>>,
}

struct BasebandPlan {
    inverse: Arc<dyn Fft<f32>>,
}

struct SubtractionPlan {
    forward: Arc<dyn Fft<f32>>,
    inverse: Arc<dyn Fft<f32>>,
    filter_spectrum: Vec<Complex32>,
    edge_correction: Vec<f32>,
}

const LONG_INPUT_SAMPLES: usize = 15 * FT8_SAMPLE_RATE as usize;
const LONG_FFT_SAMPLES: usize = 192_000;
const DOWNSAMPLE_FACTOR: usize = 60;
const SYNC8_FFT_SAMPLES: usize = FT8_SYMBOL_SAMPLES * 2;
const SYNC8_STEP_SAMPLES: usize = FT8_SYMBOL_SAMPLES / 4;
const SYNC8_MAX_LAG: isize = 62;
const SYNC8_LOCAL_LAG: isize = 10;
const SYNC8_THRESHOLD: f32 = 1.6;
const SYNC8_EARLY_THRESHOLD: f32 = 2.0;
const BASEBAND_RATE_HZ: f32 = FT8_SAMPLE_RATE as f32 / DOWNSAMPLE_FACTOR as f32;
const BASEBAND_SAMPLES: usize = LONG_FFT_SAMPLES / DOWNSAMPLE_FACTOR;
const BASEBAND_SYMBOL_SAMPLES: usize = FT8_SYMBOL_SAMPLES / DOWNSAMPLE_FACTOR;
const FFT_BIN_HZ: f32 = FT8_SAMPLE_RATE as f32 / LONG_FFT_SAMPLES as f32;
const SYNC8_BIN_HZ: f32 = FT8_SAMPLE_RATE as f32 / SYNC8_FFT_SAMPLES as f32;
const BASEBAND_TAPER_LEN: usize = 100;
const SUBTRACT_FILTER_SAMPLES: usize = 4_000;
const SUBTRACT_FILTER_HALF: usize = SUBTRACT_FILTER_SAMPLES / 2;
const EARLY_BLOCK_SAMPLES: usize = 3_456;
const EARLY_41_SAMPLES: usize = 41 * EARLY_BLOCK_SAMPLES;
const EARLY_47_SAMPLES: usize = 47 * EARLY_BLOCK_SAMPLES;
const NFQSO_HZ: f32 = 1_500.0;
pub fn decode_wav_file(
    path: impl AsRef<Path>,
    options: &DecodeOptions,
) -> Result<DecodeReport, DecoderError> {
    let audio = load_wav(path)?;
    decode_pcm(&audio, options)
}

pub fn debug_candidate_wav_file(
    path: impl AsRef<Path>,
    dt_seconds: f32,
    freq_hz: f32,
) -> Result<Option<CandidateDebugReport>, DecoderError> {
    let audio = load_wav(path)?;
    Ok(debug_candidate_pcm(&audio, dt_seconds, freq_hz))
}

pub fn debug_candidate_pcm(
    audio: &AudioBuffer,
    dt_seconds: f32,
    freq_hz: f32,
) -> Option<CandidateDebugReport> {
    if audio.sample_rate_hz != FT8_SAMPLE_RATE || audio.samples.len() < FT8_SYMBOL_SAMPLES {
        return None;
    }

    let long_spectrum = build_long_spectrum(audio);
    let baseband_plan = BasebandPlan::new();
    let refined = refine_candidate(&long_spectrum, &baseband_plan, dt_seconds + 0.5, freq_hz)?;
    let parity = ParityMatrix::global();
    let mut counters = DecodeCounters::default();
    let mut passes = Vec::new();
    let resolver = HashResolver::default();

    append_debug_passes(&mut passes, "regular", &refined.llr_sets, parity, &mut counters, &resolver);

    if let Some(seed) = extract_candidate_at(&long_spectrum, &baseband_plan, dt_seconds + 0.5, freq_hz) {
        append_debug_passes(&mut passes, "seed", &seed.llr_sets, parity, &mut counters, &resolver);
    }

    let ap_magnitude = refined.llr_sets[0]
        .iter()
        .map(|value| value.abs())
        .fold(0.0f32, f32::max)
        * 1.01;
    if ap_magnitude > 0.0 {
        for (name, known_bits) in [
            ("ap-cq", cq_ap_known_bits()),
            ("ap-mycall", mycall_ap_known_bits()),
        ] {
            let llrs = llrs_with_known_bits(&refined.llr_sets[0], known_bits, ap_magnitude);
            let mean_abs_llr =
                llrs.iter().map(|value| value.abs()).sum::<f32>() / llrs.len() as f32;
            let max_abs_llr = llrs.iter().map(|value| value.abs()).fold(0.0f32, f32::max);
            let decoded = decode_llr_set_with_known_bits(parity, &llrs, known_bits, &mut counters)
                .map(|(payload, _, iterations)| {
                    let rendered = payload.render(&resolver);
                    (rendered.text, iterations)
                });
            passes.push(CandidatePassDebug {
                pass_name: name.to_string(),
                mean_abs_llr,
                max_abs_llr,
                decoded_text: decoded.as_ref().map(|(text, _)| text.clone()),
                ldpc_iterations: decoded.map(|(_, iterations)| iterations),
            });
        }
    }

    Some(CandidateDebugReport {
        coarse_start_seconds: dt_seconds + 0.5,
        coarse_dt_seconds: dt_seconds,
        coarse_freq_hz: freq_hz,
        refined_start_seconds: refined.start_seconds,
        refined_dt_seconds: refined.start_seconds - 0.5,
        refined_freq_hz: refined.freq_hz,
        sync_score: refined.sync_score,
        snr_db: refined.snr_db,
        passes,
    })
}

fn append_debug_passes(
    passes: &mut Vec<CandidatePassDebug>,
    prefix: &str,
    llr_sets: &[Vec<f32>; 4],
    parity: &ParityMatrix,
    counters: &mut DecodeCounters,
    resolver: &HashResolver,
) {
    for (index, llrs) in llr_sets.iter().enumerate() {
        let mean_abs_llr = llrs.iter().map(|value| value.abs()).sum::<f32>() / llrs.len() as f32;
        let max_abs_llr = llrs.iter().map(|value| value.abs()).fold(0.0f32, f32::max);
        let decoded = decode_llr_set(parity, llrs, counters).map(|(payload, _, iterations)| {
            let rendered = payload.render(resolver);
            (rendered.text, iterations)
        });
        passes.push(CandidatePassDebug {
            pass_name: format!("{prefix}-{}", index + 1),
            mean_abs_llr,
            max_abs_llr,
            decoded_text: decoded.as_ref().map(|(text, _)| text.clone()),
            ldpc_iterations: decoded.map(|(_, iterations)| iterations),
        });
    }
}

pub fn decode_pcm(
    audio: &AudioBuffer,
    options: &DecodeOptions,
) -> Result<DecodeReport, DecoderError> {
    if audio.sample_rate_hz != FT8_SAMPLE_RATE {
        return Err(DecoderError::UnsupportedFormat(format!(
            "expected {} Hz audio, got {} Hz",
            FT8_SAMPLE_RATE, audio.sample_rate_hz
        )));
    }
    if audio.samples.len() < FT8_SYMBOL_SAMPLES {
        return Err(DecoderError::UnsupportedFormat(
            "audio too short".to_string(),
        ));
    }

    let subtraction_plan = SubtractionPlan::global();
    let early41 = if audio.samples.len() >= EARLY_41_SAMPLES {
        let early_audio = zero_tail(audio, EARLY_41_SAMPLES);
        Some(run_decode_search(
            &early_audio,
            options,
            None,
            Vec::new(),
            SYNC8_EARLY_THRESHOLD,
            false,
        ))
    } else {
        None
    };
    let early47 = if audio.samples.len() >= EARLY_47_SAMPLES {
        let mut partial47 = zero_tail(audio, EARLY_47_SAMPLES);
        if let Some(stage41) = &early41 {
            for success in &stage41.successes {
                if success.candidate.dt_seconds < 0.396 {
                    subtract_candidate(&mut partial47, success, subtraction_plan);
                }
            }
        }
        let initial_successes = early41
            .as_ref()
            .map(|stage| stage.successes.clone())
            .unwrap_or_default();
        Some(run_decode_search(
            &partial47,
            options,
            Some(partial47.clone()),
            initial_successes,
            SYNC8_THRESHOLD,
            false,
        ))
    } else {
        None
    };
    let prepared_full = early47.as_ref().map(|stage47| {
        let mut prepared = audio.clone();
        for success in &stage47.successes {
            subtract_candidate(&mut prepared, success, subtraction_plan);
        }
        prepared.samples[..EARLY_47_SAMPLES]
            .copy_from_slice(&stage47.residual_audio.samples[..EARLY_47_SAMPLES]);
        prepared
    });
    let initial_successes = early47
        .as_ref()
        .map(|stage| stage.successes.clone())
        .or_else(|| early41.as_ref().map(|stage| stage.successes.clone()))
        .unwrap_or_default();
    let search = run_decode_search(
        audio,
        options,
        prepared_full,
        initial_successes,
        SYNC8_THRESHOLD,
        true,
    );

    let mut resolver = HashResolver::default();
    for success in &search.successes {
        success.payload.collect_callsigns(&mut resolver);
    }

    let mut dedup = BTreeMap::<String, DecodedMessage>::new();
    for success in search.successes {
        let payload = success.payload.render(&resolver);
        let text = payload.text.clone();
        if text.trim().is_empty() {
            continue;
        }
        let message = DecodedMessage {
            utc: "000000".to_string(),
            snr_db: success.snr_db,
            dt_seconds: success.candidate.dt_seconds,
            freq_hz: success.candidate.freq_hz,
            text: text.clone(),
            candidate_score: success.candidate.score,
            ldpc_iterations: success.ldpc_iterations,
            payload,
        };
        match dedup.get(&text) {
            Some(existing) if existing.candidate_score >= message.candidate_score => {}
            _ => {
                dedup.insert(text, message);
            }
        }
    }

    let mut decodes: Vec<_> = dedup.into_values().collect();
    decodes.sort_by(|left, right| {
        left.freq_hz
            .total_cmp(&right.freq_hz)
            .then_with(|| left.text.cmp(&right.text))
    });

    Ok(DecodeReport {
        sample_rate_hz: audio.sample_rate_hz,
        duration_seconds: audio.samples.len() as f32 / audio.sample_rate_hz as f32,
        diagnostics: DecodeDiagnostics {
            frame_count: search.frame_count,
            usable_bins: search.usable_bins,
            examined_candidates: options.max_candidates,
            accepted_candidates: decodes.len(),
            ldpc_codewords: search.counters.ldpc_codewords,
            parsed_payloads: search.counters.parsed_payloads,
            top_candidates: search.top_candidates,
        },
        decodes,
    })
}

fn run_decode_search(
    audio: &AudioBuffer,
    options: &DecodeOptions,
    residual_override: Option<AudioBuffer>,
    initial_successes: Vec<SuccessfulDecode>,
    sync_threshold: f32,
    allow_ap: bool,
) -> SearchResult {
    let total_passes = options.search_passes.max(1);
    let has_residual_override = residual_override.is_some();
    let mut residual_audio = residual_override.unwrap_or_else(|| audio.clone());
    let baseband_plan = BasebandPlan::new();
    let subtraction_plan = SubtractionPlan::global();
    let parity = ParityMatrix::global();
    let mut top_candidates = Vec::new();
    let mut counters = DecodeCounters::default();
    let search_grid = search_grid(audio, options);
    let frame_count = search_grid.frame_count;
    let usable_bins = search_grid.usable_bins;

    let mut successes = initial_successes;
    if !has_residual_override {
        for success in &successes {
            subtract_candidate(&mut residual_audio, success, subtraction_plan);
        }
    }

    for pass in 0..total_passes {
        let long_spectrum = build_long_spectrum(&residual_audio);
        let candidates = collect_candidates(&residual_audio, options, sync_threshold);
        if pass == 0 {
            top_candidates = candidates.clone();
        }
        if candidates.is_empty() {
            break;
        }

        let attempts: Vec<_> = candidates
            .par_iter()
            .map(|candidate| {
                let mut local_counters = DecodeCounters::default();
                let success = try_candidate(
                    search_grid,
                    &long_spectrum,
                    &baseband_plan,
                    candidate,
                    parity,
                    allow_ap,
                    &mut local_counters,
                );
                (success, local_counters)
            })
            .collect();

        let mut pass_successes = Vec::<SuccessfulDecode>::new();
        for (success, local_counters) in attempts {
            counters.ldpc_codewords += local_counters.ldpc_codewords;
            counters.parsed_payloads += local_counters.parsed_payloads;
            if let Some(success) = success {
                pass_successes.push(success);
            }
        }
        if pass_successes.is_empty() {
            break;
        }

        pass_successes.sort_by(|left, right| {
            right
                .candidate
                .score
                .total_cmp(&left.candidate.score)
                .then_with(|| left.candidate.freq_hz.total_cmp(&right.candidate.freq_hz))
        });

        let remaining = options.max_successes.saturating_sub(successes.len());
        if remaining == 0 {
            break;
        }
        if pass_successes.len() > remaining {
            pass_successes.truncate(remaining);
        }

        let will_run_next_pass = pass + 1 < total_passes;

        if will_run_next_pass {
            for success in &pass_successes {
                subtract_candidate(&mut residual_audio, success, subtraction_plan);
            }
        }
        successes.extend(pass_successes);

        if !will_run_next_pass {
            break;
        }
    }

    SearchResult {
        successes,
        residual_audio,
        frame_count,
        usable_bins,
        top_candidates,
        counters,
    }
}

fn search_grid(audio: &AudioBuffer, options: &DecodeOptions) -> SearchGrid {
    let min_bin = (options.min_freq_hz / FT8_TONE_SPACING_HZ).floor().max(0.0) as usize;
    let max_bin = (options.max_freq_hz / FT8_TONE_SPACING_HZ).ceil() as usize + 7;
    SearchGrid {
        frame_count: (audio.samples.len().saturating_sub(FT8_SYMBOL_SAMPLES) / HOP_SAMPLES) + 1,
        usable_bins: max_bin.saturating_sub(min_bin) + 1,
        min_bin,
    }
}

fn build_spectrogram(audio: &AudioBuffer, options: &DecodeOptions) -> Spectrogram {
    let search_grid = search_grid(audio, options);
    let min_bin = search_grid.min_bin;
    let usable_bins = search_grid.usable_bins;
    let frame_count = search_grid.frame_count;
    let max_bin = min_bin + usable_bins - 1;

    let mut planner = RealFftPlanner::<f32>::new();
    let fft = planner.plan_fft_forward(FT8_SYMBOL_SAMPLES);
    let mut input = fft.make_input_vec();
    let mut spectrum = fft.make_output_vec();

    let window: Vec<f32> = (0..FT8_SYMBOL_SAMPLES)
        .map(|index| {
            let phase = 2.0 * std::f32::consts::PI * index as f32 / (FT8_SYMBOL_SAMPLES - 1) as f32;
            0.5 - 0.5 * phase.cos()
        })
        .collect();

    let mut bins = vec![0.0f32; frame_count * usable_bins];
    for frame in 0..frame_count {
        let sample_offset = frame * HOP_SAMPLES;
        for (slot, value) in input.iter_mut().enumerate() {
            *value = audio.samples[sample_offset + slot] * window[slot];
        }
        fft.process(&mut input, &mut spectrum).expect("fft forward");
        for bin in min_bin..=max_bin {
            let value = spectrum[bin];
            bins[frame * usable_bins + (bin - min_bin)] = value.norm_sqr();
        }
    }

    Spectrogram {
        bins,
        frame_count,
        usable_bins,
        min_bin,
    }
}

fn collect_candidates(
    audio: &AudioBuffer,
    options: &DecodeOptions,
    sync_threshold: f32,
) -> Vec<DecodeCandidate> {
    let nhsym = audio
        .samples
        .len()
        .saturating_div(SYNC8_STEP_SAMPLES)
        .saturating_sub(3);
    if nhsym == 0 {
        return Vec::new();
    }

    let min_bin = ((options.min_freq_hz / SYNC8_BIN_HZ).round() as usize).max(1);
    let max_bin = ((options.max_freq_hz / SYNC8_BIN_HZ).round() as usize)
        .min(SYNC8_FFT_SAMPLES / 2 - 12);
    if min_bin >= max_bin {
        return Vec::new();
    }

    let plan = Sync8Plan::global();
    let fft = &plan.forward;
    let mut input = fft.make_input_vec();
    let mut spectrum = fft.make_output_vec();
    let mut symbol_power = vec![0.0f32; nhsym * (SYNC8_FFT_SAMPLES / 2 + 1)];
    let scale = 1.0 / 300.0f32;

    for step in 0..nhsym {
        let start = step * SYNC8_STEP_SAMPLES;
        input.fill(0.0);
        input[..FT8_SYMBOL_SAMPLES]
            .copy_from_slice(&audio.samples[start..start + FT8_SYMBOL_SAMPLES]);
        for sample in &mut input[..FT8_SYMBOL_SAMPLES] {
            *sample *= scale;
        }
        fft.process(&mut input, &mut spectrum).expect("sync8 fft");
        let row = step * (SYNC8_FFT_SAMPLES / 2 + 1);
        for bin in 1..=(SYNC8_FFT_SAMPLES / 2) {
            symbol_power[row + bin] = spectrum[bin].norm_sqr();
        }
    }

    let mut primary = Vec::with_capacity(max_bin - min_bin + 1);
    let mut secondary = Vec::with_capacity(max_bin - min_bin + 1);
    let nominal_start = (0.5f32 / (SYNC8_STEP_SAMPLES as f32 / FT8_SAMPLE_RATE as f32)) as isize;

    for bin in min_bin..=max_bin {
        let mut best_local = (f32::NEG_INFINITY, 0isize);
        let mut best_wide = (f32::NEG_INFINITY, 0isize);
        for lag in -SYNC8_MAX_LAG..=SYNC8_MAX_LAG {
            let score = sync8_score(&symbol_power, nhsym, bin, lag, nominal_start);
            if (-SYNC8_LOCAL_LAG..=SYNC8_LOCAL_LAG).contains(&lag) && score > best_local.0 {
                best_local = (score, lag);
            }
            if score > best_wide.0 {
                best_wide = (score, lag);
            }
        }
        primary.push((bin, best_local.1, best_local.0));
        secondary.push((bin, best_wide.1, best_wide.0));
    }

    normalize_sync_scores(&mut primary);
    normalize_sync_scores(&mut secondary);

    let mut raw = Vec::<DecodeCandidate>::new();
    for &(bin, lag, score) in &primary {
        if score >= sync_threshold && score.is_finite() {
            raw.push(DecodeCandidate {
                start_seconds: lag as f32 * SYNC8_STEP_SAMPLES as f32 / FT8_SAMPLE_RATE as f32
                    + 0.48,
                dt_seconds: (lag as f32 - 0.5) * SYNC8_STEP_SAMPLES as f32
                    / FT8_SAMPLE_RATE as f32,
                freq_hz: bin as f32 * SYNC8_BIN_HZ,
                score,
            });
        }
    }
    for &(bin, lag, score) in &secondary {
        if score >= sync_threshold
            && score.is_finite()
            && !primary.iter().any(|&(b, local_lag, _)| b == bin && local_lag == lag)
        {
            raw.push(DecodeCandidate {
                start_seconds: lag as f32 * SYNC8_STEP_SAMPLES as f32 / FT8_SAMPLE_RATE as f32
                    + 0.48,
                dt_seconds: (lag as f32 - 0.5) * SYNC8_STEP_SAMPLES as f32
                    / FT8_SAMPLE_RATE as f32,
                freq_hz: bin as f32 * SYNC8_BIN_HZ,
                score,
            });
        }
    }

    raw.sort_by(|left, right| right.score.total_cmp(&left.score));

    let mut prioritized = Vec::with_capacity(raw.len());
    prioritized.extend(
        raw.iter()
            .filter(|candidate| (candidate.freq_hz - NFQSO_HZ).abs() <= 10.0)
            .cloned(),
    );
    prioritized.extend(
        raw.into_iter()
            .filter(|candidate| (candidate.freq_hz - NFQSO_HZ).abs() > 10.0),
    );

    let mut selected = Vec::new();
    for candidate in prioritized {
        let too_close = selected.iter().any(|existing: &DecodeCandidate| {
            (existing.dt_seconds - candidate.dt_seconds).abs() < 0.04
                && (existing.freq_hz - candidate.freq_hz).abs() < 4.0
        });
        if too_close {
            continue;
        }
        selected.push(candidate);
        if selected.len() >= options.max_candidates {
            break;
        }
    }

    if selected.is_empty() {
        collect_candidates_legacy(audio, options)
    } else {
        selected
    }
}

fn zero_tail(audio: &AudioBuffer, keep_samples: usize) -> AudioBuffer {
    let mut copy = audio.clone();
    if keep_samples < copy.samples.len() {
        copy.samples[keep_samples..].fill(0.0);
    }
    copy
}

fn collect_candidates_legacy(audio: &AudioBuffer, options: &DecodeOptions) -> Vec<DecodeCandidate> {
    let spectrogram = build_spectrogram(audio, options);
    let costas = all_costas_positions();
    let max_start_frame = spectrogram
        .frame_count
        .saturating_sub((FT8_MESSAGE_SYMBOLS - 1) * HOPS_PER_SYMBOL + 1);

    let mut raw = Vec::<DecodeCandidate>::new();
    for phase in 0..HOPS_PER_SYMBOL {
        let mut start_frame = phase;
        while start_frame < max_start_frame {
            for base in 0..spectrogram.usable_bins.saturating_sub(7) {
                let mut score = 0.0f32;
                for (symbol_index, tone) in costas {
                    let frame = start_frame + symbol_index * HOPS_PER_SYMBOL;
                    let row = frame * spectrogram.usable_bins;
                    let mut band_sum = 0.0;
                    for offset in 0..8 {
                        band_sum += spectrogram.bins[row + base + offset];
                    }
                    let expected = spectrogram.bins[row + base + tone];
                    score += expected * 8.0 - band_sum;
                }
                raw.push(DecodeCandidate {
                    start_seconds: start_frame as f32 * HOP_SAMPLES as f32 / FT8_SAMPLE_RATE as f32,
                    dt_seconds: start_frame as f32 * HOP_SAMPLES as f32 / FT8_SAMPLE_RATE as f32
                        - 0.5,
                    freq_hz: (spectrogram.min_bin + base) as f32 * FT8_TONE_SPACING_HZ,
                    score,
                });
            }
            start_frame += HOPS_PER_SYMBOL;
        }
    }

    raw.sort_by(|left, right| right.score.total_cmp(&left.score));

    let mut selected = Vec::new();
    for candidate in raw {
        let too_close = selected.iter().any(|existing: &DecodeCandidate| {
            (existing.dt_seconds - candidate.dt_seconds).abs() < 0.16
                && (existing.freq_hz - candidate.freq_hz).abs() < FT8_TONE_SPACING_HZ * 1.5
        });
        if too_close {
            continue;
        }
        selected.push(candidate);
        if selected.len() >= options.max_candidates {
            break;
        }
    }
    selected
}

fn subtract_candidate(
    audio: &mut AudioBuffer,
    success: &SuccessfulDecode,
    plan: &SubtractionPlan,
) {
    let Some(channel_symbols) = channel_symbols_from_codeword_bits(&success.codeword_bits) else {
        return;
    };
    let start_sample =
        (success.candidate.start_seconds * FT8_SAMPLE_RATE as f32).round() as isize;
    let reference = synthesize_channel_reference(&channel_symbols, success.candidate.freq_hz);
    let frame_len = reference.len();
    let mut envelope = vec![Complex32::new(0.0, 0.0); LONG_INPUT_SAMPLES];
    for (offset, sample) in reference.iter().enumerate() {
        let index = start_sample + offset as isize;
        if index < 0 || index as usize >= audio.samples.len() {
            continue;
        }
        envelope[offset] = sample.conj() * audio.samples[index as usize];
    }

    plan.forward.process(&mut envelope);
    for (value, filter) in envelope.iter_mut().zip(&plan.filter_spectrum) {
        *value *= *filter;
    }
    plan.inverse.process(&mut envelope);
    let scale = 1.0 / LONG_INPUT_SAMPLES as f32;
    for value in &mut envelope {
        *value *= scale;
    }

    for offset in 0..frame_len {
        let index = start_sample + offset as isize;
        if index < 0 || index as usize >= audio.samples.len() {
            continue;
        }
        let edge = offset.min(frame_len - 1 - offset);
        let correction = if edge < plan.edge_correction.len() {
            plan.edge_correction[edge]
        } else {
            1.0
        };
        let coeff = envelope[offset] * correction;
        audio.samples[index as usize] -= 2.0 * (coeff * reference[offset]).re;
    }
}

fn try_candidate(
    search_grid: SearchGrid,
    long_spectrum: &LongSpectrum,
    baseband_plan: &BasebandPlan,
    candidate: &DecodeCandidate,
    parity: &ParityMatrix,
    allow_ap: bool,
    counters: &mut DecodeCounters,
) -> Option<SuccessfulDecode> {
    let base_bin =
        (candidate.freq_hz / FT8_TONE_SPACING_HZ).round() as isize - search_grid.min_bin as isize;
    let hop_seconds = HOP_SAMPLES as f32 / FT8_SAMPLE_RATE as f32;

    let mut best: Option<SuccessfulDecode> = None;
    let mut refined_basebands = Vec::<(i32, Vec<Complex32>)>::new();
    for bin_delta in -2..=2 {
        let candidate_bin = base_bin + bin_delta;
        if candidate_bin < 0 {
            continue;
        }
        let candidate_bin = candidate_bin as usize;
        if candidate_bin + 7 >= search_grid.usable_bins {
            continue;
        }
        let coarse_freq_hz = (search_grid.min_bin + candidate_bin) as f32 * FT8_TONE_SPACING_HZ;
        let Some(initial_baseband) = downsample_candidate(long_spectrum, baseband_plan, coarse_freq_hz)
        else {
            continue;
        };
        for frame_delta in -2..=2 {
            let coarse_start_seconds =
                candidate.start_seconds + frame_delta as f32 * hop_seconds;
            if let Some(refined) = refine_candidate_with_cache(
                long_spectrum,
                baseband_plan,
                &initial_baseband,
                &mut refined_basebands,
                coarse_start_seconds,
                coarse_freq_hz,
            ) {
                let mut refined_hit = false;
                for llrs in &refined.llr_sets {
                    let Some((payload, bits, iterations)) =
                        decode_llr_set(parity, llrs, counters)
                    else {
                        continue;
                    };

                    let success = SuccessfulDecode {
                        payload,
                        codeword_bits: bits,
                        candidate: DecodeCandidate {
                            start_seconds: refined.start_seconds,
                            dt_seconds: refined.start_seconds - 0.5,
                            freq_hz: refined.freq_hz,
                            score: refined.sync_score.max(candidate.score),
                        },
                        ldpc_iterations: iterations,
                        snr_db: refined.snr_db,
                    };
                    match &best {
                        Some(existing) if existing.candidate.score >= success.candidate.score => {}
                        _ => best = Some(success),
                    }
                    refined_hit = true;
                    break;
                }

                if allow_ap && !refined_hit && best.is_none() {
                    let ap_magnitude = refined.llr_sets[0]
                        .iter()
                        .map(|value| value.abs())
                        .fold(0.0f32, f32::max)
                        * 1.01;
                    if ap_magnitude > 0.0 {
                        for known_bits in [cq_ap_known_bits(), mycall_ap_known_bits()] {
                            let ap_llrs = llrs_with_known_bits(
                                &refined.llr_sets[0],
                                known_bits,
                                ap_magnitude,
                            );
                            if let Some((payload, bits, iterations)) = decode_llr_set_with_known_bits(
                                parity,
                                &ap_llrs,
                                known_bits,
                                counters,
                            ) {
                                best = Some(SuccessfulDecode {
                                    payload,
                                    codeword_bits: bits,
                                    candidate: DecodeCandidate {
                                        start_seconds: refined.start_seconds,
                                        dt_seconds: refined.start_seconds - 0.5,
                                        freq_hz: refined.freq_hz,
                                        score: refined.sync_score.max(candidate.score),
                                    },
                                    ldpc_iterations: iterations,
                                    snr_db: refined.snr_db,
                                });
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
    best
}

fn refine_candidate(
    long_spectrum: &LongSpectrum,
    baseband_plan: &BasebandPlan,
    coarse_start_seconds: f32,
    coarse_freq_hz: f32,
) -> Option<RefinedCandidate> {
    let initial_baseband = downsample_candidate(long_spectrum, baseband_plan, coarse_freq_hz)?;
    let mut refined_basebands = Vec::new();
    refine_candidate_with_cache(
        long_spectrum,
        baseband_plan,
        &initial_baseband,
        &mut refined_basebands,
        coarse_start_seconds,
        coarse_freq_hz,
    )
}

fn refine_candidate_with_cache(
    long_spectrum: &LongSpectrum,
    baseband_plan: &BasebandPlan,
    initial_baseband: &[Complex32],
    refined_basebands: &mut Vec<(i32, Vec<Complex32>)>,
    coarse_start_seconds: f32,
    coarse_freq_hz: f32,
) -> Option<RefinedCandidate> {
    let mut ibest = ((coarse_start_seconds * BASEBAND_RATE_HZ).round()) as isize;
    let mut best_score = f32::NEG_INFINITY;
    for idt in (ibest - 10)..=(ibest + 10) {
        let sync_score = sync8d(initial_baseband, idt, 0.0);
        if sync_score > best_score {
            best_score = sync_score;
            ibest = idt;
        }
    }

    let mut best_freq_hz = coarse_freq_hz;
    best_score = f32::NEG_INFINITY;
    for ifr in -5..=5 {
        let residual_hz = ifr as f32 * 0.5;
        let sync_score = sync8d(initial_baseband, ibest, residual_hz);
        if sync_score > best_score {
            best_score = sync_score;
            best_freq_hz = coarse_freq_hz + residual_hz;
        }
    }

    let refined_baseband = cached_refined_baseband(
        long_spectrum,
        baseband_plan,
        refined_basebands,
        best_freq_hz,
    )?;
    let mut refined_ibest = ibest;
    best_score = f32::NEG_INFINITY;
    for delta in -4..=4 {
        let sync_score = sync8d(refined_baseband, ibest + delta, 0.0);
        if sync_score > best_score {
            best_score = sync_score;
            refined_ibest = ibest + delta;
        }
    }

    let full_tones = extract_symbol_tones(refined_baseband, refined_ibest);
    if sync_quality(&full_tones) <= 6 {
        return None;
    }
    let llr_sets = compute_bitmetric_passes(&full_tones);
    let start_seconds = (refined_ibest as f32 - 1.0) / BASEBAND_RATE_HZ;
    Some(RefinedCandidate {
        start_seconds,
        freq_hz: best_freq_hz,
        sync_score: best_score,
        snr_db: estimate_snr_db(&full_tones),
        llr_sets,
    })
}

fn extract_candidate_at(
    long_spectrum: &LongSpectrum,
    baseband_plan: &BasebandPlan,
    start_seconds: f32,
    freq_hz: f32,
) -> Option<RefinedCandidate> {
    let baseband = downsample_candidate(long_spectrum, baseband_plan, freq_hz)?;
    let start_index = (start_seconds * BASEBAND_RATE_HZ).round() as isize;
    let full_tones = extract_symbol_tones(&baseband, start_index);
    if sync_quality(&full_tones) <= 6 {
        return None;
    }
    let llr_sets = compute_bitmetric_passes(&full_tones);
    Some(RefinedCandidate {
        start_seconds,
        freq_hz,
        sync_score: sync8d(&baseband, start_index, 0.0),
        snr_db: estimate_snr_db(&full_tones),
        llr_sets,
    })
}

fn cached_refined_baseband<'a>(
    long_spectrum: &LongSpectrum,
    baseband_plan: &BasebandPlan,
    cache: &'a mut Vec<(i32, Vec<Complex32>)>,
    freq_hz: f32,
) -> Option<&'a [Complex32]> {
    let key = (freq_hz * 16.0).round() as i32;
    if let Some(index) = cache.iter().position(|(cached_key, _)| *cached_key == key) {
        return Some(cache[index].1.as_slice());
    }
    let baseband = downsample_candidate(long_spectrum, baseband_plan, freq_hz)?;
    cache.push((key, baseband));
    cache.last().map(|(_, baseband)| baseband.as_slice())
}

fn build_long_spectrum(audio: &AudioBuffer) -> LongSpectrum {
    let plan = LongSpectrumPlan::global();
    let fft = &plan.forward;
    let mut input = fft.make_input_vec();
    let mut spectrum = fft.make_output_vec();

    let usable = audio.samples.len().min(LONG_INPUT_SAMPLES);
    input[..usable].copy_from_slice(&audio.samples[..usable]);
    input[usable..].fill(0.0);

    fft.process(&mut input, &mut spectrum).expect("long fft");
    LongSpectrum { bins: spectrum }
}

impl LongSpectrumPlan {
    fn global() -> &'static Self {
        static PLAN: OnceLock<LongSpectrumPlan> = OnceLock::new();
        PLAN.get_or_init(|| {
            let mut planner = RealFftPlanner::<f32>::new();
            Self {
                forward: planner.plan_fft_forward(LONG_FFT_SAMPLES),
            }
        })
    }
}

impl Sync8Plan {
    fn global() -> &'static Self {
        static PLAN: OnceLock<Sync8Plan> = OnceLock::new();
        PLAN.get_or_init(|| {
            let mut planner = RealFftPlanner::<f32>::new();
            Self {
                forward: planner.plan_fft_forward(SYNC8_FFT_SAMPLES),
            }
        })
    }
}

impl BasebandPlan {
    fn new() -> Self {
        let mut planner = FftPlanner::<f32>::new();
        Self {
            inverse: planner.plan_fft_inverse(BASEBAND_SAMPLES),
        }
    }
}

impl SubtractionPlan {
    fn global() -> &'static Self {
        static PLAN: OnceLock<SubtractionPlan> = OnceLock::new();
        PLAN.get_or_init(Self::new)
    }

    fn new() -> Self {
        let mut planner = FftPlanner::<f32>::new();
        let forward = planner.plan_fft_forward(LONG_INPUT_SAMPLES);
        let inverse = planner.plan_fft_inverse(LONG_INPUT_SAMPLES);

        let mut window = Vec::with_capacity(SUBTRACT_FILTER_SAMPLES + 1);
        for tap in -(SUBTRACT_FILTER_HALF as isize)..=(SUBTRACT_FILTER_HALF as isize) {
            let phase =
                std::f32::consts::PI * tap as f32 / SUBTRACT_FILTER_SAMPLES as f32;
            window.push(phase.cos().powi(2));
        }
        let sumw = window.iter().copied().sum::<f32>();

        let mut kernel = vec![Complex32::new(0.0, 0.0); LONG_INPUT_SAMPLES];
        for (index, weight) in window.iter().copied().enumerate() {
            let lag = index as isize - SUBTRACT_FILTER_HALF as isize;
            let slot = if lag < 0 {
                (LONG_INPUT_SAMPLES as isize + lag) as usize
            } else {
                lag as usize
            };
            kernel[slot] = Complex32::new(weight / sumw, 0.0);
        }
        forward.process(&mut kernel);

        let mut edge_correction = Vec::with_capacity(SUBTRACT_FILTER_HALF + 1);
        for edge in 0..=SUBTRACT_FILTER_HALF {
            let first = SUBTRACT_FILTER_HALF - edge;
            let available = window[first..].iter().copied().sum::<f32>();
            edge_correction.push((sumw / available).max(1.0));
        }

        Self {
            forward,
            inverse,
            filter_spectrum: kernel,
            edge_correction,
        }
    }
}

fn downsample_candidate(
    long_spectrum: &LongSpectrum,
    baseband_plan: &BasebandPlan,
    freq_hz: f32,
) -> Option<Vec<Complex32>> {
    let i0 = (freq_hz / FFT_BIN_HZ).round() as isize;
    let fb = freq_hz - 1.5 * FT8_TONE_SPACING_HZ;
    let ft = freq_hz + 8.5 * FT8_TONE_SPACING_HZ;
    let ib = ((fb / FFT_BIN_HZ).round() as isize).max(1);
    let it = ((ft / FFT_BIN_HZ).round() as isize).min((LONG_FFT_SAMPLES / 2) as isize);
    if i0 <= 0 || ib >= it {
        return None;
    }

    let mut baseband = vec![Complex32::new(0.0, 0.0); BASEBAND_SAMPLES];
    let mut copied = 0usize;
    for bin in ib..=it {
        let index = copied;
        if index >= baseband.len() {
            break;
        }
        baseband[index] = long_spectrum.bins[bin as usize];
        copied += 1;
    }
    if copied <= BASEBAND_TAPER_LEN * 2 {
        return None;
    }

    let taper = baseband_taper();
    for index in 0..=BASEBAND_TAPER_LEN {
        baseband[index] *= taper[BASEBAND_TAPER_LEN - index];
        baseband[copied - 1 - index] *= taper[index];
    }

    let shift = (i0 - ib).max(0) as usize;
    let rotate = shift.min(baseband.len());
    baseband.rotate_left(rotate);

    baseband_plan.inverse.process(&mut baseband);
    let scale = 1.0 / (LONG_FFT_SAMPLES as f32 * BASEBAND_SAMPLES as f32).sqrt();
    for sample in &mut baseband {
        *sample *= scale;
    }
    Some(baseband)
}

fn sync8_score(
    symbol_power: &[f32],
    nhsym: usize,
    bin: usize,
    lag: isize,
    nominal_start: isize,
) -> f32 {
    let row_len = SYNC8_FFT_SAMPLES / 2 + 1;
    let mut ta = 0.0f32;
    let mut tb = 0.0f32;
    let mut tc = 0.0f32;
    let mut t0a = 0.0f32;
    let mut t0b = 0.0f32;
    let mut t0c = 0.0f32;

    for (offset, costas) in FT8_COSTAS.iter().copied().enumerate() {
        let m = lag + nominal_start + (offset as isize * 4);
        if (0..nhsym as isize).contains(&m) {
            let row = m as usize * row_len;
            ta += symbol_power[row + bin + 2 * costas];
            for tone in 0..7 {
                t0a += symbol_power[row + bin + 2 * tone];
            }
        }

        let mb = m + 36 * 4;
        if (0..nhsym as isize).contains(&mb) {
            let row = mb as usize * row_len;
            tb += symbol_power[row + bin + 2 * costas];
            for tone in 0..7 {
                t0b += symbol_power[row + bin + 2 * tone];
            }
        }

        let mc = m + 72 * 4;
        if (0..nhsym as isize).contains(&mc) {
            let row = mc as usize * row_len;
            tc += symbol_power[row + bin + 2 * costas];
            for tone in 0..7 {
                t0c += symbol_power[row + bin + 2 * tone];
            }
        }
    }

    let score_abc = ratio_sync_score(ta + tb + tc, t0a + t0b + t0c);
    let score_bc = ratio_sync_score(tb + tc, t0b + t0c);
    score_abc.max(score_bc)
}

fn ratio_sync_score(signal: f32, band_total: f32) -> f32 {
    let noise = (band_total - signal) / 6.0;
    if noise > 0.0 {
        signal / noise
    } else {
        0.0
    }
}

fn normalize_sync_scores(scores: &mut [(usize, isize, f32)]) {
    let mut values: Vec<f32> = scores
        .iter()
        .map(|&(_, _, score)| score)
        .filter(|score| score.is_finite())
        .collect();
    if values.is_empty() {
        return;
    }
    values.sort_by(|left, right| left.total_cmp(right));
    let percentile = ((values.len() as f32 * 0.40).round() as usize).clamp(1, values.len()) - 1;
    let baseline = values[percentile].max(1e-6);
    for (_, _, score) in scores.iter_mut() {
        *score /= baseline;
    }
}

fn sync8d(baseband: &[Complex32], start_index: isize, residual_hz: f32) -> f32 {
    let mut sync = 0.0f32;
    let residual = (residual_hz != 0.0).then(|| residual_tweak(residual_hz));
    for (offset, tone) in crate::protocol::FT8_COSTAS.iter().copied().enumerate() {
        for block in [0usize, 36, 72] {
            let symbol_start = start_index + ((block + offset) * BASEBAND_SYMBOL_SAMPLES) as isize;
            if symbol_start < 0
                || symbol_start as usize + BASEBAND_SYMBOL_SAMPLES > baseband.len()
            {
                continue;
            }
            let segment = &baseband[symbol_start as usize..symbol_start as usize + BASEBAND_SYMBOL_SAMPLES];
            let corr = match &residual {
                Some(residual) => correlate_tone_residual(segment, tone, residual),
                None => correlate_tone_nominal(segment, tone),
            };
            sync += corr.norm_sqr();
        }
    }
    sync
}

fn extract_symbol_tones(baseband: &[Complex32], start_index: isize) -> Vec<[Complex32; 8]> {
    let mut tones = vec![[Complex32::new(0.0, 0.0); 8]; FT8_MESSAGE_SYMBOLS];
    for (symbol_index, symbol_tones) in tones.iter_mut().enumerate() {
        let sample_index = start_index + (symbol_index * BASEBAND_SYMBOL_SAMPLES) as isize;
        if sample_index < 0
            || sample_index as usize + BASEBAND_SYMBOL_SAMPLES > baseband.len()
        {
            continue;
        }
        let segment =
            &baseband[sample_index as usize..sample_index as usize + BASEBAND_SYMBOL_SAMPLES];
        for (tone, slot) in symbol_tones.iter_mut().enumerate() {
            *slot = correlate_tone_nominal(segment, tone);
        }
    }
    tones
}

fn compute_bitmetric_passes(full_tones: &[[Complex32; 8]]) -> [Vec<f32>; 4] {
    let mut bmeta = vec![0.0f32; 174];
    let mut bmetb = vec![0.0f32; 174];
    let mut bmetc = vec![0.0f32; 174];
    let mut bmetd = vec![0.0f32; 174];
    let graymap = [0usize, 1, 3, 2, 5, 6, 4, 7];

    for nsym in 1..=3 {
        let nt = 1usize << (3 * nsym);
        let ibmax = match nsym {
            1 => 2,
            2 => 5,
            3 => 8,
            _ => unreachable!(),
        };
        for half in 0..2 {
            for k in (1..=29).step_by(nsym) {
                let ks = if half == 0 { k + 6 } else { k + 42 };
                let start_bit = (k - 1) * 3 + half * 87;
                let mut metrics = vec![0.0f32; nt];
                for (i, metric) in metrics.iter_mut().enumerate() {
                    let tone0 = graymap[i & 0b111];
                    *metric = full_tones[ks][tone0].norm();
                    if nsym >= 2 {
                        let tone1 = graymap[(i >> 3) & 0b111];
                        *metric = (full_tones[ks][tone0] + full_tones[ks + 1][tone1]).norm();
                    }
                    if nsym >= 3 {
                        let tone2 = graymap[(i >> 6) & 0b111];
                        *metric = (full_tones[ks][tone0]
                            + full_tones[ks + 1][graymap[(i >> 3) & 0b111]]
                            + full_tones[ks + 2][tone2])
                            .norm();
                    }
                }

                for ib in 0..=ibmax {
                    let target_bit = start_bit + ib;
                    if target_bit >= 174 {
                        continue;
                    }
                    let decision_bit = ibmax - ib;
                    let mut best_one = f32::NEG_INFINITY;
                    let mut best_zero = f32::NEG_INFINITY;
                    for (value, metric) in metrics.iter().enumerate() {
                        if ((value >> decision_bit) & 1) == 1 {
                            best_one = best_one.max(*metric);
                        } else {
                            best_zero = best_zero.max(*metric);
                        }
                    }
                    let bm = best_one - best_zero;
                    match nsym {
                        1 => {
                            bmeta[target_bit] = bm;
                            let denominator = best_one.max(best_zero);
                            bmetd[target_bit] = if denominator > 0.0 {
                                bm / denominator
                            } else {
                                0.0
                            };
                        }
                        2 => bmetb[target_bit] = bm,
                        3 => bmetc[target_bit] = bm,
                        _ => unreachable!(),
                    }
                }
            }
        }
    }

    normalize_metric_vector(&mut bmeta);
    normalize_metric_vector(&mut bmetb);
    normalize_metric_vector(&mut bmetc);
    normalize_metric_vector(&mut bmetd);

    const SCALE_FACTOR: f32 = 2.83;
    for metric_set in [&mut bmeta, &mut bmetb, &mut bmetc, &mut bmetd] {
        for value in metric_set.iter_mut() {
            *value *= SCALE_FACTOR;
        }
    }

    [bmeta, bmetb, bmetc, bmetd]
}

fn decode_llr_set(
    parity: &ParityMatrix,
    llrs: &[f32],
    counters: &mut DecodeCounters,
) -> Option<(Payload, Vec<u8>, usize)> {
    let Some((bits, iterations)) = parity.decode(llrs) else {
        return None;
    };
    if bits.iter().all(|bit| *bit == 0) {
        return None;
    }
    counters.ldpc_codewords += 1;
    let Some(payload) = unpack_message(&bits) else {
        return None;
    };
    counters.parsed_payloads += 1;
    Some((payload, bits, iterations))
}

fn decode_llr_set_with_known_bits(
    parity: &ParityMatrix,
    llrs: &[f32],
    known_bits: &[Option<u8>],
    counters: &mut DecodeCounters,
) -> Option<(Payload, Vec<u8>, usize)> {
    let Some((bits, iterations)) = parity.decode_with_known_bits(llrs, known_bits) else {
        return None;
    };
    if bits.iter().all(|bit| *bit == 0) {
        return None;
    }
    counters.ldpc_codewords += 1;
    let Some(payload) = unpack_message(&bits) else {
        return None;
    };
    counters.parsed_payloads += 1;
    Some((payload, bits, iterations))
}

fn cq_ap_known_bits() -> &'static [Option<u8>] {
    static BITS: OnceLock<Vec<Option<u8>>> = OnceLock::new();
    BITS.get_or_init(|| {
        let frame = crate::encode::encode_standard_message("CQ", "K1ABC", false, &GridReport::Blank)
            .expect("encode CQ AP template");
        let mut known = vec![None; 174];
        for index in 0..29 {
            known[index] = Some(frame.message_bits[index]);
        }
        for index in 74..77 {
            known[index] = Some(frame.message_bits[index]);
        }
        known
    })
}

fn mycall_ap_known_bits() -> &'static [Option<u8>] {
    static BITS: OnceLock<Vec<Option<u8>>> = OnceLock::new();
    BITS.get_or_init(|| {
        let frame = crate::encode::encode_standard_message(
            "K1ABC",
            "KA1ABC",
            false,
            &GridReport::Reply(crate::message::ReplyWord::Rrr),
        )
        .expect("encode MyCall AP template");
        let mut known = vec![None; 174];
        for index in 0..29 {
            known[index] = Some(frame.message_bits[index]);
        }
        for index in 74..77 {
            known[index] = Some(frame.message_bits[index]);
        }
        known
    })
}

fn llrs_with_known_bits(llrs: &[f32], known_bits: &[Option<u8>], magnitude: f32) -> Vec<f32> {
    let mut constrained = llrs.to_vec();
    for (slot, bit) in constrained.iter_mut().zip(known_bits.iter().copied()) {
        let Some(bit) = bit else {
            continue;
        };
        *slot = if bit == 1 { magnitude } else { -magnitude };
    }
    constrained
}

fn baseband_taper() -> &'static [f32] {
    static TAPER: OnceLock<Vec<f32>> = OnceLock::new();
    TAPER.get_or_init(|| {
        (0..=BASEBAND_TAPER_LEN)
            .map(|index| {
                0.5
                    * (1.0
                        + (index as f32 * std::f32::consts::PI / BASEBAND_TAPER_LEN as f32).cos())
            })
            .collect()
    })
}

fn correlate_tone_nominal(segment: &[Complex32], tone: usize) -> Complex32 {
    let basis = tone_basis();
    let mut acc = Complex32::new(0.0, 0.0);
    for (index, sample) in segment.iter().copied().enumerate() {
        acc += sample * basis[tone][index];
    }
    acc
}

fn correlate_tone_residual(
    segment: &[Complex32],
    tone: usize,
    residual: &[Complex32; BASEBAND_SYMBOL_SAMPLES],
) -> Complex32 {
    let basis = tone_basis();
    let mut acc = Complex32::new(0.0, 0.0);
    for (index, sample) in segment.iter().copied().enumerate() {
        acc += sample * basis[tone][index] * residual[index];
    }
    acc
}

fn tone_basis() -> &'static [[Complex32; BASEBAND_SYMBOL_SAMPLES]; 8] {
    static BASIS: OnceLock<[[Complex32; BASEBAND_SYMBOL_SAMPLES]; 8]> = OnceLock::new();
    BASIS.get_or_init(|| {
        std::array::from_fn(|tone| {
            std::array::from_fn(|index| {
                let phase = -2.0
                    * std::f32::consts::PI
                    * tone as f32
                    * index as f32
                    / BASEBAND_SYMBOL_SAMPLES as f32;
                Complex32::new(phase.cos(), phase.sin())
            })
        })
    })
}

fn residual_tweak(residual_hz: f32) -> [Complex32; BASEBAND_SYMBOL_SAMPLES] {
    std::array::from_fn(|index| {
        let phase =
            -2.0 * std::f32::consts::PI * residual_hz * index as f32 / BASEBAND_RATE_HZ;
        Complex32::new(phase.cos(), phase.sin())
    })
}

fn sync_quality(full_tones: &[[Complex32; 8]]) -> usize {
    let mut matches = 0usize;
    for (offset, expected_tone) in FT8_COSTAS.iter().copied().enumerate() {
        for block in [0usize, 36, 72] {
            let symbol = &full_tones[block + offset];
            let best_tone = symbol
                .iter()
                .enumerate()
                .max_by(|left, right| left.1.norm_sqr().total_cmp(&right.1.norm_sqr()))
                .map(|(index, _)| index)
                .unwrap_or(0);
            if best_tone == expected_tone {
                matches += 1;
            }
        }
    }
    matches
}

fn normalize_metric_vector(values: &mut [f32]) {
    let mean = values.iter().copied().sum::<f32>() / values.len() as f32;
    let second = values.iter().map(|value| value * value).sum::<f32>() / values.len() as f32;
    let variance = second - mean * mean;
    let sigma = if variance > 0.0 { variance.sqrt() } else { second.sqrt() };
    if sigma > 0.0 {
        for value in values {
            *value /= sigma;
        }
    }
}

fn estimate_snr_db(full_tones: &[[Complex32; 8]]) -> i32 {
    let mut maxima = Vec::with_capacity(crate::protocol::FT8_DATA_POSITIONS.len());
    let mut all = Vec::with_capacity(crate::protocol::FT8_DATA_POSITIONS.len() * 8);
    for &symbol_index in crate::protocol::FT8_DATA_POSITIONS.iter() {
        let symbol = &full_tones[symbol_index];
        let max = symbol
            .iter()
            .map(|tone| tone.norm_sqr())
            .fold(f32::NEG_INFINITY, f32::max);
        maxima.push(max);
        all.extend(symbol.iter().map(|tone| tone.norm_sqr()));
    }
    all.sort_by(|left, right| left.total_cmp(right));
    let noise = all[all.len() / 2].max(1e-6);
    let signal = maxima.iter().copied().sum::<f32>() / maxima.len() as f32;
    (10.0 * ((signal / noise).max(1e-6)).log10() - 24.0).round() as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "diagnostic"]
    fn debug_known_real_candidate() {
        let audio = crate::wave::load_wav(
            "/Users/bgelb/ft8-regr/artifacts/samples/kgoba-ft8-lib/191111_110115/191111_110115.wav",
        )
        .expect("wav");
        let spectrum = build_long_spectrum(&audio);
        let baseband_plan = BasebandPlan::new();
        let refined = refine_candidate(&spectrum, &baseband_plan, 1.4, 1234.0).expect("refined");
        eprintln!(
            "refined start={:.4} dt={:.4} freq={:.4} sync={:.3} snr={}",
            refined.start_seconds,
            refined.start_seconds - 0.5,
            refined.freq_hz,
            refined.sync_score,
            refined.snr_db
        );
        let parity = ParityMatrix::global();
        for (index, llrs) in refined.llr_sets.iter().enumerate() {
            let mean_abs = llrs.iter().map(|value| value.abs()).sum::<f32>() / llrs.len() as f32;
            let max_abs = llrs.iter().map(|value| value.abs()).fold(0.0f32, f32::max);
            let success = parity.decode(llrs);
            eprintln!(
                "pass={} mean_abs={:.3} max_abs={:.3} decode={}",
                index + 1,
                mean_abs,
                max_abs,
                success.is_some()
            );
        }
    }
}
