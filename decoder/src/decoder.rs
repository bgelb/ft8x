use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, OnceLock};

use num_complex::Complex32;
use rayon::prelude::*;
use realfft::RealFftPlanner;
use rustfft::{Fft, FftPlanner};
use serde::Serialize;

use crate::encode::channel_symbols_from_codeword_bits;
use crate::ldpc::ParityMatrix;
use crate::message::{DecodedPayload, HashResolver, Payload, unpack_message};
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
            max_freq_hz: 3_000.0,
            max_candidates: 192,
            max_successes: 64,
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

#[derive(Debug)]
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
    data_tones: Vec<[Complex32; 8]>,
    llr_sets: [Vec<f32>; 4],
    snr_db: i32,
}

#[derive(Default)]
struct DecodeCounters {
    ldpc_codewords: usize,
    parsed_payloads: usize,
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

struct BasebandPlan {
    inverse: Arc<dyn Fft<f32>>,
}

const LONG_INPUT_SAMPLES: usize = 15 * FT8_SAMPLE_RATE as usize;
const LONG_FFT_SAMPLES: usize = 192_000;
const DOWNSAMPLE_FACTOR: usize = 60;
const BASEBAND_RATE_HZ: f32 = FT8_SAMPLE_RATE as f32 / DOWNSAMPLE_FACTOR as f32;
const BASEBAND_SAMPLES: usize = LONG_FFT_SAMPLES / DOWNSAMPLE_FACTOR;
const BASEBAND_SYMBOL_SAMPLES: usize = FT8_SYMBOL_SAMPLES / DOWNSAMPLE_FACTOR;
const FFT_BIN_HZ: f32 = FT8_SAMPLE_RATE as f32 / LONG_FFT_SAMPLES as f32;
const BASEBAND_TAPER_LEN: usize = 100;

pub fn decode_wav_file(
    path: impl AsRef<Path>,
    options: &DecodeOptions,
) -> Result<DecodeReport, DecoderError> {
    let audio = load_wav(path)?;
    decode_pcm(&audio, options)
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

    let mut spectrogram = build_spectrogram(audio, options);
    let long_spectrum = build_long_spectrum(audio);
    let baseband_plan = BasebandPlan::new();
    let parity = ParityMatrix::global();
    let mut top_candidates = Vec::new();
    let mut counters = DecodeCounters::default();

    let mut successes = Vec::<SuccessfulDecode>::new();
    for pass in 0..options.search_passes.max(1) {
        let candidates = collect_candidates(&spectrogram, options);
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
                    &spectrogram,
                    &long_spectrum,
                    &baseband_plan,
                    candidate,
                    parity,
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

        for success in &pass_successes {
            suppress_candidate(&mut spectrogram, success);
        }
        successes.extend(pass_successes);
    }

    let mut resolver = HashResolver::default();
    for success in &successes {
        success.payload.collect_callsigns(&mut resolver);
    }

    let mut dedup = BTreeMap::<String, DecodedMessage>::new();
    for success in successes {
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
            frame_count: spectrogram.frame_count,
            usable_bins: spectrogram.usable_bins,
            examined_candidates: options.max_candidates,
            accepted_candidates: decodes.len(),
            ldpc_codewords: counters.ldpc_codewords,
            parsed_payloads: counters.parsed_payloads,
            top_candidates,
        },
        decodes,
    })
}

fn build_spectrogram(audio: &AudioBuffer, options: &DecodeOptions) -> Spectrogram {
    let min_bin = (options.min_freq_hz / FT8_TONE_SPACING_HZ).floor().max(0.0) as usize;
    let max_bin = (options.max_freq_hz / FT8_TONE_SPACING_HZ).ceil() as usize + 7;
    let usable_bins = max_bin.saturating_sub(min_bin) + 1;
    let frame_count = (audio.samples.len().saturating_sub(FT8_SYMBOL_SAMPLES) / HOP_SAMPLES) + 1;

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

fn collect_candidates(spectrogram: &Spectrogram, options: &DecodeOptions) -> Vec<DecodeCandidate> {
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

fn suppress_candidate(spectrogram: &mut Spectrogram, success: &SuccessfulDecode) {
    let start_frame =
        ((success.candidate.start_seconds * FT8_SAMPLE_RATE as f32) / HOP_SAMPLES as f32).round() as isize;
    let base_bin = (success.candidate.freq_hz / FT8_TONE_SPACING_HZ).round() as isize
        - spectrogram.min_bin as isize;
    let Some(channel_symbols) = channel_symbols_from_codeword_bits(&success.codeword_bits) else {
        return;
    };
    for (symbol_index, tone) in channel_symbols.into_iter().enumerate() {
        let frame = start_frame + (symbol_index * HOPS_PER_SYMBOL) as isize;
        let bin = base_bin + tone as isize;
        for frame_delta in -1..=1 {
            for bin_delta in -2..=2 {
                let frame_index = frame + frame_delta;
                let bin_index = bin + bin_delta;
                if frame_index < 0
                    || bin_index < 0
                    || frame_index as usize >= spectrogram.frame_count
                    || bin_index as usize >= spectrogram.usable_bins
                {
                    continue;
                }
                let row = frame_index as usize * spectrogram.usable_bins;
                spectrogram.bins[row + bin_index as usize] = 0.0;
            }
        }
    }
}

fn try_candidate(
    spectrogram: &Spectrogram,
    long_spectrum: &LongSpectrum,
    baseband_plan: &BasebandPlan,
    candidate: &DecodeCandidate,
    parity: &ParityMatrix,
    counters: &mut DecodeCounters,
) -> Option<SuccessfulDecode> {
    let start_frame =
        ((candidate.start_seconds * FT8_SAMPLE_RATE as f32) / HOP_SAMPLES as f32).round() as isize;
    let base_bin =
        (candidate.freq_hz / FT8_TONE_SPACING_HZ).round() as isize - spectrogram.min_bin as isize;

    let mut best: Option<SuccessfulDecode> = None;
    for frame_delta in -2..=2 {
        for bin_delta in -2..=2 {
            let candidate_frame = start_frame + frame_delta;
            let candidate_bin = base_bin + bin_delta;
            if candidate_frame < 0 || candidate_bin < 0 {
                continue;
            }
            let candidate_frame = candidate_frame as usize;
            let candidate_bin = candidate_bin as usize;
            if candidate_bin + 7 >= spectrogram.usable_bins {
                continue;
            }
            if candidate_frame + (FT8_MESSAGE_SYMBOLS - 1) * HOPS_PER_SYMBOL
                >= spectrogram.frame_count
            {
                continue;
            }

            let coarse_freq_hz = (spectrogram.min_bin + candidate_bin) as f32 * FT8_TONE_SPACING_HZ;
            let coarse_start_seconds = candidate_frame as f32 * HOP_SAMPLES as f32 / FT8_SAMPLE_RATE as f32;
            let Some(refined) = refine_candidate(
                long_spectrum,
                baseband_plan,
                coarse_start_seconds,
                coarse_freq_hz,
            ) else {
                continue;
            };

            for llrs in &refined.llr_sets {
                let Some((payload, bits, iterations)) = decode_llr_set(parity, llrs, counters) else {
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
    let mut ibest = ((coarse_start_seconds * BASEBAND_RATE_HZ).round()) as isize;
    let mut best_score = f32::NEG_INFINITY;
    for idt in (ibest - 20)..=(ibest + 20) {
        let sync_score = sync8d(&initial_baseband, idt, 0.0);
        if sync_score > best_score {
            best_score = sync_score;
            ibest = idt;
        }
    }

    let mut best_freq_hz = coarse_freq_hz;
    best_score = f32::NEG_INFINITY;
    for ifr in -16..=16 {
        let residual_hz = ifr as f32 * 0.25;
        let sync_score = sync8d(&initial_baseband, ibest, residual_hz);
        if sync_score > best_score {
            best_score = sync_score;
            best_freq_hz = coarse_freq_hz + residual_hz;
        }
    }

    let refined_baseband = downsample_candidate(long_spectrum, baseband_plan, best_freq_hz)?;
    let mut refined_ibest = ibest;
    best_score = f32::NEG_INFINITY;
    for delta in -8..=8 {
        let sync_score = sync8d(&refined_baseband, ibest + delta, 0.0);
        if sync_score > best_score {
            best_score = sync_score;
            refined_ibest = ibest + delta;
        }
    }

    let full_tones = extract_symbol_tones(&refined_baseband, refined_ibest);
    if sync_quality(&full_tones) <= 6 {
        return None;
    }
    let data_tones: Vec<[Complex32; 8]> = crate::protocol::FT8_DATA_POSITIONS
        .iter()
        .map(|&symbol_index| full_tones[symbol_index])
        .collect();
    let llr_sets = compute_bitmetric_passes(&full_tones);
    let start_seconds = refined_ibest.max(0) as f32 / BASEBAND_RATE_HZ;
    Some(RefinedCandidate {
        start_seconds,
        freq_hz: best_freq_hz,
        sync_score: best_score,
        snr_db: estimate_snr_db(&data_tones),
        data_tones,
        llr_sets,
    })
}

fn build_long_spectrum(audio: &AudioBuffer) -> LongSpectrum {
    let mut planner = RealFftPlanner::<f32>::new();
    let fft = planner.plan_fft_forward(LONG_FFT_SAMPLES);
    let mut input = fft.make_input_vec();
    let mut spectrum = fft.make_output_vec();

    let usable = audio.samples.len().min(LONG_INPUT_SAMPLES);
    input[..usable].copy_from_slice(&audio.samples[..usable]);
    input[usable..].fill(0.0);

    fft.process(&mut input, &mut spectrum).expect("long fft");
    LongSpectrum { bins: spectrum }
}

impl BasebandPlan {
    fn new() -> Self {
        let mut planner = FftPlanner::<f32>::new();
        Self {
            inverse: planner.plan_fft_inverse(BASEBAND_SAMPLES),
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

fn sync8d(baseband: &[Complex32], start_index: isize, residual_hz: f32) -> f32 {
    let mut sync = 0.0f32;
    for (offset, tone) in crate::protocol::FT8_COSTAS.iter().copied().enumerate() {
        for block in [0usize, 36, 72] {
            let symbol_start = start_index + ((block + offset) * BASEBAND_SYMBOL_SAMPLES) as isize;
            if symbol_start < 0
                || symbol_start as usize + BASEBAND_SYMBOL_SAMPLES > baseband.len()
            {
                continue;
            }
            let segment = &baseband[symbol_start as usize..symbol_start as usize + BASEBAND_SYMBOL_SAMPLES];
            let corr = correlate_tone(segment, tone, residual_hz);
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
            *slot = correlate_tone(segment, tone, 0.0);
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

fn correlate_tone(segment: &[Complex32], tone: usize, residual_hz: f32) -> Complex32 {
    let basis = tone_basis();
    let residual = residual_tweak(residual_hz);
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

fn estimate_snr_db(symbols: &[[Complex32; 8]]) -> i32 {
    let mut maxima = Vec::with_capacity(symbols.len());
    let mut all = Vec::with_capacity(symbols.len() * 8);
    for symbol in symbols {
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
