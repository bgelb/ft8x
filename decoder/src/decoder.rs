use std::collections::BTreeMap;
use std::path::Path;

use realfft::RealFftPlanner;
use serde::Serialize;

use crate::ldpc::ParityMatrix;
use crate::message::{DecodedPayload, HashResolver, Payload, unpack_message};
use crate::protocol::{
    FT8_MESSAGE_SYMBOLS, FT8_SAMPLE_RATE, FT8_SYMBOL_SAMPLES, FT8_TONE_SPACING_HZ, HOP_SAMPLES,
    HOPS_PER_SYMBOL, all_costas_positions,
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
            max_candidates: 48,
            max_successes: 32,
            search_passes: 1,
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
    candidate: DecodeCandidate,
    ldpc_iterations: usize,
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

    let spectrogram = build_spectrogram(audio, options);
    let mut candidates = collect_candidates(&spectrogram, options);
    let parity = ParityMatrix::global();
    let top_candidates = candidates.clone();
    let mut counters = DecodeCounters::default();

    let mut successes = Vec::<SuccessfulDecode>::new();
    for candidate in candidates.drain(..) {
        if successes.len() >= options.max_successes {
            break;
        }
        if let Some(success) = try_candidate(audio, &spectrogram, &candidate, parity, &mut counters)
        {
            successes.push(success);
        }
    }

    let mut resolver = HashResolver::default();
    for success in &successes {
        success.payload.collect_callsigns(&mut resolver);
    }

    let mut dedup = BTreeMap::<String, DecodedMessage>::new();
    for success in successes {
        let payload = success.payload.render(&resolver);
        let text = payload.text.clone();
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

fn try_candidate(
    audio: &AudioBuffer,
    spectrogram: &Spectrogram,
    candidate: &DecodeCandidate,
    parity: &ParityMatrix,
    counters: &mut DecodeCounters,
) -> Option<SuccessfulDecode> {
    const BIT_ORDERS: [[usize; 3]; 6] = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];

    let start_frame =
        ((candidate.start_seconds * FT8_SAMPLE_RATE as f32) / HOP_SAMPLES as f32).round() as isize;
    let base_bin =
        (candidate.freq_hz / FT8_TONE_SPACING_HZ).round() as isize - spectrogram.min_bin as isize;

    let mut best: Option<SuccessfulDecode> = None;
    for frame_delta in -1..=1 {
        for bin_delta in -1..=1 {
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

            let coarse_start_sample = candidate_frame * HOP_SAMPLES;
            let coarse_freq_hz = (spectrogram.min_bin + candidate_bin) as f32 * FT8_TONE_SPACING_HZ;
            let Some((refined_start_sample, refined_freq_hz, symbols, sync_score)) =
                refine_candidate(audio, coarse_start_sample, coarse_freq_hz)
            else {
                continue;
            };

            let symbol_bit_llrs = ParityMatrix::symbol_bit_llrs(&symbols);
            for bit_order in BIT_ORDERS {
                for invert in [false, true] {
                    let mut llrs = Vec::with_capacity(174);
                    for symbol in &symbol_bit_llrs {
                        for bit_index in bit_order {
                            let llr = if invert {
                                -symbol[bit_index]
                            } else {
                                symbol[bit_index]
                            };
                            llrs.push(llr);
                        }
                    }

                    let Some((bits, iterations)) = parity.decode(&llrs) else {
                        continue;
                    };
                    counters.ldpc_codewords += 1;
                    let Some(payload) = unpack_message(&bits) else {
                        continue;
                    };
                    counters.parsed_payloads += 1;

                    let snr_db = estimate_snr_db(&symbols);
                    let success = SuccessfulDecode {
                        payload,
                        candidate: DecodeCandidate {
                            start_seconds: refined_start_sample as f32 / FT8_SAMPLE_RATE as f32,
                            dt_seconds: refined_start_sample as f32 / FT8_SAMPLE_RATE as f32 - 0.5,
                            freq_hz: refined_freq_hz,
                            score: sync_score.max(candidate.score),
                        },
                        ldpc_iterations: iterations,
                        snr_db,
                    };
                    match &best {
                        Some(existing) if existing.candidate.score >= success.candidate.score => {}
                        _ => best = Some(success),
                    }
                }
            }
        }
    }
    best
}

fn refine_candidate(
    audio: &AudioBuffer,
    coarse_start_sample: usize,
    coarse_freq_hz: f32,
) -> Option<(usize, f32, Vec<[f32; 8]>, f32)> {
    let mut best_score = f32::NEG_INFINITY;
    let mut best_start_sample = coarse_start_sample;
    let mut best_freq_hz = coarse_freq_hz;
    let mut best_symbols = None;

    for sample_delta in (-120..=120).step_by(20) {
        let start_sample = coarse_start_sample as isize + sample_delta;
        if start_sample < 0 {
            continue;
        }
        let start_sample = start_sample as usize;
        if start_sample + FT8_MESSAGE_SYMBOLS * FT8_SYMBOL_SAMPLES >= audio.samples.len() {
            continue;
        }
        for freq_delta in [-3.125f32, -1.5625, 0.0, 1.5625, 3.125] {
            let freq_hz = coarse_freq_hz + freq_delta;
            if freq_hz < 0.0 {
                continue;
            }
            let sync_score = direct_sync_score(&audio.samples, start_sample, freq_hz);
            if sync_score <= best_score {
                continue;
            }
            let symbols = direct_data_symbol_energies(&audio.samples, start_sample, freq_hz);
            best_score = sync_score;
            best_start_sample = start_sample;
            best_freq_hz = freq_hz;
            best_symbols = Some(symbols);
        }
    }

    best_symbols.map(|symbols| (best_start_sample, best_freq_hz, symbols, best_score))
}

fn direct_sync_score(samples: &[f32], start_sample: usize, base_freq_hz: f32) -> f32 {
    let mut score = 0.0f32;
    for (symbol_index, expected_tone) in all_costas_positions() {
        let symbol_start = start_sample + symbol_index * FT8_SYMBOL_SAMPLES;
        let tones = direct_symbol_energies(samples, symbol_start, base_freq_hz);
        let band_sum = tones.iter().copied().sum::<f32>();
        score += tones[expected_tone] * 8.0 - band_sum;
    }
    score
}

fn direct_data_symbol_energies(
    samples: &[f32],
    start_sample: usize,
    base_freq_hz: f32,
) -> Vec<[f32; 8]> {
    let mut symbols = Vec::with_capacity(58);
    for &symbol_index in &crate::protocol::FT8_DATA_POSITIONS {
        let symbol_start = start_sample + symbol_index * FT8_SYMBOL_SAMPLES;
        symbols.push(direct_symbol_energies(samples, symbol_start, base_freq_hz));
    }
    symbols
}

fn direct_symbol_energies(samples: &[f32], symbol_start: usize, base_freq_hz: f32) -> [f32; 8] {
    const INTEGRATION_OFFSET: usize = 360;
    const INTEGRATION_LEN: usize = 1_200;

    let mut energies = [0.0f32; 8];
    let window = &samples
        [symbol_start + INTEGRATION_OFFSET..symbol_start + INTEGRATION_OFFSET + INTEGRATION_LEN];
    for (tone, slot) in energies.iter_mut().enumerate() {
        *slot = tone_energy(window, base_freq_hz + tone as f32 * FT8_TONE_SPACING_HZ);
    }
    energies
}

fn tone_energy(samples: &[f32], freq_hz: f32) -> f32 {
    let omega = 2.0 * std::f32::consts::PI * freq_hz / FT8_SAMPLE_RATE as f32;
    let cos_step = omega.cos();
    let sin_step = omega.sin();
    let mut cosine = 1.0f32;
    let mut sine = 0.0f32;
    let mut re = 0.0f32;
    let mut im = 0.0f32;
    for sample in samples {
        re += *sample * cosine;
        im -= *sample * sine;
        let next_cos = cosine * cos_step - sine * sin_step;
        let next_sin = sine * cos_step + cosine * sin_step;
        cosine = next_cos;
        sine = next_sin;
    }
    re.mul_add(re, im * im)
}

fn estimate_snr_db(symbols: &[[f32; 8]]) -> i32 {
    let mut maxima = Vec::with_capacity(symbols.len());
    let mut all = Vec::with_capacity(symbols.len() * 8);
    for symbol in symbols {
        let max = symbol.iter().copied().fold(f32::NEG_INFINITY, f32::max);
        maxima.push(max);
        all.extend_from_slice(symbol);
    }
    all.sort_by(|left, right| left.total_cmp(right));
    let noise = all[all.len() / 2].max(1e-6);
    let signal = maxima.iter().copied().sum::<f32>() / maxima.len() as f32;
    (10.0 * ((signal / noise).max(1e-6)).log10() - 24.0).round() as i32
}
