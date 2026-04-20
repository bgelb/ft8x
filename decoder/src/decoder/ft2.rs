use std::sync::OnceLock;

use num_complex::Complex32;
use realfft::RealFftPlanner;
use rustfft::FftPlanner;
use serde::Serialize;

use crate::coding::{
    FT2_COLUMN_COUNT, FT2_COLUMN_ROWS, FT2_INFO_BITS, FT2_ROW_COUNT, ft2_generator_rows,
};
use crate::modes::ft2::{
    FT2_BASEBAND_FFT_SAMPLES, FT2_BASEBAND_SYMBOL_SAMPLES, FT2_BP_MAX_ITERS, FT2_COARSE_FFT_BINS,
    FT2_COARSE_FFT_SAMPLES, FT2_COARSE_FRAME_COUNT, FT2_COARSE_MAX_FREQ_HZ, FT2_COARSE_MIN_FREQ_HZ,
    FT2_COARSE_STEP_SAMPLES, FT2_COARSE_THRESHOLD, FT2_DOWNSAMPLE_FACTOR,
    FT2_FINE_FREQ_SWEEP_MAX_HZ, FT2_FINE_FREQ_SWEEP_MIN_HZ, FT2_FINE_TIME_STEPS,
    FT2_LLR_NORMALIZATION, FT2_LLR_VARIANCE_SCALE, FT2_MESSAGE_SYMBOLS,
    FT2_OSD_MRB_SEARCH_EXTRA_COLUMNS, FT2_OSD_NTHETA_DEEP, FT2_OSD_NTHETA_MEDIUM,
    FT2_OSD_PARITY_TAIL_BITS, FT2_PADDED_INPUT_SAMPLES, FT2_PLATANH_CLAMP,
    FT2_PLATANH_LINEAR_LIMIT, FT2_PLATANH_LINEAR_SCALE, FT2_PLATANH_SEGMENT_1_LIMIT,
    FT2_PLATANH_SEGMENT_1_OFFSET, FT2_PLATANH_SEGMENT_1_SCALE, FT2_PLATANH_SEGMENT_2_LIMIT,
    FT2_PLATANH_SEGMENT_2_OFFSET, FT2_PLATANH_SEGMENT_2_SCALE, FT2_PLATANH_SEGMENT_3_LIMIT,
    FT2_PLATANH_SEGMENT_3_OFFSET, FT2_PLATANH_SEGMENT_3_SCALE, FT2_REFERENCE_SAMPLE_RATE_HZ,
    FT2_REQUIRED_SYNC_MATCHES, FT2_SEARCH_POWER_SCALE, FT2_SIGNAL_MODULATION_INDEX,
    FT2_SYNC_PATTERN, FT2_SYNC_SYMBOLS,
};

use super::session::validate_audio;
use super::{DecodeCandidate, DecodeDiagnostics, DecodeOptions, DecodeReport, DecodedMessage};
use crate::crc;
use crate::message::HashResolver;
use crate::message::unpack_message_for_mode;
use crate::modes::Mode;
use crate::wave::{AudioBuffer, DecoderError};

const FT2_SYNC_SEARCH_SAMPLE_RATE_HZ: f32 = FT2_REFERENCE_SAMPLE_RATE_HZ;
const FT2_DATA_SYMBOL_START: usize = FT2_SYNC_SYMBOLS;

#[derive(Debug, Clone, Serialize)]
pub struct Ft2SequenceTrace {
    pub nseq: usize,
    pub sync_ok: usize,
    pub mean: f32,
    pub sigma: f32,
    pub llr_head: Vec<f32>,
    pub llrs: Vec<f32>,
    pub decoded_text: Option<String>,
    pub ldpc_iterations: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Ft2CandidateTrace {
    pub coarse_freq_hz: f32,
    pub coarse_score: f32,
    pub best_df_hz: f32,
    pub best_ibest: usize,
    pub refined_dt_seconds: f32,
    pub refined_freq_hz: f32,
    pub best_sync: f32,
    pub sequences: Vec<Ft2SequenceTrace>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Ft2TraceReport {
    pub candidates: Vec<Ft2CandidateTrace>,
}

pub(super) fn decode_ft2(
    audio: &AudioBuffer,
    options: &DecodeOptions,
) -> Result<DecodeReport, DecoderError> {
    let spec = Mode::Ft2.spec();
    validate_audio(audio, spec)?;

    let padded = ft2_padded_samples(audio);
    let candidates = collect_candidates(&padded, options);
    let mut decodes = Vec::new();
    let mut seen = std::collections::HashSet::<String>::new();

    for candidate in &candidates {
        if let Some(decoded) = decode_candidate(&padded, candidate, options) {
            if seen.insert(decoded.text.clone()) {
                decodes.push(decoded);
            }
            if decodes.len() >= options.max_successes {
                break;
            }
        }
    }

    let accepted = decodes.len();
    Ok(DecodeReport {
        sample_rate_hz: audio.sample_rate_hz,
        duration_seconds: audio.samples.len() as f32 / audio.sample_rate_hz as f32,
        decodes,
        diagnostics: DecodeDiagnostics {
            frame_count: FT2_COARSE_FRAME_COUNT,
            usable_bins: FT2_COARSE_FFT_BINS,
            examined_candidates: candidates.len(),
            accepted_candidates: accepted,
            ldpc_codewords: accepted,
            parsed_payloads: accepted,
            top_candidates: candidates,
        },
    })
}

pub(super) fn debug_ft2_trace(
    audio: &AudioBuffer,
    options: &DecodeOptions,
) -> Result<Ft2TraceReport, DecoderError> {
    let spec = Mode::Ft2.spec();
    validate_audio(audio, spec)?;
    let padded = ft2_padded_samples(audio);
    let candidates = collect_candidates(&padded, options);
    let traces = candidates
        .iter()
        .filter_map(|candidate| trace_candidate(&padded, candidate))
        .collect();
    Ok(Ft2TraceReport { candidates: traces })
}

fn ft2_padded_samples(audio: &AudioBuffer) -> Vec<f32> {
    let mut padded = vec![0.0f32; FT2_PADDED_INPUT_SAMPLES];
    let copy_len = audio.samples.len().min(FT2_PADDED_INPUT_SAMPLES);
    padded[..copy_len].copy_from_slice(&audio.samples[..copy_len]);
    padded
}

fn collect_candidates(samples: &[f32], options: &DecodeOptions) -> Vec<DecodeCandidate> {
    let mut planner = RealFftPlanner::<f32>::new();
    let fft = planner.plan_fft_forward(FT2_COARSE_FFT_SAMPLES);
    let mut input = fft.make_input_vec();
    let mut spectrum = fft.make_output_vec();
    let mut savg = [0.0f32; FT2_COARSE_FFT_BINS];

    for j in 0..FT2_COARSE_FRAME_COUNT {
        let ia = j * FT2_COARSE_STEP_SAMPLES;
        let ib = ia + Mode::Ft2.spec().geometry.symbol_samples;
        input.fill(0.0);
        for (dst, sample) in input[..Mode::Ft2.spec().geometry.symbol_samples]
            .iter_mut()
            .zip(samples[ia..ib].iter().copied())
        {
            *dst = FT2_SEARCH_POWER_SCALE * sample * i16::MAX as f32;
        }
        fft.process(&mut input, &mut spectrum)
            .expect("ft2 sync fft");
        for i in 1..=FT2_COARSE_FFT_BINS {
            savg[i - 1] += spectrum[i].norm_sqr();
        }
    }

    let mut savsm = [0.0f32; FT2_COARSE_FFT_BINS];
    savsm[0] = savg[0];
    savsm[FT2_COARSE_FFT_BINS - 1] = savg[FT2_COARSE_FFT_BINS - 1];
    for i in 1..FT2_COARSE_FFT_BINS - 1 {
        savsm[i] = (savg[i - 1] + savg[i] + savg[i + 1]) / 3.0;
    }

    let df = 12_000.0 / FT2_COARSE_FFT_SAMPLES as f32;
    let nfa = (options.min_freq_hz.max(FT2_COARSE_MIN_FREQ_HZ) / df).round() as usize;
    let nfb = (options.max_freq_hz.min(FT2_COARSE_MAX_FREQ_HZ) / df).round() as usize;
    if nfa >= nfb || nfb >= FT2_COARSE_FFT_BINS {
        return Vec::new();
    }

    let mut baseline = savsm[nfa..=nfb].to_vec();
    baseline.sort_by(|left, right| left.total_cmp(right));
    let baseline_index = ((Mode::Ft2.spec().search.sync_baseline_percentile * baseline.len() as f32)
        .round() as usize)
        .clamp(1, baseline.len()) - 1;
    let xn = baseline[baseline_index].max(1e-6);
    for value in &mut savsm {
        *value /= xn;
    }

    let mut imax = None;
    let mut xmax = f32::NEG_INFINITY;
    for i in (nfa.max(1))..=(nfb.min(FT2_COARSE_FFT_BINS - 2)) {
        if savsm[i] > savsm[i - 1] && savsm[i] > savsm[i + 1] && savsm[i] > xmax {
            xmax = savsm[i];
            imax = Some(i);
        }
    }
    if let Some(i) = imax.filter(|_| xmax > FT2_COARSE_THRESHOLD) {
        vec![DecodeCandidate {
            start_seconds: 0.0,
            dt_seconds: 0.0,
            freq_hz: (i as f32 + 1.0) * df,
            score: xmax,
        }]
    } else {
        Vec::new()
    }
}

fn decode_candidate(
    samples: &[f32],
    candidate: &DecodeCandidate,
    _options: &DecodeOptions,
) -> Option<DecodedMessage> {
    let analysis = analyze_candidate(samples, candidate)?;

    let decoder = Ft2ParityMatrix::global();
    for sequence in candidate_sequences(&analysis) {
        let hard: [u8; FT2_MESSAGE_SYMBOLS] =
            std::array::from_fn(|index| u8::from(sequence.sbits[index] > 0.0));
        if sync_match_count(&hard) < FT2_REQUIRED_SYNC_MATCHES {
            break;
        }

        let rxdata = &sequence.sbits[FT2_DATA_SYMBOL_START..];
        let mean = rxdata.iter().copied().sum::<f32>() / rxdata.len() as f32;
        let mean_sq = rxdata.iter().map(|value| value * value).sum::<f32>() / rxdata.len() as f32;
        let sigma = (mean_sq - mean * mean).max(1e-6).sqrt();
        let llrs: Vec<f32> = rxdata
            .iter()
            .map(|value| FT2_LLR_NORMALIZATION * (value / sigma) / FT2_LLR_VARIANCE_SCALE)
            .collect();

        if let Some((codeword, iterations)) = decoder.decode_with_maxosd(&llrs, -1) {
            if codeword[..77].iter().all(|bit| *bit == 0) {
                continue;
            }
            let payload = unpack_message_for_mode(Mode::Ft2, &codeword)?;
            let message = payload.to_message(&HashResolver::default());
            return Some(DecodedMessage {
                utc: "000000".to_string(),
                snr_db: (10.0 * (analysis.best_sync * analysis.best_sync).max(1e-12).log10()
                    - 115.0)
                    .round() as i32,
                dt_seconds: analysis.best_ibest as f32 / FT2_REFERENCE_SAMPLE_RATE_HZ,
                freq_hz: candidate.freq_hz + analysis.best_df_hz as f32,
                text: message.to_text(),
                candidate_score: analysis.best_sync,
                ldpc_iterations: iterations,
                message,
            });
        }
    }
    None
}

fn trace_candidate(samples: &[f32], candidate: &DecodeCandidate) -> Option<Ft2CandidateTrace> {
    let analysis = analyze_candidate(samples, candidate)?;

    let decoder = Ft2ParityMatrix::global();
    let resolver = HashResolver::default();
    let mut sequences = Vec::new();
    for sequence in candidate_sequences(&analysis) {
        let hard: [u8; FT2_MESSAGE_SYMBOLS] =
            std::array::from_fn(|index| u8::from(sequence.sbits[index] > 0.0));
        let sync_ok = sync_match_count(&hard);
        if sync_ok < FT2_REQUIRED_SYNC_MATCHES {
            sequences.push(Ft2SequenceTrace {
                nseq: sequence.nseq,
                sync_ok,
                mean: 0.0,
                sigma: 0.0,
                llr_head: Vec::new(),
                llrs: Vec::new(),
                decoded_text: None,
                ldpc_iterations: None,
            });
            break;
        }

        let rxdata = &sequence.sbits[FT2_DATA_SYMBOL_START..];
        let mean = rxdata.iter().copied().sum::<f32>() / rxdata.len() as f32;
        let mean_sq = rxdata.iter().map(|value| value * value).sum::<f32>() / rxdata.len() as f32;
        let sigma = (mean_sq - mean * mean).max(1e-6).sqrt();
        let llrs: Vec<f32> = rxdata
            .iter()
            .map(|value| FT2_LLR_NORMALIZATION * (value / sigma) / FT2_LLR_VARIANCE_SCALE)
            .collect();

        let decode = decoder
            .decode_with_maxosd(&llrs, -1)
            .and_then(|(codeword, iterations)| {
                if codeword[..77].iter().all(|bit| *bit == 0) {
                    return None;
                }
                let payload = unpack_message_for_mode(Mode::Ft2, &codeword)?;
                let message = payload.to_message(&resolver);
                Some((message.to_text(), iterations))
            });
        sequences.push(Ft2SequenceTrace {
            nseq: sequence.nseq,
            sync_ok,
            mean,
            sigma,
            llr_head: llrs.iter().take(8).copied().collect(),
            llrs,
            decoded_text: decode.as_ref().map(|(text, _)| text.clone()),
            ldpc_iterations: decode.as_ref().map(|(_, iterations)| *iterations),
        });
        if decode.is_some() {
            break;
        }
    }
    Some(Ft2CandidateTrace {
        coarse_freq_hz: candidate.freq_hz,
        coarse_score: candidate.score,
        best_df_hz: analysis.best_df_hz as f32,
        best_ibest: analysis.best_ibest,
        refined_dt_seconds: analysis.best_ibest as f32 / FT2_REFERENCE_SAMPLE_RATE_HZ,
        refined_freq_hz: candidate.freq_hz + analysis.best_df_hz as f32,
        best_sync: analysis.best_sync,
        sequences,
    })
}

struct Ft2CandidateAnalysis {
    best_df_hz: i32,
    best_ibest: usize,
    best_sync: f32,
    ccor0: [Complex32; FT2_MESSAGE_SYMBOLS],
    ccor1: [Complex32; FT2_MESSAGE_SYMBOLS],
    sbits: [f32; FT2_MESSAGE_SYMBOLS],
}

struct Ft2SequenceMetrics {
    nseq: usize,
    sbits: [f32; FT2_MESSAGE_SYMBOLS],
}

fn analyze_candidate(samples: &[f32], candidate: &DecodeCandidate) -> Option<Ft2CandidateAnalysis> {
    let refs = Ft2Refs::global();
    let c2 = ft2_downsample(samples, candidate.freq_hz);
    let (best_df_hz, best_ibest, best_sync) = find_best_alignment(&c2, refs);
    let cb = twkfreq(&c2, best_df_hz as f32, FT2_SYNC_SEARCH_SAMPLE_RATE_HZ);
    let normalized = normalized_candidate_symbols(&cb, best_ibest)?;
    let (ccor0, ccor1, sbits) = initial_symbol_metrics(&normalized, refs);
    Some(Ft2CandidateAnalysis {
        best_df_hz,
        best_ibest,
        best_sync,
        ccor0,
        ccor1,
        sbits,
    })
}

fn find_best_alignment(samples: &[Complex32], refs: &Ft2Refs) -> (i32, usize, f32) {
    let mut best_ibest = 0usize;
    let mut best_df_hz = 0i32;
    let mut best_sync = f32::NEG_INFINITY;

    for df_hz in FT2_FINE_FREQ_SWEEP_MIN_HZ..=FT2_FINE_FREQ_SWEEP_MAX_HZ {
        let cb = twkfreq(samples, df_hz as f32, FT2_SYNC_SEARCH_SAMPLE_RATE_HZ);
        for ibest in 0..FT2_FINE_TIME_STEPS {
            let metric = sync_metric(&cb, refs, ibest);
            if metric > best_sync {
                best_sync = metric;
                best_ibest = ibest;
                best_df_hz = df_hz;
            }
        }
    }

    (best_df_hz, best_ibest, best_sync)
}

fn sync_metric(samples: &[Complex32], refs: &Ft2Refs, ibest: usize) -> f32 {
    let mut sync_sum = Complex32::new(0.0, 0.0);
    let mut phase_term = Complex32::new(1.0, 0.0);
    for (sync_index, &sync_tone) in FT2_SYNC_PATTERN.iter().enumerate() {
        let start = sync_index * FT2_BASEBAND_SYMBOL_SAMPLES + ibest;
        let end = start + FT2_BASEBAND_SYMBOL_SAMPLES;
        let reference = if sync_tone == 1 { &refs.c1 } else { &refs.c0 };
        let corr = dot_conj(&samples[start..end], reference);
        sync_sum += corr * phase_term;
        phase_term *= if sync_tone == 1 { refs.cc1 } else { refs.cc0 };
    }
    sync_sum.norm()
}

fn normalized_candidate_symbols(samples: &[Complex32], ibest: usize) -> Option<Vec<Complex32>> {
    let frame_len = FT2_MESSAGE_SYMBOLS * FT2_BASEBAND_SYMBOL_SAMPLES;
    let end = ibest + frame_len;
    let mut candidate = samples.get(ibest..end)?.to_vec();
    let power =
        candidate.iter().map(|value| value.norm_sqr()).sum::<f32>() / candidate.len() as f32;
    if power <= 0.0 {
        return None;
    }
    let gain = power.sqrt();
    for sample in &mut candidate {
        *sample /= gain;
    }
    Some(candidate)
}

fn initial_symbol_metrics(
    samples: &[Complex32],
    refs: &Ft2Refs,
) -> (
    [Complex32; FT2_MESSAGE_SYMBOLS],
    [Complex32; FT2_MESSAGE_SYMBOLS],
    [f32; FT2_MESSAGE_SYMBOLS],
) {
    let mut ccor0 = [Complex32::new(0.0, 0.0); FT2_MESSAGE_SYMBOLS];
    let mut ccor1 = [Complex32::new(0.0, 0.0); FT2_MESSAGE_SYMBOLS];
    let mut sbits = [0.0f32; FT2_MESSAGE_SYMBOLS];

    for symbol in 0..FT2_MESSAGE_SYMBOLS {
        let start = symbol * FT2_BASEBAND_SYMBOL_SAMPLES;
        let end = start + FT2_BASEBAND_SYMBOL_SAMPLES;
        ccor1[symbol] = dot_conj(&samples[start..end], &refs.c1);
        ccor0[symbol] = dot_conj(&samples[start..end], &refs.c0);
        sbits[symbol] = ccor1[symbol].norm() - ccor0[symbol].norm();
    }

    (ccor0, ccor1, sbits)
}

fn candidate_sequences(
    analysis: &Ft2CandidateAnalysis,
) -> impl Iterator<Item = Ft2SequenceMetrics> + '_ {
    (1..=5usize).scan(analysis.sbits, move |current, nseq| {
        if nseq > 1 {
            *current = sequence_metrics(
                nseq,
                current,
                &analysis.ccor0,
                &analysis.ccor1,
                Ft2Refs::global(),
            );
        }
        Some(Ft2SequenceMetrics {
            nseq,
            sbits: *current,
        })
    })
}

fn sync_match_count(hard_bits: &[u8; FT2_MESSAGE_SYMBOLS]) -> usize {
    FT2_SYNC_PATTERN
        .iter()
        .zip(hard_bits[..FT2_SYNC_SYMBOLS].iter())
        .filter(|(left, right)| **left as u8 == **right)
        .count()
}

fn sequence_metrics(
    nseq: usize,
    previous: &[f32; FT2_MESSAGE_SYMBOLS],
    ccor0: &[Complex32; FT2_MESSAGE_SYMBOLS],
    ccor1: &[Complex32; FT2_MESSAGE_SYMBOLS],
    refs: &Ft2Refs,
) -> [f32; FT2_MESSAGE_SYMBOLS] {
    let nbit = 2 * nseq - 1;
    let half = nbit / 2;
    let numseq = 1usize << nbit;
    let mut metrics = *previous;
    for (target, metric) in metrics
        .iter_mut()
        .enumerate()
        .take(FT2_MESSAGE_SYMBOLS - half)
        .skip(half)
    {
        let mut max1 = 0.0f32;
        let mut max0 = 0.0f32;
        for seq in 0..numseq {
            let mut csum = Complex32::new(0.0, 0.0);
            let mut cterm = Complex32::new(1.0, 0.0);
            for pos in 0..nbit {
                let bit = (seq >> (nbit - 1 - pos)) & 1;
                let index = target - half + pos;
                csum += if bit == 1 { ccor1[index] } else { ccor0[index] } * cterm;
                cterm *= if bit == 1 { refs.cc1 } else { refs.cc0 };
            }
            let score = csum.norm();
            if ((seq >> half) & 1) == 1 {
                max1 = max1.max(score);
            } else {
                max0 = max0.max(score);
            }
        }
        *metric = max1 - max0;
    }
    metrics
}

fn ft2_downsample(samples: &[f32], f0: f32) -> Vec<Complex32> {
    let mut real_planner = RealFftPlanner::<f32>::new();
    let rfft = real_planner.plan_fft_forward(FT2_PADDED_INPUT_SAMPLES);
    let mut input = rfft.make_input_vec();
    let mut spectrum = rfft.make_output_vec();
    for (dst, sample) in input.iter_mut().zip(samples.iter().copied()) {
        *dst = sample * i16::MAX as f32;
    }
    rfft.process(&mut input, &mut spectrum)
        .expect("ft2 long fft");

    let df = 12_000.0 / FT2_PADDED_INPUT_SAMPLES as f32;
    let i0 = (f0 / df).round() as usize;
    let mut c1 = vec![Complex32::new(0.0, 0.0); FT2_BASEBAND_FFT_SAMPLES];
    c1[0] = spectrum[i0];
    for i in 1..=(FT2_BASEBAND_FFT_SAMPLES / 2) {
        let arg = (i as f32 - 1.0) * df / (4.0 * 75.0);
        let win = (-arg * arg).exp();
        c1[i] = spectrum[i0 + i] * win;
        c1[FT2_BASEBAND_FFT_SAMPLES - i] = spectrum[i0 - i] * win;
    }
    let scale = 1.0 / FT2_BASEBAND_FFT_SAMPLES as f32;
    for value in &mut c1 {
        *value *= scale;
    }

    let mut planner = FftPlanner::<f32>::new();
    let ifft = planner.plan_fft_inverse(FT2_BASEBAND_FFT_SAMPLES);
    ifft.process(&mut c1);
    c1
}

fn twkfreq(samples: &[Complex32], df_hz: f32, sample_rate_hz: f32) -> Vec<Complex32> {
    let step = Complex32::from_polar(1.0, -2.0 * std::f32::consts::PI * df_hz / sample_rate_hz);
    let mut phase = Complex32::new(1.0, 0.0);
    let mut out = Vec::with_capacity(samples.len());
    for sample in samples {
        out.push(*sample * phase);
        phase *= step;
    }
    out
}

fn dot_conj(lhs: &[Complex32], rhs: &[Complex32]) -> Complex32 {
    lhs.iter()
        .zip(rhs.iter())
        .fold(Complex32::new(0.0, 0.0), |acc, (left, right)| {
            acc + *left * right.conj()
        })
}

struct Ft2Refs {
    c0: [Complex32; FT2_BASEBAND_SYMBOL_SAMPLES],
    c1: [Complex32; FT2_BASEBAND_SYMBOL_SAMPLES],
    cc0: Complex32,
    cc1: Complex32,
}

impl Ft2Refs {
    fn global() -> &'static Self {
        static REFS: OnceLock<Ft2Refs> = OnceLock::new();
        REFS.get_or_init(Self::new)
    }

    fn new() -> Self {
        let fs = 12_000.0 / FT2_DOWNSAMPLE_FACTOR as f32;
        let dt = 1.0 / fs;
        let tt = Mode::Ft2.spec().geometry.symbol_samples as f32 * dt;
        let baud = 1.0 / tt;
        let h = FT2_SIGNAL_MODULATION_INDEX;
        let twopi = 2.0 * std::f32::consts::PI;
        // Stock FT2 reference pulses advance over one 16x-downsampled symbol step.
        let dphi = twopi / 2.0 * baud * h * dt * FT2_DOWNSAMPLE_FACTOR as f32;
        let mut phi0 = 0.0f32;
        let mut phi1 = 0.0f32;
        let c1 = std::array::from_fn(|_| {
            let value = Complex32::new(phi1.cos(), phi1.sin());
            phi1 = (phi1 + dphi).rem_euclid(twopi);
            value
        });
        let c0 = std::array::from_fn(|_| {
            let value = Complex32::new(phi0.cos(), phi0.sin());
            phi0 = (phi0 - dphi).rem_euclid(twopi);
            value
        });
        let the = twopi * h / 2.0;
        Self {
            c0,
            c1,
            cc1: Complex32::new(the.cos(), -the.sin()),
            cc0: Complex32::new(the.cos(), the.sin()),
        }
    }
}

struct Ft2ParityMatrix {
    row_columns: Vec<Vec<usize>>,
    row_column_slots: Vec<Vec<usize>>,
    generator_rows: Vec<Vec<u8>>,
}

impl Ft2ParityMatrix {
    fn global() -> &'static Self {
        static MATRIX: OnceLock<Ft2ParityMatrix> = OnceLock::new();
        MATRIX.get_or_init(Self::new)
    }

    fn new() -> Self {
        let mut row_columns = vec![Vec::new(); FT2_ROW_COUNT];
        for (column, rows) in FT2_COLUMN_ROWS.iter().enumerate() {
            for &row in rows {
                row_columns[row].push(column);
            }
        }
        let mut row_column_slots = vec![Vec::new(); FT2_ROW_COUNT];
        for (row_index, columns) in row_columns.iter().enumerate() {
            row_column_slots[row_index] = columns
                .iter()
                .map(|&column| {
                    FT2_COLUMN_ROWS[column]
                        .iter()
                        .position(|&stored_row| stored_row == row_index)
                        .expect("ft2 column contains row")
                })
                .collect();
        }
        let generator_rows = ft2_generator_rows()
            .iter()
            .map(|row| row.to_vec())
            .collect();
        Self {
            row_columns,
            row_column_slots,
            generator_rows,
        }
    }

    fn parity_ok(&self, bits: &[u8]) -> bool {
        bits.len() == FT2_COLUMN_COUNT
            && self
                .row_columns
                .iter()
                .all(|row| row.iter().fold(0u8, |acc, &column| acc ^ bits[column]) == 0)
    }

    fn decode_with_maxosd(&self, llrs: &[f32], maxosd: isize) -> Option<(Vec<u8>, usize)> {
        if llrs.len() != FT2_COLUMN_COUNT {
            return None;
        }
        let maxosd = maxosd.clamp(-1, 3);

        let mut tov = [[0.0f32; 3]; FT2_COLUMN_COUNT];
        let mut toc = [[0.0f32; 11]; FT2_ROW_COUNT];
        let mut tanhtoc = [[0.0f32; 11]; FT2_ROW_COUNT];
        let mut zn = [0.0f32; FT2_COLUMN_COUNT];
        let mut zsum = [0.0f32; FT2_COLUMN_COUNT];
        let mut saved_llrs = Vec::<Vec<f32>>::new();
        if maxosd == 0 {
            saved_llrs.push(llrs.to_vec());
        }

        for (row_index, columns) in self.row_columns.iter().enumerate() {
            for (slot, &column) in columns.iter().enumerate() {
                toc[row_index][slot] = llrs[column];
            }
        }

        let mut ncnt = 0isize;
        let mut nclast = 0usize;
        for iteration in 0..=FT2_BP_MAX_ITERS {
            for column in 0..FT2_COLUMN_COUNT {
                zn[column] = llrs[column] + tov[column][0] + tov[column][1] + tov[column][2];
            }
            if maxosd > 0 {
                for (acc, value) in zsum.iter_mut().zip(zn.iter().copied()) {
                    *acc += value;
                }
                if iteration > 0 && iteration as isize <= maxosd {
                    saved_llrs.push(zsum.to_vec());
                }
            }

            let hard: [u8; FT2_COLUMN_COUNT] =
                std::array::from_fn(|index| u8::from(zn[index] > 0.0));
            let ncheck = self
                .row_columns
                .iter()
                .filter(|row| row.iter().fold(0u8, |acc, &column| acc ^ hard[column]) != 0)
                .count();
            if ncheck == 0 && crc::crc_matches_ft2(&hard[..77], &hard[77..90]) {
                return Some((hard.to_vec(), iteration));
            }

            if iteration > 0 {
                let delta = ncheck as isize - nclast as isize;
                if delta < 0 {
                    ncnt = 0;
                } else {
                    ncnt += 1;
                }
                if ncnt >= 3 && iteration >= 5 && ncheck > 10 {
                    return None;
                }
            }
            nclast = ncheck;

            for (row_index, columns) in self.row_columns.iter().enumerate() {
                for (slot, &column) in columns.iter().enumerate() {
                    let column_slot = self.row_column_slots[row_index][slot];
                    toc[row_index][slot] = zn[column] - tov[column][column_slot];
                }
            }

            for (row_index, columns) in self.row_columns.iter().enumerate() {
                for slot in 0..columns.len() {
                    tanhtoc[row_index][slot] = (-toc[row_index][slot] / 2.0).tanh();
                }
            }

            for column in 0..FT2_COLUMN_COUNT {
                for (slot, &row_index) in FT2_COLUMN_ROWS[column].iter().enumerate() {
                    let mut product = 1.0f32;
                    for (row_slot, &other_column) in self.row_columns[row_index].iter().enumerate()
                    {
                        if other_column == column {
                            continue;
                        }
                        product *= tanhtoc[row_index][row_slot];
                    }
                    tov[column][slot] = 2.0 * platanh_approx(-product);
                }
            }
        }
        let osd_ntheta = if maxosd == 0 {
            FT2_OSD_NTHETA_MEDIUM
        } else {
            FT2_OSD_NTHETA_DEEP
        };
        for (index, saved) in saved_llrs.iter().enumerate() {
            if let Some(bits) = self.decode_osd(saved, 1, osd_ntheta) {
                return Some((bits, FT2_BP_MAX_ITERS + index + 1));
            }
        }
        None
    }

    fn decode_osd(&self, llrs: &[f32], max_order: usize, ntheta: usize) -> Option<Vec<u8>> {
        if llrs.len() != FT2_COLUMN_COUNT {
            return None;
        }

        let mut indices: Vec<usize> = (0..FT2_COLUMN_COUNT).collect();
        indices.sort_by(|left, right| llrs[*right].abs().total_cmp(&llrs[*left].abs()));

        let mut genmrb: Vec<Vec<u8>> = self
            .generator_rows
            .iter()
            .map(|row| indices.iter().map(|&index| row[index]).collect())
            .collect();
        let mut permuted_indices = indices;

        for pivot in 0..FT2_INFO_BITS {
            let search_end =
                (FT2_INFO_BITS + FT2_OSD_MRB_SEARCH_EXTRA_COLUMNS).min(FT2_COLUMN_COUNT);
            let column = (pivot..search_end).find(|&column| genmrb[pivot][column] == 1)?;
            if column != pivot {
                for row in &mut genmrb {
                    row.swap(pivot, column);
                }
                permuted_indices.swap(pivot, column);
            }
            for row in 0..FT2_INFO_BITS {
                if row != pivot && genmrb[row][pivot] == 1 {
                    let pivot_row = genmrb[pivot].clone();
                    for (column, cell) in genmrb[row].iter_mut().enumerate().take(FT2_COLUMN_COUNT)
                    {
                        *cell ^= pivot_row[column];
                    }
                }
            }
        }

        let hard: Vec<u8> = permuted_indices
            .iter()
            .map(|&index| u8::from(llrs[index] >= 0.0))
            .collect();
        let reliabilities: Vec<f32> = permuted_indices
            .iter()
            .map(|&index| llrs[index].abs())
            .collect();

        let mut best_codeword = encode_mrb_ft2(&hard[..FT2_INFO_BITS], &genmrb);
        let mut best_distance = weighted_distance_ft2(&best_codeword, &hard, &reliabilities);

        for first in (0..FT2_INFO_BITS).rev() {
            let mut message = hard[..FT2_INFO_BITS].to_vec();
            message[first] ^= 1;
            let codeword = encode_mrb_ft2(&message, &genmrb);
            let parity_tail = xor_tail_ft2(&codeword, &hard, FT2_INFO_BITS);
            let nd1kpt = parity_tail
                .iter()
                .take(FT2_OSD_PARITY_TAIL_BITS)
                .map(|&bit| bit as usize)
                .sum::<usize>()
                + 1;
            if nd1kpt > ntheta {
                continue;
            }
            let distance = weighted_distance_ft2(&codeword, &hard, &reliabilities);
            if distance < best_distance {
                best_distance = distance;
                best_codeword = codeword.clone();
            }

            if max_order < 2 {
                continue;
            }
            let parity_basis = &genmrb[first][FT2_INFO_BITS..];
            for second in (0..first).rev() {
                let nd2kpt = parity_tail
                    .iter()
                    .zip(genmrb[second][FT2_INFO_BITS..].iter())
                    .take(FT2_OSD_PARITY_TAIL_BITS)
                    .map(|(&tail_bit, &basis_bit)| (tail_bit ^ basis_bit) as usize)
                    .sum::<usize>()
                    + 2;
                if nd2kpt > ntheta {
                    continue;
                }
                let mut message2 = message.clone();
                message2[second] ^= 1;
                let codeword2 = encode_mrb_ft2(&message2, &genmrb);
                let distance = weighted_distance_ft2(&codeword2, &hard, &reliabilities);
                if distance < best_distance {
                    best_distance = distance;
                    best_codeword = codeword2;
                }
            }
            let _ = parity_basis;
        }

        let mut restored = vec![0u8; FT2_COLUMN_COUNT];
        for (column, bit) in best_codeword.into_iter().enumerate() {
            restored[permuted_indices[column]] = bit;
        }
        if self.parity_ok(&restored) && crc::crc_matches_ft2(&restored[..77], &restored[77..90]) {
            Some(restored)
        } else {
            None
        }
    }
}

fn platanh_approx(x: f32) -> f32 {
    let z = x.abs();
    let y = if z <= FT2_PLATANH_LINEAR_LIMIT {
        x / FT2_PLATANH_LINEAR_SCALE
    } else if z <= FT2_PLATANH_SEGMENT_1_LIMIT {
        x.signum() * ((z - FT2_PLATANH_SEGMENT_1_OFFSET) / FT2_PLATANH_SEGMENT_1_SCALE)
    } else if z <= FT2_PLATANH_SEGMENT_2_LIMIT {
        x.signum() * ((z - FT2_PLATANH_SEGMENT_2_OFFSET) / FT2_PLATANH_SEGMENT_2_SCALE)
    } else if z <= FT2_PLATANH_SEGMENT_3_LIMIT {
        x.signum() * ((z - FT2_PLATANH_SEGMENT_3_OFFSET) / FT2_PLATANH_SEGMENT_3_SCALE)
    } else {
        x.signum() * FT2_PLATANH_CLAMP
    };
    if x == 0.0 { 0.0 } else { y }
}

fn encode_mrb_ft2(message: &[u8], generator_rows: &[Vec<u8>]) -> Vec<u8> {
    let mut codeword = vec![0u8; FT2_COLUMN_COUNT];
    for (row, bit) in generator_rows.iter().zip(message.iter().copied()) {
        if bit == 0 {
            continue;
        }
        for (slot, value) in codeword.iter_mut().zip(row) {
            *slot ^= *value;
        }
    }
    codeword
}

fn weighted_distance_ft2(codeword: &[u8], hard: &[u8], reliabilities: &[f32]) -> f32 {
    codeword
        .iter()
        .zip(hard.iter())
        .zip(reliabilities.iter())
        .map(|((&bit, &hard_bit), &weight)| if bit == hard_bit { 0.0 } else { weight })
        .sum()
}

fn xor_tail_ft2(codeword: &[u8], hard: &[u8], start: usize) -> Vec<u8> {
    codeword[start..]
        .iter()
        .zip(&hard[start..])
        .map(|(&left, &right)| left ^ right)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encode::{
        WaveformOptions, encode_standard_message_for_mode, synthesize_rectangular_waveform,
    };
    use crate::message::GridReport;

    #[test]
    fn ft2_generated_waveform_round_trips_through_decoder() {
        let frame = encode_standard_message_for_mode(
            Mode::Ft2,
            "K1ABC",
            "W1XYZ",
            false,
            &GridReport::Grid("FN31".to_string()),
        )
        .expect("encode");
        let audio = synthesize_rectangular_waveform(
            &frame,
            &WaveformOptions {
                mode: Mode::Ft2,
                base_freq_hz: 900.0,
                total_seconds: 2.5,
                start_seconds: 0.0,
                ..WaveformOptions::for_mode(Mode::Ft2)
            },
        )
        .expect("waveform");
        let report = decode_ft2(
            &audio,
            &DecodeOptions {
                mode: Mode::Ft2,
                ..DecodeOptions::for_mode(Mode::Ft2)
            },
        )
        .expect("decode");
        assert!(
            report
                .decodes
                .iter()
                .any(|decode| decode.text == "K1ABC W1XYZ FN31")
        );
    }
}
