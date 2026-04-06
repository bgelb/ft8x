use std::sync::OnceLock;

use num_complex::Complex32;
use realfft::RealFftPlanner;
use rustfft::FftPlanner;

use super::session::validate_audio;
use super::{
    DecodeCandidate, DecodeDiagnostics, DecodeOptions, DecodeReport, DecodedMessage,
};
use crate::crc;
use crate::message::HashResolver;
use crate::message::unpack_message_for_mode;
use crate::modes::Mode;
use crate::wave::{AudioBuffer, DecoderError};

const FT2_NMAX: usize = 30_000;
const FT2_NSPS: usize = 160;
const FT2_NFFT1: usize = 400;
const FT2_NH1: usize = FT2_NFFT1 / 2;
const FT2_NSTEP: usize = FT2_NSPS / 4;
const FT2_NHSYM: usize = FT2_NMAX / FT2_NSTEP - 3;
const FT2_NDOWN: usize = 16;
const FT2_NFFT2: usize = FT2_NMAX / FT2_NDOWN;
const FT2_DOWNSAMPLED_SYMBOL_SAMPLES: usize = FT2_NSPS / FT2_NDOWN;
const FT2_SYMBOL_COUNT: usize = 144;
const FT2_SYNC: [u8; 16] = [0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0];
const FT2_BP_MAX_ITERS: usize = 40;
const FT2_ROWS: usize = 38;
const FT2_COLUMNS: usize = 128;
const FT2_INFO_BITS: usize = 90;
const FT2_OSD_NT: usize = 12;
const FT2_OSD_NTHETA_MEDIUM: usize = 4;
const FT2_OSD_NTHETA_DEEP: usize = 4;

const FT2_COLUMN_ROWS: [[usize; 3]; FT2_COLUMNS] = [
    [20, 33, 35],
    [0, 7, 27],
    [1, 8, 36],
    [2, 6, 18],
    [3, 15, 31],
    [1, 4, 21],
    [5, 12, 24],
    [9, 30, 32],
    [10, 23, 26],
    [11, 14, 22],
    [13, 17, 25],
    [16, 19, 28],
    [16, 29, 33],
    [5, 33, 34],
    [0, 9, 29],
    [2, 17, 22],
    [3, 11, 24],
    [4, 27, 35],
    [6, 13, 20],
    [7, 14, 30],
    [8, 26, 31],
    [10, 18, 34],
    [12, 15, 36],
    [19, 23, 37],
    [20, 21, 25],
    [11, 28, 32],
    [0, 16, 34],
    [1, 27, 29],
    [2, 9, 31],
    [3, 7, 35],
    [4, 18, 28],
    [5, 19, 26],
    [6, 21, 36],
    [8, 10, 32],
    [12, 23, 25],
    [13, 30, 33],
    [14, 15, 24],
    [12, 17, 37],
    [7, 19, 22],
    [0, 31, 32],
    [1, 16, 18],
    [2, 23, 33],
    [3, 6, 37],
    [4, 10, 30],
    [5, 17, 20],
    [8, 14, 35],
    [9, 15, 27],
    [11, 25, 29],
    [13, 26, 28],
    [21, 24, 34],
    [22, 29, 31],
    [3, 10, 36],
    [0, 13, 22],
    [1, 7, 24],
    [2, 12, 26],
    [4, 9, 36],
    [5, 15, 30],
    [6, 14, 17],
    [8, 21, 23],
    [11, 18, 35],
    [16, 25, 37],
    [19, 20, 32],
    [19, 27, 34],
    [3, 28, 33],
    [0, 25, 35],
    [1, 22, 33],
    [2, 8, 37],
    [4, 5, 16],
    [6, 26, 34],
    [7, 13, 31],
    [9, 14, 21],
    [10, 17, 28],
    [11, 12, 27],
    [15, 18, 32],
    [20, 24, 30],
    [23, 29, 36],
    [0, 2, 20],
    [1, 17, 30],
    [3, 5, 8],
    [4, 7, 32],
    [6, 28, 31],
    [9, 12, 18],
    [10, 21, 22],
    [11, 26, 33],
    [13, 14, 29],
    [15, 26, 37],
    [16, 27, 36],
    [19, 24, 25],
    [4, 23, 34],
    [2, 5, 35],
    [0, 11, 30],
    [1, 3, 32],
    [2, 15, 29],
    [0, 1, 23],
    [4, 22, 26],
    [5, 27, 31],
    [6, 16, 35],
    [7, 21, 37],
    [8, 17, 19],
    [9, 20, 28],
    [10, 12, 33],
    [3, 13, 19],
    [10, 29, 37],
    [13, 34, 36],
    [14, 18, 25],
    [2, 27, 28],
    [6, 7, 8],
    [4, 17, 33],
    [12, 14, 16],
    [11, 15, 34],
    [9, 22, 24],
    [18, 20, 36],
    [16, 26, 30],
    [23, 24, 35],
    [0, 17, 18],
    [5, 25, 32],
    [21, 30, 31],
    [2, 19, 21],
    [3, 20, 26],
    [1, 12, 28],
    [5, 6, 11],
    [14, 23, 31],
    [8, 24, 29],
    [22, 36, 37],
    [4, 15, 25],
    [10, 13, 27],
    [32, 35, 37],
    [7, 9, 34],
];

const FT2_GENERATOR_HEX: [&str; FT2_ROWS] = [
    "a08ea80879050a5e94da994",
    "59f3b48040ca089c81ee880",
    "e4070262802e31b7b17d3dc",
    "95cbcbaf032dc3d960bacc8",
    "c4d79b5dcc21161a254ffbc",
    "93fde9cdbf2622a70868424",
    "e73b888bb1b01167379ba28",
    "45a0d0a0f39a7ad2439949c",
    "759acef19444bcad79c4964",
    "71eb4dddf4f5ed9e2ea17e0",
    "80f0ad76fb247d6b4ca8d38",
    "184fff3aa1b82dc66640104",
    "ca4e320bb382ed14cbb1094",
    "52514447b90e25b9e459e28",
    "dd10c1666e071956bd0df38",
    "99c332a0b792a2da8ef1ba8",
    "7bd9f688e7ed402e231aaac",
    "00fcad76eb647d6a0ca8c38",
    "6ac8d0499c43b02eed78d70",
    "2c2c764baf795b4788db010",
    "0e907bf9e280d2624823dd0",
    "b857a6e315afd8c1c925e64",
    "8deb58e22d73a141cae3778",
    "22d3cb80d92d6ac132dfe08",
    "754763877b28c187746855c",
    "1d1bb7cf6953732e04ebca4",
    "2c65e0ea4466ab9f5e1deec",
    "6dc530ca37fc916d1f84870",
    "49bccbbee152355be7ac984",
    "e8387f3f4367cf45a150448",
    "8ce25e03d67d51091c81884",
    "b798012ffa40a93852752c8",
    "2e43307933adfca37adc3c8",
    "ca06e0a42ca1ec782d6c06c",
    "c02b762927556a7039e638c",
    "4a3e9b7d08b6807f8619fac",
    "45e8030f68997bb68544424",
    "7e79362c16773efc6482e30",
];

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
            frame_count: FT2_NHSYM,
            usable_bins: FT2_NH1,
            examined_candidates: candidates.len(),
            accepted_candidates: accepted,
            ldpc_codewords: accepted,
            parsed_payloads: accepted,
            top_candidates: candidates,
        },
    })
}

fn ft2_padded_samples(audio: &AudioBuffer) -> Vec<f32> {
    let mut padded = vec![0.0f32; FT2_NMAX];
    let copy_len = audio.samples.len().min(FT2_NMAX);
    padded[..copy_len].copy_from_slice(&audio.samples[..copy_len]);
    padded
}

fn collect_candidates(samples: &[f32], options: &DecodeOptions) -> Vec<DecodeCandidate> {
    let mut planner = RealFftPlanner::<f32>::new();
    let fft = planner.plan_fft_forward(FT2_NFFT1);
    let mut input = fft.make_input_vec();
    let mut spectrum = fft.make_output_vec();
    let mut savg = [0.0f32; FT2_NH1];
    let fac = 1.0f32 / 300.0;

    for j in 0..FT2_NHSYM {
        let ia = j * FT2_NSTEP;
        let ib = ia + FT2_NSPS;
        input.fill(0.0);
        for (dst, sample) in input[..FT2_NSPS].iter_mut().zip(samples[ia..ib].iter().copied()) {
            *dst = fac * sample * i16::MAX as f32;
        }
        fft.process(&mut input, &mut spectrum).expect("ft2 sync fft");
        for i in 1..=FT2_NH1 {
            savg[i - 1] += spectrum[i].norm_sqr();
        }
    }

    let mut savsm = [0.0f32; FT2_NH1];
    savsm[0] = savg[0];
    savsm[FT2_NH1 - 1] = savg[FT2_NH1 - 1];
    for i in 1..FT2_NH1 - 1 {
        savsm[i] = (savg[i - 1] + savg[i] + savg[i + 1]) / 3.0;
    }

    let df = 12_000.0 / FT2_NFFT1 as f32;
    let nfa = (options.min_freq_hz.max(375.0) / df).round() as usize;
    let nfb = (options.max_freq_hz.min(3_000.0) / df).round() as usize;
    if nfa >= nfb || nfb >= FT2_NH1 {
        return Vec::new();
    }

    let mut baseline = savsm[nfa..=nfb].to_vec();
    baseline.sort_by(|left, right| left.total_cmp(right));
    let baseline_index =
        ((0.30 * baseline.len() as f32).round() as usize).clamp(1, baseline.len()) - 1;
    let xn = baseline[baseline_index].max(1e-6);
    for value in &mut savsm {
        *value /= xn;
    }

    let mut imax = None;
    let mut xmax = f32::NEG_INFINITY;
    for i in (nfa.max(1))..=(nfb.min(FT2_NH1 - 2)) {
        if savsm[i] > savsm[i - 1] && savsm[i] > savsm[i + 1] && savsm[i] > xmax {
            xmax = savsm[i];
            imax = Some(i);
        }
    }
    if let Some(i) = imax.filter(|_| xmax > 1.2) {
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
    options: &DecodeOptions,
) -> Option<DecodedMessage> {
    let c2 = ft2_downsample(samples, candidate.freq_hz);
    let refs = Ft2Refs::global();
    let mut best_ibest = 0usize;
    let mut best_df = 0i32;
    let mut best_sync = f32::NEG_INFINITY;

    for df in -30..=30 {
        let cb = twkfreq(&c2, df as f32, 750.0);
        for is in 0..375usize {
            let mut csync = Complex32::new(0.0, 0.0);
            let mut cterm = Complex32::new(1.0, 0.0);
            for (ib, &sync) in FT2_SYNC.iter().enumerate() {
                let i1 = ib * FT2_DOWNSAMPLED_SYMBOL_SAMPLES + is;
                let corr = if sync == 1 {
                    dot_conj(
                        &cb[i1..i1 + FT2_DOWNSAMPLED_SYMBOL_SAMPLES],
                        &refs.c1,
                    )
                } else {
                    dot_conj(
                        &cb[i1..i1 + FT2_DOWNSAMPLED_SYMBOL_SAMPLES],
                        &refs.c0,
                    )
                };
                csync += corr * cterm;
                cterm *= if sync == 1 { refs.cc1 } else { refs.cc0 };
            }
            let metric = csync.norm();
            if metric > best_sync {
                best_sync = metric;
                best_ibest = is;
                best_df = df;
            }
        }
    }

    let cb = twkfreq(&c2, best_df as f32, 750.0);
    let mut cd = cb[best_ibest..best_ibest + FT2_SYMBOL_COUNT * FT2_DOWNSAMPLED_SYMBOL_SAMPLES]
        .to_vec();
    let power = cd.iter().map(|value| value.norm_sqr()).sum::<f32>() / cd.len() as f32;
    if power <= 0.0 {
        return None;
    }
    let gain = power.sqrt();
    for sample in &mut cd {
        *sample /= gain;
    }

    let mut ccor0 = [Complex32::new(0.0, 0.0); FT2_SYMBOL_COUNT];
    let mut ccor1 = [Complex32::new(0.0, 0.0); FT2_SYMBOL_COUNT];
    let mut sbits = [0.0f32; FT2_SYMBOL_COUNT];
    let mut hbits = [0u8; FT2_SYMBOL_COUNT];
    for bit in 0..FT2_SYMBOL_COUNT {
        let start = bit * FT2_DOWNSAMPLED_SYMBOL_SAMPLES;
        ccor1[bit] = dot_conj(&cd[start..start + FT2_DOWNSAMPLED_SYMBOL_SAMPLES], &refs.c1);
        ccor0[bit] = dot_conj(&cd[start..start + FT2_DOWNSAMPLED_SYMBOL_SAMPLES], &refs.c0);
        sbits[bit] = ccor1[bit].norm() - ccor0[bit].norm();
        hbits[bit] = u8::from(sbits[bit] > 0.0);
    }

    let decoder = Ft2ParityMatrix::global();
    let mut best_decode = None;
    for nseq in 1..=5usize {
        let metrics = if nseq == 1 {
            sbits
        } else {
            sequence_metrics(nseq, &ccor0, &ccor1, refs)
        };
        let hard: [u8; FT2_SYMBOL_COUNT] = std::array::from_fn(|index| u8::from(metrics[index] > 0.0));
        let sync_ok = FT2_SYNC
            .iter()
            .zip(hard[..16].iter())
            .filter(|(left, right)| **left == **right)
            .count();
        if sync_ok < 10 {
            break;
        }

        let rxdata = &metrics[16..];
        let mean = rxdata.iter().copied().sum::<f32>() / rxdata.len() as f32;
        let mean_sq = rxdata.iter().map(|value| value * value).sum::<f32>() / rxdata.len() as f32;
        let sigma = (mean_sq - mean * mean).max(1e-6).sqrt();
        let llrs: Vec<f32> = rxdata
            .iter()
            .map(|value| 2.0 * (value / sigma) / (0.8 * 0.8))
            .collect();

        let max_osd = match options.profile {
            super::DecodeProfile::Quick => -1,
            super::DecodeProfile::Medium => 0,
            super::DecodeProfile::Deepest => 3,
        };
        if let Some((codeword, iterations)) = decoder.decode_with_maxosd(&llrs, max_osd) {
            if iterations > FT2_BP_MAX_ITERS && best_df.abs() > 10 {
                continue;
            }
            if codeword[..77].iter().all(|bit| *bit == 0) {
                continue;
            }
            let payload = unpack_message_for_mode(Mode::Ft2, &codeword)?;
            let message = payload.to_message(&HashResolver::default());
            best_decode = Some(DecodedMessage {
                utc: "000000".to_string(),
                snr_db: (10.0 * (best_sync * best_sync).max(1e-12).log10() - 115.0).round() as i32,
                dt_seconds: best_ibest as f32 / 750.0,
                freq_hz: candidate.freq_hz + best_df as f32,
                text: message.to_text(),
                candidate_score: best_sync,
                ldpc_iterations: iterations,
                message,
            });
            break;
        }
    }
    best_decode
}

fn sequence_metrics(
    nseq: usize,
    ccor0: &[Complex32; FT2_SYMBOL_COUNT],
    ccor1: &[Complex32; FT2_SYMBOL_COUNT],
    refs: &Ft2Refs,
) -> [f32; FT2_SYMBOL_COUNT] {
    let nbit = 2 * nseq - 1;
    let half = nbit / 2;
    let numseq = 1usize << nbit;
    let mut metrics = [0.0f32; FT2_SYMBOL_COUNT];
    for target in half..(FT2_SYMBOL_COUNT - half) {
        let mut max1 = 0.0f32;
        let mut max0 = 0.0f32;
        for seq in 0..numseq {
            let mut csum = Complex32::new(0.0, 0.0);
            let mut cterm = Complex32::new(1.0, 0.0);
            for pos in 0..nbit {
                let bit = ((seq >> (nbit - 1 - pos)) & 1) as usize;
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
        metrics[target] = max1 - max0;
    }
    metrics
}

fn ft2_downsample(samples: &[f32], f0: f32) -> Vec<Complex32> {
    let mut real_planner = RealFftPlanner::<f32>::new();
    let rfft = real_planner.plan_fft_forward(FT2_NMAX);
    let mut input = rfft.make_input_vec();
    let mut spectrum = rfft.make_output_vec();
    for (dst, sample) in input.iter_mut().zip(samples.iter().copied()) {
        *dst = sample * i16::MAX as f32;
    }
    rfft.process(&mut input, &mut spectrum).expect("ft2 long fft");

    let df = 12_000.0 / FT2_NMAX as f32;
    let i0 = (f0 / df).round() as usize;
    let mut c1 = vec![Complex32::new(0.0, 0.0); FT2_NFFT2];
    c1[0] = spectrum[i0];
    for i in 1..=(FT2_NFFT2 / 2) {
        let arg = (i as f32 - 1.0) * df / (4.0 * 75.0);
        let win = (-arg * arg).exp();
        c1[i] = spectrum[i0 + i] * win;
        c1[FT2_NFFT2 - i] = spectrum[i0 - i] * win;
    }
    let scale = 1.0 / FT2_NFFT2 as f32;
    for value in &mut c1 {
        *value *= scale;
    }

    let mut planner = FftPlanner::<f32>::new();
    let ifft = planner.plan_fft_inverse(FT2_NFFT2);
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
    c0: [Complex32; FT2_DOWNSAMPLED_SYMBOL_SAMPLES],
    c1: [Complex32; FT2_DOWNSAMPLED_SYMBOL_SAMPLES],
    cc0: Complex32,
    cc1: Complex32,
}

impl Ft2Refs {
    fn global() -> &'static Self {
        static REFS: OnceLock<Ft2Refs> = OnceLock::new();
        REFS.get_or_init(Self::new)
    }

    fn new() -> Self {
        let fs = 12_000.0 / FT2_NDOWN as f32;
        let dt = 1.0 / fs;
        let tt = FT2_NSPS as f32 * dt;
        let baud = 1.0 / tt;
        let h = 0.8;
        let twopi = 2.0 * std::f32::consts::PI;
        let dphi = twopi / 2.0 * baud * h * dt * 16.0;
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
        let mut row_columns = vec![Vec::new(); FT2_ROWS];
        for (column, rows) in FT2_COLUMN_ROWS.iter().enumerate() {
            for &row in rows {
                row_columns[row].push(column);
            }
        }
        let mut row_column_slots = vec![Vec::new(); FT2_ROWS];
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
        let generator_rows = build_ft2_generator_rows();
        Self {
            row_columns,
            row_column_slots,
            generator_rows,
        }
    }

    fn parity_ok(&self, bits: &[u8]) -> bool {
        bits.len() == FT2_COLUMNS
            && self
                .row_columns
                .iter()
                .all(|row| row.iter().fold(0u8, |acc, &column| acc ^ bits[column]) == 0)
    }

    fn decode_with_maxosd(&self, llrs: &[f32], maxosd: isize) -> Option<(Vec<u8>, usize)> {
        if llrs.len() != FT2_COLUMNS {
            return None;
        }
        let maxosd = maxosd.clamp(-1, 3);

        let mut tov = [[0.0f32; 3]; FT2_COLUMNS];
        let mut toc = [[0.0f32; 11]; FT2_ROWS];
        let mut tanhtoc = [[0.0f32; 11]; FT2_ROWS];
        let mut zn = [0.0f32; FT2_COLUMNS];
        let mut zsum = [0.0f32; FT2_COLUMNS];
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
            for column in 0..FT2_COLUMNS {
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

            let hard: [u8; FT2_COLUMNS] = std::array::from_fn(|index| u8::from(zn[index] > 0.0));
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

            for column in 0..FT2_COLUMNS {
                for (slot, &row_index) in FT2_COLUMN_ROWS[column].iter().enumerate() {
                    let mut product = 1.0f32;
                    for (row_slot, &other_column) in self.row_columns[row_index].iter().enumerate()
                    {
                        if other_column == column {
                            continue;
                        }
                        product *= tanhtoc[row_index][row_slot];
                    }
                    tov[column][slot] = 2.0 * atanh_clamped(-product);
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
        const MRB_SEARCH_EXTRA: usize = 20;

        if llrs.len() != FT2_COLUMNS {
            return None;
        }

        let mut indices: Vec<usize> = (0..FT2_COLUMNS).collect();
        indices.sort_by(|left, right| llrs[*right].abs().total_cmp(&llrs[*left].abs()));

        let mut genmrb: Vec<Vec<u8>> = self
            .generator_rows
            .iter()
            .map(|row| indices.iter().map(|&index| row[index]).collect())
            .collect();
        let mut permuted_indices = indices;

        for pivot in 0..FT2_INFO_BITS {
            let search_end = (FT2_INFO_BITS + MRB_SEARCH_EXTRA).min(FT2_COLUMNS);
            let column = (pivot..search_end).find(|&column| genmrb[pivot][column] == 1)?;
            if column != pivot {
                for row in &mut genmrb {
                    row.swap(pivot, column);
                }
                permuted_indices.swap(pivot, column);
            }
            for row in 0..FT2_INFO_BITS {
                if row != pivot && genmrb[row][pivot] == 1 {
                    for column in 0..FT2_COLUMNS {
                        genmrb[row][column] ^= genmrb[pivot][column];
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
                .take(FT2_OSD_NT)
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
                    .take(FT2_OSD_NT)
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

        let mut restored = vec![0u8; FT2_COLUMNS];
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

fn atanh_clamped(value: f32) -> f32 {
    let clamped = value.clamp(-0.999_999, 0.999_999);
    0.5 * ((1.0 + clamped) / (1.0 - clamped)).ln()
}

fn build_ft2_generator_rows() -> Vec<Vec<u8>> {
    let parity_columns: Vec<Vec<u8>> = FT2_GENERATOR_HEX
        .iter()
        .map(|hex| parse_ft2_generator_bits(hex))
        .collect();
    let mut rows = vec![vec![0u8; FT2_COLUMNS]; FT2_INFO_BITS];
    for (row_index, row) in rows.iter_mut().enumerate() {
        row[row_index] = 1;
        for (parity_index, parity_column) in parity_columns.iter().enumerate() {
            row[FT2_INFO_BITS + parity_index] = parity_column[row_index];
        }
    }
    rows
}

fn parse_ft2_generator_bits(hex: &str) -> Vec<u8> {
    let mut bits = Vec::with_capacity(FT2_INFO_BITS);
    for (index, ch) in hex.bytes().enumerate() {
        let value = match ch {
            b'0'..=b'9' => ch - b'0',
            b'a'..=b'f' => ch - b'a' + 10,
            b'A'..=b'F' => ch - b'A' + 10,
            _ => continue,
        };
        let limit = if index == hex.len().saturating_sub(1) { 2 } else { 4 };
        for shift in (4 - limit..4).rev() {
            bits.push(u8::from(((value >> shift) & 1) == 1));
        }
    }
    bits.truncate(FT2_INFO_BITS);
    bits
}

fn encode_mrb_ft2(message: &[u8], generator_rows: &[Vec<u8>]) -> Vec<u8> {
    let mut codeword = vec![0u8; FT2_COLUMNS];
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
    use crate::encode::{WaveformOptions, encode_standard_message_for_mode, synthesize_rectangular_waveform};
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
                ..WaveformOptions::default()
            },
        )
        .expect("waveform");
        let report = decode_ft2(&audio, &DecodeOptions {
            mode: Mode::Ft2,
            ..DecodeOptions::default()
        })
        .expect("decode");
        assert!(
            report
                .decodes
                .iter()
                .any(|decode| decode.text == "K1ABC W1XYZ FN31")
        );
    }
}
