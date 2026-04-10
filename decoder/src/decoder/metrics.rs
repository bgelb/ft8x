use super::*;
use crate::protocol::{
    FTX_AP_KNOWN_FIELDS, FTX_BITS_PER_SYMBOL, FTX_CODEWORD_BITS, copy_known_message_bits,
    gray_encode_3bit_value,
};

// FT8's legacy bitmetric walk evaluates 1-, 2-, and 3-symbol hypotheses.
const BITMETRIC_MAX_COMBINED_SYMBOLS: usize = 3;
const BITMETRIC_SYMBOL_VALUE_MASK: usize = (1usize << FTX_BITS_PER_SYMBOL) - 1;

pub(super) fn truth_metrics(
    spec: &ModeSpec,
    llrs: &[f32],
    truth_codeword_bits: &[u8],
) -> Option<(Option<usize>, Option<f32>)> {
    if llrs.len() != spec.coding.codeword_bits
        || truth_codeword_bits.len() < spec.coding.codeword_bits
    {
        return None;
    }
    let mut hard_errors = 0usize;
    let mut weighted_distance = 0.0f32;
    for (llr, &truth_bit) in llrs.iter().zip(truth_codeword_bits.iter()) {
        let hard_bit = u8::from(*llr >= 0.0);
        if hard_bit != truth_bit {
            hard_errors += 1;
            weighted_distance += llr.abs();
        }
    }
    Some((Some(hard_errors), Some(weighted_distance)))
}

pub(super) fn compute_bitmetric_passes(
    spec: &ModeSpec,
    full_tones: &[[Complex32; 8]],
) -> [Vec<f32>; 4] {
    match spec.mode {
        Mode::Ft8 => compute_ft8_bitmetric_passes(spec, full_tones),
        Mode::Ft4 => compute_ft4_bitmetric_passes(spec, full_tones),
        Mode::Ft2 => std::array::from_fn(|_| Vec::new()),
    }
}

pub(super) fn compute_ft4_candidate_metrics(
    spec: &ModeSpec,
    baseband: &[Complex32],
    start_index: isize,
    enforce_sync_quality: bool,
) -> Option<([Vec<f32>; 4], Vec<f32>, i32)> {
    debug_assert_eq!(spec.mode, Mode::Ft4);
    let nss = spec.baseband_symbol_samples();
    let nn = spec.geometry.message_symbols;
    let frame_len = nss * nn;
    let valid_samples = baseband.len().min(spec.baseband_valid_samples());
    let mut frame = vec![Complex32::new(0.0, 0.0); frame_len];

    for (offset, slot) in frame.iter_mut().enumerate() {
        let sample_index = start_index + offset as isize;
        if (0..valid_samples as isize).contains(&sample_index) {
            *slot = baseband[sample_index as usize];
        }
    }

    let fft = ft4_metric_fft();
    let mut cs = vec![[Complex32::new(0.0, 0.0); 4]; nn];
    let mut s4 = vec![[0.0f32; 4]; nn];
    for symbol_index in 0..nn {
        let start = symbol_index * nss;
        let mut spectrum = frame[start..start + nss].to_vec();
        fft.process(&mut spectrum);
        for tone in 0..4 {
            cs[symbol_index][tone] = spectrum[tone];
            s4[symbol_index][tone] = spectrum[tone].norm();
        }
    }

    let sync_patterns = [[0usize, 1, 3, 2], [1, 0, 2, 3], [2, 3, 1, 0], [3, 2, 0, 1]];
    let sync_offsets = [0usize, 33, 66, 99];
    let mut nsync = 0usize;
    for (block_offset, pattern) in sync_offsets.iter().zip(sync_patterns.iter()) {
        for (symbol_offset, &expected_tone) in pattern.iter().enumerate() {
            let best_tone = s4[*block_offset + symbol_offset]
                .iter()
                .enumerate()
                .max_by(|left, right| left.1.total_cmp(right.1))
                .map(|(index, _)| index)
                .unwrap_or(0);
            if best_tone == expected_tone {
                nsync += 1;
            }
        }
    }
    if enforce_sync_quality && nsync < 8 {
        return None;
    }

    const FT4_MESSAGE_BITS: usize = 206;
    const FT4_GRAY_MAP: [usize; 4] = [0, 1, 3, 2];
    let mut raw = [
        vec![0.0f32; FT4_MESSAGE_BITS],
        vec![0.0f32; FT4_MESSAGE_BITS],
        vec![0.0f32; FT4_MESSAGE_BITS],
    ];

    for (pass_index, nsym) in [1usize, 2, 4].into_iter().enumerate() {
        let nt = 1usize << (2 * nsym);
        for ks in (0..=nn.saturating_sub(nsym)).step_by(nsym) {
            let mut s2 = vec![0.0f32; nt];
            for (value, metric) in s2.iter_mut().enumerate() {
                let i1 = value / 64;
                let i2 = (value & 63) / 16;
                let i3 = (value & 15) / 4;
                let i4 = value & 3;
                *metric = match nsym {
                    1 => cs[ks][FT4_GRAY_MAP[i4]].norm(),
                    2 => (cs[ks][FT4_GRAY_MAP[i3]] + cs[ks + 1][FT4_GRAY_MAP[i4]]).norm(),
                    4 => (cs[ks][FT4_GRAY_MAP[i1]]
                        + cs[ks + 1][FT4_GRAY_MAP[i2]]
                        + cs[ks + 2][FT4_GRAY_MAP[i3]]
                        + cs[ks + 3][FT4_GRAY_MAP[i4]])
                        .norm(),
                    _ => unreachable!(),
                };
            }

            let ipt = ks * 2;
            let ibmax = match nsym {
                1 => 1,
                2 => 3,
                4 => 7,
                _ => unreachable!(),
            };
            for ib in 0..=ibmax {
                let target_bit = ipt + ib;
                if target_bit >= FT4_MESSAGE_BITS {
                    continue;
                }
                let decision_bit = ibmax - ib;
                let mut best_one = f32::NEG_INFINITY;
                let mut best_zero = f32::NEG_INFINITY;
                for (value, metric) in s2.iter().enumerate() {
                    if ((value >> decision_bit) & 1) == 1 {
                        best_one = best_one.max(*metric);
                    } else {
                        best_zero = best_zero.max(*metric);
                    }
                }
                raw[pass_index][target_bit] = best_one - best_zero;
            }
        }
    }

    let tail0 = raw[0][204..206].to_vec();
    let tail1 = raw[1][200..204].to_vec();
    raw[1][204..206].copy_from_slice(&tail0);
    raw[2][200..204].copy_from_slice(&tail1);
    raw[2][204..206].copy_from_slice(&tail0);

    if enforce_sync_quality {
        let hard_bits: Vec<u8> = raw[0].iter().map(|value| u8::from(*value >= 0.0)).collect();
        let sync_checks = [
            (&hard_bits[0..8], [0u8, 0, 0, 1, 1, 0, 1, 1]),
            (&hard_bits[66..74], [0u8, 1, 0, 0, 1, 1, 1, 0]),
            (&hard_bits[132..140], [1u8, 1, 1, 0, 0, 1, 0, 0]),
            (&hard_bits[198..206], [1u8, 0, 1, 1, 0, 0, 0, 1]),
        ];
        let nsync_qual = sync_checks
            .iter()
            .map(|(observed, expected)| {
                observed
                    .iter()
                    .zip(expected.iter())
                    .filter(|(left, right)| **left == **right)
                    .count()
            })
            .sum::<usize>();
        if nsync_qual < 20 {
            return None;
        }
    }

    for pass in &mut raw {
        normalize_metric_vector(pass);
    }

    let ranges = [(8, 66), (74, 132), (140, 198)];
    let mut outputs = std::array::from_fn(|_| vec![0.0f32; spec.coding.codeword_bits]);
    for (dest, source) in outputs.iter_mut().zip(raw.iter()) {
        let mut out = 0usize;
        for (start, end) in ranges {
            for &value in &source[start..end] {
                dest[out] = value * spec.refine.llr_scale_factor;
                out += 1;
            }
        }
    }
    outputs[3] = outputs[2].clone();

    let mut full_tones = vec![[Complex32::new(0.0, 0.0); 8]; nn];
    for (symbol_index, tones) in cs.into_iter().enumerate() {
        for (tone_index, tone) in tones.into_iter().enumerate() {
            full_tones[symbol_index][tone_index] = tone;
        }
    }
    let symbol_bit_llrs = compute_symbol_bit_llrs(spec, &full_tones);
    let snr_db = estimate_snr_db(spec, &full_tones);
    Some((outputs, symbol_bit_llrs, snr_db))
}

fn compute_ft8_bitmetric_passes(spec: &ModeSpec, full_tones: &[[Complex32; 8]]) -> [Vec<f32>; 4] {
    let mut bmeta = vec![0.0f32; FTX_CODEWORD_BITS];
    let mut bmetb = vec![0.0f32; FTX_CODEWORD_BITS];
    let mut bmetc = vec![0.0f32; FTX_CODEWORD_BITS];
    let mut bmetd = vec![0.0f32; FTX_CODEWORD_BITS];
    let half_bits = spec.codeword_half_bits();
    let groups_per_half = spec.groups_per_half();
    let half_symbol_starts = spec.bitmetric_half_start_symbols();

    for nsym in 1..=BITMETRIC_MAX_COMBINED_SYMBOLS {
        let nt = 1usize << (FTX_BITS_PER_SYMBOL * nsym);
        let decision_max_bit = bitmetric_decision_max_bit(nsym);
        for (half, &half_symbol_start) in half_symbol_starts.iter().enumerate() {
            for k in (1..=groups_per_half).step_by(nsym) {
                let ks = half_symbol_start + k;
                let start_bit = (k - 1) * FTX_BITS_PER_SYMBOL + half * half_bits;
                let mut metrics = vec![0.0f32; nt];
                for (i, metric) in metrics.iter_mut().enumerate() {
                    let tone0 = bitmetric_metric_tone(i, nsym, 0);
                    *metric = full_tones[ks][tone0].norm();
                    if nsym >= 2 {
                        let tone1 = bitmetric_metric_tone(i, nsym, 1);
                        *metric = (full_tones[ks][tone0] + full_tones[ks + 1][tone1]).norm();
                        if nsym >= 3 {
                            let tone2 = bitmetric_metric_tone(i, nsym, 2);
                            *metric = (full_tones[ks][tone0]
                                + full_tones[ks + 1][tone1]
                                + full_tones[ks + 2][tone2])
                                .norm();
                        }
                    }
                }

                for ib in 0..=decision_max_bit {
                    let target_bit = start_bit + ib;
                    if target_bit >= FTX_CODEWORD_BITS {
                        continue;
                    }
                    let decision_bit = decision_max_bit - ib;
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

    for metric_set in [&mut bmeta, &mut bmetb, &mut bmetc, &mut bmetd] {
        for value in metric_set.iter_mut() {
            *value *= spec.refine.llr_scale_factor;
        }
    }

    [bmeta, bmetb, bmetc, bmetd]
}

fn compute_ft4_bitmetric_passes(spec: &ModeSpec, full_tones: &[[Complex32; 8]]) -> [Vec<f32>; 4] {
    const FT4_MESSAGE_BITS: usize = 206;
    const FT4_GRAY_MAP: [usize; 4] = [0, 1, 3, 2];
    const FT4_PASSES: [usize; 3] = [1, 2, 4];
    let mut raw = [
        vec![0.0f32; FT4_MESSAGE_BITS],
        vec![0.0f32; FT4_MESSAGE_BITS],
        vec![0.0f32; FT4_MESSAGE_BITS],
    ];

    for (pass_index, nsym) in FT4_PASSES.into_iter().enumerate() {
        let nt = 1usize << (2 * nsym);
        for ks in (0..=spec.geometry.message_symbols.saturating_sub(nsym)).step_by(nsym) {
            let start_bit = ks * 2;
            let decision_max_bit = 2 * nsym - 1;
            let mut metrics = vec![0.0f32; nt];
            for (value, metric) in metrics.iter_mut().enumerate() {
                let mut sum = Complex32::new(0.0, 0.0);
                for symbol_offset in 0..nsym {
                    let shift = 2 * (nsym - symbol_offset - 1);
                    let tone_bits = (value >> shift) & 0b11;
                    let tone = FT4_GRAY_MAP[tone_bits];
                    sum += full_tones[ks + symbol_offset][tone];
                }
                *metric = sum.norm();
            }
            for ib in 0..=decision_max_bit {
                let target_bit = start_bit + ib;
                if target_bit >= FT4_MESSAGE_BITS {
                    continue;
                }
                let decision_bit = decision_max_bit - ib;
                let mut best_one = f32::NEG_INFINITY;
                let mut best_zero = f32::NEG_INFINITY;
                for (value, metric) in metrics.iter().enumerate() {
                    if ((value >> decision_bit) & 1) == 1 {
                        best_one = best_one.max(*metric);
                    } else {
                        best_zero = best_zero.max(*metric);
                    }
                }
                raw[pass_index][target_bit] = best_one - best_zero;
            }
        }
    }

    let tail0 = raw[0][204..206].to_vec();
    let tail1 = raw[1][200..204].to_vec();
    raw[1][204..206].copy_from_slice(&tail0);
    raw[2][200..204].copy_from_slice(&tail1);
    raw[2][204..206].copy_from_slice(&tail0);

    for pass in &mut raw {
        normalize_metric_vector(pass);
    }

    let ranges = [(8, 66), (74, 132), (140, 198)];
    let mut outputs = std::array::from_fn(|_| vec![0.0f32; spec.coding.codeword_bits]);
    for (dest, source) in outputs.iter_mut().zip(raw.iter()) {
        let mut out = 0usize;
        for (start, end) in ranges {
            for &value in &source[start..end] {
                dest[out] = value * spec.refine.llr_scale_factor;
                out += 1;
            }
        }
    }
    outputs[3] = outputs[2].clone();
    outputs
}

fn ft4_metric_fft() -> &'static Arc<dyn Fft<f32>> {
    static FFT: OnceLock<Arc<dyn Fft<f32>>> = OnceLock::new();
    FFT.get_or_init(|| {
        let mut planner = FftPlanner::<f32>::new();
        planner.plan_fft_forward(Mode::Ft4.spec().baseband_symbol_samples())
    })
}

// A combined nsym-symbol hypothesis carries nsym * 3 code bits, numbered from the most
// significant bit down to zero in the legacy FT8 bitmetric walk.
fn bitmetric_decision_max_bit(nsym: usize) -> usize {
    debug_assert!((1..=BITMETRIC_MAX_COMBINED_SYMBOLS).contains(&nsym));
    FTX_BITS_PER_SYMBOL * nsym - 1
}

// Interpret `metric_index` as `nsym` concatenated 3-bit Gray-coded tones. The earliest symbol
// lives in the most-significant triplet so the extraction order matches the legacy decoder.
fn bitmetric_metric_tone(metric_index: usize, nsym: usize, symbol_offset: usize) -> usize {
    debug_assert!(symbol_offset < nsym);
    let shift = FTX_BITS_PER_SYMBOL * (nsym - symbol_offset - 1);
    gray_encode_3bit_value(((metric_index >> shift) & BITMETRIC_SYMBOL_VALUE_MASK) as u8) as usize
}

pub(super) fn compute_symbol_bit_llrs(spec: &ModeSpec, full_tones: &[[Complex32; 8]]) -> Vec<f32> {
    match spec.mode {
        Mode::Ft8 => {
            let data_tones: Vec<[f32; 8]> = spec
                .geometry
                .data_symbol_positions
                .iter()
                .map(|&symbol_index| {
                    std::array::from_fn(|tone| full_tones[symbol_index][tone].norm_sqr())
                })
                .collect();
            ParityMatrix::symbol_bit_llrs(&data_tones)
                .into_iter()
                .flat_map(|symbol| symbol.into_iter())
                .collect()
        }
        Mode::Ft4 => spec
            .geometry
            .data_symbol_positions
            .iter()
            .flat_map(|&symbol_index| {
                let tones: [f32; 4] =
                    std::array::from_fn(|tone| full_tones[symbol_index][tone].norm_sqr());
                let noise = tones
                    .iter()
                    .copied()
                    .fold(f32::INFINITY, f32::min)
                    .max(1e-6);
                (0..2)
                    .map(|bit_index| {
                        let mut best_zero = f32::NEG_INFINITY;
                        let mut best_one = f32::NEG_INFINITY;
                        for (tone, &energy) in tones.iter().enumerate() {
                            let bits = match tone {
                                0 => [0, 0],
                                1 => [0, 1],
                                2 => [1, 1],
                                3 => [1, 0],
                                _ => unreachable!(),
                            };
                            if bits[bit_index] == 0 {
                                best_zero = best_zero.max(energy);
                            } else {
                                best_one = best_one.max(energy);
                            }
                        }
                        ((best_one - best_zero) / noise).clamp(-24.0, 24.0)
                    })
                    .collect::<Vec<_>>()
            })
            .collect(),
        Mode::Ft2 => Vec::new(),
    }
}

pub(super) fn decode_llr_set(
    mode: Mode,
    parity: &ParityMatrix,
    llrs: &[f32],
    max_osd: isize,
    counters: &mut DecodeCounters,
) -> Option<(Payload, Vec<u8>, usize)> {
    let Some((bits, iterations)) = parity.decode_with_maxosd(llrs, max_osd) else {
        return None;
    };
    if bits.iter().all(|bit| *bit == 0) {
        return None;
    }
    counters.ldpc_codewords += 1;
    let Some(payload) = unpack_message_for_mode(mode, &bits) else {
        return None;
    };
    if matches!(payload, Payload::Unsupported(_)) {
        return None;
    }
    counters.parsed_payloads += 1;
    Some((payload, bits, iterations))
}

pub(super) fn decode_llr_set_with_known_bits(
    mode: Mode,
    parity: &ParityMatrix,
    llrs: &[f32],
    known_bits: &[Option<u8>],
    max_osd: isize,
    counters: &mut DecodeCounters,
) -> Option<(Payload, Vec<u8>, usize)> {
    let Some((bits, iterations)) =
        parity.decode_with_known_bits_and_maxosd(llrs, known_bits, max_osd)
    else {
        return None;
    };
    if bits.iter().all(|bit| *bit == 0) {
        return None;
    }
    counters.ldpc_codewords += 1;
    let Some(payload) = unpack_message_for_mode(mode, &bits) else {
        return None;
    };
    if matches!(payload, Payload::Unsupported(_)) {
        return None;
    }
    counters.parsed_payloads += 1;
    Some((payload, bits, iterations))
}

pub(super) fn cq_ap_known_bits(mode: Mode) -> &'static [Option<u8>] {
    fn build(mode: Mode) -> Vec<Option<u8>> {
        let frame = crate::encode::encode_standard_message_for_mode(
            mode,
            "CQ",
            "K1ABC",
            false,
            &GridReport::Blank,
        )
        .expect("encode CQ AP template");
        let mut known = vec![None; mode.spec().coding.codeword_bits];
        match mode {
            Mode::Ft4 => {
                copy_known_message_bits(
                    &mut known,
                    &frame.codeword_bits,
                    &[crate::protocol::BitField { start: 0, len: 29 }],
                )
                .expect("copy AP template bits");
            }
            Mode::Ft8 => {
                copy_known_message_bits(&mut known, &frame.message_bits, &FTX_AP_KNOWN_FIELDS)
                    .expect("copy AP template bits");
            }
            Mode::Ft2 => {}
        }
        known
    }

    match mode {
        Mode::Ft8 => {
            static BITS: OnceLock<Vec<Option<u8>>> = OnceLock::new();
            BITS.get_or_init(|| build(mode))
        }
        Mode::Ft4 => {
            static BITS: OnceLock<Vec<Option<u8>>> = OnceLock::new();
            BITS.get_or_init(|| build(mode))
        }
        Mode::Ft2 => &[],
    }
}

pub(super) fn mycall_ap_known_bits(mode: Mode) -> &'static [Option<u8>] {
    fn build(mode: Mode) -> Vec<Option<u8>> {
        let frame = crate::encode::encode_standard_message_for_mode(
            mode,
            "K1ABC",
            "KA1ABC",
            false,
            &GridReport::Reply(crate::message::ReplyWord::Rrr),
        )
        .expect("encode MyCall AP template");
        let mut known = vec![None; mode.spec().coding.codeword_bits];
        match mode {
            Mode::Ft4 => {
                copy_known_message_bits(
                    &mut known,
                    &frame.codeword_bits,
                    &[crate::protocol::BitField { start: 0, len: 29 }],
                )
                .expect("copy AP template bits");
            }
            Mode::Ft8 => {
                copy_known_message_bits(&mut known, &frame.message_bits, &FTX_AP_KNOWN_FIELDS)
                    .expect("copy AP template bits");
            }
            Mode::Ft2 => {}
        }
        known
    }

    match mode {
        Mode::Ft8 => {
            static BITS: OnceLock<Vec<Option<u8>>> = OnceLock::new();
            BITS.get_or_init(|| build(mode))
        }
        Mode::Ft4 => {
            static BITS: OnceLock<Vec<Option<u8>>> = OnceLock::new();
            BITS.get_or_init(|| build(mode))
        }
        Mode::Ft2 => &[],
    }
}

pub(super) fn llrs_with_known_bits(
    llrs: &[f32],
    known_bits: &[Option<u8>],
    magnitude: f32,
) -> Vec<f32> {
    let mut constrained = llrs.to_vec();
    for (slot, bit) in constrained.iter_mut().zip(known_bits.iter().copied()) {
        let Some(bit) = bit else {
            continue;
        };
        *slot = if bit == 1 { magnitude } else { -magnitude };
    }
    constrained
}

pub(super) fn normalize_metric_vector(values: &mut [f32]) {
    let mean = values.iter().copied().sum::<f32>() / values.len() as f32;
    let second = values.iter().map(|value| value * value).sum::<f32>() / values.len() as f32;
    let variance = second - mean * mean;
    let sigma = if variance > 0.0 {
        variance.sqrt()
    } else {
        second.sqrt()
    };
    if sigma > 0.0 {
        for value in values {
            *value /= sigma;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::FTX_AP_KNOWN_FIELDS;

    #[test]
    fn bitmetric_passes_preserve_codeword_bit_order_on_ideal_tones() {
        let spec = Mode::Ft8.spec();
        let frame =
            crate::encode::encode_standard_message("CQ", "K1ABC", false, &GridReport::Blank)
                .expect("encode frame");
        let channel_symbols =
            crate::encode::channel_symbols_from_codeword_bits(&frame.codeword_bits)
                .expect("channel symbols");
        let mut full_tones = vec![[Complex32::new(1.0, 0.0); 8]; spec.geometry.message_symbols];
        for (symbol_index, tone) in channel_symbols.iter().copied().enumerate() {
            full_tones[symbol_index][tone as usize] = Complex32::new(9.0, 0.0);
        }

        let hard_bits: Vec<u8> = compute_bitmetric_passes(spec, &full_tones)[0]
            .iter()
            .map(|value| u8::from(*value >= 0.0))
            .collect();

        assert_eq!(hard_bits, frame.codeword_bits);
    }

    #[test]
    fn ft4_cq_ap_template_matches_stock_mcq_bits() {
        let known = cq_ap_known_bits(Mode::Ft4);
        let first_bits: String = known[FTX_AP_KNOWN_FIELDS[0].start..FTX_AP_KNOWN_FIELDS[0].end()]
            .iter()
            .map(|bit| char::from(b'0' + bit.expect("known FT4 CQ AP bit")))
            .collect();
        assert_eq!(first_bits, "01001010010111101000100110010");
        for bit in &known[FTX_AP_KNOWN_FIELDS[1].start..FTX_AP_KNOWN_FIELDS[1].end()] {
            assert_eq!(*bit, None);
        }
    }
}
