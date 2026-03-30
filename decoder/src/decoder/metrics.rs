use super::*;
use crate::protocol::{
    FTX_AP_KNOWN_FIELDS, FTX_BITS_PER_SYMBOL, FTX_CODEWORD_BITS, copy_known_message_bits,
    gray_encode_3bit_value,
};

// FT8's legacy bitmetric walk evaluates 1-, 2-, and 3-symbol hypotheses.
const BITMETRIC_MAX_COMBINED_SYMBOLS: usize = 3;
const BITMETRIC_SYMBOL_VALUE_MASK: usize = (1usize << FTX_BITS_PER_SYMBOL) - 1;

pub(super) fn truth_metrics(
    llrs: &[f32],
    truth_codeword_bits: &[u8],
) -> Option<(Option<usize>, Option<f32>)> {
    if llrs.len() != FTX_CODEWORD_BITS || truth_codeword_bits.len() < FTX_CODEWORD_BITS {
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

pub(super) fn compute_bitmetric_passes(full_tones: &[[Complex32; 8]]) -> [Vec<f32>; 4] {
    let mut bmeta = vec![0.0f32; FTX_CODEWORD_BITS];
    let mut bmetb = vec![0.0f32; FTX_CODEWORD_BITS];
    let mut bmetc = vec![0.0f32; FTX_CODEWORD_BITS];
    let mut bmetd = vec![0.0f32; FTX_CODEWORD_BITS];
    let half_bits = ACTIVE_MODE.codeword_half_bits();
    let groups_per_half = ACTIVE_MODE.groups_per_half();
    let half_symbol_starts = ACTIVE_MODE.bitmetric_half_start_symbols();

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
            *value *= ACTIVE_MODE.tuning.llr_scale_factor;
        }
    }

    [bmeta, bmetb, bmetc, bmetd]
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

pub(super) fn compute_symbol_bit_llrs(full_tones: &[[Complex32; 8]]) -> Vec<f32> {
    let data_tones: Vec<[f32; 8]> = ACTIVE_MODE
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

pub(super) fn decode_llr_set(
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
    let Some(payload) = unpack_message(&bits) else {
        return None;
    };
    if matches!(payload, Payload::Unsupported(_)) {
        return None;
    }
    counters.parsed_payloads += 1;
    Some((payload, bits, iterations))
}

pub(super) fn decode_llr_set_with_known_bits(
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
    let Some(payload) = unpack_message(&bits) else {
        return None;
    };
    if matches!(payload, Payload::Unsupported(_)) {
        return None;
    }
    counters.parsed_payloads += 1;
    Some((payload, bits, iterations))
}

pub(super) fn cq_ap_known_bits() -> &'static [Option<u8>] {
    static BITS: OnceLock<Vec<Option<u8>>> = OnceLock::new();
    BITS.get_or_init(|| {
        let frame =
            crate::encode::encode_standard_message("CQ", "K1ABC", false, &GridReport::Blank)
                .expect("encode CQ AP template");
        let mut known = vec![None; FTX_CODEWORD_BITS];
        copy_known_message_bits(&mut known, &frame.message_bits, &FTX_AP_KNOWN_FIELDS)
            .expect("copy AP template bits");
        known
    })
}

pub(super) fn mycall_ap_known_bits() -> &'static [Option<u8>] {
    static BITS: OnceLock<Vec<Option<u8>>> = OnceLock::new();
    BITS.get_or_init(|| {
        let frame = crate::encode::encode_standard_message(
            "K1ABC",
            "KA1ABC",
            false,
            &GridReport::Reply(crate::message::ReplyWord::Rrr),
        )
        .expect("encode MyCall AP template");
        let mut known = vec![None; FTX_CODEWORD_BITS];
        copy_known_message_bits(&mut known, &frame.message_bits, &FTX_AP_KNOWN_FIELDS)
            .expect("copy AP template bits");
        known
    })
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

    #[test]
    fn bitmetric_passes_preserve_codeword_bit_order_on_ideal_tones() {
        let frame =
            crate::encode::encode_standard_message("CQ", "K1ABC", false, &GridReport::Blank)
                .expect("encode frame");
        let channel_symbols =
            crate::encode::channel_symbols_from_codeword_bits(&frame.codeword_bits)
                .expect("channel symbols");
        let mut full_tones = vec![[Complex32::new(1.0, 0.0); 8]; ACTIVE_MODE.geometry.message_symbols];
        for (symbol_index, tone) in channel_symbols.iter().copied().enumerate() {
            full_tones[symbol_index][tone as usize] = Complex32::new(9.0, 0.0);
        }

        let hard_bits: Vec<u8> = compute_bitmetric_passes(&full_tones)[0]
            .iter()
            .map(|value| u8::from(*value >= 0.0))
            .collect();

        assert_eq!(hard_bits, frame.codeword_bits);
    }
}
