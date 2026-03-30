use super::*;

pub(super) fn truth_metrics(
    llrs: &[f32],
    truth_codeword_bits: &[u8],
) -> Option<(Option<usize>, Option<f32>)> {
    if llrs.len() != 174 || truth_codeword_bits.len() < 174 {
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
                    let tone0 = graymap[(i >> (3 * (nsym - 1))) & 0b111];
                    let tone1 = graymap[(i >> (3 * (nsym.saturating_sub(2)))) & 0b111];
                    *metric = full_tones[ks][tone0].norm();
                    if nsym >= 2 {
                        *metric = (full_tones[ks][tone0] + full_tones[ks + 1][tone1]).norm();
                    }
                    if nsym >= 3 {
                        let tone2 = graymap[i & 0b111];
                        *metric = (full_tones[ks][tone0]
                            + full_tones[ks + 1][tone1]
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
    if matches!(payload, Payload::Unsupported(_, _)) {
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
    if matches!(payload, Payload::Unsupported(_, _)) {
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
