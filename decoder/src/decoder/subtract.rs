use super::*;

pub(super) fn subtract_candidate(
    audio: &mut AudioBuffer,
    success: &SuccessfulDecode,
    plan: &SubtractionPlan,
) {
    subtract_candidate_with_dt_refinement(audio, success, plan, false);
}

pub(super) fn subtract_candidate_with_dt_refinement(
    audio: &mut AudioBuffer,
    success: &SuccessfulDecode,
    plan: &SubtractionPlan,
    refine_dt: bool,
) {
    let Some(channel_symbols) = channel_symbols_from_codeword_bits(&success.codeword_bits) else {
        return;
    };
    let start_sample = ACTIVE_MODE.start_sample_from_dt(success.candidate.dt_seconds);
    let reference = synthesize_channel_reference(&channel_symbols, success.candidate.freq_hz);
    let offset_samples = if refine_dt {
        let Some(offset_samples) =
            refined_subtraction_offset(audio, &reference, success.candidate.freq_hz, start_sample, plan)
        else {
            return;
        };
        offset_samples
    } else {
        0
    };
    apply_subtraction(audio, &reference, start_sample + offset_samples, plan);
}

pub(super) fn refined_subtraction_offset(
    audio: &AudioBuffer,
    reference: &[Complex32],
    freq_hz: f32,
    start_sample: isize,
    plan: &SubtractionPlan,
) -> Option<isize> {
    let probe_step = ACTIVE_MODE.tuning.subtraction_refine_probe_step_samples;
    let sqm =
        subtraction_residual_band_power(audio, reference, freq_hz, start_sample - probe_step, plan);
    let sq0 = subtraction_residual_band_power(audio, reference, freq_hz, start_sample, plan);
    let sqp =
        subtraction_residual_band_power(audio, reference, freq_hz, start_sample + probe_step, plan);
    let b = (sqp - sqm) * 0.5;
    let c = (sqp + sqm - 2.0 * sq0) * 0.5;
    if c == 0.0 {
        return None;
    }
    let dx = -b / (2.0 * c);
    if dx.abs() > 1.0 {
        return None;
    }
    Some((probe_step as f32 * dx).round() as isize)
}

pub(super) fn subtraction_residual_band_power(
    audio: &AudioBuffer,
    reference: &[Complex32],
    freq_hz: f32,
    start_sample: isize,
    plan: &SubtractionPlan,
) -> f32 {
    let mut residual = vec![Complex32::new(0.0, 0.0); LONG_INPUT_SAMPLES];
    let envelope = filtered_subtraction_envelope(audio, reference, start_sample, plan);
    for offset in 0..reference.len() {
        let index = start_sample + offset as isize;
        if index < 0 || index as usize >= audio.samples.len() {
            continue;
        }
        let corrected = envelope[offset] * reference[offset];
        residual[offset] = Complex32::new(audio.samples[index as usize] - 2.0 * corrected.re, 0.0);
    }
    plan.forward.process(&mut residual);
    let df = ACTIVE_MODE.geometry.sample_rate_hz as f32 / LONG_INPUT_SAMPLES as f32;
    let start_bin = (ACTIVE_MODE.band_low_hz(freq_hz) / df).trunc().max(0.0) as usize;
    let end_bin = (ACTIVE_MODE.band_high_hz(freq_hz) / df)
        .trunc()
        .min((LONG_INPUT_SAMPLES / 2) as f32) as usize;
    residual[start_bin..=end_bin]
        .iter()
        .map(|value| value.re * value.re + value.im * value.im)
        .sum()
}

pub(super) fn filtered_subtraction_envelope(
    audio: &AudioBuffer,
    reference: &[Complex32],
    start_sample: isize,
    plan: &SubtractionPlan,
) -> Vec<Complex32> {
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

    envelope
}

pub(super) fn apply_subtraction(
    audio: &mut AudioBuffer,
    reference: &[Complex32],
    start_sample: isize,
    plan: &SubtractionPlan,
) {
    let frame_len = reference.len();
    let envelope = filtered_subtraction_envelope(audio, reference, start_sample, plan);
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

impl SubtractionPlan {
    pub(super) fn global() -> &'static Self {
        static PLAN: OnceLock<SubtractionPlan> = OnceLock::new();
        PLAN.get_or_init(Self::new)
    }

    fn new() -> Self {
        let mut planner = FftPlanner::<f32>::new();
        let forward = planner.plan_fft_forward(LONG_INPUT_SAMPLES);
        let inverse = planner.plan_fft_inverse(LONG_INPUT_SAMPLES);

        let mut window = Vec::with_capacity(SUBTRACT_FILTER_SAMPLES);
        for tap in -(SUBTRACT_FILTER_HALF as isize)..=(SUBTRACT_FILTER_HALF as isize) {
            let phase = std::f32::consts::PI * tap as f32 / SUBTRACT_FILTER_SAMPLES as f32;
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
            let missing = window[SUBTRACT_FILTER_HALF + edge..]
                .iter()
                .copied()
                .sum::<f32>();
            edge_correction.push(1.0 / (1.0 - missing / sumw));
        }

        Self {
            forward,
            inverse,
            filter_spectrum: kernel,
            edge_correction,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refined_subtraction_offset_is_zero_for_aligned_reference() {
        let frame =
            crate::encode::encode_standard_message("CQ", "K1ABC", false, &GridReport::Blank)
                .expect("encode frame");
        let audio = crate::encode::synthesize_rectangular_waveform(
            &frame,
            &crate::encode::WaveformOptions {
                base_freq_hz: 1_234.0,
                amplitude: 1.0,
                ..crate::encode::WaveformOptions::default()
            },
        )
        .expect("audio");
        let reference = synthesize_channel_reference(&frame.channel_symbols, 1_234.0);
        let start_sample = ACTIVE_MODE.start_sample_from_dt(0.0);

        assert_eq!(
            refined_subtraction_offset(
                &audio,
                &reference,
                1_234.0,
                start_sample,
                SubtractionPlan::global(),
            ),
            Some(0)
        );
    }
}
