use super::*;

#[derive(Debug, Clone, Copy)]
struct OverlapWindow {
    reference_start: usize,
    signal_start: usize,
    len: usize,
}

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
    let Some(channel_symbols) =
        channel_symbols_from_codeword_bits_for_mode(success.mode, &success.codeword_bits)
    else {
        return;
    };
    let spec = success.mode.spec();
    let start_sample = spec.start_sample_from_dt(success.candidate.dt_seconds);
    let reference =
        synthesize_channel_reference_for_mode(
            success.mode,
            &channel_symbols,
            success.candidate.freq_hz,
        );
    let offset_samples = if refine_dt {
        let Some(offset_samples) = refined_subtraction_offset(
            audio,
            &reference,
            success.candidate.freq_hz,
            start_sample,
            plan,
        ) else {
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
    let spec = plan.spec;
    let probe_step = spec.tuning.subtraction_refine_probe_step_samples;
    let sqm =
        subtraction_residual_band_power(audio, reference, freq_hz, start_sample - probe_step, plan);
    let sq0 = subtraction_residual_band_power(audio, reference, freq_hz, start_sample, plan);
    let sqp =
        subtraction_residual_band_power(audio, reference, freq_hz, start_sample + probe_step, plan);
    // Fit a parabola through the residual power at -step / 0 / +step and keep the sub-sample
    // offset only when the quadratic minimum falls inside that probe window.
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

fn overlapping_window(
    start_sample: isize,
    reference_len: usize,
    signal_len: usize,
) -> Option<OverlapWindow> {
    // Convert an arbitrary signed start sample into aligned slices over the reference
    // waveform and the available audio window without duplicating offset arithmetic.
    let reference_start = if start_sample < 0 {
        (-start_sample) as usize
    } else {
        0
    };
    let signal_start = start_sample.max(0) as usize;
    let len = reference_len
        .saturating_sub(reference_start)
        .min(signal_len.saturating_sub(signal_start));
    (len > 0).then_some(OverlapWindow {
        reference_start,
        signal_start,
        len,
    })
}

fn edge_correction(plan: &SubtractionPlan, frame_len: usize, offset: usize) -> f32 {
    // Compensate for the truncated subtraction filter near the beginning and end of the overlap.
    let edge = offset.min(frame_len - 1 - offset);
    if edge < plan.edge_correction.len() {
        plan.edge_correction[edge]
    } else {
        1.0
    }
}

pub(super) fn subtraction_residual_band_power(
    audio: &AudioBuffer,
    reference: &[Complex32],
    freq_hz: f32,
    start_sample: isize,
    plan: &SubtractionPlan,
) -> f32 {
    let spec = plan.spec;
    let mut residual = vec![Complex32::new(0.0, 0.0); long_input_samples(spec)];
    let envelope = filtered_subtraction_envelope(audio, reference, start_sample, plan);
    if let Some(window) = overlapping_window(start_sample, reference.len(), audio.samples.len()) {
        let residual_window =
            &mut residual[window.reference_start..window.reference_start + window.len];
        let envelope_window =
            &envelope[window.reference_start..window.reference_start + window.len];
        let reference_window =
            &reference[window.reference_start..window.reference_start + window.len];
        let audio_window = &audio.samples[window.signal_start..window.signal_start + window.len];
        for (residual_slot, ((&sample, &envelope_value), &reference_value)) in
            residual_window.iter_mut().zip(
                audio_window
                    .iter()
                    .zip(envelope_window.iter())
                    .zip(reference_window.iter()),
            )
        {
            let corrected = envelope_value * reference_value;
            *residual_slot = Complex32::new(sample - 2.0 * corrected.re, 0.0);
        }
    }
    plan.forward.process(&mut residual);
    let df = spec.geometry.sample_rate_hz as f32 / long_input_samples(spec) as f32;
    let start_bin = (spec.band_low_hz(freq_hz) / df).trunc().max(0.0) as usize;
    let end_bin = (spec.band_high_hz(freq_hz) / df)
        .trunc()
        .min((long_input_samples(spec) / 2) as f32) as usize;
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
    let mut envelope = vec![Complex32::new(0.0, 0.0); long_input_samples(plan.spec)];
    if let Some(window) = overlapping_window(start_sample, reference.len(), audio.samples.len()) {
        let envelope_window =
            &mut envelope[window.reference_start..window.reference_start + window.len];
        let reference_window =
            &reference[window.reference_start..window.reference_start + window.len];
        let audio_window = &audio.samples[window.signal_start..window.signal_start + window.len];
        for (slot, (&reference_value, &audio_sample)) in envelope_window
            .iter_mut()
            .zip(reference_window.iter().zip(audio_window.iter()))
        {
            *slot = reference_value.conj() * audio_sample;
        }
    }

    plan.forward.process(&mut envelope);
    for (value, filter) in envelope.iter_mut().zip(&plan.filter_spectrum) {
        *value *= *filter;
    }
    plan.inverse.process(&mut envelope);
    let scale = 1.0 / long_input_samples(plan.spec) as f32;
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
    if let Some(window) = overlapping_window(start_sample, frame_len, audio.samples.len()) {
        let audio_window =
            &mut audio.samples[window.signal_start..window.signal_start + window.len];
        let envelope_window =
            &envelope[window.reference_start..window.reference_start + window.len];
        let reference_window =
            &reference[window.reference_start..window.reference_start + window.len];
        for (local_offset, (audio_sample, (&envelope_value, &reference_value))) in audio_window
            .iter_mut()
            .zip(envelope_window.iter().zip(reference_window.iter()))
            .enumerate()
        {
            let global_offset = window.reference_start + local_offset;
            let coeff = envelope_value * edge_correction(plan, frame_len, global_offset);
            *audio_sample -= 2.0 * (coeff * reference_value).re;
        }
    }
}

impl SubtractionPlan {
    pub(super) fn for_mode(mode: Mode) -> &'static Self {
        match mode {
            Mode::Ft8 => {
                static PLAN: OnceLock<SubtractionPlan> = OnceLock::new();
                PLAN.get_or_init(|| SubtractionPlan::new(mode.spec()))
            }
            Mode::Ft4 => {
                static PLAN: OnceLock<SubtractionPlan> = OnceLock::new();
                PLAN.get_or_init(|| SubtractionPlan::new(mode.spec()))
            }
            Mode::Ft2 => {
                static PLAN: OnceLock<SubtractionPlan> = OnceLock::new();
                PLAN.get_or_init(|| SubtractionPlan::new(mode.spec()))
            }
        }
    }

    fn new(spec: &'static ModeSpec) -> Self {
        let mut planner = FftPlanner::<f32>::new();
        let long_input_samples = long_input_samples(spec);
        let forward = planner.plan_fft_forward(long_input_samples);
        let inverse = planner.plan_fft_inverse(long_input_samples);
        let subtract_filter_samples = spec.tuning.subtract_filter_samples;
        let subtract_filter_half = subtract_filter_samples / 2;

        let mut window = Vec::with_capacity(subtract_filter_samples);
        for tap in -(subtract_filter_half as isize)..=(subtract_filter_half as isize) {
            let phase = std::f32::consts::PI * tap as f32 / subtract_filter_samples as f32;
            window.push(phase.cos().powi(2));
        }
        let sumw = window.iter().copied().sum::<f32>();

        let mut kernel = vec![Complex32::new(0.0, 0.0); long_input_samples];
        for (index, weight) in window.iter().copied().enumerate() {
            let lag = index as isize - subtract_filter_half as isize;
            let slot = if lag < 0 {
                (long_input_samples as isize + lag) as usize
            } else {
                lag as usize
            };
            kernel[slot] = Complex32::new(weight / sumw, 0.0);
        }
        forward.process(&mut kernel);

        let mut edge_correction = Vec::with_capacity(subtract_filter_half + 1);
        for edge in 0..=subtract_filter_half {
            let missing = window[subtract_filter_half + edge..]
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
            spec,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refined_subtraction_offset_is_zero_for_aligned_reference() {
        let mode = Mode::Ft8;
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
        let reference = synthesize_channel_reference_for_mode(mode, &frame.channel_symbols, 1_234.0);
        let start_sample = mode.spec().start_sample_from_dt(0.0);

        assert_eq!(
            refined_subtraction_offset(
                &audio,
                &reference,
                1_234.0,
                start_sample,
                SubtractionPlan::for_mode(mode),
            ),
            Some(0)
        );
    }
}
