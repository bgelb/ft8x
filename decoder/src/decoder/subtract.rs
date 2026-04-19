use super::*;
use crate::encode::synthesize_subtraction_reference_for_mode;

const QUADRATIC_FIT_HALF_WEIGHT: f32 = 0.5;
const QUADRATIC_FIT_CENTER_WEIGHT: f32 = 2.0;

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
    let mut workspace = SubtractionWorkspace::new(plan);
    subtract_candidate_with_workspace(audio, success, plan, false, &mut workspace);
}

pub(super) fn subtract_candidate_with_workspace(
    audio: &mut AudioBuffer,
    success: &SuccessfulDecode,
    plan: &SubtractionPlan,
    refine_dt: bool,
    workspace: &mut SubtractionWorkspace,
) {
    subtract_candidate_inner(
        audio,
        success,
        plan,
        refine_dt,
        workspace,
        SubtractionApplication::Contiguous,
    );
}

pub(super) fn subtract_candidate_by_blocks_with_workspace(
    audio: &mut AudioBuffer,
    success: &SuccessfulDecode,
    plan: &SubtractionPlan,
    refine_dt: bool,
    block_samples: usize,
    workspace: &mut SubtractionWorkspace,
) {
    subtract_candidate_inner(
        audio,
        success,
        plan,
        refine_dt,
        workspace,
        SubtractionApplication::Blocks { block_samples },
    );
}

fn subtract_candidate_inner(
    audio: &mut AudioBuffer,
    success: &SuccessfulDecode,
    plan: &SubtractionPlan,
    refine_dt: bool,
    workspace: &mut SubtractionWorkspace,
    application: SubtractionApplication,
) {
    let Some(channel_symbols) =
        channel_symbols_from_codeword_bits_for_mode(success.mode, &success.codeword_bits)
    else {
        return;
    };
    let spec = success.mode.spec();
    let reference = synthesize_subtraction_reference_for_mode(
        success.mode,
        &channel_symbols,
        success.candidate.freq_hz,
    );
    let subtraction_dt_seconds = success.candidate.dt_seconds;
    let start_sample = match success.mode {
        Mode::Ft4 => {
            spec.start_sample_from_dt(subtraction_dt_seconds)
                - spec.geometry.symbol_samples as isize
        }
        Mode::Ft8 | Mode::Ft2 => spec.start_sample_from_dt(subtraction_dt_seconds),
    };
    let (offset_samples, reuse_current_envelope) = if refine_dt {
        let Some(offset_samples) = refined_subtraction_offset_with_workspace(
            audio,
            &reference,
            success.candidate.freq_hz,
            start_sample,
            plan,
            workspace,
        ) else {
            return;
        };
        (offset_samples, offset_samples == 0)
    } else {
        (0, false)
    };
    if reuse_current_envelope {
        apply_subtraction_from_envelope(
            audio,
            &reference,
            start_sample,
            plan,
            workspace,
            application,
        );
    } else {
        apply_subtraction_with_workspace(
            audio,
            &reference,
            start_sample + offset_samples,
            plan,
            workspace,
            application,
        );
    }
}

#[cfg(test)]
fn refined_subtraction_offset(
    audio: &AudioBuffer,
    reference: &[Complex32],
    freq_hz: f32,
    start_sample: isize,
    plan: &SubtractionPlan,
) -> Option<isize> {
    let mut workspace = SubtractionWorkspace::new(plan);
    refined_subtraction_offset_with_workspace(
        audio,
        reference,
        freq_hz,
        start_sample,
        plan,
        &mut workspace,
    )
}

pub(super) fn refined_subtraction_offset_with_workspace(
    audio: &AudioBuffer,
    reference: &[Complex32],
    freq_hz: f32,
    start_sample: isize,
    plan: &SubtractionPlan,
    workspace: &mut SubtractionWorkspace,
) -> Option<isize> {
    let spec = plan.spec;
    let probe_step = spec.subtraction.refine_probe_step_samples;
    let sqm = subtraction_residual_band_power_with_workspace(
        audio,
        reference,
        freq_hz,
        start_sample - probe_step,
        plan,
        workspace,
    );
    let sqp = subtraction_residual_band_power_with_workspace(
        audio,
        reference,
        freq_hz,
        start_sample + probe_step,
        plan,
        workspace,
    );
    let sq0 = subtraction_residual_band_power_with_workspace(
        audio,
        reference,
        freq_hz,
        start_sample,
        plan,
        workspace,
    );
    // Fit a parabola through the residual power at -step / 0 / +step and keep the sub-sample
    // offset only when the quadratic minimum falls inside that probe window.
    let b = (sqp - sqm) * QUADRATIC_FIT_HALF_WEIGHT;
    let c = (sqp + sqm - QUADRATIC_FIT_CENTER_WEIGHT * sq0) * QUADRATIC_FIT_HALF_WEIGHT;
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
    if plan.spec.mode == Mode::Ft4 {
        return 1.0;
    }
    // Compensate for the truncated subtraction filter near the beginning and end of the overlap.
    let edge = offset.min(frame_len - 1 - offset);
    if edge < plan.edge_correction.len() {
        plan.edge_correction[edge]
    } else {
        1.0
    }
}

fn subtraction_residual_band_power_with_workspace(
    audio: &AudioBuffer,
    reference: &[Complex32],
    freq_hz: f32,
    start_sample: isize,
    plan: &SubtractionPlan,
    workspace: &mut SubtractionWorkspace,
) -> f32 {
    let spec = plan.spec;
    filtered_subtraction_envelope_with_workspace(audio, reference, start_sample, plan, workspace);
    if let Some(window) = overlapping_window(start_sample, reference.len(), audio.samples.len()) {
        workspace.residual[..window.reference_start].fill(0.0);
        workspace.residual[window.reference_start + window.len..].fill(0.0);
        let residual_window =
            &mut workspace.residual[window.reference_start..window.reference_start + window.len];
        let envelope_window =
            &workspace.envelope[window.reference_start..window.reference_start + window.len];
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
            *residual_slot = sample - 2.0 * corrected.re;
        }
    } else {
        workspace.residual.fill(0.0);
    }
    plan.residual_forward
        .process_with_scratch(
            &mut workspace.residual,
            &mut workspace.residual_spectrum,
            &mut workspace.residual_scratch,
        )
        .expect("subtraction residual FFT");
    let df = spec.geometry.sample_rate_hz as f32 / long_input_samples(spec) as f32;
    let start_bin = (spec.band_low_hz(freq_hz) / df).trunc().max(0.0) as usize;
    let end_bin = (spec.band_high_hz(freq_hz) / df)
        .trunc()
        .min((long_input_samples(spec) / 2) as f32) as usize;
    workspace.residual_spectrum[start_bin..=end_bin]
        .iter()
        .map(|value| value.re * value.re + value.im * value.im)
        .sum()
}

fn filtered_subtraction_envelope_with_workspace(
    audio: &AudioBuffer,
    reference: &[Complex32],
    start_sample: isize,
    plan: &SubtractionPlan,
    workspace: &mut SubtractionWorkspace,
) {
    if let Some(window) = overlapping_window(start_sample, reference.len(), audio.samples.len()) {
        workspace.envelope[..window.reference_start].fill(Complex32::new(0.0, 0.0));
        workspace.envelope[window.reference_start + window.len..].fill(Complex32::new(0.0, 0.0));
        let envelope_window =
            &mut workspace.envelope[window.reference_start..window.reference_start + window.len];
        let reference_window =
            &reference[window.reference_start..window.reference_start + window.len];
        let audio_window = &audio.samples[window.signal_start..window.signal_start + window.len];
        for (slot, (&reference_value, &audio_sample)) in envelope_window
            .iter_mut()
            .zip(reference_window.iter().zip(audio_window.iter()))
        {
            *slot = reference_value.conj() * audio_sample;
        }
    } else {
        workspace.envelope.fill(Complex32::new(0.0, 0.0));
    }

    plan.forward
        .process_with_scratch(&mut workspace.envelope, &mut workspace.scratch);
    for (value, filter) in workspace.envelope.iter_mut().zip(&plan.filter_spectrum) {
        *value *= *filter;
    }
    plan.inverse
        .process_with_scratch(&mut workspace.envelope, &mut workspace.scratch);
    let scale = 1.0 / long_input_samples(plan.spec) as f32;
    for value in &mut workspace.envelope {
        *value *= scale;
    }
}

fn apply_subtraction_with_workspace(
    audio: &mut AudioBuffer,
    reference: &[Complex32],
    start_sample: isize,
    plan: &SubtractionPlan,
    workspace: &mut SubtractionWorkspace,
    application: SubtractionApplication,
) {
    filtered_subtraction_envelope_with_workspace(audio, reference, start_sample, plan, workspace);
    apply_subtraction_from_envelope(audio, reference, start_sample, plan, workspace, application);
}

#[derive(Debug, Clone, Copy)]
enum SubtractionApplication {
    Contiguous,
    Blocks { block_samples: usize },
}

fn apply_subtraction_from_envelope(
    audio: &mut AudioBuffer,
    reference: &[Complex32],
    start_sample: isize,
    plan: &SubtractionPlan,
    workspace: &SubtractionWorkspace,
    application: SubtractionApplication,
) {
    let frame_len = reference.len();
    if let Some(window) = overlapping_window(start_sample, frame_len, audio.samples.len()) {
        match application {
            SubtractionApplication::Contiguous => {
                apply_subtraction_window(audio, reference, plan, workspace, frame_len, window)
            }
            SubtractionApplication::Blocks { block_samples } => apply_subtraction_window_by_blocks(
                audio,
                reference,
                plan,
                workspace,
                frame_len,
                window,
                block_samples,
            ),
        }
    }
}

fn apply_subtraction_window(
    audio: &mut AudioBuffer,
    reference: &[Complex32],
    plan: &SubtractionPlan,
    workspace: &SubtractionWorkspace,
    frame_len: usize,
    window: OverlapWindow,
) {
    apply_subtraction_range(
        audio, reference, plan, workspace, frame_len, window, 0, window.len,
    );
}

fn apply_subtraction_window_by_blocks(
    audio: &mut AudioBuffer,
    reference: &[Complex32],
    plan: &SubtractionPlan,
    workspace: &SubtractionWorkspace,
    frame_len: usize,
    window: OverlapWindow,
    block_samples: usize,
) {
    let block_samples = block_samples.max(1);
    let absolute_start = window.reference_start;
    let absolute_end = window.reference_start + window.len;
    let first_block_end = absolute_start.next_multiple_of(block_samples);

    let mut local_start = 0;
    if first_block_end > absolute_start {
        let local_end = (first_block_end - absolute_start).min(window.len);
        apply_subtraction_range(
            audio,
            reference,
            plan,
            workspace,
            frame_len,
            window,
            local_start,
            local_end,
        );
        local_start = local_end;
    }

    while local_start < window.len {
        let absolute = absolute_start + local_start;
        let next_boundary = (absolute + block_samples).min(absolute_end);
        let local_end = next_boundary - absolute_start;
        apply_subtraction_range(
            audio,
            reference,
            plan,
            workspace,
            frame_len,
            window,
            local_start,
            local_end,
        );
        local_start = local_end;
    }
}

fn apply_subtraction_range(
    audio: &mut AudioBuffer,
    reference: &[Complex32],
    plan: &SubtractionPlan,
    workspace: &SubtractionWorkspace,
    frame_len: usize,
    window: OverlapWindow,
    local_start: usize,
    local_end: usize,
) {
    let signal_start = window.signal_start + local_start;
    let reference_start = window.reference_start + local_start;
    let len = local_end - local_start;
    let audio_window = &mut audio.samples[signal_start..signal_start + len];
    let envelope_window = &workspace.envelope[reference_start..reference_start + len];
    let reference_window = &reference[reference_start..reference_start + len];
    for (local_offset, (audio_sample, (&envelope_value, &reference_value))) in audio_window
        .iter_mut()
        .zip(envelope_window.iter().zip(reference_window.iter()))
        .enumerate()
    {
        let global_offset = reference_start + local_offset;
        let coeff = envelope_value * edge_correction(plan, frame_len, global_offset);
        *audio_sample -= 2.0 * (coeff * reference_value).re;
    }
}

pub(super) struct SubtractionWorkspace {
    envelope: Vec<Complex32>,
    residual: Vec<f32>,
    residual_spectrum: Vec<Complex32>,
    residual_scratch: Vec<Complex32>,
    scratch: Vec<Complex32>,
}

impl SubtractionWorkspace {
    pub(super) fn new(plan: &SubtractionPlan) -> Self {
        let len = long_input_samples(plan.spec);
        let scratch_len = plan
            .forward
            .get_inplace_scratch_len()
            .max(plan.inverse.get_inplace_scratch_len());
        Self {
            envelope: vec![Complex32::new(0.0, 0.0); len],
            residual: plan.residual_forward.make_input_vec(),
            residual_spectrum: plan.residual_forward.make_output_vec(),
            residual_scratch: plan.residual_forward.make_scratch_vec(),
            scratch: vec![Complex32::new(0.0, 0.0); scratch_len],
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
        let mut real_planner = RealFftPlanner::<f32>::new();
        let long_input_samples = long_input_samples(spec);
        let forward = planner.plan_fft_forward(long_input_samples);
        let inverse = planner.plan_fft_inverse(long_input_samples);
        let residual_forward = real_planner.plan_fft_forward(long_input_samples);
        let subtract_filter_samples = spec.subtraction.filter_samples;
        let subtract_filter_half = subtract_filter_samples / 2;

        let mut window = Vec::with_capacity(subtract_filter_samples);
        for tap in -(subtract_filter_half as isize)..=(subtract_filter_half as isize) {
            let phase = std::f32::consts::PI * tap as f32 / subtract_filter_samples as f32;
            window.push(phase.cos().powi(2));
        }
        let sumw = window.iter().copied().sum::<f32>();

        let mut kernel = vec![Complex32::new(0.0, 0.0); long_input_samples];
        for (index, weight) in window.iter().copied().enumerate() {
            kernel[index] = Complex32::new(weight / sumw, 0.0);
        }
        kernel.rotate_left(subtract_filter_half + 1);
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
            residual_forward,
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
        let audio = crate::encode::pad_audio_buffer_for_mode(&audio, mode).expect("padded audio");
        let reference = crate::encode::synthesize_channel_reference_for_mode(
            mode,
            &frame.channel_symbols,
            1_234.0,
        );
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

    #[test]
    fn block_subtraction_matches_contiguous_subtraction() {
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
        let mut contiguous =
            crate::encode::pad_audio_buffer_for_mode(&audio, mode).expect("padded audio");
        let mut blocks = contiguous.clone();
        let reference =
            synthesize_subtraction_reference_for_mode(mode, &frame.channel_symbols, 1_234.0);
        let plan = SubtractionPlan::for_mode(mode);
        let start_sample = mode.spec().start_sample_from_dt(0.0);

        let mut contiguous_workspace = SubtractionWorkspace::new(plan);
        apply_subtraction_with_workspace(
            &mut contiguous,
            &reference,
            start_sample,
            plan,
            &mut contiguous_workspace,
            SubtractionApplication::Contiguous,
        );

        let mut block_workspace = SubtractionWorkspace::new(plan);
        apply_subtraction_with_workspace(
            &mut blocks,
            &reference,
            start_sample,
            plan,
            &mut block_workspace,
            SubtractionApplication::Blocks {
                block_samples: mode.spec().refine.early_block_samples,
            },
        );

        assert_eq!(blocks.samples, contiguous.samples);
    }
}
