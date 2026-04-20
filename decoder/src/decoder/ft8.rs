use super::*;

const BASELINE_LOW_EDGE_MIN_HZ: f32 = 100.0;
const BASELINE_DB_REFERENCE_LEVEL: f32 = 40.0;
const REPORTED_SNR_DENOMINATOR: f32 = 3.0e6;
const REPORTED_SNR_MIN_ARG: f32 = 0.001;
const REPORTED_SNR_ARG_THRESHOLD: f32 = 0.1;
const REPORTED_SNR_DB_OFFSET: f32 = 27.0;
const REPORTED_SNR_FLOOR_DB: f32 = -24.0;

pub(super) struct Ft8ReportedSnrContext {
    long_spectrum: LongSpectrum,
    pub(super) baseband_plan: &'static BasebandPlan,
    baseline_db: Vec<f32>,
}

pub(super) fn build_ft8_reported_snr_context(
    audio: &AudioBuffer,
    options: &DecodeOptions,
) -> Option<Ft8ReportedSnrContext> {
    let spec = Mode::Ft8.spec();
    Some(Ft8ReportedSnrContext {
        long_spectrum: build_long_spectrum(audio, spec),
        baseband_plan: BasebandPlan::for_mode(Mode::Ft8),
        baseline_db: spectrum_baseline_db(audio, options.min_freq_hz, options.max_freq_hz)?,
    })
}

pub(super) fn ft8_reported_snr_db_with_workspace(
    context: &Ft8ReportedSnrContext,
    success: &SuccessfulDecode,
    workspace: &mut BasebandWorkspace,
) -> Option<i32> {
    let spec = Mode::Ft8.spec();
    let baseband = downsample_candidate_with_workspace(
        &context.long_spectrum,
        context.baseband_plan,
        spec,
        success.candidate.freq_hz,
        workspace,
    )?;
    let start_index = (success.candidate.start_seconds * spec.baseband_rate_hz()).round() as isize;
    let full_tones = extract_symbol_tones(spec, &baseband, start_index);
    let channel_symbols = crate::encode::channel_symbols_from_codeword_bits_for_mode(
        Mode::Ft8,
        &success.codeword_bits,
    )?;
    if channel_symbols.len() != full_tones.len() {
        return None;
    }

    let xsig = channel_symbols
        .iter()
        .enumerate()
        .map(|(index, &tone)| full_tones[index][tone as usize].norm_sqr())
        .sum::<f32>();
    let bin = ((success.candidate.freq_hz / spec.sync_bin_hz()).round() as isize)
        .clamp(0, context.baseline_db.len().saturating_sub(1) as isize) as usize;
    let xbase = 10.0f32.powf(0.1 * (context.baseline_db[bin] - BASELINE_DB_REFERENCE_LEVEL));
    if !xbase.is_finite() || xbase <= 0.0 {
        return None;
    }

    let mut xsnr = REPORTED_SNR_MIN_ARG;
    let arg = xsig / xbase / REPORTED_SNR_DENOMINATOR - 1.0;
    if arg > REPORTED_SNR_ARG_THRESHOLD {
        xsnr = arg;
    }
    Some(
        (10.0 * xsnr.log10() - REPORTED_SNR_DB_OFFSET)
            .max(REPORTED_SNR_FLOOR_DB)
            .round() as i32,
    )
}

fn spectrum_baseline_db(
    audio: &AudioBuffer,
    min_freq_hz: f32,
    max_freq_hz: f32,
) -> Option<Vec<f32>> {
    let spec = Mode::Ft8.spec();
    let nfft = spec.sync_fft_samples();
    let nh1 = nfft / 2;
    let step = nfft / 2;
    let sample_len = audio.samples.len().min(spec.search.long_input_samples);
    if sample_len < nfft {
        return None;
    }

    let mut planner = RealFftPlanner::<f32>::new();
    let fft = planner.plan_fft_forward(nfft);
    let mut input = fft.make_input_vec();
    let mut spectrum = fft.make_output_vec();
    let mut window = build_nuttall_window(nfft);
    let scale = spec.geometry.symbol_samples as f32 * 2.0
        / (window.iter().sum::<f32>() * WSJTX_SIGNAL_SCALE);
    for value in &mut window {
        *value *= scale;
    }

    let mut savg = vec![0.0f32; nh1 + 1];
    let mut start = 0usize;
    while start + nfft <= sample_len {
        for (slot, (&sample, &gain)) in input
            .iter_mut()
            .zip(audio.samples[start..start + nfft].iter().zip(window.iter()))
        {
            *slot = sample * gain;
        }
        fft.process(&mut input, &mut spectrum)
            .expect("ft8 baseline fft");
        for bin in 1..=nh1 {
            savg[bin] += spectrum[bin].norm_sqr();
        }
        start += step;
    }

    let df = spec.sync_bin_hz();
    let mut nfa = min_freq_hz;
    let mut nfb = max_freq_hz;
    let nwin = nfb - nfa;
    if nfa < BASELINE_LOW_EDGE_MIN_HZ {
        nfa = BASELINE_LOW_EDGE_MIN_HZ;
        if nwin < BASELINE_WINDOW_MIN_HZ {
            nfb = nfa + nwin;
        }
    }
    if nfb > WSJTX_FREQ_MAX_HZ {
        nfb = WSJTX_FREQ_MAX_HZ;
        if nwin < BASELINE_WINDOW_MIN_HZ {
            nfa = nfb - nwin;
        }
    }

    let ia = ((nfa / df).round() as usize).clamp(1, nh1);
    let ib = ((nfb / df).round() as usize).clamp(ia, nh1);
    let mut spectrum_db = savg;
    for value in &mut spectrum_db[ia..=ib] {
        *value = 10.0 * value.max(1e-12).log10();
    }

    let nlen = (ib - ia + 1) / BASELINE_SEGMENTS;
    if nlen == 0 {
        return None;
    }
    let i0 = ((ib - ia + 1) / 2) as f64;
    let mut xs = Vec::<f64>::with_capacity(BASELINE_MAX_POINTS);
    let mut ys = Vec::<f64>::with_capacity(BASELINE_MAX_POINTS);
    for seg in 0..BASELINE_SEGMENTS {
        let ja = ia + seg * nlen;
        let jb = ja + nlen - 1;
        let base = percentile_10(&spectrum_db[ja..=jb]);
        for (offset, &value) in spectrum_db[ja..=jb].iter().enumerate() {
            if value <= base && xs.len() < BASELINE_MAX_POINTS {
                let bin = ja + offset;
                xs.push(bin as f64 - i0);
                ys.push(value as f64);
            }
        }
    }

    let coeffs = polyfit_degree4(&xs, &ys)?;
    let mut baseline = vec![0.0f32; nh1 + 1];
    for (bin, slot) in baseline.iter_mut().enumerate().take(ib + 1).skip(ia) {
        let t = bin as f64 - i0;
        *slot = (coeffs[0]
            + t * (coeffs[1] + t * (coeffs[2] + t * (coeffs[3] + t * coeffs[4])))
            + BASELINE_DB_OFFSET) as f32;
    }
    Some(baseline)
}
