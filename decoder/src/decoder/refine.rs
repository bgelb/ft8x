use super::*;

pub(super) fn refine_candidate(
    long_spectrum: &LongSpectrum,
    baseband_plan: &BasebandPlan,
    spec: &ModeSpec,
    coarse_start_seconds: f32,
    coarse_freq_hz: f32,
) -> Option<RefinedCandidate> {
    let initial_baseband =
        downsample_candidate(long_spectrum, baseband_plan, spec, coarse_freq_hz)?;
    let mut refined_basebands = Vec::new();
    refine_candidate_with_cache(
        long_spectrum,
        baseband_plan,
        spec,
        &initial_baseband,
        &mut refined_basebands,
        coarse_start_seconds,
        coarse_freq_hz,
    )
}

pub(super) fn refine_candidate_with_cache(
    long_spectrum: &LongSpectrum,
    baseband_plan: &BasebandPlan,
    spec: &ModeSpec,
    initial_baseband: &[Complex32],
    refined_basebands: &mut Vec<(i32, Vec<Complex32>)>,
    coarse_start_seconds: f32,
    coarse_freq_hz: f32,
) -> Option<RefinedCandidate> {
    let baseband_rate_hz = spec.baseband_rate_hz();
    let mut ibest = ((coarse_start_seconds * baseband_rate_hz).round()) as isize;
    let mut best_score = f32::NEG_INFINITY;
    for idt in (ibest - 10)..=(ibest + 10) {
        let sync_score = sync8d(spec, initial_baseband, idt, 0.0);
        if sync_score > best_score {
            best_score = sync_score;
            ibest = idt;
        }
    }

    let mut best_freq_hz = coarse_freq_hz;
    best_score = f32::NEG_INFINITY;
    for ifr in -5..=5 {
        let residual_hz = spec.residual_hz_from_half_step(ifr);
        let sync_score = sync8d(spec, initial_baseband, ibest, residual_hz);
        if sync_score > best_score {
            best_score = sync_score;
            best_freq_hz = coarse_freq_hz + residual_hz;
        }
    }

    let refined_baseband = cached_refined_baseband(
        long_spectrum,
        baseband_plan,
        spec,
        refined_basebands,
        best_freq_hz,
    )?;
    let mut refined_ibest = ibest;
    best_score = f32::NEG_INFINITY;
    for delta in -4..=4 {
        let sync_score = sync8d(spec, refined_baseband, ibest + delta, 0.0);
        if sync_score > best_score {
            best_score = sync_score;
            refined_ibest = ibest + delta;
        }
    }

    let full_tones = extract_symbol_tones(spec, refined_baseband, refined_ibest);
    if sync_quality(spec, &full_tones)
        <= match spec.mode {
            Mode::Ft8 => 6,
            Mode::Ft4 => 7,
            Mode::Ft2 => 4,
        }
    {
        return None;
    }
    let llr_sets = compute_bitmetric_passes(spec, &full_tones);
    let symbol_bit_llrs = compute_symbol_bit_llrs(spec, &full_tones);
    let start_seconds = (refined_ibest as f32 - 1.0) / baseband_rate_hz;
    Some(RefinedCandidate {
        start_seconds,
        freq_hz: best_freq_hz,
        sync_score: best_score,
        snr_db: estimate_snr_db(spec, &full_tones),
        llr_sets,
        symbol_bit_llrs,
    })
}

pub(super) fn extract_candidate_at(
    long_spectrum: &LongSpectrum,
    baseband_plan: &BasebandPlan,
    spec: &ModeSpec,
    start_seconds: f32,
    freq_hz: f32,
) -> Option<RefinedCandidate> {
    let baseband = downsample_candidate(long_spectrum, baseband_plan, spec, freq_hz)?;
    extract_candidate_from_baseband(&baseband, spec, start_seconds, freq_hz)
}

pub(super) fn extract_candidate_at_relaxed(
    long_spectrum: &LongSpectrum,
    baseband_plan: &BasebandPlan,
    spec: &ModeSpec,
    start_seconds: f32,
    freq_hz: f32,
) -> Option<RefinedCandidate> {
    let baseband = downsample_candidate(long_spectrum, baseband_plan, spec, freq_hz)?;
    extract_candidate_from_baseband_with_threshold(&baseband, spec, start_seconds, freq_hz, false)
}

pub(super) fn extract_candidate_from_baseband(
    baseband: &[Complex32],
    spec: &ModeSpec,
    start_seconds: f32,
    freq_hz: f32,
) -> Option<RefinedCandidate> {
    extract_candidate_from_baseband_with_threshold(baseband, spec, start_seconds, freq_hz, true)
}

fn extract_candidate_from_baseband_with_threshold(
    baseband: &[Complex32],
    spec: &ModeSpec,
    start_seconds: f32,
    freq_hz: f32,
    enforce_sync_quality: bool,
) -> Option<RefinedCandidate> {
    let start_index = (start_seconds * spec.baseband_rate_hz()).round() as isize;
    let full_tones = extract_symbol_tones(spec, baseband, start_index);
    if enforce_sync_quality
        && sync_quality(spec, &full_tones)
            <= match spec.mode {
                Mode::Ft8 => 6,
                Mode::Ft4 => 7,
                Mode::Ft2 => 4,
            }
    {
        return None;
    }
    let llr_sets = compute_bitmetric_passes(spec, &full_tones);
    let symbol_bit_llrs = compute_symbol_bit_llrs(spec, &full_tones);
    Some(RefinedCandidate {
        start_seconds,
        freq_hz,
        sync_score: sync8d(spec, baseband, start_index, 0.0),
        snr_db: estimate_snr_db(spec, &full_tones),
        llr_sets,
        symbol_bit_llrs,
    })
}

pub(super) fn cached_refined_baseband<'a>(
    long_spectrum: &LongSpectrum,
    baseband_plan: &BasebandPlan,
    spec: &ModeSpec,
    cache: &'a mut Vec<(i32, Vec<Complex32>)>,
    freq_hz: f32,
) -> Option<&'a [Complex32]> {
    let key = (freq_hz * 16.0).round() as i32;
    if let Some(index) = cache.iter().position(|(cached_key, _)| *cached_key == key) {
        return Some(cache[index].1.as_slice());
    }
    let baseband = downsample_candidate(long_spectrum, baseband_plan, spec, freq_hz)?;
    cache.push((key, baseband));
    cache.last().map(|(_, baseband)| baseband.as_slice())
}

pub(super) fn build_long_spectrum(audio: &AudioBuffer, spec: &ModeSpec) -> LongSpectrum {
    let plan = LongSpectrumPlan::for_mode(spec.mode);
    let fft = &plan.forward;
    let mut input = fft.make_input_vec();
    let mut spectrum = fft.make_output_vec();

    let usable = audio.samples.len().min(long_input_samples(spec));
    input[..usable].copy_from_slice(&audio.samples[..usable]);
    input[usable..].fill(0.0);

    fft.process(&mut input, &mut spectrum).expect("long fft");
    LongSpectrum { bins: spectrum }
}

impl LongSpectrumPlan {
    pub(super) fn for_mode(mode: Mode) -> &'static Self {
        match mode {
            Mode::Ft8 => {
                static PLAN: OnceLock<LongSpectrumPlan> = OnceLock::new();
                PLAN.get_or_init(|| LongSpectrumPlan::new(mode.spec()))
            }
            Mode::Ft4 => {
                static PLAN: OnceLock<LongSpectrumPlan> = OnceLock::new();
                PLAN.get_or_init(|| LongSpectrumPlan::new(mode.spec()))
            }
            Mode::Ft2 => {
                static PLAN: OnceLock<LongSpectrumPlan> = OnceLock::new();
                PLAN.get_or_init(|| LongSpectrumPlan::new(mode.spec()))
            }
        }
    }

    fn new(spec: &ModeSpec) -> Self {
        let mut planner = RealFftPlanner::<f32>::new();
        Self {
            forward: planner.plan_fft_forward(spec.tuning.long_fft_samples),
        }
    }
}

impl BasebandPlan {
    pub(super) fn new(spec: &ModeSpec) -> Self {
        let mut planner = FftPlanner::<f32>::new();
        Self {
            inverse: planner.plan_fft_inverse(spec.baseband_samples()),
        }
    }
}

pub(super) fn downsample_candidate(
    long_spectrum: &LongSpectrum,
    baseband_plan: &BasebandPlan,
    spec: &ModeSpec,
    freq_hz: f32,
) -> Option<Vec<Complex32>> {
    let fft_bin_hz = spec.fft_bin_hz();
    let i0 = (freq_hz / fft_bin_hz).round() as isize;
    let ib = ((spec.band_low_hz(freq_hz) / fft_bin_hz).round() as isize).max(1);
    let it = ((spec.band_high_hz(freq_hz) / fft_bin_hz).round() as isize)
        .min((spec.tuning.long_fft_samples / 2) as isize);
    if i0 <= 0 || ib >= it {
        return None;
    }

    let mut baseband = vec![Complex32::new(0.0, 0.0); spec.baseband_samples()];
    let copied = copy_band_into_baseband(
        &mut baseband,
        &long_spectrum.bins,
        ib as usize,
        it as usize + 1,
    );
    if copied <= spec.baseband_taper_len() * 2 {
        return None;
    }

    apply_symmetric_taper(&mut baseband[..copied], baseband_taper(spec.mode));

    let shift = (i0 - ib).max(0) as usize;
    let rotate = shift.min(baseband.len());
    baseband.rotate_left(rotate);

    baseband_plan.inverse.process(&mut baseband);
    let scale = 1.0 / (spec.tuning.long_fft_samples as f32 * spec.baseband_samples() as f32).sqrt();
    for sample in &mut baseband {
        *sample *= scale;
    }
    Some(baseband)
}

pub(super) fn sync8d(
    spec: &ModeSpec,
    baseband: &[Complex32],
    start_index: isize,
    residual_hz: f32,
) -> f32 {
    let geometry = &spec.geometry;
    let mut sync = 0.0f32;
    let baseband_symbol_samples = spec.baseband_symbol_samples();
    let valid_samples = baseband.len().min(spec.baseband_valid_samples());
    let waveforms = sync8d_waveforms(spec.mode);
    let tweak = (residual_hz != 0.0).then(|| sync8d_tweak(spec, residual_hz));
    for (&block_start, pattern) in geometry
        .sync_block_starts
        .iter()
        .zip(geometry.sync_patterns.iter().copied())
    {
        for (offset, tone) in pattern.iter().copied().enumerate() {
            let symbol_start =
                start_index + ((block_start + offset) * baseband_symbol_samples) as isize;
            if symbol_start < 0 || symbol_start as usize + baseband_symbol_samples > valid_samples
            {
                continue;
            }
            let segment =
                &baseband[symbol_start as usize..symbol_start as usize + baseband_symbol_samples];
            let mut corr = Complex32::new(0.0, 0.0);
            for (index, sample) in segment.iter().copied().enumerate() {
                let mut sync_wave = waveforms[tone][index];
                if let Some(tweak) = &tweak {
                    sync_wave *= tweak[index];
                }
                corr += sample * sync_wave.conj();
            }
            sync += corr.norm_sqr();
        }
    }
    sync
}

pub(super) fn extract_symbol_tones(
    spec: &ModeSpec,
    baseband: &[Complex32],
    start_index: isize,
) -> Vec<[Complex32; 8]> {
    let geometry = &spec.geometry;
    let baseband_symbol_samples = spec.baseband_symbol_samples();
    let basis = sync8d_basis(spec.mode);
    let mut tones = vec![[Complex32::new(0.0, 0.0); 8]; geometry.message_symbols];
    let valid_samples = baseband.len().min(spec.baseband_valid_samples());
    for (symbol_index, symbol_tones) in tones.iter_mut().enumerate() {
        let sample_index = start_index + (symbol_index * baseband_symbol_samples) as isize;
        if sample_index < 0 || sample_index as usize + baseband_symbol_samples > valid_samples {
            continue;
        }
        let segment =
            &baseband[sample_index as usize..sample_index as usize + baseband_symbol_samples];
        for (tone, slot) in symbol_tones.iter_mut().enumerate() {
            *slot = correlate_tone_nominal(segment, &basis[tone]);
        }
    }
    tones
}

pub(super) fn baseband_taper(mode: Mode) -> &'static [f32] {
    fn build(spec: &ModeSpec) -> Vec<f32> {
        let taper_len = spec.baseband_taper_len();
        if taper_len == 0 {
            return vec![1.0];
        }
        (0..=taper_len)
            .map(|index| {
                0.5 * (1.0 + (index as f32 * std::f32::consts::PI / taper_len as f32).cos())
            })
            .collect()
    }

    match mode {
        Mode::Ft8 => {
            static TAPER: OnceLock<Vec<f32>> = OnceLock::new();
            TAPER.get_or_init(|| build(mode.spec()))
        }
        Mode::Ft4 => {
            static TAPER: OnceLock<Vec<f32>> = OnceLock::new();
            TAPER.get_or_init(|| build(mode.spec()))
        }
        Mode::Ft2 => {
            static TAPER: OnceLock<Vec<f32>> = OnceLock::new();
            TAPER.get_or_init(|| build(mode.spec()))
        }
    }
}

// Copy the requested FFT bins into the downsample buffer and return the copied prefix length.
pub(super) fn copy_band_into_baseband(
    baseband: &mut [Complex32],
    bins: &[Complex32],
    start_bin: usize,
    end_bin: usize,
) -> usize {
    let copied = baseband.len().min(end_bin.saturating_sub(start_bin));
    baseband[..copied].copy_from_slice(&bins[start_bin..start_bin + copied]);
    copied
}

// Apply the same edge taper at the front and back of the copied baseband band.
pub(super) fn apply_symmetric_taper(baseband: &mut [Complex32], taper: &[f32]) {
    for (offset, &gain) in taper.iter().rev().enumerate() {
        baseband[offset] *= gain;
        let tail_index = baseband.len() - 1 - offset;
        baseband[tail_index] *= gain;
    }
}

pub(super) fn correlate_tone_nominal(segment: &[Complex32], basis: &[Complex32]) -> Complex32 {
    let mut acc = Complex32::new(0.0, 0.0);
    for (index, sample) in segment.iter().copied().enumerate() {
        acc += sample * basis[index];
    }
    acc
}

pub(super) fn sync8d_basis(mode: Mode) -> &'static [Vec<Complex32>] {
    fn build(spec: &ModeSpec, invert_imag: bool) -> Vec<Vec<Complex32>> {
        let n = spec.baseband_symbol_samples();
        (0..8)
            .map(|tone| {
                let tone = tone as f32;
                let mut phase = 0.0f32;
                let delta = 2.0 * std::f32::consts::PI * tone / n as f32;
                (0..n)
                    .map(|index| {
                        let sample = if invert_imag {
                            Complex32::new(phase.cos(), -phase.sin())
                        } else {
                            Complex32::new(phase.cos(), phase.sin())
                        };
                        if index + 1 < n {
                            phase = (phase + delta).rem_euclid(2.0 * std::f32::consts::PI);
                        }
                        sample
                    })
                    .collect()
            })
            .collect()
    }

    match mode {
        Mode::Ft8 => {
            static BASIS: OnceLock<Vec<Vec<Complex32>>> = OnceLock::new();
            BASIS.get_or_init(|| build(mode.spec(), true))
        }
        Mode::Ft4 => {
            static BASIS: OnceLock<Vec<Vec<Complex32>>> = OnceLock::new();
            BASIS.get_or_init(|| build(mode.spec(), true))
        }
        Mode::Ft2 => {
            static BASIS: OnceLock<Vec<Vec<Complex32>>> = OnceLock::new();
            BASIS.get_or_init(|| build(mode.spec(), true))
        }
    }
}

pub(super) fn sync8d_waveforms(mode: Mode) -> &'static [Vec<Complex32>] {
    match mode {
        Mode::Ft8 => {
            static WAVEFORMS: OnceLock<Vec<Vec<Complex32>>> = OnceLock::new();
            WAVEFORMS.get_or_init(|| sync8d_basis(mode).iter().map(|row| {
                row.iter().map(|sample| sample.conj()).collect()
            }).collect())
        }
        Mode::Ft4 => {
            static WAVEFORMS: OnceLock<Vec<Vec<Complex32>>> = OnceLock::new();
            WAVEFORMS.get_or_init(|| sync8d_basis(mode).iter().map(|row| {
                row.iter().map(|sample| sample.conj()).collect()
            }).collect())
        }
        Mode::Ft2 => {
            static WAVEFORMS: OnceLock<Vec<Vec<Complex32>>> = OnceLock::new();
            WAVEFORMS.get_or_init(|| sync8d_basis(mode).iter().map(|row| {
                row.iter().map(|sample| sample.conj()).collect()
            }).collect())
        }
    }
}

pub(super) fn sync8d_tweak(spec: &ModeSpec, residual_hz: f32) -> Vec<Complex32> {
    let mut phase = 0.0f32;
    let delta = 2.0 * std::f32::consts::PI * residual_hz / spec.baseband_rate_hz();
    (0..spec.baseband_symbol_samples())
        .map(|index| {
        let sample = Complex32::new(phase.cos(), phase.sin());
        if index + 1 < spec.baseband_symbol_samples() {
            phase = (phase + delta).rem_euclid(2.0 * std::f32::consts::PI);
        }
        sample
    })
        .collect()
}

pub(super) fn sync_quality(spec: &ModeSpec, full_tones: &[[Complex32; 8]]) -> usize {
    let geometry = &spec.geometry;
    let mut matches = 0usize;
    for (&block_start, pattern) in geometry
        .sync_block_starts
        .iter()
        .zip(geometry.sync_patterns.iter().copied())
    {
        for (offset, expected_tone) in pattern.iter().copied().enumerate() {
            let symbol = &full_tones[block_start + offset];
            let best_tone = symbol
                .iter()
                .enumerate()
                .max_by(|left, right| left.1.norm_sqr().total_cmp(&right.1.norm_sqr()))
                .map(|(index, _)| index)
                .unwrap_or(0);
            if best_tone == expected_tone {
                matches += 1;
            }
        }
    }
    matches
}

pub(super) fn estimate_snr_db(spec: &ModeSpec, full_tones: &[[Complex32; 8]]) -> i32 {
    let data_positions = spec.geometry.data_symbol_positions;
    let mut maxima = Vec::with_capacity(data_positions.len());
    let mut all = Vec::with_capacity(data_positions.len() * 8);
    for &symbol_index in data_positions {
        let symbol = &full_tones[symbol_index];
        let max = symbol
            .iter()
            .map(|tone| tone.norm_sqr())
            .fold(f32::NEG_INFINITY, f32::max);
        maxima.push(max);
        all.extend(symbol.iter().map(|tone| tone.norm_sqr()));
    }
    all.sort_by(|left, right| left.total_cmp(right));
    let noise = all[all.len() / 2].max(1e-6);
    let signal = maxima.iter().copied().sum::<f32>() / maxima.len() as f32;
    (10.0 * ((signal / noise).max(1e-6)).log10() - 24.0).round() as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tail_samples_past_valid_window_do_not_change_extracted_tones() {
        let spec = Mode::Ft8.spec();
        let valid = spec.tuning.baseband_valid_samples;
        let len = valid + spec.baseband_symbol_samples() * 4;
        let clean = vec![Complex32::new(0.0, 0.0); len];
        let mut dirty = clean.clone();
        for sample in &mut dirty[valid..] {
            *sample = Complex32::new(123.0, -45.0);
        }

        assert_eq!(
            extract_symbol_tones(spec, &clean, 0),
            extract_symbol_tones(spec, &dirty, 0)
        );
    }

    #[test]
    fn baseband_taper_application_is_symmetric() {
        let spec = Mode::Ft8.spec();
        let copied = spec.baseband_taper_len() * 2 + 8;
        let mut gains: Vec<_> = vec![1.0f32; copied]
            .into_iter()
            .map(|value| Complex32::new(value, 0.0))
            .collect();
        apply_symmetric_taper(&mut gains, baseband_taper(spec.mode));

        for index in 0..=spec.baseband_taper_len() {
            assert_eq!(gains[index], gains[copied - 1 - index]);
        }
    }
}
