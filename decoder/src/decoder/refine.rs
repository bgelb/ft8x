use super::*;

pub(super) fn refine_candidate(
    long_spectrum: &LongSpectrum,
    baseband_plan: &BasebandPlan,
    coarse_start_seconds: f32,
    coarse_freq_hz: f32,
) -> Option<RefinedCandidate> {
    let initial_baseband = downsample_candidate(long_spectrum, baseband_plan, coarse_freq_hz)?;
    let mut refined_basebands = Vec::new();
    refine_candidate_with_cache(
        long_spectrum,
        baseband_plan,
        &initial_baseband,
        &mut refined_basebands,
        coarse_start_seconds,
        coarse_freq_hz,
    )
}

pub(super) fn refine_candidate_with_cache(
    long_spectrum: &LongSpectrum,
    baseband_plan: &BasebandPlan,
    initial_baseband: &[Complex32],
    refined_basebands: &mut Vec<(i32, Vec<Complex32>)>,
    coarse_start_seconds: f32,
    coarse_freq_hz: f32,
) -> Option<RefinedCandidate> {
    let baseband_rate_hz = ACTIVE_MODE.baseband_rate_hz();
    let mut ibest = ((coarse_start_seconds * baseband_rate_hz).round()) as isize;
    let mut best_score = f32::NEG_INFINITY;
    for idt in (ibest - 10)..=(ibest + 10) {
        let sync_score = sync8d(initial_baseband, idt, 0.0);
        if sync_score > best_score {
            best_score = sync_score;
            ibest = idt;
        }
    }

    let mut best_freq_hz = coarse_freq_hz;
    best_score = f32::NEG_INFINITY;
    for ifr in -5..=5 {
        let residual_hz = ifr as f32 * 0.5;
        let sync_score = sync8d(initial_baseband, ibest, residual_hz);
        if sync_score > best_score {
            best_score = sync_score;
            best_freq_hz = coarse_freq_hz + residual_hz;
        }
    }

    let refined_baseband = cached_refined_baseband(
        long_spectrum,
        baseband_plan,
        refined_basebands,
        best_freq_hz,
    )?;
    let mut refined_ibest = ibest;
    best_score = f32::NEG_INFINITY;
    for delta in -4..=4 {
        let sync_score = sync8d(refined_baseband, ibest + delta, 0.0);
        if sync_score > best_score {
            best_score = sync_score;
            refined_ibest = ibest + delta;
        }
    }

    let full_tones = extract_symbol_tones(refined_baseband, refined_ibest);
    if sync_quality(&full_tones) <= 6 {
        return None;
    }
    let llr_sets = compute_bitmetric_passes(&full_tones);
    let symbol_bit_llrs = compute_symbol_bit_llrs(&full_tones);
    let start_seconds = (refined_ibest as f32 - 1.0) / baseband_rate_hz;
    Some(RefinedCandidate {
        start_seconds,
        freq_hz: best_freq_hz,
        sync_score: best_score,
        snr_db: estimate_snr_db(&full_tones),
        llr_sets,
        symbol_bit_llrs,
    })
}

pub(super) fn extract_candidate_at(
    long_spectrum: &LongSpectrum,
    baseband_plan: &BasebandPlan,
    start_seconds: f32,
    freq_hz: f32,
) -> Option<RefinedCandidate> {
    let baseband = downsample_candidate(long_spectrum, baseband_plan, freq_hz)?;
    extract_candidate_from_baseband(&baseband, start_seconds, freq_hz)
}

pub(super) fn extract_candidate_from_baseband(
    baseband: &[Complex32],
    start_seconds: f32,
    freq_hz: f32,
) -> Option<RefinedCandidate> {
    let start_index = (start_seconds * ACTIVE_MODE.baseband_rate_hz()).round() as isize;
    let full_tones = extract_symbol_tones(baseband, start_index);
    if sync_quality(&full_tones) <= 6 {
        return None;
    }
    let llr_sets = compute_bitmetric_passes(&full_tones);
    let symbol_bit_llrs = compute_symbol_bit_llrs(&full_tones);
    Some(RefinedCandidate {
        start_seconds,
        freq_hz,
        sync_score: sync8d(baseband, start_index, 0.0),
        snr_db: estimate_snr_db(&full_tones),
        llr_sets,
        symbol_bit_llrs,
    })
}

pub(super) fn cached_refined_baseband<'a>(
    long_spectrum: &LongSpectrum,
    baseband_plan: &BasebandPlan,
    cache: &'a mut Vec<(i32, Vec<Complex32>)>,
    freq_hz: f32,
) -> Option<&'a [Complex32]> {
    let key = (freq_hz * 16.0).round() as i32;
    if let Some(index) = cache.iter().position(|(cached_key, _)| *cached_key == key) {
        return Some(cache[index].1.as_slice());
    }
    let baseband = downsample_candidate(long_spectrum, baseband_plan, freq_hz)?;
    cache.push((key, baseband));
    cache.last().map(|(_, baseband)| baseband.as_slice())
}

pub(super) fn build_long_spectrum(audio: &AudioBuffer) -> LongSpectrum {
    let plan = LongSpectrumPlan::global();
    let fft = &plan.forward;
    let mut input = fft.make_input_vec();
    let mut spectrum = fft.make_output_vec();

    let usable = audio.samples.len().min(LONG_INPUT_SAMPLES);
    input[..usable].copy_from_slice(&audio.samples[..usable]);
    input[usable..].fill(0.0);

    fft.process(&mut input, &mut spectrum).expect("long fft");
    LongSpectrum { bins: spectrum }
}

impl LongSpectrumPlan {
    pub(super) fn global() -> &'static Self {
        static PLAN: OnceLock<LongSpectrumPlan> = OnceLock::new();
        PLAN.get_or_init(|| {
            let mut planner = RealFftPlanner::<f32>::new();
            Self {
                forward: planner.plan_fft_forward(LONG_FFT_SAMPLES),
            }
        })
    }
}

impl BasebandPlan {
    pub(super) fn new() -> Self {
        let mut planner = FftPlanner::<f32>::new();
        Self {
            inverse: planner.plan_fft_inverse(BASEBAND_SAMPLES),
        }
    }
}

pub(super) fn downsample_candidate(
    long_spectrum: &LongSpectrum,
    baseband_plan: &BasebandPlan,
    freq_hz: f32,
) -> Option<Vec<Complex32>> {
    let fft_bin_hz = ACTIVE_MODE.fft_bin_hz();
    let i0 = (freq_hz / fft_bin_hz).round() as isize;
    let ib = ((ACTIVE_MODE.band_low_hz(freq_hz) / fft_bin_hz).round() as isize).max(1);
    let it = ((ACTIVE_MODE.band_high_hz(freq_hz) / fft_bin_hz).round() as isize)
        .min((LONG_FFT_SAMPLES / 2) as isize);
    if i0 <= 0 || ib >= it {
        return None;
    }

    let mut baseband = vec![Complex32::new(0.0, 0.0); BASEBAND_SAMPLES];
    let mut copied = 0usize;
    for bin in ib..=it {
        let index = copied;
        if index >= baseband.len() {
            break;
        }
        baseband[index] = long_spectrum.bins[bin as usize];
        copied += 1;
    }
    if copied <= BASEBAND_TAPER_LEN * 2 {
        return None;
    }

    let taper = baseband_taper();
    for index in 0..=BASEBAND_TAPER_LEN {
        baseband[index] *= taper[BASEBAND_TAPER_LEN - index];
        baseband[copied - 1 - index] *= taper[BASEBAND_TAPER_LEN - index];
    }

    let shift = (i0 - ib).max(0) as usize;
    let rotate = shift.min(baseband.len());
    baseband.rotate_left(rotate);

    baseband_plan.inverse.process(&mut baseband);
    let scale = 1.0 / (LONG_FFT_SAMPLES as f32 * BASEBAND_SAMPLES as f32).sqrt();
    for sample in &mut baseband {
        *sample *= scale;
    }
    Some(baseband)
}

pub(super) fn sync8d(baseband: &[Complex32], start_index: isize, residual_hz: f32) -> f32 {
    let geometry = &ACTIVE_MODE.geometry;
    let mut sync = 0.0f32;
    let valid_samples = baseband.len().min(ACTIVE_MODE.tuning.baseband_valid_samples);
    let waveforms = sync8d_waveforms();
    let tweak = (residual_hz != 0.0).then(|| sync8d_tweak(residual_hz));
    for (offset, tone) in geometry.costas_pattern.iter().copied().enumerate() {
        for &block_start in geometry.sync_block_starts {
            let symbol_start =
                start_index + ((block_start + offset) * BASEBAND_SYMBOL_SAMPLES) as isize;
            if symbol_start < 0
                || symbol_start as usize + BASEBAND_SYMBOL_SAMPLES > valid_samples
            {
                continue;
            }
            let segment =
                &baseband[symbol_start as usize..symbol_start as usize + BASEBAND_SYMBOL_SAMPLES];
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
    baseband: &[Complex32],
    start_index: isize,
) -> Vec<[Complex32; 8]> {
    let geometry = &ACTIVE_MODE.geometry;
    let mut tones = vec![[Complex32::new(0.0, 0.0); 8]; geometry.message_symbols];
    let valid_samples = baseband.len().min(ACTIVE_MODE.tuning.baseband_valid_samples);
    for (symbol_index, symbol_tones) in tones.iter_mut().enumerate() {
        let sample_index = start_index + (symbol_index * BASEBAND_SYMBOL_SAMPLES) as isize;
        if sample_index < 0 || sample_index as usize + BASEBAND_SYMBOL_SAMPLES > valid_samples {
            continue;
        }
        let segment =
            &baseband[sample_index as usize..sample_index as usize + BASEBAND_SYMBOL_SAMPLES];
        for (tone, slot) in symbol_tones.iter_mut().enumerate() {
            *slot = correlate_tone_nominal(segment, tone);
        }
    }
    tones
}

pub(super) fn baseband_taper() -> &'static [f32] {
    static TAPER: OnceLock<Vec<f32>> = OnceLock::new();
    TAPER.get_or_init(|| {
        let taper_len = ACTIVE_MODE.tuning.baseband_taper_len;
        (0..=taper_len)
            .map(|index| {
                0.5 * (1.0
                    + (index as f32 * std::f32::consts::PI / taper_len as f32).cos())
            })
            .collect()
    })
}

pub(super) fn correlate_tone_nominal(segment: &[Complex32], tone: usize) -> Complex32 {
    let basis = sync8d_basis();
    let mut acc = Complex32::new(0.0, 0.0);
    for (index, sample) in segment.iter().copied().enumerate() {
        acc += sample * basis[tone][index];
    }
    acc
}

pub(super) fn sync8d_basis() -> &'static [[Complex32; BASEBAND_SYMBOL_SAMPLES]; 8] {
    static BASIS: OnceLock<[[Complex32; BASEBAND_SYMBOL_SAMPLES]; 8]> = OnceLock::new();
    BASIS.get_or_init(|| {
        std::array::from_fn(|tone| {
            let tone = tone as f32;
            let mut phase = 0.0f32;
            let delta = 2.0 * std::f32::consts::PI * tone / BASEBAND_SYMBOL_SAMPLES as f32;
            std::array::from_fn(|index| {
                let sample = Complex32::new(phase.cos(), -phase.sin());
                if index + 1 < BASEBAND_SYMBOL_SAMPLES {
                    phase = (phase + delta).rem_euclid(2.0 * std::f32::consts::PI);
                }
                sample
            })
        })
    })
}

pub(super) fn sync8d_waveforms() -> &'static [[Complex32; BASEBAND_SYMBOL_SAMPLES]; 8] {
    static WAVEFORMS: OnceLock<[[Complex32; BASEBAND_SYMBOL_SAMPLES]; 8]> = OnceLock::new();
    WAVEFORMS.get_or_init(|| {
        std::array::from_fn(|tone| {
            let tone = tone as f32;
            let mut phase = 0.0f32;
            let delta = 2.0 * std::f32::consts::PI * tone / BASEBAND_SYMBOL_SAMPLES as f32;
            std::array::from_fn(|index| {
                let sample = Complex32::new(phase.cos(), phase.sin());
                if index + 1 < BASEBAND_SYMBOL_SAMPLES {
                    phase = (phase + delta).rem_euclid(2.0 * std::f32::consts::PI);
                }
                sample
            })
        })
    })
}

pub(super) fn sync8d_tweak(residual_hz: f32) -> [Complex32; BASEBAND_SYMBOL_SAMPLES] {
    let mut phase = 0.0f32;
    let delta = 2.0 * std::f32::consts::PI * residual_hz / ACTIVE_MODE.baseband_rate_hz();
    std::array::from_fn(|index| {
        let sample = Complex32::new(phase.cos(), phase.sin());
        if index + 1 < BASEBAND_SYMBOL_SAMPLES {
            phase = (phase + delta).rem_euclid(2.0 * std::f32::consts::PI);
        }
        sample
    })
}

pub(super) fn sync_quality(full_tones: &[[Complex32; 8]]) -> usize {
    let geometry = &ACTIVE_MODE.geometry;
    let mut matches = 0usize;
    for (offset, expected_tone) in geometry.costas_pattern.iter().copied().enumerate() {
        for &block_start in geometry.sync_block_starts {
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

pub(super) fn estimate_snr_db(full_tones: &[[Complex32; 8]]) -> i32 {
    let data_positions = ACTIVE_MODE.geometry.data_symbol_positions;
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
        let valid = ACTIVE_MODE.tuning.baseband_valid_samples;
        let len = valid + BASEBAND_SYMBOL_SAMPLES * 4;
        let clean = vec![Complex32::new(0.0, 0.0); len];
        let mut dirty = clean.clone();
        for sample in &mut dirty[valid..] {
            *sample = Complex32::new(123.0, -45.0);
        }

        assert_eq!(extract_symbol_tones(&clean, 0), extract_symbol_tones(&dirty, 0));
    }

    #[test]
    fn baseband_taper_application_is_symmetric() {
        let taper = baseband_taper();
        let copied = BASEBAND_TAPER_LEN * 2 + 8;
        let mut gains = vec![1.0f32; copied];
        for index in 0..=BASEBAND_TAPER_LEN {
            gains[index] *= taper[BASEBAND_TAPER_LEN - index];
            gains[copied - 1 - index] *= taper[BASEBAND_TAPER_LEN - index];
        }

        for index in 0..=BASEBAND_TAPER_LEN {
            assert_eq!(gains[index], gains[copied - 1 - index]);
        }
    }
}
