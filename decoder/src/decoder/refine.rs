use super::*;

pub(super) fn refine_candidate(
    long_spectrum: &LongSpectrum,
    baseband_plan: &BasebandPlan,
    spec: &ModeSpec,
    coarse_start_seconds: f32,
    coarse_freq_hz: f32,
) -> Option<RefinedCandidate> {
    let initial_baseband = if spec.mode == Mode::Ft4 {
        downsample_candidate_ft4_search(long_spectrum, baseband_plan, spec, coarse_freq_hz)?
    } else {
        downsample_candidate(long_spectrum, baseband_plan, spec, coarse_freq_hz)?
    };
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
    if spec.mode == Mode::Ft4 {
        return refine_candidate_ft4_with_cache(
            long_spectrum,
            baseband_plan,
            spec,
            initial_baseband,
            refined_basebands,
            coarse_freq_hz,
        );
    }

    let baseband_rate_hz = spec.baseband_rate_hz();
    let mut ibest = ((coarse_start_seconds * baseband_rate_hz).round()) as isize;
    let mut best_score = f32::NEG_INFINITY;
    for idt in (ibest - 10)..=(ibest + 10) {
        let sync_score = refined_sync_score(spec, initial_baseband, idt, 0.0);
        if sync_score > best_score {
            best_score = sync_score;
            ibest = idt;
        }
    }

    let mut best_freq_hz = coarse_freq_hz;
    best_score = f32::NEG_INFINITY;
    for ifr in -5..=5 {
        let residual_hz = spec.residual_hz_from_half_step(ifr);
        let sync_score = refined_sync_score(spec, initial_baseband, ibest, residual_hz);
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
        let sync_score = refined_sync_score(spec, refined_baseband, ibest + delta, 0.0);
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

fn refine_candidate_ft4_with_cache(
    long_spectrum: &LongSpectrum,
    baseband_plan: &BasebandPlan,
    spec: &ModeSpec,
    initial_baseband: &[Complex32],
    refined_basebands: &mut Vec<(i32, Vec<Complex32>)>,
    coarse_freq_hz: f32,
) -> Option<RefinedCandidate> {
    refine_candidate_ft4_variants_with_cache(
        long_spectrum,
        baseband_plan,
        spec,
        initial_baseband,
        refined_basebands,
        coarse_freq_hz,
    )
    .into_iter()
    .max_by(|left, right| left.sync_score.total_cmp(&right.sync_score))
}

fn ft4_start_seconds_from_ibest(spec: &ModeSpec, ibest: isize) -> f32 {
    debug_assert_eq!(spec.mode, Mode::Ft4);
    // WSJT-X uses the literal constant 666.67 for FT4 `ibest -> dt` conversion.
    // Matching that truncation path exactly matters because subtraction rounds the
    // resulting start sample, and the exact downsample rate (12000/18) differs by
    // enough to shift some candidates by one sample.
    ibest as f32 / 666.67
}

pub(super) fn refine_candidate_ft4_variants_with_cache(
    long_spectrum: &LongSpectrum,
    baseband_plan: &BasebandPlan,
    spec: &ModeSpec,
    initial_baseband: &[Complex32],
    refined_basebands: &mut Vec<(i32, Vec<Complex32>)>,
    coarse_freq_hz: f32,
) -> Vec<RefinedCandidate> {
    debug_assert_eq!(spec.mode, Mode::Ft4);

    const FT4_SEGMENT_SEED_LIMIT: usize = 1;
    const FT4_SYNC4D_THRESHOLD: f32 = 1.2;

    let mut segment_one_score = f32::NEG_INFINITY;
    let mut winners = Vec::<(isize, f32, f32)>::new();
    let segment_ranges = [(108isize, 560isize), (560, 1012), (-344, 108)];

    for (segment_index, (coarse_start, coarse_end)) in segment_ranges.into_iter().enumerate() {
        let mut coarse_seeds = Vec::<(f32, isize, f32)>::new();
        for residual_hz in (-12..=12).step_by(3).map(|value| value as f32) {
            let mut start_index = coarse_start;
            while start_index <= coarse_end {
                let score = sync4d(spec, initial_baseband, start_index, residual_hz);
                if score >= FT4_SYNC4D_THRESHOLD {
                    coarse_seeds.push((score, start_index, residual_hz));
                }
                start_index += 4;
            }
        }
        coarse_seeds.sort_by(|left, right| right.0.total_cmp(&left.0));
        let mut segment_winners = Vec::<(isize, f32, f32)>::new();

        for (_, coarse_ibest, coarse_residual_hz) in coarse_seeds {
            let coarse_freq_hz_seed = coarse_freq_hz + coarse_residual_hz;
            let coarse_duplicate =
                segment_winners
                    .iter()
                    .any(|(existing_ibest, existing_freq_hz, _)| {
                        (*existing_ibest - coarse_ibest).abs() <= 24
                            && (*existing_freq_hz - coarse_freq_hz_seed).abs() <= 5.0
                    });
            if coarse_duplicate {
                continue;
            }

            let mut fine_best_score = f32::NEG_INFINITY;
            let mut fine_ibest = coarse_ibest;
            let mut fine_residual_hz = coarse_residual_hz;
            for residual_hz in ((coarse_residual_hz as i32 - 4)..=(coarse_residual_hz as i32 + 4))
                .map(|v| v as f32)
            {
                for start_index in (coarse_ibest - 5)..=(coarse_ibest + 5) {
                    let score = sync4d(spec, initial_baseband, start_index, residual_hz);
                    if score > fine_best_score {
                        fine_best_score = score;
                        fine_ibest = start_index;
                        fine_residual_hz = residual_hz;
                    }
                }
            }

            if fine_best_score < FT4_SYNC4D_THRESHOLD {
                continue;
            }
            if segment_index == 0 {
                segment_one_score = segment_one_score.max(fine_best_score);
            }
            if segment_index > 0 && fine_best_score < segment_one_score {
                continue;
            }
            let freq_hz = coarse_freq_hz + fine_residual_hz;
            let duplicate = segment_winners
                .iter()
                .any(|(existing_ibest, existing_freq_hz, _)| {
                    (*existing_ibest - fine_ibest).abs() <= 8
                        && (*existing_freq_hz - freq_hz).abs() <= 5.0
                });
            if duplicate {
                continue;
            }
            segment_winners.push((fine_ibest, freq_hz, fine_best_score));
            if segment_winners.len() >= FT4_SEGMENT_SEED_LIMIT {
                break;
            }
        }
        winners.extend(segment_winners);
    }

    let mut refined = Vec::new();
    for (ibest, freq_hz, score) in winners {
        let Some(refined_baseband) = cached_refined_baseband(
            long_spectrum,
            baseband_plan,
            spec,
            refined_basebands,
            freq_hz,
        ) else {
            continue;
        };
        let start_seconds = ft4_start_seconds_from_ibest(spec, ibest);
        let Some((llr_sets, symbol_bit_llrs, snr_db)) =
            compute_ft4_candidate_metrics(spec, refined_baseband, ibest, true)
        else {
            continue;
        };
        refined.push(RefinedCandidate {
            start_seconds,
            freq_hz,
            sync_score: score,
            snr_db,
            llr_sets,
            symbol_bit_llrs,
        });
    }
    refined
}

fn refined_sync_score(
    spec: &ModeSpec,
    baseband: &[Complex32],
    start_index: isize,
    residual_hz: f32,
) -> f32 {
    match spec.mode {
        Mode::Ft4 => sync4d(spec, baseband, start_index, residual_hz),
        Mode::Ft8 | Mode::Ft2 => sync8d(spec, baseband, start_index, residual_hz),
    }
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
    if spec.mode == Mode::Ft4 {
        let (llr_sets, symbol_bit_llrs, snr_db) =
            compute_ft4_candidate_metrics(spec, baseband, start_index, enforce_sync_quality)?;
        return Some(RefinedCandidate {
            start_seconds,
            freq_hz,
            sync_score: refined_sync_score(spec, baseband, start_index, 0.0),
            snr_db,
            llr_sets,
            symbol_bit_llrs,
        });
    }
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
        sync_score: refined_sync_score(spec, baseband, start_index, 0.0),
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
            forward: planner.plan_fft_forward(spec.search.long_fft_samples),
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
    if spec.mode == Mode::Ft4 {
        return downsample_candidate_ft4(long_spectrum, baseband_plan, spec, freq_hz);
    }

    let fft_bin_hz = spec.fft_bin_hz();
    let i0 = (freq_hz / fft_bin_hz).round() as isize;
    let ib = ((spec.band_low_hz(freq_hz) / fft_bin_hz).round() as isize).max(1);
    let it = ((spec.band_high_hz(freq_hz) / fft_bin_hz).round() as isize)
        .min((spec.search.long_fft_samples / 2) as isize);
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
    let scale = 1.0 / (spec.search.long_fft_samples as f32 * spec.baseband_samples() as f32).sqrt();
    for sample in &mut baseband {
        *sample *= scale;
    }
    Some(baseband)
}

fn downsample_candidate_ft4(
    long_spectrum: &LongSpectrum,
    baseband_plan: &BasebandPlan,
    spec: &ModeSpec,
    freq_hz: f32,
) -> Option<Vec<Complex32>> {
    downsample_candidate_ft4_with_normalization(
        long_spectrum,
        baseband_plan,
        spec,
        freq_hz,
        spec.baseband_symbol_samples() * spec.geometry.message_symbols,
    )
}

pub(super) fn downsample_candidate_ft4_search(
    long_spectrum: &LongSpectrum,
    baseband_plan: &BasebandPlan,
    spec: &ModeSpec,
    freq_hz: f32,
) -> Option<Vec<Complex32>> {
    downsample_candidate_ft4_with_normalization(
        long_spectrum,
        baseband_plan,
        spec,
        freq_hz,
        spec.baseband_samples(),
    )
}

fn downsample_candidate_ft4_with_normalization(
    long_spectrum: &LongSpectrum,
    baseband_plan: &BasebandPlan,
    spec: &ModeSpec,
    freq_hz: f32,
    norm_samples: usize,
) -> Option<Vec<Complex32>> {
    debug_assert_eq!(spec.mode, Mode::Ft4);
    let nfft2 = spec.baseband_samples();
    let i0 = (freq_hz / spec.fft_bin_hz()).round() as isize;
    let max_bin = long_spectrum.bins.len().saturating_sub(1) as isize;
    if !(0..=max_bin).contains(&i0) {
        return None;
    }

    let mut baseband = vec![Complex32::new(0.0, 0.0); nfft2];
    baseband[0] = long_spectrum.bins[i0 as usize];
    for offset in 1..=(nfft2 / 2) {
        let plus = i0 + offset as isize;
        let minus = i0 - offset as isize;
        if (0..=max_bin).contains(&plus) {
            baseband[offset] = long_spectrum.bins[plus as usize];
        }
        if (0..=max_bin).contains(&minus) {
            baseband[nfft2 - offset] = long_spectrum.bins[minus as usize];
        }
    }

    let scale = 1.0 / nfft2 as f32;
    for (value, &gain) in baseband.iter_mut().zip(ft4_downsample_window(spec).iter()) {
        *value *= gain * scale;
    }
    baseband_plan.inverse.process(&mut baseband);

    let rms =
        (baseband.iter().map(|value| value.norm_sqr()).sum::<f32>() / norm_samples as f32).sqrt();
    if rms > 0.0 {
        for value in &mut baseband {
            *value /= rms;
        }
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
            if symbol_start < 0 || symbol_start as usize + baseband_symbol_samples > valid_samples {
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

fn sync4d(spec: &ModeSpec, baseband: &[Complex32], start_index: isize, residual_hz: f32) -> f32 {
    debug_assert_eq!(spec.mode, Mode::Ft4);
    let valid_samples = baseband.len().min(spec.baseband_valid_samples());
    if valid_samples == 0 {
        return 0.0;
    }

    let sync_scale = 1.0 / sync4d_waveforms().first().map_or(1, Vec::len) as f32;
    let precomputed_waveforms = if residual_hz.fract() == 0.0 {
        sync4d_precomputed_waveforms(residual_hz as i32)
    } else {
        None
    };
    let tweak_step = if precomputed_waveforms.is_none() && residual_hz != 0.0 {
        let sample_rate_hz = spec.baseband_rate_hz() * 0.5;
        let delta = 2.0 * std::f32::consts::PI * residual_hz / sample_rate_hz;
        Some(Complex32::new(delta.cos(), delta.sin()))
    } else {
        None
    };

    if let Some(precomputed) = precomputed_waveforms
        && let Some(sync) = sync4d_fast_precomputed(
            spec,
            baseband,
            valid_samples,
            start_index,
            precomputed,
            sync_scale,
        )
    {
        return sync;
    }

    let mut sync = 0.0f32;
    for (block_index, (&block_start, waveform)) in spec
        .geometry
        .sync_block_starts
        .iter()
        .zip(sync4d_waveforms().iter())
        .enumerate()
    {
        let sample_index = start_index + (block_start * spec.baseband_symbol_samples()) as isize;
        sync += if let Some(precomputed) = precomputed_waveforms {
            sync4d_block_precomputed(
                baseband,
                valid_samples,
                sample_index,
                &precomputed[block_index],
                sync_scale,
            )
        } else {
            sync4d_block(
                baseband,
                valid_samples,
                sample_index,
                waveform,
                tweak_step,
                sync_scale,
            )
        };
    }
    sync
}

#[inline(always)]
fn sync4d_fast_precomputed(
    spec: &ModeSpec,
    baseband: &[Complex32],
    valid_samples: usize,
    start_index: isize,
    waveforms: &[Vec<Complex32>],
    sync_scale: f32,
) -> Option<f32> {
    let waveform_len = waveforms.first()?.len();
    if waveform_len == 0 {
        return Some(0.0);
    }

    let symbol_samples = spec.baseband_symbol_samples() as isize;
    let last_block_start = *spec.geometry.sync_block_starts.last()? as isize * symbol_samples;
    let last_sample_index = start_index + last_block_start + ((waveform_len - 1) * 2) as isize;
    if start_index < 0 || last_sample_index >= valid_samples as isize {
        return None;
    }

    let start_index = start_index as usize;
    let mut sync = 0.0f32;
    for (block_index, waveform) in waveforms.iter().enumerate() {
        let block_start = start_index
            + spec.geometry.sync_block_starts[block_index] * spec.baseband_symbol_samples();
        sync += sync4d_block_precomputed_full(baseband, block_start, waveform, sync_scale);
    }
    Some(sync)
}

#[inline(always)]
fn sync4d_block_precomputed_full(
    baseband: &[Complex32],
    mut sample_index: usize,
    waveform_conj: &[Complex32],
    sync_scale: f32,
) -> f32 {
    let mut corr_re = 0.0f32;
    let mut corr_im = 0.0f32;
    for &expected_conj in waveform_conj {
        let sample = unsafe { *baseband.get_unchecked(sample_index) };
        corr_re += sample.re * expected_conj.re - sample.im * expected_conj.im;
        corr_im += sample.re * expected_conj.im + sample.im * expected_conj.re;
        sample_index += 2;
    }
    (corr_re * corr_re + corr_im * corr_im).sqrt() * sync_scale
}

fn sync4d_block(
    baseband: &[Complex32],
    valid_samples: usize,
    start_index: isize,
    waveform: &[Complex32],
    tweak_step: Option<Complex32>,
    sync_scale: f32,
) -> f32 {
    let Some((first_waveform_index, last_waveform_index)) =
        sync4d_overlap_range(start_index, waveform.len(), valid_samples)
    else {
        return 0.0;
    };
    let mut corr = Complex32::new(0.0, 0.0);
    match tweak_step {
        Some(step) => {
            let mut tweak = step.powu(first_waveform_index as u32);
            for index in first_waveform_index..=last_waveform_index {
                let sample_index = (start_index + (index * 2) as isize) as usize;
                corr += baseband[sample_index] * (waveform[index] * tweak).conj();
                tweak *= step;
            }
        }
        None => {
            for index in first_waveform_index..=last_waveform_index {
                let sample_index = (start_index + (index * 2) as isize) as usize;
                corr += baseband[sample_index] * waveform[index].conj();
            }
        }
    }
    corr.norm() * sync_scale
}

#[inline(always)]
fn sync4d_block_precomputed(
    baseband: &[Complex32],
    valid_samples: usize,
    start_index: isize,
    waveform_conj: &[Complex32],
    sync_scale: f32,
) -> f32 {
    let Some((first_waveform_index, last_waveform_index)) =
        sync4d_overlap_range(start_index, waveform_conj.len(), valid_samples)
    else {
        return 0.0;
    };
    let mut corr_re = 0.0f32;
    let mut corr_im = 0.0f32;
    for index in first_waveform_index..=last_waveform_index {
        let sample_index = (start_index + (index * 2) as isize) as usize;
        let sample = unsafe { *baseband.get_unchecked(sample_index) };
        let expected_conj = unsafe { *waveform_conj.get_unchecked(index) };
        corr_re += sample.re * expected_conj.re - sample.im * expected_conj.im;
        corr_im += sample.re * expected_conj.im + sample.im * expected_conj.re;
    }
    (corr_re * corr_re + corr_im * corr_im).sqrt() * sync_scale
}

fn sync4d_overlap_range(
    start_index: isize,
    waveform_len: usize,
    valid_samples: usize,
) -> Option<(usize, usize)> {
    if waveform_len == 0 || valid_samples == 0 {
        return None;
    }
    let last_sample_index = start_index + ((waveform_len - 1) * 2) as isize;
    if last_sample_index < 0 || start_index >= valid_samples as isize {
        return None;
    }
    let first_waveform_index = if start_index < 0 {
        ((-start_index) as usize).div_ceil(2)
    } else {
        0
    };
    let last_waveform_index = if last_sample_index >= valid_samples as isize {
        ((valid_samples as isize - 1 - start_index) / 2) as usize
    } else {
        waveform_len - 1
    };
    (first_waveform_index <= last_waveform_index)
        .then_some((first_waveform_index, last_waveform_index))
}

pub(super) fn extract_symbol_tones(
    spec: &ModeSpec,
    baseband: &[Complex32],
    start_index: isize,
) -> Vec<[Complex32; 8]> {
    if spec.mode == Mode::Ft4 {
        return extract_symbol_tones_ft4(spec, baseband, start_index);
    }

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

fn extract_symbol_tones_ft4(
    spec: &ModeSpec,
    baseband: &[Complex32],
    start_index: isize,
) -> Vec<[Complex32; 8]> {
    debug_assert_eq!(spec.mode, Mode::Ft4);
    let geometry = &spec.geometry;
    let symbol_samples = spec.baseband_symbol_samples();
    let fft = ft4_symbol_fft();
    let valid_samples = baseband.len().min(spec.baseband_valid_samples());
    let mut tones = vec![[Complex32::new(0.0, 0.0); 8]; geometry.message_symbols];

    for (symbol_index, symbol_tones) in tones.iter_mut().enumerate() {
        let sample_index = start_index + (symbol_index * symbol_samples) as isize;
        if sample_index < 0 || sample_index as usize + symbol_samples > valid_samples {
            continue;
        }
        let mut spectrum =
            baseband[sample_index as usize..sample_index as usize + symbol_samples].to_vec();
        fft.process(&mut spectrum);
        for tone in 0..4 {
            symbol_tones[tone] = spectrum[tone];
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

fn ft4_symbol_fft() -> &'static Arc<dyn Fft<f32>> {
    static FFT: OnceLock<Arc<dyn Fft<f32>>> = OnceLock::new();
    FFT.get_or_init(|| {
        let mut planner = FftPlanner::<f32>::new();
        planner.plan_fft_forward(Mode::Ft4.spec().baseband_symbol_samples())
    })
}

pub(super) fn sync8d_waveforms(mode: Mode) -> &'static [Vec<Complex32>] {
    match mode {
        Mode::Ft8 => {
            static WAVEFORMS: OnceLock<Vec<Vec<Complex32>>> = OnceLock::new();
            WAVEFORMS.get_or_init(|| {
                sync8d_basis(mode)
                    .iter()
                    .map(|row| row.iter().map(|sample| sample.conj()).collect())
                    .collect()
            })
        }
        Mode::Ft4 => {
            static WAVEFORMS: OnceLock<Vec<Vec<Complex32>>> = OnceLock::new();
            WAVEFORMS.get_or_init(|| {
                sync8d_basis(mode)
                    .iter()
                    .map(|row| row.iter().map(|sample| sample.conj()).collect())
                    .collect()
            })
        }
        Mode::Ft2 => {
            static WAVEFORMS: OnceLock<Vec<Vec<Complex32>>> = OnceLock::new();
            WAVEFORMS.get_or_init(|| {
                sync8d_basis(mode)
                    .iter()
                    .map(|row| row.iter().map(|sample| sample.conj()).collect())
                    .collect()
            })
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

fn sync4d_waveforms() -> &'static [Vec<Complex32>] {
    static WAVEFORMS: OnceLock<Vec<Vec<Complex32>>> = OnceLock::new();
    WAVEFORMS.get_or_init(|| {
        let spec = Mode::Ft4.spec();
        let n = spec.baseband_symbol_samples();
        let half_symbol = n / 2;
        spec.geometry
            .sync_patterns
            .iter()
            .map(|pattern| {
                let mut waveform = Vec::with_capacity(pattern.len() * half_symbol);
                let mut phase = 0.0f32;
                for &tone in *pattern {
                    let delta = 2.0 * std::f32::consts::PI * tone as f32 / n as f32;
                    for _ in 0..half_symbol {
                        waveform.push(Complex32::new(phase.cos(), phase.sin()));
                        phase = (phase + 2.0 * delta).rem_euclid(2.0 * std::f32::consts::PI);
                    }
                }
                waveform
            })
            .collect()
    })
}

fn sync4d_precomputed_waveforms(residual_hz: i32) -> Option<&'static [Vec<Complex32>]> {
    const MIN_RESIDUAL_HZ: i32 = -16;
    const MAX_RESIDUAL_HZ: i32 = 16;

    if !(MIN_RESIDUAL_HZ..=MAX_RESIDUAL_HZ).contains(&residual_hz) {
        return None;
    }

    static TABLE: OnceLock<Vec<Vec<Vec<Complex32>>>> = OnceLock::new();
    let table = TABLE.get_or_init(|| {
        let spec = Mode::Ft4.spec();
        let sample_rate_hz = spec.baseband_rate_hz() * 0.5;
        let base_waveforms = sync4d_waveforms();
        (MIN_RESIDUAL_HZ..=MAX_RESIDUAL_HZ)
            .map(|residual| {
                let delta = 2.0 * std::f32::consts::PI * residual as f32 / sample_rate_hz;
                let step = Complex32::new(delta.cos(), delta.sin()).conj();
                base_waveforms
                    .iter()
                    .map(|waveform| {
                        let mut tweak = Complex32::new(1.0, 0.0);
                        waveform
                            .iter()
                            .map(|&expected| {
                                let combined = expected.conj() * tweak;
                                tweak *= step;
                                combined
                            })
                            .collect()
                    })
                    .collect()
            })
            .collect()
    });
    Some(table[(residual_hz - MIN_RESIDUAL_HZ) as usize].as_slice())
}

fn ft4_downsample_window(spec: &ModeSpec) -> &'static [f32] {
    static WINDOW: OnceLock<Vec<f32>> = OnceLock::new();
    WINDOW.get_or_init(|| {
        debug_assert_eq!(spec.mode, Mode::Ft4);
        let nfft2 = spec.baseband_samples();
        let mut window = vec![0.0f32; nfft2];
        let baud = spec.geometry.tone_spacing_hz;
        let df = spec.fft_bin_hz();
        let iwt = ((0.5 * baud) / df) as usize;
        let iwf = ((4.0 * baud) / df) as usize;
        let pi = std::f32::consts::PI;

        if iwt > 0 {
            for (index, slot) in window.iter_mut().take(iwt).enumerate() {
                let phase = pi * (iwt - index - 1) as f32 / iwt as f32;
                *slot = 0.5 * (1.0 + phase.cos());
            }
            for (index, slot) in window.iter_mut().skip(iwt + iwf).take(iwt).enumerate() {
                let phase = pi * index as f32 / iwt as f32;
                *slot = 0.5 * (1.0 + phase.cos());
            }
        }
        for slot in window.iter_mut().skip(iwt).take(iwf) {
            *slot = 1.0;
        }

        let iws = (baud / df) as usize;
        window.rotate_left(iws);
        window
    })
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
        let valid = spec.refine.baseband_valid_samples;
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

    #[test]
    fn ft4_refined_start_seconds_match_stock_ibest_mapping() {
        let spec = Mode::Ft4.spec();
        let ibest = 65;
        let expected = 65.0 / 666.67;
        assert!((ft4_start_seconds_from_ibest(spec, ibest) - expected).abs() < 1e-8);
    }
}
