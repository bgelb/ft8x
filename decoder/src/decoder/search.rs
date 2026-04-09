use super::*;

pub(super) fn search_grid(audio: &AudioBuffer, options: &DecodeOptions) -> SearchGrid {
    let spec = options.mode.spec();
    let geometry = &spec.geometry;
    let min_bin = (options.min_freq_hz / geometry.tone_spacing_hz)
        .floor()
        .max(0.0) as usize;
    let max_bin = (options.max_freq_hz / geometry.tone_spacing_hz).ceil() as usize
        + spec.sync_tone_span_bins();
    SearchGrid {
        frame_count: (audio.samples.len().saturating_sub(geometry.symbol_samples)
            / geometry.hop_samples)
            + 1,
        usable_bins: max_bin.saturating_sub(min_bin) + 1,
        min_bin,
    }
}

pub(super) fn build_spectrogram(audio: &AudioBuffer, options: &DecodeOptions) -> Spectrogram {
    let search_grid = search_grid(audio, options);
    let geometry = &options.mode.spec().geometry;
    let min_bin = search_grid.min_bin;
    let usable_bins = search_grid.usable_bins;
    let frame_count = search_grid.frame_count;
    let max_bin = min_bin + usable_bins - 1;

    let mut planner = RealFftPlanner::<f32>::new();
    let fft = planner.plan_fft_forward(geometry.symbol_samples);
    let mut input = fft.make_input_vec();
    let mut spectrum = fft.make_output_vec();

    // Standard Hann window over one symbol before the per-hop FFT.
    let window: Vec<f32> = (0..geometry.symbol_samples)
        .map(|index| {
            let phase =
                2.0 * std::f32::consts::PI * index as f32 / (geometry.symbol_samples - 1) as f32;
            0.5 - 0.5 * phase.cos()
        })
        .collect();

    let mut bins = vec![0.0f32; frame_count * usable_bins];
    for frame in 0..frame_count {
        let sample_offset = frame * geometry.hop_samples;
        for (slot, value) in input.iter_mut().enumerate() {
            *value = audio.samples[sample_offset + slot] * window[slot];
        }
        fft.process(&mut input, &mut spectrum).expect("fft forward");
        for bin in min_bin..=max_bin {
            let value = spectrum[bin];
            bins[frame * usable_bins + (bin - min_bin)] = value.norm_sqr();
        }
    }

    Spectrogram {
        bins,
        frame_count,
        usable_bins,
        min_bin,
    }
}

pub(super) fn collect_candidates(
    audio: &AudioBuffer,
    options: &DecodeOptions,
    sync_threshold: f32,
) -> Vec<DecodeCandidate> {
    let spec = options.mode.spec();
    if spec.mode == Mode::Ft4 {
        return collect_candidates_ft4(audio, options, sync_threshold);
    }
    let geometry = &spec.geometry;
    let tuning = &spec.tuning;
    let sync_step_samples = spec.sync_step_samples();
    let sync_fft_samples = spec.sync_fft_samples();
    let sync_bin_hz = spec.sync_bin_hz();
    let sync_step_seconds = spec.sync_step_seconds();
    let nhsym = audio
        .samples
        .len()
        .saturating_div(sync_step_samples)
        .saturating_sub(3);
    if nhsym == 0 {
        return Vec::new();
    }

    let min_bin = ((options.min_freq_hz / sync_bin_hz).round() as usize).max(1);
    let max_bin = ((options.max_freq_hz / sync_bin_hz).round() as usize)
        .min(sync_fft_samples / 2 - tuning.sync_guard_bins);
    if min_bin >= max_bin {
        return Vec::new();
    }

    let plan = Sync8Plan::for_mode(options.mode);
    let fft = &plan.forward;
    let mut input = fft.make_input_vec();
    let mut spectrum = fft.make_output_vec();
    let mut symbol_power = vec![0.0f32; nhsym * (sync_fft_samples / 2 + 1)];
    let scale = tuning.sync_power_scale;

    for step in 0..nhsym {
        let start = step * sync_step_samples;
        input.fill(0.0);
        input[..geometry.symbol_samples]
            .copy_from_slice(&audio.samples[start..start + geometry.symbol_samples]);
        for sample in &mut input[..geometry.symbol_samples] {
            *sample *= scale;
        }
        fft.process(&mut input, &mut spectrum).expect("sync8 fft");
        let row = step * (sync_fft_samples / 2 + 1);
        for bin in 1..=(sync_fft_samples / 2) {
            symbol_power[row + bin] = spectrum[bin].norm_sqr();
        }
    }

    let mut primary = Vec::with_capacity(max_bin - min_bin + 1);
    let mut secondary = Vec::with_capacity(max_bin - min_bin + 1);
    let nominal_start = spec.nominal_start_sync_lag();

    for bin in min_bin..=max_bin {
        let mut best_local = (f32::NEG_INFINITY, 0isize);
        let mut best_wide = (f32::NEG_INFINITY, 0isize);
        for lag in -tuning.sync_max_lag..=tuning.sync_max_lag {
            let score = sync8_score(spec, &symbol_power, nhsym, bin, lag, nominal_start);
            if (-tuning.sync_local_lag..=tuning.sync_local_lag).contains(&lag)
                && score > best_local.0
            {
                best_local = (score, lag);
            }
            if score > best_wide.0 {
                best_wide = (score, lag);
            }
        }
        primary.push((bin, best_local.1, best_local.0));
        secondary.push((bin, best_wide.1, best_wide.0));
    }

    normalize_sync_scores(spec, &mut primary);
    normalize_sync_scores(spec, &mut secondary);

    let mut raw = Vec::<DecodeCandidate>::new();
    for &(bin, lag, score) in &primary {
        if score >= sync_threshold && score.is_finite() {
            let dt_seconds = spec.candidate_dt_seconds_from_lag(lag);
            raw.push(DecodeCandidate {
                start_seconds: spec.candidate_start_seconds_from_lag(lag),
                dt_seconds,
                freq_hz: bin as f32 * sync_bin_hz,
                score,
            });
        }
    }
    for &(bin, lag, score) in &secondary {
        if score >= sync_threshold
            && score.is_finite()
            && !primary
                .iter()
                .any(|&(b, local_lag, _)| b == bin && local_lag == lag)
        {
            let dt_seconds = spec.candidate_dt_seconds_from_lag(lag);
            raw.push(DecodeCandidate {
                start_seconds: spec.candidate_start_seconds_from_lag(lag),
                dt_seconds,
                freq_hz: bin as f32 * sync_bin_hz,
                score,
            });
        }
    }

    raw.sort_by(|left, right| right.score.total_cmp(&left.score));

    let mut prioritized = Vec::with_capacity(raw.len());
    prioritized.extend(
        raw.iter()
            .filter(|candidate| {
                (candidate.freq_hz - tuning.nfqso_hz).abs() <= tuning.nfqso_priority_window_hz
            })
            .cloned(),
    );
    prioritized.extend(raw.into_iter().filter(|candidate| {
        (candidate.freq_hz - tuning.nfqso_hz).abs() > tuning.nfqso_priority_window_hz
    }));

    let mut selected = Vec::new();
    for candidate in prioritized {
        let too_close = selected.iter().any(|existing: &DecodeCandidate| {
            (existing.dt_seconds - candidate.dt_seconds).abs() < sync_step_seconds
                && (existing.freq_hz - candidate.freq_hz).abs() < tuning.candidate_separation_hz
        });
        if too_close {
            continue;
        }
        selected.push(candidate);
        if selected.len() >= options.max_candidates {
            break;
        }
    }

    if selected.is_empty() {
        collect_candidates_legacy(audio, options)
    } else {
        selected
    }
}

fn collect_candidates_ft4(
    audio: &AudioBuffer,
    options: &DecodeOptions,
    sync_threshold: f32,
) -> Vec<DecodeCandidate> {
    let spec = options.mode.spec();
    debug_assert_eq!(spec.mode, Mode::Ft4);

    let nfft = spec.sync_fft_samples();
    let nh1 = nfft / 2;
    let nstep = spec.geometry.symbol_samples;
    if audio.samples.len() < nfft {
        return Vec::new();
    }

    let mut planner = RealFftPlanner::<f32>::new();
    let fft = planner.plan_fft_forward(nfft);
    let mut input = fft.make_input_vec();
    let mut spectrum = fft.make_output_vec();
    let window = ft4_nuttall_window(nfft);
    let mut savg = vec![0.0f32; nh1 + 1];
    let fac = 1.0f32 / 300.0;
    let mut frames = 0usize;

    let mut start = 0usize;
    while start + nfft <= audio.samples.len() {
        for (slot, (&sample, &gain)) in input
            .iter_mut()
            .zip(audio.samples[start..start + nfft].iter().zip(window.iter()))
        {
            *slot = fac * sample * gain;
        }
        fft.process(&mut input, &mut spectrum).expect("ft4 coarse fft");
        for bin in 1..=nh1 {
            savg[bin] += spectrum[bin].norm_sqr();
        }
        frames += 1;
        start += nstep;
    }
    if frames == 0 {
        return Vec::new();
    }
    for value in &mut savg[1..=nh1] {
        *value /= frames as f32;
    }

    let mut savsm = vec![0.0f32; nh1 + 1];
    for bin in 8..=nh1.saturating_sub(7) {
        savsm[bin] = savg[bin - 7..=bin + 7].iter().copied().sum::<f32>() / 15.0;
    }

    let df = spec.sync_bin_hz();
    let nfa = ((options.min_freq_hz / df).round() as usize).max((200.0 / df).round() as usize);
    let nfb = ((options.max_freq_hz / df).round() as usize).min((4910.0 / df).round() as usize);
    if nfa >= nfb || nfb > nh1 {
        return Vec::new();
    }

    let sbase = ft4_baseline(&savg, nfa, nfb);
    for bin in nfa..=nfb {
        if sbase[bin] <= 0.0 {
            return Vec::new();
        }
        savsm[bin] /= sbase[bin];
    }

    let f_offset = -1.5 * spec.geometry.tone_spacing_hz;
    let mut near_qso = Vec::new();
    let mut others = Vec::new();
    for bin in (nfa + 1)..=(nfb.saturating_sub(1)) {
        if savsm[bin] < sync_threshold
            || savsm[bin] < savsm[bin - 1]
            || savsm[bin] < savsm[bin + 1]
        {
            continue;
        }
        let den = savsm[bin - 1] - 2.0 * savsm[bin] + savsm[bin + 1];
        let del = if den != 0.0 {
            0.5 * (savsm[bin - 1] - savsm[bin + 1]) / den
        } else {
            0.0
        };
        let freq_hz = (bin as f32 + del) * df + f_offset;
        if !(200.0..=4910.0).contains(&freq_hz) {
            continue;
        }
        let score = savsm[bin] - 0.25 * (savsm[bin - 1] - savsm[bin + 1]) * del;
        let candidate = DecodeCandidate {
            start_seconds: spec.start_seconds_from_dt(0.0),
            dt_seconds: 0.0,
            freq_hz,
            score,
        };
        if (freq_hz - spec.tuning.nfqso_hz).abs() <= spec.tuning.nfqso_priority_window_hz {
            near_qso.push(candidate);
        } else {
            others.push(candidate);
        }
    }

    near_qso
        .into_iter()
        .chain(others)
        .take(options.max_candidates)
        .collect()
}

fn ft4_nuttall_window(nfft: usize) -> &'static [f32] {
    static WINDOW: OnceLock<Vec<f32>> = OnceLock::new();
    WINDOW.get_or_init(|| {
        let pi = std::f32::consts::PI;
        (0..nfft)
            .map(|index| {
                let phase = 2.0 * pi * index as f32 / nfft as f32;
                0.3635819
                    - 0.4891775 * phase.cos()
                    + 0.1365995 * (2.0 * phase).cos()
                    - 0.0106411 * (3.0 * phase).cos()
            })
            .collect()
    })
}

fn ft4_baseline(savg: &[f32], nfa: usize, nfb: usize) -> Vec<f32> {
    let nh1 = savg.len().saturating_sub(1);
    let ia = nfa.max(1);
    let ib = nfb.min(nh1);
    let mut spectrum_db = savg.to_vec();
    for value in &mut spectrum_db[ia..=ib] {
        *value = 10.0 * value.max(1e-12).log10();
    }

    let nseg = 10usize;
    let nlen = ((ib - ia + 1) / nseg).max(1);
    let i0 = (ib - ia + 1) as f64 / 2.0;
    let mut xs = Vec::<f64>::new();
    let mut ys = Vec::<f64>::new();

    for seg in 0..nseg {
        let ja = ia + seg * nlen;
        if ja > ib {
            break;
        }
        let jb = (ja + nlen - 1).min(ib);
        let base = percentile_10(&spectrum_db[ja..=jb]);
        for (offset, &value) in spectrum_db[ja..=jb].iter().enumerate() {
            if value <= base {
                let bin = ja + offset;
                xs.push(bin as f64 - i0);
                ys.push(value as f64);
            }
        }
    }

    let coeffs = polyfit_degree4(&xs, &ys).unwrap_or([0.0; 5]);
    let mut baseline = vec![0.0f32; savg.len()];
    for (bin, slot) in baseline.iter_mut().enumerate().take(ib + 1).skip(ia) {
        let t = bin as f64 - i0;
        let db = coeffs[0]
            + t * (coeffs[1] + t * (coeffs[2] + t * (coeffs[3] + t * coeffs[4])))
            + 0.65;
        *slot = 10.0f64.powf(db / 10.0) as f32;
    }
    baseline
}

fn percentile_10(values: &[f32]) -> f32 {
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.total_cmp(right));
    let rank = ((sorted.len() as f32 * 0.10).round() as usize)
        .clamp(1, sorted.len());
    let index = rank - 1;
    sorted[index]
}

fn polyfit_degree4(xs: &[f64], ys: &[f64]) -> Option<[f64; 5]> {
    if xs.len() != ys.len() || xs.len() < 5 {
        return None;
    }

    let mut a = [[0.0f64; 6]; 5];
    for row in 0..5 {
        for col in 0..5 {
            a[row][col] = xs.iter().map(|&x| x.powi((row + col) as i32)).sum();
        }
        a[row][5] = xs
            .iter()
            .zip(ys.iter())
            .map(|(&x, &y)| y * x.powi(row as i32))
            .sum();
    }

    for pivot in 0..5 {
        let best = (pivot..5)
            .max_by(|&left, &right| a[left][pivot].abs().total_cmp(&a[right][pivot].abs()))?;
        if a[best][pivot].abs() < 1e-12 {
            return None;
        }
        if best != pivot {
            a.swap(best, pivot);
        }
        let scale = a[pivot][pivot];
        for col in pivot..=5 {
            a[pivot][col] /= scale;
        }
        for row in 0..5 {
            if row == pivot {
                continue;
            }
            let factor = a[row][pivot];
            if factor == 0.0 {
                continue;
            }
            for col in pivot..=5 {
                a[row][col] -= factor * a[pivot][col];
            }
        }
    }

    Some(std::array::from_fn(|index| a[index][5]))
}

pub(super) fn zero_tail(audio: &AudioBuffer, keep_samples: usize) -> AudioBuffer {
    let mut copy = audio.clone();
    if keep_samples < copy.samples.len() {
        copy.samples[keep_samples..].fill(0.0);
    }
    copy
}

pub(super) fn collect_candidates_legacy(
    audio: &AudioBuffer,
    options: &DecodeOptions,
) -> Vec<DecodeCandidate> {
    let spec = options.mode.spec();
    let geometry = &spec.geometry;
    let tuning = &spec.tuning;
    let hops_per_symbol = geometry.hops_per_symbol();
    let spectrogram = build_spectrogram(audio, options);
    let costas = all_costas_positions(geometry);
    let max_start_frame = spectrogram
        .frame_count
        .saturating_sub((geometry.message_symbols - 1) * hops_per_symbol + 1);

    let mut raw = Vec::<DecodeCandidate>::new();
    for phase in 0..hops_per_symbol {
        let mut start_frame = phase;
        while start_frame < max_start_frame {
            for base in 0..spectrogram
                .usable_bins
                .saturating_sub(spec.tone_count().saturating_sub(1))
            {
                let mut score = 0.0f32;
                for &(symbol_index, tone) in &costas {
                    let frame = start_frame + symbol_index * hops_per_symbol;
                    let row = frame * spectrogram.usable_bins;
                    let mut band_sum = 0.0;
                    for offset in 0..spec.tone_count() {
                        band_sum += spectrogram.bins[row + base + offset];
                    }
                    let expected = spectrogram.bins[row + base + tone];
                    score += expected * spec.tone_count() as f32 - band_sum;
                }
                raw.push(DecodeCandidate {
                    start_seconds: start_frame as f32 * geometry.hop_samples as f32
                        / geometry.sample_rate_hz as f32,
                    dt_seconds: spec.dt_seconds_from_start(
                        start_frame as f32 * geometry.hop_samples as f32
                            / geometry.sample_rate_hz as f32,
                    ),
                    freq_hz: (spectrogram.min_bin + base) as f32 * geometry.tone_spacing_hz,
                    score,
                });
            }
            start_frame += hops_per_symbol;
        }
    }

    raw.sort_by(|left, right| right.score.total_cmp(&left.score));

    let mut selected = Vec::new();
    for candidate in raw {
        let too_close = selected.iter().any(|existing: &DecodeCandidate| {
            (existing.dt_seconds - candidate.dt_seconds).abs()
                < tuning.legacy_candidate_separation_dt_seconds
                && (existing.freq_hz - candidate.freq_hz).abs()
                    < geometry.tone_spacing_hz * tuning.legacy_candidate_separation_tone_factor
        });
        if too_close {
            continue;
        }
        selected.push(candidate);
        if selected.len() >= options.max_candidates {
            break;
        }
    }
    selected
}

impl Sync8Plan {
    pub(super) fn for_mode(mode: Mode) -> &'static Self {
        match mode {
            Mode::Ft8 => {
                static PLAN: OnceLock<Sync8Plan> = OnceLock::new();
                PLAN.get_or_init(|| Sync8Plan::new(mode.spec()))
            }
            Mode::Ft4 => {
                static PLAN: OnceLock<Sync8Plan> = OnceLock::new();
                PLAN.get_or_init(|| Sync8Plan::new(mode.spec()))
            }
            Mode::Ft2 => {
                static PLAN: OnceLock<Sync8Plan> = OnceLock::new();
                PLAN.get_or_init(|| Sync8Plan::new(mode.spec()))
            }
        }
    }

    fn new(spec: &ModeSpec) -> Self {
        let mut planner = RealFftPlanner::<f32>::new();
        Self {
            forward: planner.plan_fft_forward(spec.sync_fft_samples()),
        }
    }
}

pub(super) fn sync8_score(
    spec: &ModeSpec,
    symbol_power: &[f32],
    nhsym: usize,
    bin: usize,
    lag: isize,
    nominal_start: isize,
) -> f32 {
    let geometry = &spec.geometry;
    let row_len = spec.sync_fft_samples() / 2 + 1;
    let sync_steps_per_symbol = spec.tuning.sync_step_divisor;
    let tone_bin_stride = spec.sync_tone_bin_stride();
    let tone_count = spec.tone_count();
    let mut block_signal = vec![0.0f32; geometry.sync_block_starts.len()];
    let mut block_band = vec![0.0f32; geometry.sync_block_starts.len()];

    for (block_index, (&block_start, pattern)) in geometry
        .sync_block_starts
        .iter()
        .zip(geometry.sync_patterns.iter().copied())
        .enumerate()
    {
        for (offset, costas) in pattern.iter().copied().enumerate() {
            let row_start =
                lag + nominal_start + ((block_start + offset) * sync_steps_per_symbol) as isize;
            if !(1..=nhsym as isize).contains(&row_start) {
                continue;
            }
            let row = (row_start as usize - 1) * row_len;
            block_signal[block_index] += symbol_power[row + bin + tone_bin_stride * costas];
            for tone in 0..tone_count {
                block_band[block_index] += symbol_power[row + bin + tone_bin_stride * tone];
            }
        }
    }

    let score_all = ratio_sync_score(
        spec,
        block_signal.iter().copied().sum(),
        block_band.iter().copied().sum(),
    );
    let score_tail = if block_signal.len() > 1 {
        ratio_sync_score(
            spec,
            block_signal[1..].iter().copied().sum(),
            block_band[1..].iter().copied().sum(),
        )
    } else {
        0.0
    };
    score_all.max(score_tail)
}

pub(super) fn ratio_sync_score(spec: &ModeSpec, signal: f32, band_total: f32) -> f32 {
    let noise = (band_total - signal) / (spec.tone_count().saturating_sub(1).max(1)) as f32;
    if noise > 0.0 { signal / noise } else { 0.0 }
}

pub(super) fn normalize_sync_scores(spec: &ModeSpec, scores: &mut [(usize, isize, f32)]) {
    let mut values: Vec<f32> = scores
        .iter()
        .map(|&(_, _, score)| score)
        .filter(|score| score.is_finite())
        .collect();
    if values.is_empty() {
        return;
    }
    values.sort_by(|left, right| left.total_cmp(right));
    let percentile =
        ((values.len() as f32 * spec.tuning.sync_baseline_percentile).round() as usize)
            .clamp(1, values.len())
            - 1;
    let baseline = values[percentile].max(spec.tuning.sync_baseline_floor);
    for (_, _, score) in scores.iter_mut() {
        *score /= baseline;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync8_score_uses_quarter_symbol_row_mapping() {
        let spec = Mode::Ft8.spec();
        let geometry = &spec.geometry;
        let row_len = spec.sync_fft_samples() / 2 + 1;
        let nhsym = 400usize;
        let bin = 10usize;
        let nominal_start = 2isize;
        let lag = 0isize;
        let mut symbol_power = vec![0.0f32; nhsym * row_len];

        for (&block_start, pattern) in geometry
            .sync_block_starts
            .iter()
            .zip(geometry.sync_patterns.iter().copied())
        {
            for (offset, costas) in pattern.iter().copied().enumerate() {
                let row_start =
                    nominal_start + ((block_start + offset) * spec.tuning.sync_step_divisor) as isize;
                let row = (row_start as usize - 1) * row_len;
                for tone in 0..spec.tone_count() {
                    symbol_power[row + bin + spec.sync_tone_bin_stride() * tone] = 1.0;
                }
                symbol_power[row + bin + spec.sync_tone_bin_stride() * costas] = 8.0;
            }
        }

        assert!(sync8_score(spec, &symbol_power, nhsym, bin, lag, nominal_start) > 5.0);
    }
}
