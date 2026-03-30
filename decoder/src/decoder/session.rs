use super::*;

impl DecoderSession {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset(&mut self) {
        *self = Self::default();
    }

    pub fn emitted_stages(&self) -> &[DecodeStage] {
        &self.emitted_stages
    }

    pub fn decode_available(
        &mut self,
        audio: &AudioBuffer,
        options: &DecodeOptions,
    ) -> Result<Vec<StageDecodeReport>, DecoderError> {
        validate_audio(audio)?;

        let mut updates = Vec::new();
        for stage in [DecodeStage::Early41, DecodeStage::Early47, DecodeStage::Full] {
            if self.emitted_stages.contains(&stage) {
                continue;
            }
            if !stage_is_enabled(stage, options) || audio.samples.len() < stage.required_samples() {
                continue;
            }
            let update = self.decode_stage(audio, options, stage)?;
            updates.push(update);
        }
        Ok(updates)
    }

    pub fn decode_available_with_state(
        &mut self,
        audio: &AudioBuffer,
        options: &DecodeOptions,
        state: Option<&DecoderState>,
    ) -> Result<Vec<(StageDecodeReport, DecoderState)>, DecoderError> {
        validate_audio(audio)?;

        let mut updates = Vec::new();
        let mut current_state = state.cloned();
        for stage in [DecodeStage::Early41, DecodeStage::Early47, DecodeStage::Full] {
            if self.emitted_stages.contains(&stage) {
                continue;
            }
            if !stage_is_enabled(stage, options) || audio.samples.len() < stage.required_samples() {
                continue;
            }
            let (update, next_state) =
                self.decode_stage_with_state(audio, options, stage, current_state.as_ref())?;
            current_state = Some(next_state.clone());
            updates.push((update, next_state));
        }
        Ok(updates)
    }

    pub fn decode_stage(
        &mut self,
        audio: &AudioBuffer,
        options: &DecodeOptions,
        stage: DecodeStage,
    ) -> Result<StageDecodeReport, DecoderError> {
        let (update, _) = self.decode_stage_with_state(audio, options, stage, None)?;
        Ok(update)
    }

    pub fn decode_stage_with_state(
        &mut self,
        audio: &AudioBuffer,
        options: &DecodeOptions,
        stage: DecodeStage,
        state: Option<&DecoderState>,
    ) -> Result<(StageDecodeReport, DecoderState), DecoderError> {
        validate_audio(audio)?;
        if !stage_is_enabled(stage, options) {
            return Err(DecoderError::UnsupportedFormat(format!(
                "stage {} is not enabled for profile {}",
                stage.as_str(),
                options.profile.as_str()
            )));
        }
        if audio.samples.len() < stage.required_samples() {
            return Err(DecoderError::UnsupportedFormat(format!(
                "audio too short for stage {}",
                stage.as_str()
            )));
        }

        let search = match stage {
            DecodeStage::Early41 => {
                let early_audio = zero_tail(audio, EARLY_41_SAMPLES);
                let search = run_decode_search(
                    &early_audio,
                    options,
                    None,
                    Vec::new(),
                    state.map(|state| &state.resolver),
                    SYNC8_EARLY_THRESHOLD,
                    false,
                );
                self.early41 = Some(search.clone());
                search
            }
            DecodeStage::Early47 => {
                let search = run_early47_search(audio, options, self.early41.as_ref(), state);
                self.early47 = Some(search.clone());
                search
            }
            DecodeStage::Full => {
                run_full_search(audio, options, self.early41.as_ref(), self.early47.as_ref(), state)
            }
        };

        let report = build_decode_report_with_resolver(audio, options, search.clone(), state);
        let state = build_decoder_state(state, &search);
        let (new_decodes, updated_decodes) = diff_decodes(&self.last_decodes, &report.decodes);
        self.last_decodes = report
            .decodes
            .iter()
            .cloned()
            .map(|decode| (decode.text.clone(), decode))
            .collect();
        self.emitted_stages.push(stage);

        Ok((
            StageDecodeReport {
                stage,
                report,
                new_decodes,
                updated_decodes,
            },
            state,
        ))
    }
}

pub(super) fn merged_resolver(
    base: Option<&HashResolver>,
    successes: &[SuccessfulDecode],
) -> HashResolver {
    let mut resolver = base.cloned().unwrap_or_default();
    for success in successes {
        success.payload.collect_callsigns(&mut resolver);
    }
    resolver
}

pub(super) fn rendered_success_key(success: &SuccessfulDecode, resolver: &HashResolver) -> String {
    let rendered = success.payload.to_message(resolver).to_text();
    if rendered.trim().is_empty() {
        format!("{:?}", success.payload)
    } else {
        rendered
    }
}

pub(super) fn is_new_success(
    existing: &[SuccessfulDecode],
    current_pass: &[SuccessfulDecode],
    candidate: &SuccessfulDecode,
    base_resolver: Option<&HashResolver>,
) -> bool {
    let mut combined = Vec::with_capacity(existing.len() + current_pass.len() + 1);
    combined.extend(existing.iter().cloned());
    combined.extend(current_pass.iter().cloned());
    combined.push(candidate.clone());
    let resolver = merged_resolver(base_resolver, &combined);
    let candidate_key = rendered_success_key(candidate, &resolver);
    let mut seen = HashSet::<String>::new();
    for success in existing.iter().chain(current_pass.iter()) {
        seen.insert(rendered_success_key(success, &resolver));
    }
    !seen.contains(&candidate_key)
}

pub(super) fn validate_audio(audio: &AudioBuffer) -> Result<(), DecoderError> {
    let geometry = &ACTIVE_MODE.geometry;
    if audio.sample_rate_hz != geometry.sample_rate_hz {
        return Err(DecoderError::UnsupportedFormat(format!(
            "expected {} Hz audio, got {} Hz",
            geometry.sample_rate_hz, audio.sample_rate_hz
        )));
    }
    if audio.samples.len() < geometry.symbol_samples {
        return Err(DecoderError::UnsupportedFormat(
            "audio too short".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn stage_is_enabled(stage: DecodeStage, options: &DecodeOptions) -> bool {
    match stage {
        DecodeStage::Full => true,
        DecodeStage::Early41 | DecodeStage::Early47 => options.uses_early_decodes(),
    }
}

pub(super) fn run_early47_search(
    audio: &AudioBuffer,
    options: &DecodeOptions,
    early41: Option<&SearchResult>,
    state: Option<&DecoderState>,
) -> SearchResult {
    let subtraction_plan = SubtractionPlan::global();
    let mut partial47 = zero_tail(audio, EARLY_47_SAMPLES);
    if let Some(stage41) = early41 {
        for success in &stage41.successes {
            if success.candidate.dt_seconds < ACTIVE_MODE.tuning.subtraction_refine_cutoff_seconds {
                subtract_candidate_with_dt_refinement(
                    &mut partial47,
                    success,
                    subtraction_plan,
                    true,
                );
            }
        }
    }
    let initial_successes = early41
        .map(|stage| stage.successes.clone())
        .unwrap_or_default();
    run_decode_search(
        &partial47,
        options,
        Some(partial47.clone()),
        initial_successes,
        state.map(|state| &state.resolver),
        options.sync_threshold(),
        false,
    )
}

pub(super) fn run_full_search(
    audio: &AudioBuffer,
    options: &DecodeOptions,
    early41: Option<&SearchResult>,
    early47: Option<&SearchResult>,
    state: Option<&DecoderState>,
) -> SearchResult {
    let subtraction_plan = SubtractionPlan::global();
    let initial_successes = early47
        .map(|stage| stage.successes.clone())
        .or_else(|| early41.map(|stage| stage.successes.clone()))
        .unwrap_or_default();
    let prepared_full = (!initial_successes.is_empty()).then(|| {
        let mut prepared = audio.clone();
        for success in &initial_successes {
            subtract_candidate_with_dt_refinement(&mut prepared, success, subtraction_plan, true);
        }
        prepared
    });
    run_decode_search(
        audio,
        options,
        prepared_full,
        initial_successes,
        state.map(|state| &state.resolver),
        options.sync_threshold(),
        true,
    )
}

pub(super) fn build_decode_report_with_resolver(
    audio: &AudioBuffer,
    options: &DecodeOptions,
    search: SearchResult,
    base_state: Option<&DecoderState>,
) -> DecodeReport {
    let resolver = base_state
        .map(|state| state.resolver.clone())
        .unwrap_or_default();

    let mut dedup = BTreeMap::<String, DecodedMessage>::new();
    for success in search.successes {
        let message = success.payload.to_message(&resolver);
        if matches!(message, StructuredMessage::Unsupported { .. }) {
            continue;
        }
        let text = message.to_text();
        if text.trim().is_empty() {
            continue;
        }
        let decode = DecodedMessage {
            utc: "000000".to_string(),
            snr_db: success.snr_db,
            dt_seconds: success.candidate.dt_seconds,
            freq_hz: success.candidate.freq_hz,
            text: text.clone(),
            candidate_score: success.candidate.score,
            ldpc_iterations: success.ldpc_iterations,
            message,
        };
        match dedup.get(&text) {
            Some(existing) if existing.candidate_score >= decode.candidate_score => {}
            _ => {
                dedup.insert(text, decode);
            }
        }
    }

    let mut decodes: Vec<_> = dedup.into_values().collect();
    decodes.sort_by(|left, right| {
        left.freq_hz
            .total_cmp(&right.freq_hz)
            .then_with(|| left.text.cmp(&right.text))
    });

    DecodeReport {
        sample_rate_hz: audio.sample_rate_hz,
        duration_seconds: audio.samples.len() as f32 / audio.sample_rate_hz as f32,
        diagnostics: DecodeDiagnostics {
            frame_count: search.frame_count,
            usable_bins: search.usable_bins,
            examined_candidates: options.max_candidates,
            accepted_candidates: decodes.len(),
            ldpc_codewords: search.counters.ldpc_codewords,
            parsed_payloads: search.counters.parsed_payloads,
            top_candidates: search.top_candidates,
        },
        decodes,
    }
}

pub(super) fn build_decoder_state(
    base_state: Option<&DecoderState>,
    search: &SearchResult,
) -> DecoderState {
    DecoderState {
        resolver: merged_resolver(base_state.map(|state| &state.resolver), &search.successes),
    }
}

pub(super) fn diff_decodes(
    previous: &BTreeMap<String, DecodedMessage>,
    current: &[DecodedMessage],
) -> (Vec<DecodedMessage>, Vec<DecodedMessage>) {
    let mut new_decodes = Vec::new();
    let mut updated_decodes = Vec::new();
    for decode in current {
        match previous.get(&decode.text) {
            None => new_decodes.push(decode.clone()),
            Some(existing)
                if existing.candidate_score != decode.candidate_score
                    || existing.dt_seconds != decode.dt_seconds
                    || existing.freq_hz != decode.freq_hz
                    || existing.snr_db != decode.snr_db =>
            {
                updated_decodes.push(decode.clone());
            }
            Some(_) => {}
        }
    }
    (new_decodes, updated_decodes)
}

pub(super) fn run_decode_search(
    audio: &AudioBuffer,
    options: &DecodeOptions,
    residual_override: Option<AudioBuffer>,
    initial_successes: Vec<SuccessfulDecode>,
    base_resolver: Option<&HashResolver>,
    sync_threshold: f32,
    allow_ap: bool,
) -> SearchResult {
    let total_passes = options.search_passes.max(1);
    let has_residual_override = residual_override.is_some();
    let mut residual_audio = residual_override.unwrap_or_else(|| audio.clone());
    let baseband_plan = BasebandPlan::new();
    let subtraction_plan = SubtractionPlan::global();
    let parity = ParityMatrix::global();
    let mut top_candidates = Vec::new();
    let mut counters = DecodeCounters::default();
    let search_grid = search_grid(audio, options);
    let frame_count = search_grid.frame_count;
    let usable_bins = search_grid.usable_bins;

    let mut successes = initial_successes;
    if !has_residual_override {
        for success in &successes {
            subtract_candidate(&mut residual_audio, success, subtraction_plan);
        }
    }

    for pass in 0..total_passes {
        let long_spectrum = build_long_spectrum(&residual_audio);
        let mut pass_options = options.clone();
        pass_options.max_candidates = options.max_candidates_for_pass(pass);
        let candidates = collect_candidates(&residual_audio, &pass_options, sync_threshold);
        if pass == 0 {
            top_candidates = candidates.clone();
        }
        if candidates.is_empty() {
            break;
        }

        let mut pass_successes = Vec::<SuccessfulDecode>::new();
        let mut pass_changed = false;
        for candidate in &candidates {
            let mut local_counters = DecodeCounters::default();
            let success = try_candidate(
                search_grid,
                &long_spectrum,
                &baseband_plan,
                candidate,
                options,
                pass,
                parity,
                allow_ap,
                &mut local_counters,
            );
            counters.ldpc_codewords += local_counters.ldpc_codewords;
            counters.parsed_payloads += local_counters.parsed_payloads;
            if let Some(success) = success {
                pass_changed = true;
                subtract_candidate(&mut residual_audio, &success, subtraction_plan);
                if !is_new_success(&successes, &pass_successes, &success, base_resolver) {
                    continue;
                }
                pass_successes.push(success);
            }
        }
        if !pass_changed {
            break;
        }

        let remaining = options.max_successes.saturating_sub(successes.len());
        if remaining == 0 {
            break;
        }
        if pass_successes.len() > remaining {
            pass_successes.truncate(remaining);
        }
        successes.extend(pass_successes);
    }

    SearchResult {
        successes,
        residual_audio,
        frame_count,
        usable_bins,
        top_candidates,
        counters,
    }
}

pub(super) fn try_candidate(
    _search_grid: SearchGrid,
    long_spectrum: &LongSpectrum,
    baseband_plan: &BasebandPlan,
    candidate: &DecodeCandidate,
    options: &DecodeOptions,
    outer_pass: usize,
    parity: &ParityMatrix,
    allow_ap: bool,
    counters: &mut DecodeCounters,
) -> Option<SuccessfulDecode> {
    let mut best: Option<SuccessfulDecode> = None;
    let mut refined_basebands = Vec::<(i32, Vec<Complex32>)>::new();
    let coarse_freq_hz = candidate.freq_hz;
    if coarse_freq_hz < options.min_freq_hz || coarse_freq_hz > options.max_freq_hz {
        return None;
    }
    let Some(initial_baseband) = downsample_candidate(long_spectrum, baseband_plan, coarse_freq_hz)
    else {
        return None;
    };
    if let Some(refined) = refine_candidate_with_cache(
        long_spectrum,
        baseband_plan,
        &initial_baseband,
        &mut refined_basebands,
        candidate.start_seconds,
        coarse_freq_hz,
    ) {
        let max_osd = options.max_osd_passes(outer_pass, refined.freq_hz);
        best = try_refined_candidate(
            &refined,
            candidate.score,
            max_osd,
            allow_ap,
            parity,
            counters,
        );

        if best.is_none() && matches!(options.profile, DecodeProfile::Medium) {
            if let Some(seed) = extract_candidate_from_baseband(
                &initial_baseband,
                candidate.start_seconds,
                coarse_freq_hz,
            ) {
                best = try_refined_candidate(
                    &seed,
                    candidate.score,
                    max_osd,
                    allow_ap,
                    parity,
                    counters,
                );
            }
        }
    }
    best
}

pub(super) fn try_refined_candidate(
    refined: &RefinedCandidate,
    candidate_score: f32,
    max_osd: isize,
    allow_ap: bool,
    parity: &ParityMatrix,
    counters: &mut DecodeCounters,
) -> Option<SuccessfulDecode> {
    for llrs in &refined.llr_sets {
        let Some((payload, bits, iterations)) = decode_llr_set(parity, llrs, max_osd, counters)
        else {
            continue;
        };
        return Some(SuccessfulDecode {
            payload,
            codeword_bits: bits,
            candidate: DecodeCandidate {
                start_seconds: refined.start_seconds,
                dt_seconds: refined.start_seconds - 0.5,
                freq_hz: refined.freq_hz,
                score: refined.sync_score.max(candidate_score),
            },
            ldpc_iterations: iterations,
            snr_db: refined.snr_db,
        });
    }

    if allow_ap {
        let ap_magnitude = refined.llr_sets[0]
            .iter()
            .map(|value| value.abs())
            .fold(0.0f32, f32::max)
            * 1.01;
        if ap_magnitude > 0.0 {
            for known_bits in [cq_ap_known_bits(), mycall_ap_known_bits()] {
                let ap_llrs = llrs_with_known_bits(&refined.llr_sets[0], known_bits, ap_magnitude);
                if let Some((payload, bits, iterations)) =
                    decode_llr_set_with_known_bits(parity, &ap_llrs, known_bits, max_osd, counters)
                {
                    return Some(SuccessfulDecode {
                        payload,
                        codeword_bits: bits,
                        candidate: DecodeCandidate {
                            start_seconds: refined.start_seconds,
                            dt_seconds: refined.start_seconds - 0.5,
                            freq_hz: refined.freq_hz,
                            score: refined.sync_score.max(candidate_score),
                        },
                        ldpc_iterations: iterations,
                        snr_db: refined.snr_db,
                    });
                }
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::hash_callsign;
    use crate::message::ReplyWord;

    #[test]
    fn merged_resolver_collects_callsigns_from_successes() {
        let frame =
            crate::encode::encode_nonstandard_message("K1ABC", "HF19NY", false, ReplyWord::Blank, true)
                .expect("encode frame");
        let payload = unpack_message(&frame.codeword_bits)
            .expect("payload")
            .clone();
        let resolver = merged_resolver(
            None,
            &[SuccessfulDecode {
                payload,
                codeword_bits: frame.codeword_bits.to_vec(),
                candidate: DecodeCandidate {
                    start_seconds: 0.5,
                    dt_seconds: 0.0,
                    freq_hz: 1_200.0,
                    score: 1.0,
                },
                ldpc_iterations: 0,
                snr_db: 0,
            }],
        );

        assert_eq!(resolver.resolve12(hash_callsign("HF19NY", 12) as u16), Some("HF19NY"));
    }
}
