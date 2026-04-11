use std::collections::HashSet;

use super::*;
use crate::message::{Payload, ReplyWord, StructuredMessage};

pub(super) struct DebugSearchTrace {
    pub(super) search: SearchResult,
    pub(super) passes: Vec<SearchPassTrace>,
}

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
        let spec = options.mode.spec();
        validate_audio(audio, spec)?;

        let mut updates = Vec::new();
        for stage in DecodeStage::ordered() {
            if self.emitted_stages.contains(&stage) {
                continue;
            }
            if !stage_is_enabled(stage, options)
                || audio.samples.len() < stage.required_samples(spec)
            {
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
        let spec = options.mode.spec();
        validate_audio(audio, spec)?;

        let mut updates = Vec::new();
        let mut current_state = state.cloned();
        for stage in DecodeStage::ordered() {
            if self.emitted_stages.contains(&stage) {
                continue;
            }
            if !stage_is_enabled(stage, options)
                || audio.samples.len() < stage.required_samples(spec)
            {
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
        let spec = options.mode.spec();
        validate_audio(audio, spec)?;
        if !stage_is_enabled(stage, options) {
            return Err(DecoderError::UnsupportedFormat(format!(
                "stage {} is not enabled for profile {}",
                stage.as_str(),
                options.profile.as_str()
            )));
        }
        if audio.samples.len() < stage.required_samples(spec) {
            return Err(DecoderError::UnsupportedFormat(format!(
                "audio too short for stage {}",
                stage.as_str()
            )));
        }

        let search = match stage {
            DecodeStage::Early41 => {
                let early_audio = zero_tail(audio, early_41_samples(spec));
                let search = run_decode_search(
                    &early_audio,
                    options,
                    None,
                    Vec::new(),
                    state.map(|state| &state.resolver),
                    sync8_early_threshold(spec),
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
            DecodeStage::Full => run_full_search(
                audio,
                options,
                self.early41.as_ref(),
                self.early47.as_ref(),
                state,
            ),
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
    for success in existing.iter().chain(current_pass.iter()) {
        let success_key = rendered_success_key(success, &resolver);
        if success_key != candidate_key {
            continue;
        }
        return false;
    }
    true
}

pub(super) fn validate_audio(audio: &AudioBuffer, spec: &ModeSpec) -> Result<(), DecoderError> {
    let geometry = &spec.geometry;
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
    let spec = options.mode.spec();
    let subtraction_plan = SubtractionPlan::for_mode(options.mode);
    let mut partial47 = zero_tail(audio, early_47_samples(spec));
    if !options.disable_subtraction
        && let Some(stage41) = early41
    {
        for success in &stage41.successes {
            if success.candidate.dt_seconds < spec.subtraction.refine_cutoff_seconds {
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
    let subtraction_plan = SubtractionPlan::for_mode(options.mode);
    let initial_successes = early47
        .map(|stage| stage.successes.clone())
        .or_else(|| early41.map(|stage| stage.successes.clone()))
        .unwrap_or_default();
    let prepared_full =
        (!options.disable_subtraction && !initial_successes.is_empty()).then(|| {
            let mut prepared = audio.clone();
            for success in &initial_successes {
                subtract_candidate_with_dt_refinement(
                    &mut prepared,
                    success,
                    subtraction_plan,
                    true,
                );
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
    debug_run_decode_search_inner(
        audio,
        options,
        residual_override,
        initial_successes,
        base_resolver,
        sync_threshold,
        allow_ap,
        false,
    )
    .search
}

pub(super) fn debug_run_decode_search(
    audio: &AudioBuffer,
    options: &DecodeOptions,
) -> Result<DebugSearchTrace, DecoderError> {
    let spec = options.mode.spec();
    validate_audio(audio, spec)?;
    Ok(debug_run_decode_search_inner(
        audio,
        options,
        None,
        Vec::new(),
        None,
        options.sync_threshold(),
        true,
        true,
    ))
}

fn debug_run_decode_search_inner(
    audio: &AudioBuffer,
    options: &DecodeOptions,
    residual_override: Option<AudioBuffer>,
    initial_successes: Vec<SuccessfulDecode>,
    base_resolver: Option<&HashResolver>,
    _sync_threshold: f32,
    allow_ap: bool,
    capture_trace: bool,
) -> DebugSearchTrace {
    let total_passes = options.search_passes.max(1);
    let has_residual_override = residual_override.is_some();
    let mut residual_audio = residual_override.unwrap_or_else(|| audio.clone());
    let spec = options.mode.spec();
    let baseband_plan = BasebandPlan::new(spec);
    let subtraction_plan = SubtractionPlan::for_mode(options.mode);
    let parity = ParityMatrix::global();
    let mut top_candidates = Vec::new();
    let mut counters = DecodeCounters::default();
    let search_grid = search_grid(audio, options);
    let frame_count = search_grid.frame_count;
    let usable_bins = search_grid.usable_bins;
    let mut pass_traces = Vec::new();

    let mut successes = initial_successes;
    if !options.disable_subtraction && !has_residual_override {
        for success in &successes {
            subtract_candidate(&mut residual_audio, success, subtraction_plan);
        }
    }

    for pass in 0..total_passes {
        let pass_sync_threshold = options.sync_threshold_for_pass(pass);
        let cached_long_spectrum = if options.mode == Mode::Ft4 {
            None
        } else {
            Some(build_long_spectrum(&residual_audio, spec))
        };
        let mut pass_options = options.clone();
        pass_options.max_candidates = options.max_candidates_for_pass(pass);
        let candidates = collect_candidates(&residual_audio, &pass_options, pass_sync_threshold);
        if pass == 0 {
            top_candidates = candidates.clone();
        }
        if candidates.is_empty() {
            break;
        }

        let mut pass_successes = Vec::<SuccessfulDecode>::new();
        let mut pass_changed = false;
        let mut candidate_traces = Vec::new();
        let mut ft4_long_spectrum = if options.mode == Mode::Ft4 {
            Some(build_long_spectrum(&residual_audio, spec))
        } else {
            None
        };
        for candidate in &candidates {
            if options.mode == Mode::Ft4 && ft4_long_spectrum.is_none() {
                ft4_long_spectrum = Some(build_long_spectrum(&residual_audio, spec));
            }
            let long_spectrum = ft4_long_spectrum
                .as_ref()
                .or_else(|| cached_long_spectrum.as_ref())
                .expect("decode pass spectrum should be available");
            let mut local_counters = DecodeCounters::default();
            let ft4_ap_known_bits = Vec::new();
            let (resolver, seen_messages) =
                current_seen_message_texts(base_resolver, &successes, &pass_successes);
            let successes_for_candidate = try_candidate(
                search_grid,
                &long_spectrum,
                &baseband_plan,
                candidate,
                spec,
                options,
                pass,
                parity,
                allow_ap,
                &ft4_ap_known_bits,
                &resolver,
                &seen_messages,
                &mut local_counters,
            );
            counters.ldpc_codewords += local_counters.ldpc_codewords;
            counters.parsed_payloads += local_counters.parsed_payloads;
            let raw_successes: Vec<String> = if capture_trace {
                successes_for_candidate
                    .iter()
                    .map(|success| rendered_success_key(success, &resolver))
                    .collect()
            } else {
                Vec::new()
            };
            let mut accepted_successes = Vec::new();
            let mut accepted_subtractions = Vec::new();
            for success in successes_for_candidate {
                if options.mode == Mode::Ft4
                    && ft4_plain_report_after_rr73_conflict(
                        &success,
                        successes.iter().chain(pass_successes.iter()),
                    )
                {
                    continue;
                }
                let is_new = is_new_success(&successes, &pass_successes, &success, base_resolver);
                if !is_new {
                    // A duplicate decode still changes the residual if we subtract it, which can
                    // expose additional messages on later passes. The pre-debug-search behavior
                    // did this for all modes, and FT8 depends on it as well.
                    pass_changed = true;
                    if !options.disable_subtraction {
                        subtract_candidate(&mut residual_audio, &success, subtraction_plan);
                        ft4_long_spectrum = None;
                    }
                    continue;
                }
                pass_changed = true;
                if !options.disable_subtraction {
                    subtract_candidate(&mut residual_audio, &success, subtraction_plan);
                    ft4_long_spectrum = None;
                }
                if capture_trace {
                    accepted_successes.push(rendered_success_key(&success, &resolver));
                    accepted_subtractions.push(SearchAcceptedTrace {
                        text: success.payload.to_message(&resolver).to_text(),
                        dt_seconds: success.candidate.dt_seconds,
                        freq_hz: success.candidate.freq_hz,
                        codeword_bits: success
                            .codeword_bits
                            .iter()
                            .map(|bit| if *bit == 0 { '0' } else { '1' })
                            .collect(),
                    });
                }
                pass_successes.push(success);
            }
            if capture_trace {
                candidate_traces.push(SearchCandidateTrace {
                    coarse_start_seconds: candidate.start_seconds,
                    coarse_dt_seconds: candidate.dt_seconds,
                    coarse_freq_hz: candidate.freq_hz,
                    coarse_score: candidate.score,
                    raw_successes,
                    accepted_successes,
                    accepted_subtractions,
                });
            }
        }
        if capture_trace {
            pass_traces.push(SearchPassTrace {
                pass_index: pass,
                candidates: candidate_traces,
                residual_signature: Some(compute_residual_signature(&residual_audio)),
            });
        }
        let pass_added_unique = !pass_successes.is_empty();
        if options.mode == Mode::Ft4 && !pass_added_unique {
            break;
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

    DebugSearchTrace {
        search: SearchResult {
            successes,
            frame_count,
            usable_bins,
            top_candidates,
            counters,
        },
        passes: pass_traces,
    }
}

fn compute_residual_signature(audio: &AudioBuffer) -> SearchResidualSignature {
    const PROBE_INDICES: [usize; 10] = [0, 1, 2, 3, 576, 1_152, 4_096, 24_000, 48_000, 72_575];
    let sample_sum = audio.samples.iter().map(|&sample| sample as f64).sum();
    let sample_sq_sum = audio
        .samples
        .iter()
        .map(|&sample| (sample as f64) * (sample as f64))
        .sum();
    let probe_values = PROBE_INDICES
        .iter()
        .filter_map(|&index| audio.samples.get(index).copied())
        .collect();
    SearchResidualSignature {
        sample_sum,
        sample_sq_sum,
        probe_values,
    }
}

fn ft4_plain_report_after_rr73_conflict<'a>(
    candidate: &SuccessfulDecode,
    mut existing: impl Iterator<Item = &'a SuccessfulDecode>,
) -> bool {
    let StructuredMessage::Standard {
        first: candidate_first,
        second: candidate_second,
        acknowledge,
        info:
            crate::message::StructuredInfoField {
                value: crate::message::StructuredInfoValue::SignalReport { .. },
                ..
            },
        ..
    } = candidate.payload.to_message(&HashResolver::default())
    else {
        return false;
    };
    if acknowledge {
        return false;
    }

    existing.any(|success| {
        let StructuredMessage::Standard {
            first,
            second,
            acknowledge: false,
            info:
                crate::message::StructuredInfoField {
                    value:
                        crate::message::StructuredInfoValue::Reply {
                            word: ReplyWord::Rr73,
                        },
                    ..
                },
            ..
        } = success.payload.to_message(&HashResolver::default())
        else {
            return false;
        };

        first.raw == candidate_first.raw
            && second.raw == candidate_second.raw
            && (success.candidate.freq_hz - candidate.candidate.freq_hz).abs() <= 0.25
            && (success.candidate.dt_seconds - candidate.candidate.dt_seconds).abs() <= 0.008
    })
}

pub(super) fn try_candidate(
    _search_grid: SearchGrid,
    long_spectrum: &LongSpectrum,
    baseband_plan: &BasebandPlan,
    candidate: &DecodeCandidate,
    spec: &ModeSpec,
    options: &DecodeOptions,
    outer_pass: usize,
    parity: &ParityMatrix,
    allow_ap: bool,
    ft4_ap_known_bits: &[Vec<Option<u8>>],
    resolver: &HashResolver,
    seen_messages: &HashSet<String>,
    counters: &mut DecodeCounters,
) -> Vec<SuccessfulDecode> {
    if options.mode == Mode::Ft4 && candidate.dt_seconds == 0.0 {
        return try_candidate_ft4_ordered(
            long_spectrum,
            baseband_plan,
            candidate,
            spec,
            options,
            outer_pass,
            parity,
            allow_ap,
            ft4_ap_known_bits,
            resolver,
            seen_messages,
            counters,
        );
    }

    let mut best: Option<SuccessfulDecode> = None;
    let mut refined_basebands = Vec::<(i32, Vec<Complex32>)>::new();
    let coarse_freq_hz = candidate.freq_hz;
    if coarse_freq_hz < options.min_freq_hz || coarse_freq_hz > options.max_freq_hz {
        return Vec::new();
    }
    let initial_baseband = if options.mode == Mode::Ft4 {
        downsample_candidate_ft4_search(long_spectrum, baseband_plan, spec, coarse_freq_hz)
    } else {
        downsample_candidate(long_spectrum, baseband_plan, spec, coarse_freq_hz)
    };
    let Some(initial_baseband) = initial_baseband else {
        return Vec::new();
    };
    if let Some(refined) = refine_candidate_with_cache(
        long_spectrum,
        baseband_plan,
        spec,
        &initial_baseband,
        &mut refined_basebands,
        candidate.start_seconds,
        coarse_freq_hz,
    ) {
        let max_osd = options.max_osd_passes(outer_pass, refined.freq_hz);
        best = try_refined_candidate(
            &refined,
            options.mode,
            candidate.score,
            max_osd,
            allow_ap,
            ft4_ap_known_bits,
            resolver,
            seen_messages,
            parity,
            counters,
        );
        if options.mode == Mode::Ft4
            && best.as_ref().is_some_and(|success| {
                candidate.dt_seconds != 0.0 && !ft4_refinement_is_latched(candidate, success)
            })
        {
            best = None;
        }

        if matches!(options.profile, DecodeProfile::Medium) {
            if let Some(seed) = extract_candidate_from_baseband(
                &initial_baseband,
                spec,
                candidate.start_seconds,
                coarse_freq_hz,
            ) {
                let seed_success = try_refined_candidate(
                    &seed,
                    options.mode,
                    candidate.score,
                    max_osd,
                    allow_ap,
                    ft4_ap_known_bits,
                    resolver,
                    seen_messages,
                    parity,
                    counters,
                );
                best = match (best, seed_success) {
                    (None, other) => other,
                    (some @ Some(_), None) => some,
                    (Some(refined_success), Some(seed_success))
                        if options.mode == Mode::Ft4
                            && success_is_duplicate(&refined_success, resolver, seen_messages)
                            && !success_is_duplicate(&seed_success, resolver, seen_messages) =>
                    {
                        Some(seed_success)
                    }
                    (some @ Some(_), Some(_)) => some,
                };
            }
        }
    }
    best.into_iter().collect()
}

fn try_candidate_ft4_ordered(
    long_spectrum: &LongSpectrum,
    baseband_plan: &BasebandPlan,
    candidate: &DecodeCandidate,
    spec: &ModeSpec,
    options: &DecodeOptions,
    outer_pass: usize,
    parity: &ParityMatrix,
    allow_ap: bool,
    ft4_ap_known_bits: &[Vec<Option<u8>>],
    resolver: &HashResolver,
    seen_messages: &HashSet<String>,
    counters: &mut DecodeCounters,
) -> Vec<SuccessfulDecode> {
    let coarse_freq_hz = candidate.freq_hz;
    if coarse_freq_hz < options.min_freq_hz || coarse_freq_hz > options.max_freq_hz {
        return Vec::new();
    }
    let Some(initial_baseband) =
        downsample_candidate_ft4_search(long_spectrum, baseband_plan, spec, coarse_freq_hz)
    else {
        return Vec::new();
    };
    let mut refined_basebands = Vec::<(i32, Vec<Complex32>)>::new();
    let refined_candidates = refine_candidate_ft4_variants_with_cache(
        long_spectrum,
        baseband_plan,
        spec,
        &initial_baseband,
        &mut refined_basebands,
        coarse_freq_hz,
    );
    let max_osd = options.max_osd_passes(outer_pass, coarse_freq_hz);
    for refined in refined_candidates {
        if let Some(success) = try_refined_candidate(
            &refined,
            options.mode,
            candidate.score,
            max_osd,
            allow_ap,
            ft4_ap_known_bits,
            resolver,
            seen_messages,
            parity,
            counters,
        ) {
            return vec![success];
        }
    }
    Vec::new()
}

fn ft4_refinement_is_latched(coarse: &DecodeCandidate, refined: &SuccessfulDecode) -> bool {
    (refined.candidate.freq_hz - coarse.freq_hz).abs() <= 4.5
        && (refined.candidate.dt_seconds - coarse.dt_seconds).abs() <= 0.03
}

pub(super) fn try_refined_candidate(
    refined: &RefinedCandidate,
    mode: Mode,
    candidate_score: f32,
    max_osd: isize,
    allow_ap: bool,
    ft4_ap_known_bits: &[Vec<Option<u8>>],
    _resolver: &HashResolver,
    _seen_messages: &HashSet<String>,
    parity: &ParityMatrix,
    counters: &mut DecodeCounters,
) -> Option<SuccessfulDecode> {
    for llrs in &refined.llr_sets {
        let Some((payload, bits, iterations)) =
            decode_llr_set(mode, parity, llrs, max_osd, counters)
        else {
            continue;
        };
        let success =
            build_successful_decode(refined, mode, candidate_score, payload, bits, iterations);
        return Some(success);
    }

    if allow_ap {
        let ap_llrs = if mode == Mode::Ft4 {
            &refined.llr_sets[2]
        } else {
            &refined.llr_sets[0]
        };
        let ap_magnitude_source = if mode == Mode::Ft4 {
            &refined.llr_sets[0]
        } else {
            ap_llrs
        };
        let ap_magnitude_scale = if mode == Mode::Ft4 { 1.1 } else { 1.01 };
        let ap_magnitude = ap_magnitude_source
            .iter()
            .map(|value| value.abs())
            .fold(0.0f32, f32::max)
            * ap_magnitude_scale;
        if ap_magnitude > 0.0 {
            let known_bit_sets: Vec<&[Option<u8>]> = if mode == Mode::Ft4 {
                std::iter::once(cq_ap_known_bits(mode))
                    .chain(ft4_ap_known_bits.iter().map(|bits| bits.as_slice()))
                    .collect()
            } else {
                vec![cq_ap_known_bits(mode), mycall_ap_known_bits(mode)]
            };
            for known_bits in known_bit_sets {
                let ap_llrs = llrs_with_known_bits(ap_llrs, known_bits, ap_magnitude);
                if let Some((payload, bits, iterations)) = decode_llr_set_with_known_bits(
                    mode, parity, &ap_llrs, known_bits, max_osd, counters,
                ) {
                    let success = build_successful_decode(
                        refined,
                        mode,
                        candidate_score,
                        payload,
                        bits,
                        iterations,
                    );
                    return Some(success);
                }
            }
        }
    }

    None
}

fn build_successful_decode(
    refined: &RefinedCandidate,
    mode: Mode,
    candidate_score: f32,
    payload: Payload,
    bits: Vec<u8>,
    iterations: usize,
) -> SuccessfulDecode {
    SuccessfulDecode {
        mode,
        payload,
        codeword_bits: bits,
        candidate: DecodeCandidate {
            start_seconds: refined.start_seconds,
            dt_seconds: mode.spec().dt_seconds_from_start(refined.start_seconds),
            freq_hz: refined.freq_hz,
            score: refined.sync_score.max(candidate_score),
        },
        ldpc_iterations: iterations,
        snr_db: refined.snr_db,
    }
}

fn current_seen_message_texts(
    base_resolver: Option<&HashResolver>,
    existing: &[SuccessfulDecode],
    current_pass: &[SuccessfulDecode],
) -> (HashResolver, HashSet<String>) {
    let mut combined = Vec::with_capacity(existing.len() + current_pass.len());
    combined.extend(existing.iter().cloned());
    combined.extend(current_pass.iter().cloned());
    let resolver = merged_resolver(base_resolver, &combined);
    let seen = combined
        .iter()
        .map(|success| rendered_success_key(success, &resolver))
        .collect();
    (resolver, seen)
}

fn success_is_duplicate(
    success: &SuccessfulDecode,
    resolver: &HashResolver,
    seen_messages: &HashSet<String>,
) -> bool {
    seen_messages.contains(&success.payload.to_message(resolver).to_text())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::ReplyWord;
    use crate::message::hash_callsign;
    use crate::message::unpack_message_for_mode;

    #[test]
    fn merged_resolver_collects_callsigns_from_successes() {
        let frame = crate::encode::encode_nonstandard_message(
            "K1ABC",
            "HF19NY",
            false,
            ReplyWord::Blank,
            true,
        )
        .expect("encode frame");
        let payload = unpack_message_for_mode(Mode::Ft8, &frame.codeword_bits)
            .expect("payload")
            .clone();
        let resolver = merged_resolver(
            None,
            &[SuccessfulDecode {
                mode: Mode::Ft8,
                payload,
                codeword_bits: frame.codeword_bits.to_vec(),
                candidate: DecodeCandidate {
                    start_seconds: Mode::Ft8.spec().nominal_start_seconds(),
                    dt_seconds: 0.0,
                    freq_hz: 1_200.0,
                    score: 1.0,
                },
                ldpc_iterations: 0,
                snr_db: 0,
            }],
        );

        assert_eq!(
            resolver.resolve12(hash_callsign("HF19NY", 12) as u16),
            Some("HF19NY")
        );
    }
}
