use std::collections::HashSet;

use super::*;
use crate::encode::encode_standard_message_for_mode;
use crate::message::{
    CallField, GridReport, Payload, ReplyWord, StructuredCallField, StructuredCallValue,
    StructuredInfoValue, StructuredMessage,
};
use crate::protocol::{BitField, copy_known_message_bits};

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
    let mut seen = HashSet::<String>::new();
    for success in existing.iter().chain(current_pass.iter()) {
        seen.insert(rendered_success_key(success, &resolver));
    }
    !seen.contains(&candidate_key)
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
    if !options.disable_subtraction && let Some(stage41) = early41 {
        for success in &stage41.successes {
            if success.candidate.dt_seconds < spec.tuning.subtraction_refine_cutoff_seconds {
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
    let prepared_full = (!options.disable_subtraction && !initial_successes.is_empty()).then(|| {
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
    if options.mode == Mode::Ft4 {
        decodes = apply_ft4_stock_compat_filters(decodes);
    }
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

fn apply_ft4_stock_compat_filters(decodes: Vec<DecodedMessage>) -> Vec<DecodedMessage> {
    let mut keep = vec![true; decodes.len()];

    for left in 0..decodes.len() {
        if !keep[left] {
            continue;
        }
        for right in (left + 1)..decodes.len() {
            if !keep[right] {
                continue;
            }
            if ft4_same_pair_nearby_conflict(&decodes[left], &decodes[right]) {
                let left_loses = ft4_left_should_lose(&decodes[left], &decodes[right]);
                keep[if left_loses { left } else { right }] = false;
            }
        }
    }

    for index in 0..decodes.len() {
        if !keep[index] || !is_ft4_marginal_seventy_three(&decodes[index]) {
            continue;
        }
        let has_same_pair_context = decodes.iter().enumerate().any(|(other_index, other)| {
            keep[other_index]
                && other_index != index
                && ft4_same_standard_pair(&decodes[index].message, &other.message)
        });
        if has_same_pair_context {
            keep[index] = false;
        }
    }

    decodes
        .into_iter()
        .zip(keep)
        .filter_map(|(decode, keep)| keep.then_some(decode))
        .collect()
}

fn ft4_left_should_lose(left: &DecodedMessage, right: &DecodedMessage) -> bool {
    right
        .candidate_score
        .total_cmp(&left.candidate_score)
        .then_with(|| left.ldpc_iterations.cmp(&right.ldpc_iterations))
        .then_with(|| left.freq_hz.total_cmp(&right.freq_hz))
        .is_gt()
}

fn ft4_same_pair_nearby_conflict(left: &DecodedMessage, right: &DecodedMessage) -> bool {
    ft4_same_standard_pair(&left.message, &right.message)
        && (left.dt_seconds - right.dt_seconds).abs() <= 0.12
        && (left.freq_hz - right.freq_hz).abs() <= 5.0
}

fn ft4_same_standard_pair(left: &StructuredMessage, right: &StructuredMessage) -> bool {
    let StructuredMessage::Standard {
        first: left_first,
        second: left_second,
        ..
    } = left
    else {
        return false;
    };
    let StructuredMessage::Standard {
        first: right_first,
        second: right_second,
        ..
    } = right
    else {
        return false;
    };
    standard_call(left_first) == standard_call(right_first)
        && standard_call(left_second) == standard_call(right_second)
}

fn standard_call(field: &StructuredCallField) -> Option<&str> {
    match &field.value {
        StructuredCallValue::StandardCall { callsign } => Some(callsign.as_str()),
        _ => None,
    }
}

fn is_ft4_marginal_seventy_three(decode: &DecodedMessage) -> bool {
    let StructuredMessage::Standard { info, .. } = &decode.message else {
        return false;
    };
    matches!(
        info.value,
        StructuredInfoValue::Reply {
            word: ReplyWord::SeventyThree
        }
    ) && decode.ldpc_iterations > 0
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
    let spec = options.mode.spec();
    let baseband_plan = BasebandPlan::new(spec);
    let subtraction_plan = SubtractionPlan::for_mode(options.mode);
    let parity = ParityMatrix::global();
    let mut top_candidates = Vec::new();
    let mut counters = DecodeCounters::default();
    let search_grid = search_grid(audio, options);
    let frame_count = search_grid.frame_count;
    let usable_bins = search_grid.usable_bins;

    let mut successes = initial_successes;
    if !options.disable_subtraction && !has_residual_override {
        for success in &successes {
            subtract_candidate(&mut residual_audio, success, subtraction_plan);
        }
    }

    for pass in 0..total_passes {
        let long_spectrum = build_long_spectrum(&residual_audio, spec);
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
            let ft4_ap_known_bits = ft4_context_ap_known_bits(&successes, &pass_successes);
            let (resolver, seen_messages) =
                current_seen_message_texts(base_resolver, &successes, &pass_successes);
            let success = try_candidate(
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
            if let Some(success) = success {
                pass_changed = true;
                if !options.disable_subtraction {
                    subtract_candidate(&mut residual_audio, &success, subtraction_plan);
                }
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
    spec: &ModeSpec,
    options: &DecodeOptions,
    outer_pass: usize,
    parity: &ParityMatrix,
    allow_ap: bool,
    ft4_ap_known_bits: &[Vec<Option<u8>>],
    resolver: &HashResolver,
    seen_messages: &HashSet<String>,
    counters: &mut DecodeCounters,
) -> Option<SuccessfulDecode> {
    let mut best: Option<SuccessfulDecode> = None;
    let mut refined_basebands = Vec::<(i32, Vec<Complex32>)>::new();
    let coarse_freq_hz = candidate.freq_hz;
    if coarse_freq_hz < options.min_freq_hz || coarse_freq_hz > options.max_freq_hz {
        return None;
    }
    let Some(initial_baseband) =
        downsample_candidate(long_spectrum, baseband_plan, spec, coarse_freq_hz)
    else {
        return None;
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
            && best
                .as_ref()
                .is_some_and(|success| !ft4_refinement_is_latched(candidate, success))
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
    if best.is_none() && options.mode == Mode::Ft4 {
        best = try_ft4_local_rescue(
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
    best
}

fn ft4_refinement_is_latched(coarse: &DecodeCandidate, refined: &SuccessfulDecode) -> bool {
    (refined.candidate.freq_hz - coarse.freq_hz).abs() <= 4.5
        && (refined.candidate.dt_seconds - coarse.dt_seconds).abs() <= 0.03
}

fn try_ft4_local_rescue(
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
) -> Option<SuccessfulDecode> {
    let hop_seconds = spec.geometry.hop_samples as f32 / spec.geometry.sample_rate_hz as f32;
    let freq_probe_hz = spec.geometry.tone_spacing_hz / 8.0;
    let max_osd = options.max_osd_passes(outer_pass, candidate.freq_hz);

    for start_offset in [hop_seconds, 0.0, -hop_seconds] {
        for freq_offset in [-freq_probe_hz, freq_probe_hz, -2.0 * freq_probe_hz, 2.0 * freq_probe_hz]
        {
            let start_seconds = candidate.start_seconds + start_offset;
            let freq_hz = candidate.freq_hz + freq_offset;
            for refined in [
                extract_candidate_at(long_spectrum, baseband_plan, spec, start_seconds, freq_hz),
                extract_candidate_at_relaxed(
                    long_spectrum,
                    baseband_plan,
                    spec,
                    start_seconds,
                    freq_hz,
                ),
            ]
            .into_iter()
            .flatten()
            {
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
                    return Some(success);
                }
            }
        }
    }

    None
}

pub(super) fn try_refined_candidate(
    refined: &RefinedCandidate,
    mode: Mode,
    candidate_score: f32,
    max_osd: isize,
    allow_ap: bool,
    ft4_ap_known_bits: &[Vec<Option<u8>>],
    resolver: &HashResolver,
    seen_messages: &HashSet<String>,
    parity: &ParityMatrix,
    counters: &mut DecodeCounters,
) -> Option<SuccessfulDecode> {
    let mut duplicate_fallback = None;
    for llrs in &refined.llr_sets {
        let Some((payload, bits, iterations)) =
            decode_llr_set(mode, parity, llrs, max_osd, counters)
        else {
            continue;
        };
        let success = build_successful_decode(
            refined,
            mode,
            candidate_score,
            payload,
            bits,
            iterations,
        );
        if mode == Mode::Ft4
            && seen_messages.contains(&success.payload.to_message(resolver).to_text())
        {
            duplicate_fallback.get_or_insert(success);
            continue;
        }
        return Some(success);
    }

    if allow_ap {
        let ap_llrs = if mode == Mode::Ft4 {
            &refined.llr_sets[2]
        } else {
            &refined.llr_sets[0]
        };
        let ap_magnitude = ap_llrs
            .iter()
            .map(|value| value.abs())
            .fold(0.0f32, f32::max)
            * 1.01;
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
                if let Some((payload, bits, iterations)) =
                    decode_llr_set_with_known_bits(
                        mode, parity, &ap_llrs, known_bits, max_osd, counters,
                    )
                {
                    let success = build_successful_decode(
                        refined,
                        mode,
                        candidate_score,
                        payload,
                        bits,
                        iterations,
                    );
                    if mode == Mode::Ft4
                        && seen_messages.contains(&success.payload.to_message(resolver).to_text())
                    {
                        duplicate_fallback.get_or_insert(success);
                        continue;
                    }
                    return Some(success);
                }
            }
        }
    }

    duplicate_fallback
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

fn ft4_context_ap_known_bits(
    existing: &[SuccessfulDecode],
    current_pass: &[SuccessfulDecode],
) -> Vec<Vec<Option<u8>>> {
    let mut contexts = Vec::<Vec<Option<u8>>>::new();
    let mut seen = HashSet::<Vec<Option<u8>>>::new();

    for success in existing.iter().chain(current_pass.iter()) {
        let Payload::Standard(message) = &success.payload else {
            continue;
        };
        let (CallField::Standard(first), CallField::Standard(second)) = (&message.first, &message.second)
        else {
            continue;
        };

        push_ft4_ap_context(
            &mut contexts,
            &mut seen,
            build_ft4_standard_ap_bits(
                first,
                second,
                &GridReport::Blank,
                &[BitField { start: 0, len: 58 }],
            ),
        );
        for reply in [ReplyWord::Rrr, ReplyWord::SeventyThree, ReplyWord::Rr73] {
            push_ft4_ap_context(
                &mut contexts,
                &mut seen,
                build_ft4_standard_ap_bits(
                    first,
                    second,
                    &GridReport::Reply(reply),
                    &[BitField { start: 0, len: 77 }],
                ),
            );
        }
    }

    contexts
}

fn push_ft4_ap_context(
    contexts: &mut Vec<Vec<Option<u8>>>,
    seen: &mut HashSet<Vec<Option<u8>>>,
    known_bits: Option<Vec<Option<u8>>>,
) {
    let Some(known_bits) = known_bits else {
        return;
    };
    if seen.insert(known_bits.clone()) {
        contexts.push(known_bits);
    }
}

fn build_ft4_standard_ap_bits(
    first: &str,
    second: &str,
    info: &GridReport,
    fields: &[BitField],
) -> Option<Vec<Option<u8>>> {
    let frame = encode_standard_message_for_mode(Mode::Ft4, first, second, false, info).ok()?;
    let mut known = vec![None; Mode::Ft4.spec().coding.codeword_bits];
    copy_known_message_bits(&mut known, &frame.message_bits, fields)?;
    Some(known)
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
