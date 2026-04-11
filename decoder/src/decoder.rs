use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::{Arc, OnceLock};

use num_complex::Complex32;
use realfft::{RealFftPlanner, RealToComplex};
use rustfft::{Fft, FftPlanner};
use serde::Serialize;

use crate::encode::channel_symbols_from_codeword_bits_for_mode;
use crate::ldpc::{LdpcDebugState, ParityMatrix};
use crate::message::{
    GridReport, HashResolver, Payload, StructuredMessage, unpack_message_for_mode,
};
use crate::modes::{Mode, ModeSpec, all_costas_positions};
use crate::protocol::{BitField, copy_known_message_bits};
use crate::wave::{AudioBuffer, DecoderError, load_wav};

#[cfg(test)]
use crate::modes::ft8::FT8_SAMPLE_RATE;

mod ft2;
mod metrics;
mod refine;
mod search;
mod session;
mod subtract;

use ft2::*;
use metrics::*;
use refine::*;
use search::*;
use subtract::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum DecodeProfile {
    Quick,
    Medium,
    Deepest,
}

impl DecodeProfile {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Quick => "quick",
            Self::Medium => "medium",
            Self::Deepest => "deepest",
        }
    }
}

impl Default for DecodeProfile {
    fn default() -> Self {
        Self::Medium
    }
}

impl std::str::FromStr for DecodeProfile {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "quick" => Ok(Self::Quick),
            "medium" => Ok(Self::Medium),
            "deep" | "deepest" => Ok(Self::Deepest),
            other => Err(format!(
                "unsupported profile '{other}'; expected quick, medium, or deepest"
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DecodeOptions {
    pub mode: Mode,
    pub profile: DecodeProfile,
    pub min_freq_hz: f32,
    pub max_freq_hz: f32,
    pub max_candidates: usize,
    pub max_successes: usize,
    pub search_passes: usize,
    pub target_freq_hz: f32,
    pub tx_freq_hz: f32,
    pub ap_width_hz: f32,
    pub disable_subtraction: bool,
}

impl Default for DecodeOptions {
    fn default() -> Self {
        Self::for_mode(Mode::Ft8)
    }
}

impl DecodeOptions {
    pub fn for_mode(mode: Mode) -> Self {
        Self {
            mode,
            profile: DecodeProfile::Medium,
            min_freq_hz: 200.0,
            max_freq_hz: 4_000.0,
            max_candidates: 600,
            max_successes: 200,
            search_passes: match mode {
                Mode::Ft4 => 2,
                Mode::Ft8 | Mode::Ft2 => 3,
            },
            target_freq_hz: mode.spec().search.nfqso_hz,
            tx_freq_hz: mode.spec().search.nfqso_hz,
            ap_width_hz: 75.0,
            disable_subtraction: false,
        }
    }
    fn uses_early_decodes(&self) -> bool {
        self.mode == Mode::Ft8 && !matches!(self.profile, DecodeProfile::Quick)
    }

    fn sync_threshold(&self) -> f32 {
        if matches!(self.profile, DecodeProfile::Deepest) && self.mode != Mode::Ft4 {
            1.3
        } else {
            self.mode.spec().search.sync_threshold
        }
    }

    fn max_osd_passes(&self, outer_pass: usize, freq_hz: f32) -> isize {
        match (self.mode, self.profile) {
            (_, DecodeProfile::Quick) => -1,
            // Stock FT4 medium disables OSD entirely (`maxosd = -1`).
            (Mode::Ft4, DecodeProfile::Medium) => -1,
            (_, DecodeProfile::Medium) => 0,
            (Mode::Ft4, DecodeProfile::Deepest) => {
                let near_qso = (freq_hz - self.target_freq_hz).abs() <= self.ap_width_hz.max(80.0);
                if near_qso { 3 } else { 2 }
            }
            (_, DecodeProfile::Deepest) => {
                if outer_pass == 0 {
                    0
                } else {
                    3
                }
            }
        }
    }

    fn max_candidates_for_pass(&self, _outer_pass: usize) -> usize {
        self.max_candidates
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DecodeCandidate {
    pub start_seconds: f32,
    pub dt_seconds: f32,
    pub freq_hz: f32,
    pub score: f32,
}

#[derive(Debug, Clone, Serialize)]
pub struct DecodedMessage {
    pub utc: String,
    pub snr_db: i32,
    pub dt_seconds: f32,
    pub freq_hz: f32,
    pub text: String,
    pub candidate_score: f32,
    pub ldpc_iterations: usize,
    pub message: StructuredMessage,
}

#[derive(Debug, Clone, Serialize)]
pub struct DecodeDiagnostics {
    pub frame_count: usize,
    pub usable_bins: usize,
    pub examined_candidates: usize,
    pub accepted_candidates: usize,
    pub ldpc_codewords: usize,
    pub parsed_payloads: usize,
    pub top_candidates: Vec<DecodeCandidate>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DecodeReport {
    pub sample_rate_hz: u32,
    pub duration_seconds: f32,
    pub decodes: Vec<DecodedMessage>,
    pub diagnostics: DecodeDiagnostics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum DecodeStage {
    Early41,
    Early47,
    Full,
}

impl DecodeStage {
    pub const fn ordered() -> [Self; 3] {
        [Self::Early41, Self::Early47, Self::Full]
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Early41 => "early41",
            Self::Early47 => "early47",
            Self::Full => "full",
        }
    }

    pub fn required_samples(self, spec: &ModeSpec) -> usize {
        match self {
            Self::Early41 => spec.early41_samples(),
            Self::Early47 => spec.early47_samples(),
            Self::Full => long_input_samples(spec),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct StageDecodeReport {
    pub stage: DecodeStage,
    pub report: DecodeReport,
    pub new_decodes: Vec<DecodedMessage>,
    pub updated_decodes: Vec<DecodedMessage>,
}

#[derive(Debug, Default, Clone)]
pub struct DecoderState {
    resolver: HashResolver,
}

impl DecoderState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset(&mut self) {
        *self = Self::default();
    }

    pub fn insert_callsign(&mut self, callsign: &str) {
        self.resolver.insert_callsign(callsign);
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CandidatePassDebug {
    pub pass_name: String,
    pub mean_abs_llr: f32,
    pub max_abs_llr: f32,
    pub decoded_text: Option<String>,
    pub ldpc_iterations: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truth_hard_errors: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truth_weighted_distance: Option<f32>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CandidateDebugReport {
    pub coarse_start_seconds: f32,
    pub coarse_dt_seconds: f32,
    pub coarse_freq_hz: f32,
    pub refined_start_seconds: f32,
    pub refined_dt_seconds: f32,
    pub refined_freq_hz: f32,
    pub sync_score: f32,
    pub snr_db: i32,
    pub passes: Vec<CandidatePassDebug>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Ft4VariantDebug {
    pub refined_start_seconds: f32,
    pub refined_dt_seconds: f32,
    pub refined_freq_hz: f32,
    pub sync_score: f32,
    pub snr_db: i32,
    pub decoded_text: Option<String>,
    pub ldpc_iterations: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Ft4MetricsDebug {
    pub coarse_start_seconds: f32,
    pub coarse_dt_seconds: f32,
    pub coarse_freq_hz: f32,
    pub refined_start_seconds: f32,
    pub refined_dt_seconds: f32,
    pub refined_freq_hz: f32,
    pub sync_score: f32,
    pub snr_db: i32,
    pub llr_sets: [Vec<f32>; 4],
    pub symbol_bit_llrs: Vec<f32>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Ft4Decode174Debug {
    pub coarse_start_seconds: f32,
    pub coarse_dt_seconds: f32,
    pub coarse_freq_hz: f32,
    pub refined_start_seconds: f32,
    pub refined_dt_seconds: f32,
    pub refined_freq_hz: f32,
    pub sync_score: f32,
    pub snr_db: i32,
    pub llr_set_index: usize,
    pub ldpc: LdpcDebugState,
}

#[derive(Debug, Clone, Serialize)]
pub struct Ft4SearchProbeBin {
    pub bin: usize,
    pub freq_hz: f32,
    pub savg: f32,
    pub sbase: f32,
    pub savsm: f32,
}

#[derive(Debug, Clone, Serialize)]
pub struct Ft4SearchProbeDebug {
    pub target_freq_hz: f32,
    pub df_hz: f32,
    pub f_offset_hz: f32,
    pub target_bin: usize,
    pub candidate_bin: usize,
    pub candidate_freq_hz: f32,
    pub candidate_score: f32,
    pub del: f32,
    pub probe_bins: Vec<Ft4SearchProbeBin>,
    pub top_candidates: Vec<DecodeCandidate>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SearchAcceptedTrace {
    pub text: String,
    pub dt_seconds: f32,
    pub freq_hz: f32,
    pub codeword_bits: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SearchCandidateTrace {
    pub coarse_start_seconds: f32,
    pub coarse_dt_seconds: f32,
    pub coarse_freq_hz: f32,
    pub coarse_score: f32,
    pub raw_successes: Vec<String>,
    pub accepted_successes: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub accepted_subtractions: Vec<SearchAcceptedTrace>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SearchResidualSignature {
    pub sample_sum: f64,
    pub sample_sq_sum: f64,
    pub probe_values: Vec<f32>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SearchPassTrace {
    pub pass_index: usize,
    pub candidates: Vec<SearchCandidateTrace>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub residual_signature: Option<SearchResidualSignature>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SearchDebugReport {
    pub sample_rate_hz: u32,
    pub duration_seconds: f32,
    pub passes: Vec<SearchPassTrace>,
    pub final_report: DecodeReport,
}

pub use ft2::{Ft2CandidateTrace, Ft2SequenceTrace, Ft2TraceReport};

#[derive(Debug, Clone)]
struct SuccessfulDecode {
    mode: Mode,
    payload: Payload,
    codeword_bits: Vec<u8>,
    candidate: DecodeCandidate,
    ldpc_iterations: usize,
    snr_db: i32,
}

#[derive(Debug)]
struct RefinedCandidate {
    start_seconds: f32,
    freq_hz: f32,
    sync_score: f32,
    llr_sets: [Vec<f32>; 4],
    symbol_bit_llrs: Vec<f32>,
    snr_db: i32,
}

#[derive(Debug, Default, Clone)]
struct DecodeCounters {
    ldpc_codewords: usize,
    parsed_payloads: usize,
}

#[derive(Debug, Clone)]
struct SearchResult {
    successes: Vec<SuccessfulDecode>,
    frame_count: usize,
    usable_bins: usize,
    top_candidates: Vec<DecodeCandidate>,
    counters: DecodeCounters,
}

#[derive(Debug, Clone, Copy)]
struct SearchGrid {
    frame_count: usize,
    usable_bins: usize,
    min_bin: usize,
}

#[derive(Debug, Default, Clone)]
pub struct DecoderSession {
    early41: Option<SearchResult>,
    early47: Option<SearchResult>,
    emitted_stages: Vec<DecodeStage>,
    last_decodes: BTreeMap<String, DecodedMessage>,
}

#[derive(Debug)]
struct Spectrogram {
    bins: Vec<f32>,
    frame_count: usize,
    usable_bins: usize,
    min_bin: usize,
}

#[derive(Debug)]
struct LongSpectrum {
    bins: Vec<Complex32>,
}

struct LongSpectrumPlan {
    forward: Arc<dyn RealToComplex<f32>>,
}

struct Sync8Plan {
    forward: Arc<dyn RealToComplex<f32>>,
}

struct BasebandPlan {
    inverse: Arc<dyn Fft<f32>>,
}

struct SubtractionPlan {
    spec: &'static ModeSpec,
    forward: Arc<dyn Fft<f32>>,
    inverse: Arc<dyn Fft<f32>>,
    filter_spectrum: Vec<Complex32>,
    edge_correction: Vec<f32>,
}

fn long_input_samples(spec: &ModeSpec) -> usize {
    spec.search.long_input_samples
}

fn stock_window_audio<'a>(audio: &'a AudioBuffer, mode: Mode) -> Cow<'a, AudioBuffer> {
    if mode == Mode::Ft8 {
        return Cow::Borrowed(audio);
    }
    let max_samples = mode.spec().search.long_input_samples;
    if audio.samples.len() <= max_samples {
        Cow::Borrowed(audio)
    } else {
        let mut clipped = audio.clone();
        clipped.samples.truncate(max_samples);
        Cow::Owned(clipped)
    }
}

fn sync8_early_threshold(spec: &ModeSpec) -> f32 {
    spec.search.sync_early_threshold
}

fn early_41_samples(spec: &ModeSpec) -> usize {
    spec.early41_samples()
}

fn early_47_samples(spec: &ModeSpec) -> usize {
    spec.early47_samples()
}
pub fn decode_wav_file(
    path: impl AsRef<Path>,
    options: &DecodeOptions,
) -> Result<DecodeReport, DecoderError> {
    let audio = load_wav(path)?;
    decode_pcm(&audio, options)
}

pub fn decode_wav_file_with_state(
    path: impl AsRef<Path>,
    options: &DecodeOptions,
    state: Option<&DecoderState>,
) -> Result<(DecodeReport, DecoderState), DecoderError> {
    let audio = load_wav(path)?;
    decode_pcm_with_state(&audio, options, state)
}

pub fn debug_candidate_wav_file(
    path: impl AsRef<Path>,
    mode: Mode,
    dt_seconds: f32,
    freq_hz: f32,
) -> Result<Option<CandidateDebugReport>, DecoderError> {
    let audio = load_wav(path)?;
    Ok(debug_candidate_pcm(&audio, mode, dt_seconds, freq_hz))
}

pub fn debug_candidate_truth_wav_file(
    path: impl AsRef<Path>,
    mode: Mode,
    dt_seconds: f32,
    freq_hz: f32,
    truth_codeword_bits: &[u8],
) -> Result<Option<CandidateDebugReport>, DecoderError> {
    let audio = load_wav(path)?;
    let prepared = stock_window_audio(&audio, mode);
    Ok(debug_candidate_pcm_inner(
        &prepared,
        mode,
        dt_seconds,
        freq_hz,
        Some(truth_codeword_bits),
    ))
}

pub fn subtract_truth_wav_file(
    path: impl AsRef<Path>,
    mode: Mode,
    dt_seconds: f32,
    freq_hz: f32,
    truth_codeword_bits: &[u8],
) -> Result<Option<AudioBuffer>, DecoderError> {
    let audio = load_wav(path)?;
    Ok(subtract_truth_pcm(
        &audio,
        mode,
        dt_seconds,
        freq_hz,
        truth_codeword_bits,
    ))
}

pub fn debug_candidate_pcm(
    audio: &AudioBuffer,
    mode: Mode,
    dt_seconds: f32,
    freq_hz: f32,
) -> Option<CandidateDebugReport> {
    let prepared = stock_window_audio(audio, mode);
    debug_candidate_pcm_inner(&prepared, mode, dt_seconds, freq_hz, None)
}

fn debug_candidate_pcm_inner(
    audio: &AudioBuffer,
    mode: Mode,
    dt_seconds: f32,
    freq_hz: f32,
    truth_codeword_bits: Option<&[u8]>,
) -> Option<CandidateDebugReport> {
    let spec = mode.spec();
    if audio.sample_rate_hz != spec.geometry.sample_rate_hz
        || audio.samples.len() < spec.geometry.symbol_samples
    {
        return None;
    }

    let long_spectrum = build_long_spectrum(audio, spec);
    let baseband_plan = BasebandPlan::new(spec);
    let refined = refine_candidate(
        &long_spectrum,
        &baseband_plan,
        spec,
        spec.start_seconds_from_dt(dt_seconds),
        freq_hz,
    )
    .or_else(|| {
        extract_candidate_at_relaxed(
            &long_spectrum,
            &baseband_plan,
            spec,
            spec.start_seconds_from_dt(dt_seconds),
            freq_hz,
        )
    })?;
    let parity = ParityMatrix::global();
    let mut counters = DecodeCounters::default();
    let mut passes = Vec::new();
    let resolver = HashResolver::default();

    append_debug_passes(
        &mut passes,
        "regular",
        mode,
        &refined.llr_sets,
        parity,
        &mut counters,
        &resolver,
        0,
        truth_codeword_bits,
    );
    append_debug_passes(
        &mut passes,
        "regular-osd2",
        mode,
        &refined.llr_sets,
        parity,
        &mut counters,
        &resolver,
        2,
        truth_codeword_bits,
    );
    append_debug_passes(
        &mut passes,
        "regular-osd3",
        mode,
        &refined.llr_sets,
        parity,
        &mut counters,
        &resolver,
        3,
        truth_codeword_bits,
    );
    append_debug_single_pass(
        &mut passes,
        "symbol",
        mode,
        &refined.symbol_bit_llrs,
        parity,
        &mut counters,
        &resolver,
        0,
        truth_codeword_bits,
    );
    append_debug_single_pass(
        &mut passes,
        "symbol-osd2",
        mode,
        &refined.symbol_bit_llrs,
        parity,
        &mut counters,
        &resolver,
        2,
        truth_codeword_bits,
    );
    append_debug_single_pass(
        &mut passes,
        "symbol-osd3",
        mode,
        &refined.symbol_bit_llrs,
        parity,
        &mut counters,
        &resolver,
        3,
        truth_codeword_bits,
    );

    if let Some(seed) = extract_candidate_at(
        &long_spectrum,
        &baseband_plan,
        spec,
        spec.start_seconds_from_dt(dt_seconds),
        freq_hz,
    ) {
        append_debug_passes(
            &mut passes,
            "seed",
            mode,
            &seed.llr_sets,
            parity,
            &mut counters,
            &resolver,
            0,
            truth_codeword_bits,
        );
        append_debug_passes(
            &mut passes,
            "seed-osd2",
            mode,
            &seed.llr_sets,
            parity,
            &mut counters,
            &resolver,
            2,
            truth_codeword_bits,
        );
        append_debug_passes(
            &mut passes,
            "seed-osd3",
            mode,
            &seed.llr_sets,
            parity,
            &mut counters,
            &resolver,
            3,
            truth_codeword_bits,
        );
    }

    let ap_magnitude = refined.llr_sets[0]
        .iter()
        .map(|value| value.abs())
        .fold(0.0f32, f32::max)
        * 1.01;
    if ap_magnitude > 0.0 {
        for (name, known_bits) in [
            ("ap-cq", cq_ap_known_bits(mode)),
            ("ap-mycall", mycall_ap_known_bits(mode)),
        ] {
            for (suffix, max_osd) in [("", 0), ("-osd2", 2), ("-osd3", 3)] {
                let llrs = llrs_with_known_bits(&refined.llr_sets[0], known_bits, ap_magnitude);
                let mean_abs_llr =
                    llrs.iter().map(|value| value.abs()).sum::<f32>() / llrs.len() as f32;
                let max_abs_llr = llrs.iter().map(|value| value.abs()).fold(0.0f32, f32::max);
                let decoded = decode_llr_set_with_known_bits(
                    mode,
                    parity,
                    &llrs,
                    known_bits,
                    max_osd,
                    &mut counters,
                )
                .map(|(payload, _, iterations)| {
                    let message = payload.to_message(&resolver);
                    (message.to_text(), iterations)
                });
                let (truth_hard_errors, truth_weighted_distance) = truth_codeword_bits
                    .and_then(|truth| truth_metrics(spec, &llrs, truth))
                    .unwrap_or((None, None));
                passes.push(CandidatePassDebug {
                    pass_name: format!("{name}{suffix}"),
                    mean_abs_llr,
                    max_abs_llr,
                    decoded_text: decoded.as_ref().map(|(text, _)| text.clone()),
                    ldpc_iterations: decoded.map(|(_, iterations)| iterations),
                    truth_hard_errors,
                    truth_weighted_distance,
                });
            }
        }

        if mode == Mode::Ft4
            && let Some(truth) = truth_codeword_bits
        {
            let mut truth_known = vec![None; mode.spec().coding.codeword_bits];
            let truth_message_bits = &truth[..crate::protocol::FTX_MESSAGE_BITS];
            copy_known_message_bits(
                &mut truth_known,
                truth_message_bits,
                &[BitField {
                    start: 0,
                    len: crate::protocol::FTX_MESSAGE_BITS,
                }],
            )
            .expect("copy FT4 truth AP bits");
            let llrs = llrs_with_known_bits(&refined.llr_sets[2], &truth_known, ap_magnitude);
            let mean_abs_llr =
                llrs.iter().map(|value| value.abs()).sum::<f32>() / llrs.len() as f32;
            let max_abs_llr = llrs.iter().map(|value| value.abs()).fold(0.0f32, f32::max);
            let decoded =
                decode_llr_set_with_known_bits(mode, parity, &llrs, &truth_known, 0, &mut counters)
                    .map(|(payload, _, iterations)| {
                        let message = payload.to_message(&resolver);
                        (message.to_text(), iterations)
                    });
            let (truth_hard_errors, truth_weighted_distance) =
                truth_metrics(spec, &llrs, truth).unwrap_or((None, None));
            passes.push(CandidatePassDebug {
                pass_name: "ap-truth77".to_string(),
                mean_abs_llr,
                max_abs_llr,
                decoded_text: decoded.as_ref().map(|(text, _)| text.clone()),
                ldpc_iterations: decoded.map(|(_, iterations)| iterations),
                truth_hard_errors,
                truth_weighted_distance,
            });
        }
    }

    Some(CandidateDebugReport {
        coarse_start_seconds: spec.start_seconds_from_dt(dt_seconds),
        coarse_dt_seconds: dt_seconds,
        coarse_freq_hz: freq_hz,
        refined_start_seconds: refined.start_seconds,
        refined_dt_seconds: spec.dt_seconds_from_start(refined.start_seconds),
        refined_freq_hz: refined.freq_hz,
        sync_score: refined.sync_score,
        snr_db: refined.snr_db,
        passes,
    })
}

pub fn debug_ft4_variants_wav_file(
    path: impl AsRef<Path>,
    freq_hz: f32,
) -> Result<Vec<Ft4VariantDebug>, DecoderError> {
    let audio = load_wav(path)?;
    Ok(debug_ft4_variants_pcm(&audio, freq_hz))
}

pub fn debug_ft4_variants_pcm(audio: &AudioBuffer, freq_hz: f32) -> Vec<Ft4VariantDebug> {
    let prepared = stock_window_audio(audio, Mode::Ft4);
    let audio = prepared.as_ref();
    let spec = Mode::Ft4.spec();
    if audio.sample_rate_hz != spec.geometry.sample_rate_hz
        || audio.samples.len() < spec.geometry.symbol_samples
    {
        return Vec::new();
    }

    let long_spectrum = build_long_spectrum(audio, spec);
    let baseband_plan = BasebandPlan::new(spec);
    let Some(initial_baseband) =
        downsample_candidate_ft4_search(&long_spectrum, &baseband_plan, spec, freq_hz)
    else {
        return Vec::new();
    };
    let mut refined_basebands = Vec::new();
    let parity = ParityMatrix::global();
    let mut counters = DecodeCounters::default();
    let resolver = HashResolver::default();
    refine_candidate_ft4_variants_with_cache(
        &long_spectrum,
        &baseband_plan,
        spec,
        &initial_baseband,
        &mut refined_basebands,
        freq_hz,
    )
    .into_iter()
    .map(|refined| {
        let decoded = session::try_refined_candidate(
            &refined,
            Mode::Ft4,
            refined.sync_score,
            0,
            false,
            &[],
            &resolver,
            &HashSet::new(),
            parity,
            &mut counters,
        );
        Ft4VariantDebug {
            refined_start_seconds: refined.start_seconds,
            refined_dt_seconds: spec.dt_seconds_from_start(refined.start_seconds),
            refined_freq_hz: refined.freq_hz,
            sync_score: refined.sync_score,
            snr_db: refined.snr_db,
            decoded_text: decoded
                .as_ref()
                .map(|success| success.payload.to_message(&resolver).to_text()),
            ldpc_iterations: decoded.map(|success| success.ldpc_iterations),
        }
    })
    .collect()
}

pub fn debug_ft4_metrics_wav_file(
    path: impl AsRef<Path>,
    dt_seconds: f32,
    freq_hz: f32,
) -> Result<Option<Ft4MetricsDebug>, DecoderError> {
    let audio = load_wav(path)?;
    Ok(debug_ft4_metrics_pcm(&audio, dt_seconds, freq_hz))
}

pub fn debug_ft4_decode174_wav_file(
    path: impl AsRef<Path>,
    dt_seconds: f32,
    freq_hz: f32,
    llr_set_index: usize,
    max_osd: isize,
    norder: usize,
) -> Result<Option<Ft4Decode174Debug>, DecoderError> {
    let audio = load_wav(path)?;
    Ok(debug_ft4_decode174_pcm(
        &audio,
        dt_seconds,
        freq_hz,
        llr_set_index,
        max_osd,
        norder,
    ))
}

pub fn debug_ft4_metrics_pcm(
    audio: &AudioBuffer,
    dt_seconds: f32,
    freq_hz: f32,
) -> Option<Ft4MetricsDebug> {
    let prepared = stock_window_audio(audio, Mode::Ft4);
    let audio = prepared.as_ref();
    let spec = Mode::Ft4.spec();
    if audio.sample_rate_hz != spec.geometry.sample_rate_hz
        || audio.samples.len() < spec.geometry.symbol_samples
    {
        return None;
    }

    let long_spectrum = build_long_spectrum(audio, spec);
    let baseband_plan = BasebandPlan::new(spec);
    let refined = refine_candidate(
        &long_spectrum,
        &baseband_plan,
        spec,
        spec.start_seconds_from_dt(dt_seconds),
        freq_hz,
    )
    .or_else(|| {
        extract_candidate_at_relaxed(
            &long_spectrum,
            &baseband_plan,
            spec,
            spec.start_seconds_from_dt(dt_seconds),
            freq_hz,
        )
    })?;

    Some(Ft4MetricsDebug {
        coarse_start_seconds: spec.start_seconds_from_dt(dt_seconds),
        coarse_dt_seconds: dt_seconds,
        coarse_freq_hz: freq_hz,
        refined_start_seconds: refined.start_seconds,
        refined_dt_seconds: spec.dt_seconds_from_start(refined.start_seconds),
        refined_freq_hz: refined.freq_hz,
        sync_score: refined.sync_score,
        snr_db: refined.snr_db,
        llr_sets: refined.llr_sets.clone(),
        symbol_bit_llrs: refined.symbol_bit_llrs.clone(),
    })
}

pub fn debug_ft4_decode174_pcm(
    audio: &AudioBuffer,
    dt_seconds: f32,
    freq_hz: f32,
    llr_set_index: usize,
    max_osd: isize,
    norder: usize,
) -> Option<Ft4Decode174Debug> {
    let prepared = stock_window_audio(audio, Mode::Ft4);
    let audio = prepared.as_ref();
    let spec = Mode::Ft4.spec();
    if audio.sample_rate_hz != spec.geometry.sample_rate_hz
        || audio.samples.len() < spec.geometry.symbol_samples
    {
        return None;
    }

    let long_spectrum = build_long_spectrum(audio, spec);
    let baseband_plan = BasebandPlan::new(spec);
    let refined = refine_candidate(
        &long_spectrum,
        &baseband_plan,
        spec,
        spec.start_seconds_from_dt(dt_seconds),
        freq_hz,
    )
    .or_else(|| {
        extract_candidate_at_relaxed(
            &long_spectrum,
            &baseband_plan,
            spec,
            spec.start_seconds_from_dt(dt_seconds),
            freq_hz,
        )
    })?;
    let llrs = refined
        .llr_sets
        .get(llr_set_index.saturating_sub(1))
        .cloned()
        .filter(|set| !set.is_empty())?;
    let ldpc = ParityMatrix::global().debug_bp_osd_state(&llrs, None, max_osd, norder)?;

    Some(Ft4Decode174Debug {
        coarse_start_seconds: spec.start_seconds_from_dt(dt_seconds),
        coarse_dt_seconds: dt_seconds,
        coarse_freq_hz: freq_hz,
        refined_start_seconds: refined.start_seconds,
        refined_dt_seconds: spec.dt_seconds_from_start(refined.start_seconds),
        refined_freq_hz: refined.freq_hz,
        sync_score: refined.sync_score,
        snr_db: refined.snr_db,
        llr_set_index,
        ldpc,
    })
}

pub fn debug_ft4_search_probe_wav_file(
    path: impl AsRef<Path>,
    target_freq_hz: f32,
) -> Result<Option<Ft4SearchProbeDebug>, DecoderError> {
    let audio = load_wav(path)?;
    let prepared = stock_window_audio(&audio, Mode::Ft4);
    Ok(search::debug_ft4_search_probe_pcm(
        &prepared,
        target_freq_hz,
    ))
}

pub fn debug_search_wav_file(
    path: impl AsRef<Path>,
    options: &DecodeOptions,
) -> Result<SearchDebugReport, DecoderError> {
    let audio = load_wav(path)?;
    debug_search_pcm(&audio, options)
}

pub fn debug_search_pcm(
    audio: &AudioBuffer,
    options: &DecodeOptions,
) -> Result<SearchDebugReport, DecoderError> {
    if options.mode == Mode::Ft2 {
        return Err(DecoderError::UnsupportedFormat(
            "debug search tracing is not implemented for FT2".to_string(),
        ));
    }
    let prepared = stock_window_audio(audio, options.mode);
    let trace = session::debug_run_decode_search(&prepared, options)?;
    let final_report =
        session::build_decode_report_with_resolver(&prepared, options, trace.search.clone(), None);
    Ok(SearchDebugReport {
        sample_rate_hz: prepared.sample_rate_hz,
        duration_seconds: prepared.samples.len() as f32 / prepared.sample_rate_hz as f32,
        passes: trace.passes,
        final_report,
    })
}

pub fn debug_ft2_trace_wav_file(
    path: impl AsRef<Path>,
    options: &DecodeOptions,
) -> Result<Ft2TraceReport, DecoderError> {
    let audio = load_wav(path)?;
    debug_ft2_trace_pcm(&audio, options)
}

pub fn debug_ft2_trace_pcm(
    audio: &AudioBuffer,
    options: &DecodeOptions,
) -> Result<Ft2TraceReport, DecoderError> {
    if options.mode != Mode::Ft2 {
        return Err(DecoderError::UnsupportedFormat(
            "ft2 trace requires --mode ft2".to_string(),
        ));
    }
    let prepared = stock_window_audio(audio, options.mode);
    debug_ft2_trace(&prepared, options)
}

pub fn subtract_truth_pcm(
    audio: &AudioBuffer,
    mode: Mode,
    dt_seconds: f32,
    freq_hz: f32,
    truth_codeword_bits: &[u8],
) -> Option<AudioBuffer> {
    let prepared = stock_window_audio(audio, mode);
    let payload = unpack_message_for_mode(mode, truth_codeword_bits)?;
    let mut residual = prepared.into_owned();
    let success = SuccessfulDecode {
        mode,
        payload: payload.clone(),
        codeword_bits: truth_codeword_bits.to_vec(),
        candidate: DecodeCandidate {
            start_seconds: mode.spec().start_seconds_from_dt(dt_seconds),
            dt_seconds,
            freq_hz,
            score: 0.0,
        },
        ldpc_iterations: 0,
        snr_db: 0,
    };
    subtract_candidate(&mut residual, &success, SubtractionPlan::for_mode(mode));
    Some(residual)
}

fn append_debug_passes(
    passes: &mut Vec<CandidatePassDebug>,
    prefix: &str,
    mode: Mode,
    llr_sets: &[Vec<f32>; 4],
    parity: &ParityMatrix,
    counters: &mut DecodeCounters,
    resolver: &HashResolver,
    max_osd: isize,
    truth_codeword_bits: Option<&[u8]>,
) {
    for (index, llrs) in llr_sets.iter().enumerate() {
        append_debug_single_pass(
            passes,
            &format!("{prefix}-{}", index + 1),
            mode,
            llrs,
            parity,
            counters,
            resolver,
            max_osd,
            truth_codeword_bits,
        );
    }
}

fn append_debug_single_pass(
    passes: &mut Vec<CandidatePassDebug>,
    pass_name: &str,
    mode: Mode,
    llrs: &[f32],
    parity: &ParityMatrix,
    counters: &mut DecodeCounters,
    resolver: &HashResolver,
    max_osd: isize,
    truth_codeword_bits: Option<&[u8]>,
) {
    if llrs.is_empty() {
        return;
    }
    let mean_abs_llr = llrs.iter().map(|value| value.abs()).sum::<f32>() / llrs.len() as f32;
    let max_abs_llr = llrs.iter().map(|value| value.abs()).fold(0.0f32, f32::max);
    let decoded =
        decode_llr_set(mode, parity, llrs, max_osd, counters).map(|(payload, _, iterations)| {
            let message = payload.to_message(resolver);
            (message.to_text(), iterations)
        });
    let (truth_hard_errors, truth_weighted_distance) = truth_codeword_bits
        .and_then(|truth| truth_metrics(mode.spec(), llrs, truth))
        .unwrap_or((None, None));
    passes.push(CandidatePassDebug {
        pass_name: pass_name.to_string(),
        mean_abs_llr,
        max_abs_llr,
        decoded_text: decoded.as_ref().map(|(text, _)| text.clone()),
        ldpc_iterations: decoded.map(|(_, iterations)| iterations),
        truth_hard_errors,
        truth_weighted_distance,
    });
}

pub fn decode_pcm(
    audio: &AudioBuffer,
    options: &DecodeOptions,
) -> Result<DecodeReport, DecoderError> {
    let prepared = stock_window_audio(audio, options.mode);
    if options.mode == Mode::Ft2 {
        return decode_ft2(&prepared, options);
    }
    let mut session = DecoderSession::new();
    let mut updates = session.decode_available(&prepared, options)?;
    if let Some(update) = updates.pop() {
        Ok(update.report)
    } else {
        Err(DecoderError::UnsupportedFormat(
            "audio too short for selected decode profile".to_string(),
        ))
    }
}

pub fn decode_pcm_with_state(
    audio: &AudioBuffer,
    options: &DecodeOptions,
    state: Option<&DecoderState>,
) -> Result<(DecodeReport, DecoderState), DecoderError> {
    let prepared = stock_window_audio(audio, options.mode);
    if options.mode == Mode::Ft2 {
        let report = decode_ft2(&prepared, options)?;
        return Ok((report, state.cloned().unwrap_or_default()));
    }
    let mut session = DecoderSession::new();
    let mut updates = session.decode_available_with_state(&prepared, options, state)?;
    if let Some((update, state)) = updates.pop() {
        Ok((update.report, state))
    } else {
        Err(DecoderError::UnsupportedFormat(
            "audio too short for selected decode profile".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decoder::session::try_refined_candidate;
    use crate::encode::{
        TxDirectedPayload, TxMessage, TxRttyExchange, WaveformOptions, encode_nonstandard_message,
        encode_standard_message, encode_standard_message_for_mode, synthesize_rectangular_waveform,
        synthesize_tx_message,
    };
    use crate::message::{GridReport, ReplyWord};

    #[test]
    #[ignore = "diagnostic"]
    fn debug_known_real_candidate() {
        let spec = Mode::Ft8.spec();
        let audio = crate::wave::load_wav(
            "/Users/bgelb/ft8-regr/artifacts/samples/kgoba-ft8-lib/191111_110115/191111_110115.wav",
        )
        .expect("wav");
        let spectrum = build_long_spectrum(&audio, spec);
        let baseband_plan = BasebandPlan::new(spec);
        let refined =
            refine_candidate(&spectrum, &baseband_plan, spec, 1.4, 1234.0).expect("refined");
        eprintln!(
            "refined start={:.4} dt={:.4} freq={:.4} sync={:.3} snr={}",
            refined.start_seconds,
            spec.dt_seconds_from_start(refined.start_seconds),
            refined.freq_hz,
            refined.sync_score,
            refined.snr_db
        );
        let parity = ParityMatrix::global();
        for (index, llrs) in refined.llr_sets.iter().enumerate() {
            let mean_abs = llrs.iter().map(|value| value.abs()).sum::<f32>() / llrs.len() as f32;
            let max_abs = llrs.iter().map(|value| value.abs()).fold(0.0f32, f32::max);
            let success = parity.decode(llrs);
            eprintln!(
                "pass={} mean_abs={:.3} max_abs={:.3} decode={}",
                index + 1,
                mean_abs,
                max_abs,
                success.is_some()
            );
        }
    }

    #[test]
    fn parses_known_profiles() {
        assert_eq!(
            "quick".parse::<DecodeProfile>().unwrap(),
            DecodeProfile::Quick
        );
        assert_eq!(
            "medium".parse::<DecodeProfile>().unwrap(),
            DecodeProfile::Medium
        );
        assert_eq!(
            "deepest".parse::<DecodeProfile>().unwrap(),
            DecodeProfile::Deepest
        );
        assert_eq!(
            "deep".parse::<DecodeProfile>().unwrap(),
            DecodeProfile::Deepest
        );
    }

    #[test]
    fn deepest_profile_uses_extra_osd_after_first_pass() {
        let options = DecodeOptions {
            profile: DecodeProfile::Deepest,
            ..DecodeOptions::default()
        };
        assert_eq!(options.max_osd_passes(0, 1_500.0), 0);
        assert_eq!(options.max_osd_passes(1, 1_500.0), 3);
        assert_eq!(options.max_osd_passes(1, 1_700.0), 3);
    }

    #[test]
    fn ft4_medium_profile_disables_osd() {
        let options = DecodeOptions {
            mode: Mode::Ft4,
            profile: DecodeProfile::Medium,
            ..DecodeOptions::default()
        };
        assert_eq!(options.max_osd_passes(0, 1_500.0), -1);
        assert_eq!(options.max_osd_passes(1, 1_500.0), -1);
    }

    #[test]
    fn ft4_defaults_to_two_search_passes() {
        assert_eq!(DecodeOptions::for_mode(Mode::Ft4).search_passes, 2);
    }

    #[test]
    fn session_emits_stages_for_progressively_longer_buffers() {
        let options = DecodeOptions::default();
        let spec = options.mode.spec();
        let mut session = DecoderSession::new();

        let early41 = AudioBuffer {
            sample_rate_hz: FT8_SAMPLE_RATE,
            samples: vec![0.0; early_41_samples(spec)],
        };
        let updates = session
            .decode_available(&early41, &options)
            .expect("early41 decode");
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].stage, DecodeStage::Early41);

        let early47 = AudioBuffer {
            sample_rate_hz: FT8_SAMPLE_RATE,
            samples: vec![0.0; early_47_samples(spec)],
        };
        let updates = session
            .decode_available(&early47, &options)
            .expect("early47 decode");
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].stage, DecodeStage::Early47);

        let full = AudioBuffer {
            sample_rate_hz: FT8_SAMPLE_RATE,
            samples: vec![0.0; long_input_samples(spec)],
        };
        let updates = session
            .decode_available(&full, &options)
            .expect("full decode");
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].stage, DecodeStage::Full);

        let updates = session
            .decode_available(&full, &options)
            .expect("repeat decode");
        assert!(updates.is_empty());
    }

    #[test]
    fn quick_profile_skips_early_stages() {
        let options = DecodeOptions {
            profile: DecodeProfile::Quick,
            ..DecodeOptions::default()
        };
        let spec = options.mode.spec();
        let mut session = DecoderSession::new();
        let full = AudioBuffer {
            sample_rate_hz: FT8_SAMPLE_RATE,
            samples: vec![0.0; long_input_samples(spec)],
        };
        let updates = session
            .decode_available(&full, &options)
            .expect("quick decode");
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].stage, DecodeStage::Full);
    }

    #[test]
    fn decode_pcm_with_state_resolves_hash22_callsign() {
        let learned_frame =
            encode_nonstandard_message("K1ABC", "HF19NY", false, ReplyWord::Blank, true)
                .expect("encode learned");
        let learned_audio = synthesize_rectangular_waveform(
            &learned_frame,
            &WaveformOptions {
                base_freq_hz: 900.0,
                ..WaveformOptions::default()
            },
        )
        .expect("learned waveform");
        let hashed_frame = encode_standard_message("CQ", "HF19NY", false, &GridReport::Blank)
            .expect("encode hashed");
        let hashed_audio = synthesize_rectangular_waveform(
            &hashed_frame,
            &WaveformOptions {
                base_freq_hz: 1_234.0,
                ..WaveformOptions::default()
            },
        )
        .expect("hashed waveform");
        let options = DecodeOptions {
            max_candidates: 8,
            max_successes: 2,
            ..DecodeOptions::default()
        };

        let (unresolved, _) =
            decode_pcm_with_state(&hashed_audio, &options, None).expect("decode unresolved");
        let unresolved_texts: Vec<_> = unresolved
            .decodes
            .iter()
            .map(|decode| decode.text.as_str())
            .collect();
        assert!(
            unresolved_texts.iter().any(|text| text.contains("<...>")),
            "expected unresolved hash in {unresolved_texts:?}"
        );

        let (learned, state) =
            decode_pcm_with_state(&learned_audio, &options, None).expect("decode learned");
        let learned_texts: Vec<_> = learned
            .decodes
            .iter()
            .map(|decode| decode.text.as_str())
            .collect();
        assert!(
            learned_texts.iter().any(|text| *text == "CQ HF19NY"),
            "expected plain learned call in {learned_texts:?}"
        );

        let (resolved, _) =
            decode_pcm_with_state(&hashed_audio, &options, Some(&state)).expect("decode resolved");
        let resolved_texts: Vec<_> = resolved
            .decodes
            .iter()
            .map(|decode| decode.text.as_str())
            .collect();
        assert!(
            resolved_texts.iter().any(|text| *text == "CQ HF19NY"),
            "expected resolved call in {resolved_texts:?}"
        );
    }

    #[test]
    fn standard_message_tx_round_trips_cover_core_qso_messages() {
        struct Case {
            message: TxMessage,
            expected: &'static str,
            base_freq_hz: f32,
        }

        let cases = [
            Case {
                message: TxMessage::Cq {
                    my_call: "K1ABC".to_string(),
                    my_grid: None,
                },
                expected: "CQ K1ABC",
                base_freq_hz: 650.0,
            },
            Case {
                message: TxMessage::Directed {
                    peer_call: "W1XYZ".to_string(),
                    my_call: "K1ABC".to_string(),
                    payload: TxDirectedPayload::Grid("FN31".to_string()),
                },
                expected: "W1XYZ K1ABC FN31",
                base_freq_hz: 900.0,
            },
            Case {
                message: TxMessage::Directed {
                    peer_call: "W1XYZ".to_string(),
                    my_call: "K1ABC".to_string(),
                    payload: TxDirectedPayload::Signal(-7),
                },
                expected: "W1XYZ K1ABC -07",
                base_freq_hz: 1_150.0,
            },
            Case {
                message: TxMessage::Directed {
                    peer_call: "W1XYZ".to_string(),
                    my_call: "K1ABC".to_string(),
                    payload: TxDirectedPayload::SignalWithAck(-7),
                },
                expected: "W1XYZ K1ABC R-07",
                base_freq_hz: 1_400.0,
            },
            Case {
                message: TxMessage::Directed {
                    peer_call: "W1XYZ".to_string(),
                    my_call: "K1ABC".to_string(),
                    payload: TxDirectedPayload::Reply(ReplyWord::Rrr),
                },
                expected: "W1XYZ K1ABC RRR",
                base_freq_hz: 1_650.0,
            },
            Case {
                message: TxMessage::Directed {
                    peer_call: "W1XYZ".to_string(),
                    my_call: "K1ABC".to_string(),
                    payload: TxDirectedPayload::Reply(ReplyWord::Rr73),
                },
                expected: "W1XYZ K1ABC RR73",
                base_freq_hz: 1_900.0,
            },
            Case {
                message: TxMessage::Directed {
                    peer_call: "W1XYZ".to_string(),
                    my_call: "K1ABC".to_string(),
                    payload: TxDirectedPayload::Reply(ReplyWord::SeventyThree),
                },
                expected: "W1XYZ K1ABC 73",
                base_freq_hz: 2_150.0,
            },
            Case {
                message: TxMessage::FieldDay {
                    first_call: "WA9XYZ".to_string(),
                    second_call: "KA1ABC".to_string(),
                    acknowledge: true,
                    transmitter_count: 16,
                    class: 'A',
                    section: "EMA".to_string(),
                },
                expected: "WA9XYZ KA1ABC R 16A EMA",
                base_freq_hz: 2_300.0,
            },
            Case {
                message: TxMessage::RttyContest {
                    tu: true,
                    first_call: "W9XYZ".to_string(),
                    second_call: "K1ABC".to_string(),
                    acknowledge: true,
                    report: 579,
                    exchange: TxRttyExchange::Multiplier("MA".to_string()),
                },
                expected: "TU; W9XYZ K1ABC R 579 MA",
                base_freq_hz: 2_450.0,
            },
        ];

        let options = DecodeOptions {
            max_candidates: 16,
            max_successes: 4,
            ..DecodeOptions::default()
        };

        for case in cases {
            let synthesized = synthesize_tx_message(
                &case.message,
                &WaveformOptions {
                    base_freq_hz: case.base_freq_hz,
                    ..WaveformOptions::default()
                },
            )
            .expect("synthesize");
            assert_eq!(synthesized.rendered_text, case.expected);
            let report = decode_pcm(&synthesized.audio, &options).expect("decode");
            let decoded: Vec<_> = report
                .decodes
                .iter()
                .map(|decode| decode.text.as_str())
                .collect();
            assert!(
                decoded.contains(&case.expected),
                "expected {:?} in decoded messages {:?}",
                case.expected,
                decoded
            );
        }
    }

    #[test]
    fn tx_api_supports_hashed_partner_callsign() {
        let learned_frame =
            encode_nonstandard_message("K1ABC", "HF19NY", false, ReplyWord::Blank, true)
                .expect("encode learned");
        let learned_audio = synthesize_rectangular_waveform(
            &learned_frame,
            &WaveformOptions {
                base_freq_hz: 900.0,
                ..WaveformOptions::default()
            },
        )
        .expect("learned waveform");
        let message = TxMessage::Directed {
            peer_call: "HF19NY".to_string(),
            my_call: "K1ABC".to_string(),
            payload: TxDirectedPayload::SignalWithAck(-7),
        };
        let synthesized = synthesize_tx_message(
            &message,
            &WaveformOptions {
                base_freq_hz: 1_234.0,
                ..WaveformOptions::default()
            },
        )
        .expect("synthesize hashed partner");
        let options = DecodeOptions {
            max_candidates: 8,
            max_successes: 2,
            ..DecodeOptions::default()
        };

        let (unresolved, _) =
            decode_pcm_with_state(&synthesized.audio, &options, None).expect("decode unresolved");
        let unresolved_texts: Vec<_> = unresolved
            .decodes
            .iter()
            .map(|decode| decode.text.as_str())
            .collect();
        assert!(
            unresolved_texts
                .iter()
                .any(|text| text.contains("<...> K1ABC R-07")),
            "expected unresolved hashed partner in {unresolved_texts:?}"
        );

        let (_, state) =
            decode_pcm_with_state(&learned_audio, &options, None).expect("learn learned call");
        let (resolved, _) = decode_pcm_with_state(&synthesized.audio, &options, Some(&state))
            .expect("decode resolved");
        let resolved_texts: Vec<_> = resolved
            .decodes
            .iter()
            .map(|decode| decode.text.as_str())
            .collect();
        assert!(
            resolved_texts
                .iter()
                .any(|text| *text == "HF19NY K1ABC R-07"),
            "expected resolved hashed partner in {resolved_texts:?}"
        );
    }

    #[test]
    fn ft4_exact_candidate_round_trips_on_generated_signal() {
        let spec = Mode::Ft4.spec();
        let frame = encode_standard_message_for_mode(
            Mode::Ft4,
            "K1ABC",
            "W1XYZ",
            false,
            &GridReport::Grid("FN31".to_string()),
        )
        .expect("encode ft4");
        let audio = synthesize_rectangular_waveform(
            &frame,
            &WaveformOptions {
                mode: Mode::Ft4,
                base_freq_hz: 900.0,
                start_seconds: 0.1,
                total_seconds: 7.5,
                amplitude: 0.8,
            },
        )
        .expect("ft4 waveform");
        let spectrum = build_long_spectrum(&audio, spec);
        let baseband_plan = BasebandPlan::new(spec);
        let refined =
            refine_candidate(&spectrum, &baseband_plan, spec, 0.1, 900.0).expect("refined");
        let mut counters = DecodeCounters::default();
        let success = try_refined_candidate(
            &refined,
            Mode::Ft4,
            1.0,
            0,
            false,
            &[],
            &HashResolver::default(),
            &std::collections::HashSet::new(),
            ParityMatrix::global(),
            &mut counters,
        )
        .expect("decode ft4 candidate");
        assert_eq!(
            success
                .payload
                .to_message(&HashResolver::default())
                .to_text(),
            "K1ABC W1XYZ FN31"
        );
    }

    #[test]
    fn ft4_generated_waveform_round_trips_through_decoder() {
        let frame = encode_standard_message_for_mode(
            Mode::Ft4,
            "K1ABC",
            "W1XYZ",
            false,
            &GridReport::Grid("FN31".to_string()),
        )
        .expect("encode ft4");
        let audio = synthesize_rectangular_waveform(
            &frame,
            &WaveformOptions {
                mode: Mode::Ft4,
                base_freq_hz: 900.0,
                start_seconds: 0.1,
                total_seconds: 7.5,
                amplitude: 0.8,
            },
        )
        .expect("ft4 waveform");
        let report = decode_pcm(
            &audio,
            &DecodeOptions {
                mode: Mode::Ft4,
                max_candidates: 32,
                max_successes: 4,
                ..DecodeOptions::default()
            },
        )
        .expect("decode ft4");
        let decoded: Vec<_> = report
            .decodes
            .iter()
            .map(|decode| decode.text.as_str())
            .collect();
        assert!(
            decoded.contains(&"K1ABC W1XYZ FN31"),
            "expected FT4 decode in {decoded:?}"
        );
    }

    #[test]
    fn dxpedition_compound_message_round_trips_with_hash10_resolution() {
        let synthesized = synthesize_tx_message(
            &TxMessage::DxpeditionCompound {
                finished_call: "K1ABC".to_string(),
                next_call: "W9XYZ".to_string(),
                my_call: "KH1/KH7Z".to_string(),
                report_db: -11,
            },
            &WaveformOptions {
                base_freq_hz: 1_550.0,
                ..WaveformOptions::default()
            },
        )
        .expect("synthesize dxpedition");
        let options = DecodeOptions {
            max_candidates: 8,
            max_successes: 2,
            ..DecodeOptions::default()
        };

        let (unresolved, _) =
            decode_pcm_with_state(&synthesized.audio, &options, None).expect("decode unresolved");
        let unresolved_texts: Vec<_> = unresolved
            .decodes
            .iter()
            .map(|decode| decode.text.as_str())
            .collect();
        assert!(
            unresolved_texts
                .iter()
                .any(|text| *text == "K1ABC RR73; W9XYZ <...> -12"),
            "expected unresolved dxpedition text in {unresolved_texts:?}"
        );

        let mut state = DecoderState::new();
        state.insert_callsign("KH1/KH7Z");
        let (resolved, _) = decode_pcm_with_state(&synthesized.audio, &options, Some(&state))
            .expect("decode resolved");
        let resolved_texts: Vec<_> = resolved
            .decodes
            .iter()
            .map(|decode| decode.text.as_str())
            .collect();
        assert!(
            resolved_texts
                .iter()
                .any(|text| *text == "K1ABC RR73; W9XYZ <KH1/KH7Z> -12"),
            "expected resolved dxpedition text in {resolved_texts:?}"
        );
    }

    #[test]
    fn shared_decoder_modules_do_not_reference_ft8_named_constants() {
        for source in [
            include_str!("decoder/search.rs"),
            include_str!("decoder/refine.rs"),
            include_str!("decoder/subtract.rs"),
            include_str!("decoder/metrics.rs"),
            include_str!("decoder/session.rs"),
        ] {
            assert!(
                !source.contains("FT8_"),
                "shared decoder module still references FT8_* directly"
            );
            assert!(
                source.contains("options.mode.spec()")
                    || source.contains("spec: &ModeSpec")
                    || source.contains("mode.spec()"),
                "shared decoder module should route geometry through ModeSpec"
            );
        }
    }

    #[test]
    fn shared_decoder_modules_avoid_raw_ft8_timing_literals() {
        for source in [
            include_str!("decoder/search.rs"),
            include_str!("decoder/refine.rs"),
            include_str!("decoder/subtract.rs"),
            include_str!("decoder/metrics.rs"),
            include_str!("decoder/session.rs"),
        ] {
            for fragment in [
                "dt_seconds + 0.5",
                "start_seconds - 0.5",
                "(lag as f32 - 0.5)",
            ] {
                assert!(
                    !source.contains(fragment),
                    "shared decoder module still contains raw FT8 timing literal: {fragment}"
                );
            }
        }
    }

    #[test]
    fn decoder_agents_file_codifies_cleanup_contract() {
        let agents = include_str!("../AGENTS.md");
        for required in [
            "## Decoder Code Contract",
            "Route geometry, layout, and timing semantics through `ModeSpec`",
            "Don’t add direct `FT8_` references to shared decoder submodules.",
            "Don’t add raw FT8 timing arithmetic like `dt + 0.5`",
            "## Future Modes",
            "Run `cargo test`, `cargo build --release`, and the full `medium` regression",
        ] {
            assert!(
                agents.contains(required),
                "decoder/AGENTS.md missing required guidance: {required}"
            );
        }
    }
}
