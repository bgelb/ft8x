use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::{Arc, OnceLock};

use num_complex::Complex32;
use realfft::{RealFftPlanner, RealToComplex};
use rustfft::{Fft, FftPlanner};
use serde::Serialize;

use crate::encode::{channel_symbols_from_codeword_bits, synthesize_channel_reference};
use crate::ldpc::ParityMatrix;
use crate::message::{GridReport, HashResolver, Payload, StructuredMessage, unpack_message};
use crate::modes::ft8::FT8_SPEC;
use crate::modes::{ModeSpec, all_costas_positions};
use crate::wave::{AudioBuffer, DecoderError, load_wav};

#[cfg(test)]
use crate::modes::ft8::FT8_SAMPLE_RATE;

mod metrics;
mod refine;
mod search;
mod session;
mod subtract;

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
    pub profile: DecodeProfile,
    pub min_freq_hz: f32,
    pub max_freq_hz: f32,
    pub max_candidates: usize,
    pub max_successes: usize,
    pub search_passes: usize,
    pub target_freq_hz: f32,
    pub tx_freq_hz: f32,
    pub ap_width_hz: f32,
}

impl Default for DecodeOptions {
    fn default() -> Self {
        Self {
            profile: DecodeProfile::Medium,
            min_freq_hz: 200.0,
            max_freq_hz: 4_000.0,
            max_candidates: 600,
            max_successes: 200,
            search_passes: 3,
            target_freq_hz: 1_500.0,
            tx_freq_hz: 1_500.0,
            ap_width_hz: 75.0,
        }
    }
}

impl DecodeOptions {
    fn uses_early_decodes(&self) -> bool {
        !matches!(self.profile, DecodeProfile::Quick)
    }

    fn sync_threshold(&self) -> f32 {
        if matches!(self.profile, DecodeProfile::Deepest) {
            1.3
        } else {
            ACTIVE_MODE.tuning.sync_threshold
        }
    }

    fn max_osd_passes(&self, outer_pass: usize, _freq_hz: f32) -> isize {
        match self.profile {
            DecodeProfile::Quick => -1,
            DecodeProfile::Medium => 0,
            DecodeProfile::Deepest => {
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

    pub fn required_samples(self) -> usize {
        match self {
            Self::Early41 => ACTIVE_MODE.early41_samples(),
            Self::Early47 => ACTIVE_MODE.early47_samples(),
            Self::Full => LONG_INPUT_SAMPLES,
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

#[derive(Debug, Clone)]
struct SuccessfulDecode {
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
    forward: Arc<dyn Fft<f32>>,
    inverse: Arc<dyn Fft<f32>>,
    filter_spectrum: Vec<Complex32>,
    edge_correction: Vec<f32>,
}

const ACTIVE_MODE: ModeSpec = FT8_SPEC;

const LONG_INPUT_SAMPLES: usize = ACTIVE_MODE.tuning.long_input_samples;
const LONG_FFT_SAMPLES: usize = ACTIVE_MODE.tuning.long_fft_samples;
const SYNC8_EARLY_THRESHOLD: f32 = ACTIVE_MODE.tuning.sync_early_threshold;
const BASEBAND_SAMPLES: usize = ACTIVE_MODE.baseband_samples();
const BASEBAND_SYMBOL_SAMPLES: usize = ACTIVE_MODE.baseband_symbol_samples();
const BASEBAND_TAPER_LEN: usize = ACTIVE_MODE.tuning.baseband_taper_len;
const SUBTRACT_FILTER_SAMPLES: usize = ACTIVE_MODE.tuning.subtract_filter_samples;
const SUBTRACT_FILTER_HALF: usize = SUBTRACT_FILTER_SAMPLES / 2;
const EARLY_BLOCK_SAMPLES: usize = ACTIVE_MODE.tuning.early_block_samples;
const EARLY_41_SAMPLES: usize = 41 * EARLY_BLOCK_SAMPLES;
const EARLY_47_SAMPLES: usize = 47 * EARLY_BLOCK_SAMPLES;
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
    dt_seconds: f32,
    freq_hz: f32,
) -> Result<Option<CandidateDebugReport>, DecoderError> {
    let audio = load_wav(path)?;
    Ok(debug_candidate_pcm(&audio, dt_seconds, freq_hz))
}

pub fn debug_candidate_truth_wav_file(
    path: impl AsRef<Path>,
    dt_seconds: f32,
    freq_hz: f32,
    truth_codeword_bits: &[u8],
) -> Result<Option<CandidateDebugReport>, DecoderError> {
    let audio = load_wav(path)?;
    Ok(debug_candidate_pcm_inner(
        &audio,
        dt_seconds,
        freq_hz,
        Some(truth_codeword_bits),
    ))
}

pub fn debug_candidate_pcm(
    audio: &AudioBuffer,
    dt_seconds: f32,
    freq_hz: f32,
) -> Option<CandidateDebugReport> {
    debug_candidate_pcm_inner(audio, dt_seconds, freq_hz, None)
}

fn debug_candidate_pcm_inner(
    audio: &AudioBuffer,
    dt_seconds: f32,
    freq_hz: f32,
    truth_codeword_bits: Option<&[u8]>,
) -> Option<CandidateDebugReport> {
    if audio.sample_rate_hz != ACTIVE_MODE.geometry.sample_rate_hz
        || audio.samples.len() < ACTIVE_MODE.geometry.symbol_samples
    {
        return None;
    }

    let long_spectrum = build_long_spectrum(audio);
    let baseband_plan = BasebandPlan::new();
    let refined = refine_candidate(
        &long_spectrum,
        &baseband_plan,
        ACTIVE_MODE.start_seconds_from_dt(dt_seconds),
        freq_hz,
    )?;
    let parity = ParityMatrix::global();
    let mut counters = DecodeCounters::default();
    let mut passes = Vec::new();
    let resolver = HashResolver::default();

    append_debug_passes(
        &mut passes,
        "regular",
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
        ACTIVE_MODE.start_seconds_from_dt(dt_seconds),
        freq_hz,
    ) {
        append_debug_passes(
            &mut passes,
            "seed",
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
            ("ap-cq", cq_ap_known_bits()),
            ("ap-mycall", mycall_ap_known_bits()),
        ] {
            let llrs = llrs_with_known_bits(&refined.llr_sets[0], known_bits, ap_magnitude);
            let mean_abs_llr =
                llrs.iter().map(|value| value.abs()).sum::<f32>() / llrs.len() as f32;
            let max_abs_llr = llrs.iter().map(|value| value.abs()).fold(0.0f32, f32::max);
            let decoded =
                decode_llr_set_with_known_bits(parity, &llrs, known_bits, 0, &mut counters).map(
                    |(payload, _, iterations)| {
                        let message = payload.to_message(&resolver);
                        (message.to_text(), iterations)
                    },
                );
            let (truth_hard_errors, truth_weighted_distance) = truth_codeword_bits
                .and_then(|truth| truth_metrics(&llrs, truth))
                .unwrap_or((None, None));
            passes.push(CandidatePassDebug {
                pass_name: name.to_string(),
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
        coarse_start_seconds: ACTIVE_MODE.start_seconds_from_dt(dt_seconds),
        coarse_dt_seconds: dt_seconds,
        coarse_freq_hz: freq_hz,
        refined_start_seconds: refined.start_seconds,
        refined_dt_seconds: ACTIVE_MODE.dt_seconds_from_start(refined.start_seconds),
        refined_freq_hz: refined.freq_hz,
        sync_score: refined.sync_score,
        snr_db: refined.snr_db,
        passes,
    })
}

fn append_debug_passes(
    passes: &mut Vec<CandidatePassDebug>,
    prefix: &str,
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
    llrs: &[f32],
    parity: &ParityMatrix,
    counters: &mut DecodeCounters,
    resolver: &HashResolver,
    max_osd: isize,
    truth_codeword_bits: Option<&[u8]>,
) {
    let mean_abs_llr = llrs.iter().map(|value| value.abs()).sum::<f32>() / llrs.len() as f32;
    let max_abs_llr = llrs.iter().map(|value| value.abs()).fold(0.0f32, f32::max);
    let decoded =
        decode_llr_set(parity, llrs, max_osd, counters).map(|(payload, _, iterations)| {
            let message = payload.to_message(resolver);
            (message.to_text(), iterations)
        });
    let (truth_hard_errors, truth_weighted_distance) = truth_codeword_bits
        .and_then(|truth| truth_metrics(llrs, truth))
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
    let mut session = DecoderSession::new();
    let mut updates = session.decode_available(audio, options)?;
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
    let mut session = DecoderSession::new();
    let mut updates = session.decode_available_with_state(audio, options, state)?;
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
    use crate::encode::{
        TxDirectedPayload, TxMessage, WaveformOptions, encode_nonstandard_message,
        encode_standard_message, synthesize_rectangular_waveform, synthesize_tx_message,
    };
    use crate::message::{GridReport, ReplyWord};

    #[test]
    #[ignore = "diagnostic"]
    fn debug_known_real_candidate() {
        let audio = crate::wave::load_wav(
            "/Users/bgelb/ft8-regr/artifacts/samples/kgoba-ft8-lib/191111_110115/191111_110115.wav",
        )
        .expect("wav");
        let spectrum = build_long_spectrum(&audio);
        let baseband_plan = BasebandPlan::new();
        let refined = refine_candidate(&spectrum, &baseband_plan, 1.4, 1234.0).expect("refined");
        eprintln!(
            "refined start={:.4} dt={:.4} freq={:.4} sync={:.3} snr={}",
            refined.start_seconds,
            ACTIVE_MODE.dt_seconds_from_start(refined.start_seconds),
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
    fn session_emits_stages_for_progressively_longer_buffers() {
        let options = DecodeOptions::default();
        let mut session = DecoderSession::new();

        let early41 = AudioBuffer {
            sample_rate_hz: FT8_SAMPLE_RATE,
            samples: vec![0.0; EARLY_41_SAMPLES],
        };
        let updates = session
            .decode_available(&early41, &options)
            .expect("early41 decode");
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].stage, DecodeStage::Early41);

        let early47 = AudioBuffer {
            sample_rate_hz: FT8_SAMPLE_RATE,
            samples: vec![0.0; EARLY_47_SAMPLES],
        };
        let updates = session
            .decode_available(&early47, &options)
            .expect("early47 decode");
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].stage, DecodeStage::Early47);

        let full = AudioBuffer {
            sample_rate_hz: FT8_SAMPLE_RATE,
            samples: vec![0.0; LONG_INPUT_SAMPLES],
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
        let mut session = DecoderSession::new();
        let full = AudioBuffer {
            sample_rate_hz: FT8_SAMPLE_RATE,
            samples: vec![0.0; LONG_INPUT_SAMPLES],
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
                source.contains("ACTIVE_MODE"),
                "shared decoder module should route geometry through ACTIVE_MODE"
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
