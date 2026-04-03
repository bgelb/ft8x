use super::{FrameGeometry, ModeSpec, SearchTuning};
use crate::protocol::FTX_DATA_SYMBOLS;

// FT8 uses 12 kHz sample rate, 6.25 Hz tone spacing, and 79 total symbols:
// 58 payload symbols plus three 7-symbol Costas sync blocks.
pub const FT8_SAMPLE_RATE: u32 = 12_000;
pub const FT8_SYMBOL_SAMPLES: usize = 1_920;
pub const FT8_TONE_SPACING_HZ: f32 = 6.25;
pub const FT8_HOPS_PER_SYMBOL: usize = 8;
pub const FT8_COSTAS: [usize; 7] = [3, 1, 4, 0, 6, 5, 2];
pub const FT8_SYNC_BLOCK_COUNT: usize = 3;
pub const FT8_PAYLOAD_SYMBOLS: usize = FTX_DATA_SYMBOLS;
pub const FT8_DATA_SYMBOLS_PER_HALF: usize = FT8_PAYLOAD_SYMBOLS / 2;
pub const FT8_SYNC_BLOCK_STRIDE: usize = FT8_COSTAS.len() + FT8_DATA_SYMBOLS_PER_HALF;
pub const FT8_MESSAGE_SYMBOLS: usize =
    FT8_PAYLOAD_SYMBOLS + FT8_SYNC_BLOCK_COUNT * FT8_COSTAS.len();
pub const FT8_SYNC_BLOCK_STARTS: [usize; FT8_SYNC_BLOCK_COUNT] =
    [0, FT8_SYNC_BLOCK_STRIDE, FT8_SYNC_BLOCK_STRIDE * 2];
pub const FT8_HOP_SAMPLES: usize = FT8_SYMBOL_SAMPLES / FT8_HOPS_PER_SYMBOL;
pub const FT8_DATA_POSITIONS: [usize; FT8_PAYLOAD_SYMBOLS] = build_ft8_data_positions();

const FT8_LONG_FFT_SAMPLES: usize = 100 * FT8_SYMBOL_SAMPLES;
const FT8_SYNC_GUARD_BINS: usize = 12;
const FT8_BASEBAND_VALID_SAMPLES: usize = 2_812;
const FT8_EARLY_BLOCK_SAMPLES: usize = 3_456;
const FT8_SUBTRACTION_REFINE_CUTOFF_SECONDS: f32 = 0.396;
const FT8_SUBTRACTION_REFINE_PROBE_STEP_SAMPLES: isize = 90;
const FT8_REFINE_RESIDUAL_STEP_HZ: f32 = 0.5;
const FT8_LEGACY_CANDIDATE_SEPARATION_DT_SECONDS: f32 = 0.16;
const FT8_LEGACY_CANDIDATE_SEPARATION_TONE_FACTOR: f32 = 1.5;
const FT8_BAND_LOWER_TONE_OFFSET: f32 = 1.5;
const FT8_BAND_UPPER_TONE_OFFSET: f32 = 8.5;
const FT8_LLR_SCALE_FACTOR: f32 = 2.83;

// FT8 data symbols occupy every non-Costas slot in the 79-symbol frame.
const fn is_ft8_sync_symbol(symbol_index: usize) -> bool {
    let mut block = 0;
    while block < FT8_SYNC_BLOCK_STARTS.len() {
        let start = FT8_SYNC_BLOCK_STARTS[block];
        if symbol_index >= start && symbol_index < start + FT8_COSTAS.len() {
            return true;
        }
        block += 1;
    }
    false
}

// Derive the 58 payload symbol positions from the sync-block geometry instead of listing them.
const fn build_ft8_data_positions() -> [usize; FT8_PAYLOAD_SYMBOLS] {
    let mut positions = [0usize; FT8_PAYLOAD_SYMBOLS];
    let mut symbol_index = 0usize;
    let mut out = 0usize;
    while symbol_index < FT8_MESSAGE_SYMBOLS {
        if !is_ft8_sync_symbol(symbol_index) {
            positions[out] = symbol_index;
            out += 1;
        }
        symbol_index += 1;
    }
    positions
}

pub const FT8_GEOMETRY: FrameGeometry = FrameGeometry {
    sample_rate_hz: FT8_SAMPLE_RATE,
    symbol_samples: FT8_SYMBOL_SAMPLES,
    tone_spacing_hz: FT8_TONE_SPACING_HZ,
    message_symbols: FT8_MESSAGE_SYMBOLS,
    sync_block_starts: &FT8_SYNC_BLOCK_STARTS,
    costas_pattern: &FT8_COSTAS,
    data_symbol_positions: &FT8_DATA_POSITIONS,
    hop_samples: FT8_HOP_SAMPLES,
};

pub const FT8_TUNING: SearchTuning = SearchTuning {
    long_input_samples: 15 * FT8_SAMPLE_RATE as usize,
    // Legacy 100-symbol analysis window; at 12 kHz this gives 0.0625 Hz FFT bins.
    long_fft_samples: FT8_LONG_FFT_SAMPLES,
    downsample_factor: 60,
    sync_fft_symbol_window: 2,
    sync_step_divisor: 4,
    sync_max_lag: 62,
    sync_local_lag: 10,
    sync_threshold: 1.6,
    sync_early_threshold: 2.0,
    // Legacy sync8 search guard width retained for parity with the pre-refactor decoder.
    sync_guard_bins: FT8_SYNC_GUARD_BINS,
    sync_power_scale: 1.0 / 300.0,
    sync_baseline_percentile: 0.40,
    sync_baseline_floor: 1e-6,
    nominal_start_seconds: 0.5,
    baseband_taper_len: 100,
    // Legacy usable downsampled window after discarding the zero-padded tail.
    baseband_valid_samples: FT8_BASEBAND_VALID_SAMPLES,
    subtract_filter_samples: 4_000,
    // Legacy early decode gate at 3_456 / 12_000 = 0.288 s per block.
    early_block_samples: FT8_EARLY_BLOCK_SAMPLES,
    // Legacy "close enough to nominal start" threshold for dt-refined subtraction.
    subtraction_refine_cutoff_seconds: FT8_SUBTRACTION_REFINE_CUTOFF_SECONDS,
    // Legacy +/-90-sample quadratic probe spacing (7.5 ms at 12 kHz).
    subtraction_refine_probe_step_samples: FT8_SUBTRACTION_REFINE_PROBE_STEP_SAMPLES,
    // Refinement probes half-Hz residual offsets around the coarse candidate.
    refine_residual_step_hz: FT8_REFINE_RESIDUAL_STEP_HZ,
    nfqso_hz: 1_500.0,
    nfqso_priority_window_hz: 10.0,
    candidate_separation_hz: 4.0,
    // Legacy duplicate-suppression window from the pre-refactor decoder.
    legacy_candidate_separation_dt_seconds: FT8_LEGACY_CANDIDATE_SEPARATION_DT_SECONDS,
    legacy_candidate_separation_tone_factor: FT8_LEGACY_CANDIDATE_SEPARATION_TONE_FACTOR,
    // Keep 1.5 tones below and 8.5 tones above the candidate to cover all 8 FSK tones.
    band_lower_tone_offset: FT8_BAND_LOWER_TONE_OFFSET,
    band_upper_tone_offset: FT8_BAND_UPPER_TONE_OFFSET,
    // Legacy post-normalization bitmetric scale retained for bit-exact parity.
    llr_scale_factor: FT8_LLR_SCALE_FACTOR,
};

pub const FT8_SPEC: ModeSpec = ModeSpec {
    name: "ft8",
    geometry: FT8_GEOMETRY,
    tuning: FT8_TUNING,
};
