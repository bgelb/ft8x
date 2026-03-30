use super::{FrameGeometry, ModeSpec, SearchTuning};
use crate::protocol::FTX_DATA_SYMBOLS;

pub const FT8_SAMPLE_RATE: u32 = 12_000;
pub const FT8_SYMBOL_SAMPLES: usize = 1_920;
pub const FT8_TONE_SPACING_HZ: f32 = 6.25;
pub const FT8_MESSAGE_SYMBOLS: usize = 79;
pub const FT8_PAYLOAD_SYMBOLS: usize = FTX_DATA_SYMBOLS;
pub const FT8_SYNC_BLOCK_STARTS: [usize; 3] = [0, 36, 72];
pub const FT8_COSTAS: [usize; 7] = [3, 1, 4, 0, 6, 5, 2];
pub const FT8_DATA_POSITIONS: [usize; FT8_PAYLOAD_SYMBOLS] = [
    7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
    31, 32, 33, 34, 35, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61,
    62, 63, 64, 65, 66, 67, 68, 69, 70, 71,
];
pub const FT8_HOP_SAMPLES: usize = 240;
pub const FT8_HOPS_PER_SYMBOL: usize = FT8_SYMBOL_SAMPLES / FT8_HOP_SAMPLES;

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
    long_fft_samples: 192_000,
    downsample_factor: 60,
    sync_fft_symbol_window: 2,
    sync_step_divisor: 4,
    sync_max_lag: 62,
    sync_local_lag: 10,
    sync_threshold: 1.6,
    sync_early_threshold: 2.0,
    sync_guard_bins: 12,
    sync_power_scale: 1.0 / 300.0,
    sync_baseline_percentile: 0.40,
    sync_baseline_floor: 1e-6,
    nominal_start_seconds: 0.5,
    baseband_taper_len: 100,
    baseband_valid_samples: 2_812,
    subtract_filter_samples: 4_000,
    early_block_samples: 3_456,
    subtraction_refine_cutoff_seconds: 0.396,
    subtraction_refine_probe_step_samples: 90,
    nfqso_hz: 1_500.0,
    nfqso_priority_window_hz: 10.0,
    candidate_separation_hz: 4.0,
    legacy_candidate_separation_dt_seconds: 0.16,
    legacy_candidate_separation_tone_factor: 1.5,
    band_lower_tone_offset: 1.5,
    band_upper_tone_offset: 8.5,
    llr_scale_factor: 2.83,
};

pub const FT8_SPEC: ModeSpec = ModeSpec {
    name: "ft8",
    geometry: FT8_GEOMETRY,
    tuning: FT8_TUNING,
};
