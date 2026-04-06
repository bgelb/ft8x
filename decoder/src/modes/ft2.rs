use super::{ChannelCoding, FrameGeometry, Mode, ModeSpec, SearchTuning};

pub const FT2_SAMPLE_RATE: u32 = 12_000;
pub const FT2_SYMBOL_SAMPLES: usize = 160;
pub const FT2_TONE_SPACING_HZ: f32 = FT2_SAMPLE_RATE as f32 / FT2_SYMBOL_SAMPLES as f32;
pub const FT2_MESSAGE_SYMBOLS: usize = 144;
pub const FT2_SYNC_PATTERN: [usize; 16] = [0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0];
pub const FT2_SYNC_PATTERNS: [&[usize]; 1] = [&FT2_SYNC_PATTERN];
pub const FT2_SYNC_BLOCK_STARTS: [usize; 1] = [0];
pub const FT2_DATA_SYMBOL_GROUP_STARTS: [usize; 1] = [16];
pub const FT2_HOP_SAMPLES: usize = FT2_SYMBOL_SAMPLES / 4;
pub const FT2_DATA_SYMBOLS: usize = 128;
pub const FT2_DATA_POSITIONS: [usize; FT2_DATA_SYMBOLS] = build_ft2_data_positions();

const FT2_LONG_INPUT_SAMPLES: usize = 30_000;
const FT2_LONG_FFT_SAMPLES: usize = FT2_LONG_INPUT_SAMPLES;
const FT2_BASEBAND_VALID_SAMPLES: usize = FT2_LONG_FFT_SAMPLES / 16;
const FT2_EARLY_BLOCK_SAMPLES: usize = FT2_SYMBOL_SAMPLES;

const fn build_ft2_data_positions() -> [usize; FT2_DATA_SYMBOLS] {
    let mut positions = [0usize; FT2_DATA_SYMBOLS];
    let mut index = 0usize;
    while index < FT2_DATA_SYMBOLS {
        positions[index] = 16 + index;
        index += 1;
    }
    positions
}

pub const FT2_GEOMETRY: FrameGeometry = FrameGeometry {
    sample_rate_hz: FT2_SAMPLE_RATE,
    symbol_samples: FT2_SYMBOL_SAMPLES,
    tone_spacing_hz: FT2_TONE_SPACING_HZ,
    message_symbols: FT2_MESSAGE_SYMBOLS,
    sync_block_starts: &FT2_SYNC_BLOCK_STARTS,
    sync_patterns: &FT2_SYNC_PATTERNS,
    data_symbol_positions: &FT2_DATA_POSITIONS,
    data_symbol_group_starts: &FT2_DATA_SYMBOL_GROUP_STARTS,
    hop_samples: FT2_HOP_SAMPLES,
};

pub const FT2_CODING: ChannelCoding = ChannelCoding {
    message_bits: 77,
    crc_bits: 13,
    info_bits: 90,
    codeword_bits: 128,
    bits_per_symbol: 1,
};

pub const FT2_TUNING: SearchTuning = SearchTuning {
    long_input_samples: FT2_LONG_INPUT_SAMPLES,
    long_fft_samples: FT2_LONG_FFT_SAMPLES,
    downsample_factor: 16,
    sync_fft_symbol_window: 1,
    sync_step_divisor: 4,
    sync_max_lag: 0,
    sync_local_lag: 0,
    sync_threshold: 1.2,
    sync_early_threshold: 1.2,
    sync_guard_bins: 1,
    sync_power_scale: 1.0 / 300.0,
    sync_baseline_percentile: 0.30,
    sync_baseline_floor: 1e-6,
    nominal_start_seconds: 0.0,
    baseband_taper_len: 0,
    baseband_valid_samples: FT2_BASEBAND_VALID_SAMPLES,
    subtract_filter_samples: 512,
    early_block_samples: FT2_EARLY_BLOCK_SAMPLES,
    subtraction_refine_cutoff_seconds: 0.0,
    subtraction_refine_probe_step_samples: 0,
    refine_residual_step_hz: 1.0,
    nfqso_hz: 1_500.0,
    nfqso_priority_window_hz: 20.0,
    candidate_separation_hz: FT2_TONE_SPACING_HZ,
    legacy_candidate_separation_dt_seconds: 0.04,
    legacy_candidate_separation_tone_factor: 1.0,
    band_lower_tone_offset: 0.5,
    band_upper_tone_offset: 1.5,
    llr_scale_factor: 2.0,
};

pub const FT2_SPEC: ModeSpec = ModeSpec {
    mode: Mode::Ft2,
    coding: FT2_CODING,
    geometry: FT2_GEOMETRY,
    tuning: FT2_TUNING,
};
