use super::{
    ChannelCoding, FrameGeometry, Mode, ModeSpec, RefineSpec, SearchSpec, SubtractionSpec,
    WaveformSpec,
};

pub const FT4_SAMPLE_RATE: u32 = 12_000;
pub const FT4_SYMBOL_SAMPLES: usize = 576;
pub const FT4_TONE_SPACING_HZ: f32 = FT4_SAMPLE_RATE as f32 / FT4_SYMBOL_SAMPLES as f32;
pub const FT4_SYNC_BLOCK_COUNT: usize = 4;
pub const FT4_SYNC_LEN: usize = 4;
pub const FT4_DATA_SYMBOLS_PER_GROUP: usize = 29;
pub const FT4_DATA_SYMBOLS: usize = FT4_DATA_SYMBOLS_PER_GROUP * 3;
pub const FT4_MESSAGE_SYMBOLS: usize = FT4_DATA_SYMBOLS + FT4_SYNC_BLOCK_COUNT * FT4_SYNC_LEN;
pub const FT4_SYNC_A: [usize; FT4_SYNC_LEN] = [0, 1, 3, 2];
pub const FT4_SYNC_B: [usize; FT4_SYNC_LEN] = [1, 0, 2, 3];
pub const FT4_SYNC_C: [usize; FT4_SYNC_LEN] = [2, 3, 1, 0];
pub const FT4_SYNC_D: [usize; FT4_SYNC_LEN] = [3, 2, 0, 1];
pub const FT4_RVEC: [u8; 77] = [
    0, 1, 0, 0, 1, 0, 1, 0, 0, 1, 0, 1, 1, 1, 1, 0, 1, 0, 0, 0, 1, 0, 0, 1, 1, 0, 1, 1, 0, 1, 0, 0,
    1, 0, 1, 1, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 1, 0, 0, 1, 1, 1, 1, 0, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1,
    1, 0, 1, 1, 1, 1, 1, 0, 0, 0, 1, 0, 1,
];
pub const FT4_SYNC_PATTERNS: [&[usize]; FT4_SYNC_BLOCK_COUNT] =
    [&FT4_SYNC_A, &FT4_SYNC_B, &FT4_SYNC_C, &FT4_SYNC_D];
pub const FT4_SYNC_BLOCK_STARTS: [usize; FT4_SYNC_BLOCK_COUNT] = [0, 33, 66, 99];
pub const FT4_DATA_SYMBOL_GROUP_STARTS: [usize; 3] = [4, 37, 70];
pub const FT4_HOP_SAMPLES: usize = FT4_SYMBOL_SAMPLES / 4;
pub const FT4_DATA_POSITIONS: [usize; FT4_DATA_SYMBOLS] = build_ft4_data_positions();

const FT4_LONG_INPUT_SAMPLES: usize = 21 * 3_456;
const FT4_LONG_FFT_SAMPLES: usize = FT4_LONG_INPUT_SAMPLES;
const FT4_BASEBAND_VALID_SAMPLES: usize = FT4_LONG_FFT_SAMPLES / 18;
const FT4_EARLY_BLOCK_SAMPLES: usize = FT4_SYMBOL_SAMPLES;

const fn is_ft4_sync_symbol(symbol_index: usize) -> bool {
    let mut block = 0;
    while block < FT4_SYNC_BLOCK_STARTS.len() {
        let start = FT4_SYNC_BLOCK_STARTS[block];
        if symbol_index >= start && symbol_index < start + FT4_SYNC_LEN {
            return true;
        }
        block += 1;
    }
    false
}

const fn build_ft4_data_positions() -> [usize; FT4_DATA_SYMBOLS] {
    let mut positions = [0usize; FT4_DATA_SYMBOLS];
    let mut symbol_index = 0usize;
    let mut out = 0usize;
    while symbol_index < FT4_MESSAGE_SYMBOLS {
        if !is_ft4_sync_symbol(symbol_index) {
            positions[out] = symbol_index;
            out += 1;
        }
        symbol_index += 1;
    }
    positions
}

pub const FT4_GEOMETRY: FrameGeometry = FrameGeometry {
    sample_rate_hz: FT4_SAMPLE_RATE,
    symbol_samples: FT4_SYMBOL_SAMPLES,
    tone_spacing_hz: FT4_TONE_SPACING_HZ,
    message_symbols: FT4_MESSAGE_SYMBOLS,
    sync_block_starts: &FT4_SYNC_BLOCK_STARTS,
    sync_patterns: &FT4_SYNC_PATTERNS,
    data_symbol_positions: &FT4_DATA_POSITIONS,
    data_symbol_group_starts: &FT4_DATA_SYMBOL_GROUP_STARTS,
    hop_samples: FT4_HOP_SAMPLES,
};

pub const FT4_CODING: ChannelCoding = ChannelCoding {
    message_bits: 77,
    crc_bits: 14,
    info_bits: 91,
    codeword_bits: 174,
    bits_per_symbol: 2,
};

pub const FT4_WAVEFORM: WaveformSpec = WaveformSpec {
    default_frequency_hz: 1_000.0,
    default_start_seconds: 0.5,
    default_total_seconds: 7.5,
    default_amplitude: 0.8,
};

pub const FT4_SEARCH: SearchSpec = SearchSpec {
    long_input_samples: FT4_LONG_INPUT_SAMPLES,
    long_fft_samples: FT4_LONG_FFT_SAMPLES,
    downsample_factor: 18,
    sync_fft_symbol_window: 4,
    sync_step_divisor: 1,
    sync_max_lag: 0,
    sync_local_lag: 0,
    sync_threshold: 1.2,
    sync_early_threshold: 1.2,
    sync_guard_bins: 2,
    sync_power_scale: 1.0 / 300.0,
    sync_baseline_percentile: 0.40,
    sync_baseline_floor: 1e-6,
    nfqso_hz: 1_500.0,
    nfqso_priority_window_hz: 20.0,
    candidate_separation_hz: FT4_TONE_SPACING_HZ,
    legacy_candidate_separation_dt_seconds: 0.03,
    legacy_candidate_separation_tone_factor: 1.0,
    band_lower_tone_offset: 1.5,
    band_upper_tone_offset: 3.5,
};

pub const FT4_REFINE: RefineSpec = RefineSpec {
    nominal_start_seconds: 0.5,
    baseband_taper_len: 0,
    baseband_valid_samples: FT4_BASEBAND_VALID_SAMPLES,
    early_block_samples: FT4_EARLY_BLOCK_SAMPLES,
    refine_residual_step_hz: 1.0,
    llr_scale_factor: 2.83,
};

pub const FT4_SUBTRACTION: SubtractionSpec = SubtractionSpec {
    filter_samples: 1_400,
    refine_cutoff_seconds: 0.0,
    refine_probe_step_samples: 0,
};

pub const FT4_SPEC: ModeSpec = ModeSpec {
    mode: Mode::Ft4,
    coding: FT4_CODING,
    geometry: FT4_GEOMETRY,
    waveform: FT4_WAVEFORM,
    search: FT4_SEARCH,
    refine: FT4_REFINE,
    subtraction: FT4_SUBTRACTION,
};
