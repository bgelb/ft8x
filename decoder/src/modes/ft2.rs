use super::{
    ChannelCoding, FrameGeometry, Mode, ModeSpec, RefineSpec, SearchSpec, SubtractionSpec,
    WaveformSpec,
};

pub const FT2_SAMPLE_RATE: u32 = 12_000;
pub const FT2_SYMBOL_SAMPLES: usize = 160;
pub const FT2_TONE_SPACING_HZ: f32 = FT2_SAMPLE_RATE as f32 / FT2_SYMBOL_SAMPLES as f32;
pub const FT2_MESSAGE_SYMBOLS: usize = 144;
pub const FT2_SYNC_SYMBOLS: usize = 16;
pub const FT2_SYNC_PATTERN: [usize; 16] = [0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0];
pub const FT2_SYNC_PATTERNS: [&[usize]; 1] = [&FT2_SYNC_PATTERN];
pub const FT2_SYNC_BLOCK_STARTS: [usize; 1] = [0];
pub const FT2_DATA_SYMBOL_GROUP_STARTS: [usize; 1] = [16];
pub const FT2_HOP_SAMPLES: usize = FT2_SYMBOL_SAMPLES / 4;
pub const FT2_DATA_SYMBOLS: usize = 128;
pub const FT2_DATA_POSITIONS: [usize; FT2_DATA_SYMBOLS] = build_ft2_data_positions();

pub const FT2_PADDED_INPUT_SAMPLES: usize = 30_000;
pub const FT2_COARSE_FFT_SAMPLES: usize = 400;
pub const FT2_COARSE_FFT_BINS: usize = FT2_COARSE_FFT_SAMPLES / 2;
pub const FT2_COARSE_STEP_SAMPLES: usize = FT2_SYMBOL_SAMPLES / 4;
pub const FT2_COARSE_FRAME_COUNT: usize = FT2_PADDED_INPUT_SAMPLES / FT2_COARSE_STEP_SAMPLES - 3;
pub const FT2_DOWNSAMPLE_FACTOR: usize = 16;
pub const FT2_BASEBAND_FFT_SAMPLES: usize = FT2_PADDED_INPUT_SAMPLES / FT2_DOWNSAMPLE_FACTOR;
pub const FT2_BASEBAND_SYMBOL_SAMPLES: usize = FT2_SYMBOL_SAMPLES / FT2_DOWNSAMPLE_FACTOR;

pub const FT2_LONG_INPUT_SAMPLES: usize = FT2_PADDED_INPUT_SAMPLES;
pub const FT2_LONG_FFT_SAMPLES: usize = FT2_PADDED_INPUT_SAMPLES;
pub const FT2_BASEBAND_VALID_SAMPLES: usize = FT2_BASEBAND_FFT_SAMPLES;
pub const FT2_EARLY_BLOCK_SAMPLES: usize = FT2_SYMBOL_SAMPLES;

pub const FT2_COARSE_MIN_FREQ_HZ: f32 = 375.0;
pub const FT2_COARSE_MAX_FREQ_HZ: f32 = 3_000.0;
pub const FT2_COARSE_THRESHOLD: f32 = 1.2;
pub const FT2_BASELINE_FLOOR: f32 = 1e-6;
pub const FT2_BASELINE_PERCENTILE: f32 = 0.30;
pub const FT2_SEARCH_POWER_SCALE: f32 = 1.0 / 300.0;

pub const FT2_FINE_FREQ_SWEEP_MIN_HZ: i32 = -30;
pub const FT2_FINE_FREQ_SWEEP_MAX_HZ: i32 = 30;
pub const FT2_FINE_TIME_STEPS: usize = 375;
pub const FT2_REQUIRED_SYNC_MATCHES: usize = 10;

pub const FT2_BP_MAX_ITERS: usize = 40;
pub const FT2_OSD_PARITY_TAIL_BITS: usize = 12;
pub const FT2_OSD_NTHETA_MEDIUM: usize = 4;
pub const FT2_OSD_NTHETA_DEEP: usize = 4;

pub const FT2_SIGNAL_MODULATION_INDEX: f32 = 0.8;
pub const FT2_LLR_VARIANCE_SCALE: f32 = FT2_SIGNAL_MODULATION_INDEX * FT2_SIGNAL_MODULATION_INDEX;
pub const FT2_LLR_NORMALIZATION: f32 = 2.0;
pub const FT2_REFERENCE_SAMPLE_RATE_HZ: f32 = 750.0;
pub const FT2_OSD_MRB_SEARCH_EXTRA_COLUMNS: usize = 20;

pub const FT2_PLATANH_LINEAR_LIMIT: f32 = 0.664;
pub const FT2_PLATANH_LINEAR_SCALE: f32 = 0.83;
pub const FT2_PLATANH_SEGMENT_1_LIMIT: f32 = 0.9217;
pub const FT2_PLATANH_SEGMENT_1_OFFSET: f32 = 0.4064;
pub const FT2_PLATANH_SEGMENT_1_SCALE: f32 = 0.322;
pub const FT2_PLATANH_SEGMENT_2_LIMIT: f32 = 0.9951;
pub const FT2_PLATANH_SEGMENT_2_OFFSET: f32 = 0.8378;
pub const FT2_PLATANH_SEGMENT_2_SCALE: f32 = 0.0524;
pub const FT2_PLATANH_SEGMENT_3_LIMIT: f32 = 0.9998;
pub const FT2_PLATANH_SEGMENT_3_OFFSET: f32 = 0.9914;
pub const FT2_PLATANH_SEGMENT_3_SCALE: f32 = 0.0012;
pub const FT2_PLATANH_CLAMP: f32 = 7.0;

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

pub const FT2_WAVEFORM: WaveformSpec = WaveformSpec {
    default_frequency_hz: 1_000.0,
    default_start_seconds: 0.0,
    default_total_seconds: 2.5,
    default_amplitude: 0.8,
};

pub const FT2_SEARCH: SearchSpec = SearchSpec {
    long_input_samples: FT2_LONG_INPUT_SAMPLES,
    long_fft_samples: FT2_LONG_FFT_SAMPLES,
    downsample_factor: FT2_DOWNSAMPLE_FACTOR,
    sync_fft_symbol_window: 1,
    sync_step_divisor: 4,
    sync_max_lag: 0,
    sync_local_lag: 0,
    sync_threshold: FT2_COARSE_THRESHOLD,
    sync_early_threshold: FT2_COARSE_THRESHOLD,
    sync_guard_bins: 1,
    sync_power_scale: FT2_SEARCH_POWER_SCALE,
    sync_baseline_percentile: FT2_BASELINE_PERCENTILE,
    sync_baseline_floor: FT2_BASELINE_FLOOR,
    nfqso_hz: 1_500.0,
    nfqso_priority_window_hz: 20.0,
    candidate_separation_hz: FT2_TONE_SPACING_HZ,
    legacy_candidate_separation_dt_seconds: 0.04,
    legacy_candidate_separation_tone_factor: 1.0,
    band_lower_tone_offset: 0.5,
    band_upper_tone_offset: 1.5,
};

pub const FT2_REFINE: RefineSpec = RefineSpec {
    nominal_start_seconds: 0.0,
    baseband_taper_len: 0,
    baseband_valid_samples: FT2_BASEBAND_VALID_SAMPLES,
    early_block_samples: FT2_EARLY_BLOCK_SAMPLES,
    refine_residual_step_hz: 1.0,
    llr_scale_factor: 2.0,
};

pub const FT2_SUBTRACTION: SubtractionSpec = SubtractionSpec {
    filter_samples: 512,
    refine_cutoff_seconds: 0.0,
    refine_probe_step_samples: 0,
};

pub const FT2_SPEC: ModeSpec = ModeSpec {
    mode: Mode::Ft2,
    coding: FT2_CODING,
    geometry: FT2_GEOMETRY,
    waveform: FT2_WAVEFORM,
    search: FT2_SEARCH,
    refine: FT2_REFINE,
    subtraction: FT2_SUBTRACTION,
};
