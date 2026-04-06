pub mod ft2;
pub mod ft4;
pub mod ft8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Mode {
    Ft8,
    Ft4,
    Ft2,
}

impl Mode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ft8 => "ft8",
            Self::Ft4 => "ft4",
            Self::Ft2 => "ft2",
        }
    }

    pub const fn spec(self) -> &'static ModeSpec {
        match self {
            Self::Ft8 => &ft8::FT8_SPEC,
            Self::Ft4 => &ft4::FT4_SPEC,
            Self::Ft2 => &ft2::FT2_SPEC,
        }
    }
}

impl Default for Mode {
    fn default() -> Self {
        Self::Ft8
    }
}

impl std::str::FromStr for Mode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "ft8" => Ok(Self::Ft8),
            "ft4" => Ok(Self::Ft4),
            "ft2" => Ok(Self::Ft2),
            other => Err(format!(
                "unsupported mode '{other}'; expected ft8, ft4, or ft2"
            )),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ChannelCoding {
    pub message_bits: usize,
    pub crc_bits: usize,
    pub info_bits: usize,
    pub codeword_bits: usize,
    pub bits_per_symbol: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct FrameGeometry {
    pub sample_rate_hz: u32,
    pub symbol_samples: usize,
    pub tone_spacing_hz: f32,
    pub message_symbols: usize,
    pub sync_block_starts: &'static [usize],
    pub sync_patterns: &'static [&'static [usize]],
    pub data_symbol_positions: &'static [usize],
    pub data_symbol_group_starts: &'static [usize],
    pub hop_samples: usize,
}

impl FrameGeometry {
    pub fn sync_symbol_count(&self) -> usize {
        self.sync_patterns.iter().map(|pattern| pattern.len()).sum()
    }

    pub const fn sync_block_count(&self) -> usize {
        self.sync_block_starts.len()
    }

    pub fn sync_pattern_len(&self) -> usize {
        self.sync_patterns.first().map_or(0, |pattern| pattern.len())
    }

    pub const fn hops_per_symbol(&self) -> usize {
        self.symbol_samples / self.hop_samples
    }

    pub const fn frame_samples(&self) -> usize {
        self.message_symbols * self.symbol_samples
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SearchTuning {
    pub long_input_samples: usize,
    pub long_fft_samples: usize,
    pub downsample_factor: usize,
    pub sync_fft_symbol_window: usize,
    pub sync_step_divisor: usize,
    pub sync_max_lag: isize,
    pub sync_local_lag: isize,
    pub sync_threshold: f32,
    pub sync_early_threshold: f32,
    pub sync_guard_bins: usize,
    pub sync_power_scale: f32,
    pub sync_baseline_percentile: f32,
    pub sync_baseline_floor: f32,
    pub nominal_start_seconds: f32,
    pub baseband_taper_len: usize,
    pub baseband_valid_samples: usize,
    pub subtract_filter_samples: usize,
    pub early_block_samples: usize,
    pub subtraction_refine_cutoff_seconds: f32,
    pub subtraction_refine_probe_step_samples: isize,
    pub refine_residual_step_hz: f32,
    pub nfqso_hz: f32,
    pub nfqso_priority_window_hz: f32,
    pub candidate_separation_hz: f32,
    pub legacy_candidate_separation_dt_seconds: f32,
    pub legacy_candidate_separation_tone_factor: f32,
    pub band_lower_tone_offset: f32,
    pub band_upper_tone_offset: f32,
    pub llr_scale_factor: f32,
}

#[derive(Debug, Clone, Copy)]
pub struct ModeSpec {
    pub mode: Mode,
    pub coding: ChannelCoding,
    pub geometry: FrameGeometry,
    pub tuning: SearchTuning,
}

impl ModeSpec {
    pub const fn nominal_start_seconds(&self) -> f32 {
        self.tuning.nominal_start_seconds
    }

    pub fn sync_fft_samples(&self) -> usize {
        self.geometry.symbol_samples * self.tuning.sync_fft_symbol_window
    }

    pub fn sync_step_samples(&self) -> usize {
        self.geometry.symbol_samples / self.tuning.sync_step_divisor
    }

    pub fn sync_step_seconds(&self) -> f32 {
        self.sync_step_samples() as f32 / self.geometry.sample_rate_hz as f32
    }

    pub fn nominal_start_sync_lag(&self) -> isize {
        (self.nominal_start_seconds() / self.sync_step_seconds()) as isize
    }

    pub fn nominal_start_sync_fraction(&self) -> f32 {
        self.nominal_start_seconds() / self.sync_step_seconds()
            - self.nominal_start_sync_lag() as f32
    }

    pub fn baseband_rate_hz(&self) -> f32 {
        self.geometry.sample_rate_hz as f32 / self.tuning.downsample_factor as f32
    }

    pub const fn baseband_samples(&self) -> usize {
        self.tuning.long_fft_samples / self.tuning.downsample_factor
    }

    pub const fn baseband_symbol_samples(&self) -> usize {
        self.geometry.symbol_samples / self.tuning.downsample_factor
    }

    pub fn fft_bin_hz(&self) -> f32 {
        self.geometry.sample_rate_hz as f32 / self.tuning.long_fft_samples as f32
    }

    pub fn sync_bin_hz(&self) -> f32 {
        self.geometry.sample_rate_hz as f32 / self.sync_fft_samples() as f32
    }

    pub const fn tone_count(&self) -> usize {
        1usize << self.coding.bits_per_symbol
    }

    pub fn sync_tone_bin_stride(&self) -> usize {
        (self.sync_fft_samples() / self.geometry.symbol_samples).max(1)
    }

    pub fn early41_samples(&self) -> usize {
        41 * self.tuning.early_block_samples
    }

    pub fn early47_samples(&self) -> usize {
        47 * self.tuning.early_block_samples
    }

    pub const fn baseband_taper_len(&self) -> usize {
        self.tuning.baseband_taper_len
    }

    pub const fn baseband_valid_samples(&self) -> usize {
        self.tuning.baseband_valid_samples
    }

    pub const fn data_symbol_groups(&self) -> usize {
        self.geometry.data_symbol_group_starts.len()
    }

    /// Number of LDPC code bits carried by one FT8-style data half of the frame.
    pub const fn codeword_half_bits(&self) -> usize {
        self.coding.codeword_bits / self.geometry.data_symbol_group_starts.len()
    }

    /// Number of payload symbols carried by one FT8-style data half of the frame.
    pub const fn groups_per_half(&self) -> usize {
        self.geometry.data_symbol_positions.len() / self.geometry.data_symbol_group_starts.len()
    }

    /// Legacy FT8 bitmetric loops index from the symbol just before each 29-symbol data half.
    pub fn bitmetric_half_start_symbols(&self) -> [usize; 2] {
        [
            self.geometry.data_symbol_group_starts[0] - 1,
            self.geometry.data_symbol_group_starts[1] - 1,
        ]
    }

    pub fn sync_tone_span_bins(&self) -> usize {
        self.geometry.sync_pattern_len()
    }

    pub fn start_seconds_from_dt(&self, dt_seconds: f32) -> f32 {
        dt_seconds + self.nominal_start_seconds()
    }

    pub fn dt_seconds_from_start(&self, start_seconds: f32) -> f32 {
        start_seconds - self.nominal_start_seconds()
    }

    pub fn candidate_start_seconds_from_lag(&self, lag: isize) -> f32 {
        self.start_seconds_from_dt(self.candidate_dt_seconds_from_lag(lag))
    }

    /// Convert a sync-search lag back into dt while keeping the nominal start offset centralized.
    pub fn candidate_dt_seconds_from_lag(&self, lag: isize) -> f32 {
        (lag as f32 - self.nominal_start_sync_fraction()) * self.sync_step_seconds()
    }

    /// Shared helper for the half-Hz residual probes used during candidate refinement.
    pub fn residual_hz_from_half_step(&self, step: isize) -> f32 {
        step as f32 * self.tuning.refine_residual_step_hz
    }

    /// Preserve the legacy sample rounding used by subtraction and debug candidate paths.
    pub fn start_sample_from_dt(&self, dt_seconds: f32) -> isize {
        ((self.start_seconds_from_dt(dt_seconds) * self.geometry.sample_rate_hz as f32 + 1.0)
            .trunc()) as isize
            - 1
    }

    pub fn band_low_hz(&self, freq_hz: f32) -> f32 {
        freq_hz - self.tuning.band_lower_tone_offset * self.geometry.tone_spacing_hz
    }

    pub fn band_high_hz(&self, freq_hz: f32) -> f32 {
        freq_hz + self.tuning.band_upper_tone_offset * self.geometry.tone_spacing_hz
    }
}

pub fn all_costas_positions(geometry: &FrameGeometry) -> Vec<(usize, usize)> {
    let mut positions = Vec::with_capacity(geometry.sync_symbol_count());
    for (&block_start, pattern) in geometry
        .sync_block_starts
        .iter()
        .zip(geometry.sync_patterns.iter().copied())
    {
        for (offset, tone) in pattern.iter().copied().enumerate() {
            positions.push((block_start + offset, tone));
        }
    }
    positions
}

pub fn populate_channel_symbols(
    channel_symbols: &mut [u8],
    geometry: &FrameGeometry,
    data_symbols: &[u8],
) -> Option<()> {
    if channel_symbols.len() != geometry.message_symbols
        || data_symbols.len() != geometry.data_symbol_positions.len()
    {
        return None;
    }

    channel_symbols.fill(0);
    for (&block_start, pattern) in geometry
        .sync_block_starts
        .iter()
        .zip(geometry.sync_patterns.iter().copied())
    {
        if block_start + pattern.len() > channel_symbols.len() {
            return None;
        }
        for (offset, tone) in pattern.iter().copied().enumerate() {
            channel_symbols[block_start + offset] = tone as u8;
        }
    }
    for (symbol, &position) in data_symbols
        .iter()
        .zip(geometry.data_symbol_positions.iter())
    {
        if position >= channel_symbols.len() {
            return None;
        }
        channel_symbols[position] = *symbol;
    }
    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const MOCK_SYNC_BLOCKS: [usize; 2] = [0, 3];
    const MOCK_COSTAS: [usize; 2] = [1, 0];
    const MOCK_SYNC_PATTERNS: [&[usize]; 2] = [&MOCK_COSTAS, &MOCK_COSTAS];
    const MOCK_DATA_POSITIONS: [usize; 4] = [2, 5, 6, 7];
    const MOCK_GROUP_STARTS: [usize; 2] = [2, 5];
    const MOCK_GEOMETRY: FrameGeometry = FrameGeometry {
        sample_rate_hz: 4_000,
        symbol_samples: 160,
        tone_spacing_hz: 12.5,
        message_symbols: 8,
        sync_block_starts: &MOCK_SYNC_BLOCKS,
        sync_patterns: &MOCK_SYNC_PATTERNS,
        data_symbol_positions: &MOCK_DATA_POSITIONS,
        data_symbol_group_starts: &MOCK_GROUP_STARTS,
        hop_samples: 20,
    };
    const MOCK_CODING: ChannelCoding = ChannelCoding {
        message_bits: 6,
        crc_bits: 2,
        info_bits: 8,
        codeword_bits: 12,
        bits_per_symbol: 3,
    };
    const MOCK_TUNING: SearchTuning = SearchTuning {
        long_input_samples: 4_000,
        long_fft_samples: 6_400,
        downsample_factor: 8,
        sync_fft_symbol_window: 2,
        sync_step_divisor: 4,
        sync_max_lag: 8,
        sync_local_lag: 3,
        sync_threshold: 1.0,
        sync_early_threshold: 2.0,
        sync_guard_bins: 4,
        sync_power_scale: 0.25,
        sync_baseline_percentile: 0.4,
        sync_baseline_floor: 1e-6,
        nominal_start_seconds: 0.25,
        baseband_taper_len: 8,
        baseband_valid_samples: 128,
        subtract_filter_samples: 160,
        early_block_samples: 96,
        subtraction_refine_cutoff_seconds: 0.125,
        subtraction_refine_probe_step_samples: 12,
        refine_residual_step_hz: 0.25,
        nfqso_hz: 900.0,
        nfqso_priority_window_hz: 5.0,
        candidate_separation_hz: 2.0,
        legacy_candidate_separation_dt_seconds: 0.1,
        legacy_candidate_separation_tone_factor: 1.5,
        band_lower_tone_offset: 1.0,
        band_upper_tone_offset: 3.0,
        llr_scale_factor: 2.0,
    };
    const MOCK_SPEC: ModeSpec = ModeSpec {
        mode: Mode::Ft8,
        coding: MOCK_CODING,
        geometry: MOCK_GEOMETRY,
        tuning: MOCK_TUNING,
    };

    #[test]
    fn all_costas_positions_follow_geometry() {
        let positions = all_costas_positions(&MOCK_GEOMETRY);
        assert_eq!(positions, vec![(0, 1), (1, 0), (3, 1), (4, 0)]);
    }

    #[test]
    fn populate_channel_symbols_uses_data_positions() {
        let mut channel_symbols = [0u8; 8];
        populate_channel_symbols(&mut channel_symbols, &MOCK_GEOMETRY, &[7, 6, 5, 4])
            .expect("populate");
        assert_eq!(channel_symbols, [1, 0, 7, 1, 0, 6, 5, 4]);
    }

    #[test]
    fn derived_timing_comes_from_mode_spec() {
        assert_eq!(MOCK_GEOMETRY.sync_symbol_count(), 4);
        assert_eq!(MOCK_GEOMETRY.hops_per_symbol(), 8);
        assert_eq!(MOCK_SPEC.sync_fft_samples(), 320);
        assert_eq!(MOCK_SPEC.sync_step_samples(), 40);
        assert_eq!(MOCK_SPEC.baseband_samples(), 800);
        assert_eq!(MOCK_SPEC.baseband_symbol_samples(), 20);
        assert_eq!(MOCK_SPEC.early41_samples(), 3_936);
        assert_eq!(MOCK_SPEC.early47_samples(), 4_512);
        assert_eq!(MOCK_SPEC.nominal_start_sync_lag(), 25);
        assert_eq!(MOCK_SPEC.nominal_start_sync_fraction(), 0.0);
        assert_eq!(MOCK_SPEC.codeword_half_bits(), 6);
        assert_eq!(MOCK_SPEC.groups_per_half(), 2);
        assert_eq!(MOCK_SPEC.bitmetric_half_start_symbols(), [1, 4]);
        assert_eq!(MOCK_SPEC.sync_tone_span_bins(), 2);
        assert_eq!(MOCK_SPEC.nominal_start_seconds(), 0.25);
        assert!((MOCK_SPEC.start_seconds_from_dt(0.1) - 0.35).abs() < 1e-6);
        assert!((MOCK_SPEC.dt_seconds_from_start(0.35) - 0.1).abs() < 1e-6);
        assert!((MOCK_SPEC.candidate_start_seconds_from_lag(3) - 0.28).abs() < 1e-6);
        assert!((MOCK_SPEC.candidate_dt_seconds_from_lag(3) - 0.03).abs() < 1e-6);
        assert_eq!(MOCK_SPEC.residual_hz_from_half_step(2), 0.5);
        assert_eq!(MOCK_SPEC.start_sample_from_dt(0.0), 1_000);
    }
}
