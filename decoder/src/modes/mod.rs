pub mod ft8;

#[derive(Debug, Clone, Copy)]
pub struct FrameGeometry {
    pub sample_rate_hz: u32,
    pub symbol_samples: usize,
    pub tone_spacing_hz: f32,
    pub message_symbols: usize,
    pub sync_block_starts: &'static [usize],
    pub costas_pattern: &'static [usize],
    pub data_symbol_positions: &'static [usize],
    pub hop_samples: usize,
}

impl FrameGeometry {
    pub const fn sync_symbol_count(&self) -> usize {
        self.sync_block_starts.len() * self.costas_pattern.len()
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
    pub name: &'static str,
    pub geometry: FrameGeometry,
    pub tuning: SearchTuning,
}

impl ModeSpec {
    pub fn sync_fft_samples(&self) -> usize {
        self.geometry.symbol_samples * self.tuning.sync_fft_symbol_window
    }

    pub fn sync_step_samples(&self) -> usize {
        self.geometry.symbol_samples / self.tuning.sync_step_divisor
    }

    pub fn sync_step_seconds(&self) -> f32 {
        self.sync_step_samples() as f32 / self.geometry.sample_rate_hz as f32
    }

    pub fn baseband_rate_hz(&self) -> f32 {
        self.geometry.sample_rate_hz as f32 / self.tuning.downsample_factor as f32
    }

    pub fn baseband_samples(&self) -> usize {
        self.tuning.long_fft_samples / self.tuning.downsample_factor
    }

    pub fn baseband_symbol_samples(&self) -> usize {
        self.geometry.symbol_samples / self.tuning.downsample_factor
    }

    pub fn fft_bin_hz(&self) -> f32 {
        self.geometry.sample_rate_hz as f32 / self.tuning.long_fft_samples as f32
    }

    pub fn sync_bin_hz(&self) -> f32 {
        self.geometry.sample_rate_hz as f32 / self.sync_fft_samples() as f32
    }

    pub fn early41_samples(&self) -> usize {
        41 * self.tuning.early_block_samples
    }

    pub fn early47_samples(&self) -> usize {
        47 * self.tuning.early_block_samples
    }

    pub fn start_sample_from_dt(&self, dt_seconds: f32) -> isize {
        ((dt_seconds + self.tuning.nominal_start_seconds) * self.geometry.sample_rate_hz as f32
            + 1.0)
            .trunc() as isize
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
    for &block_start in geometry.sync_block_starts {
        for (offset, tone) in geometry.costas_pattern.iter().copied().enumerate() {
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
    for &block_start in geometry.sync_block_starts {
        if block_start + geometry.costas_pattern.len() > channel_symbols.len() {
            return None;
        }
        for (offset, tone) in geometry.costas_pattern.iter().copied().enumerate() {
            channel_symbols[block_start + offset] = tone as u8;
        }
    }
    for (symbol, &position) in data_symbols.iter().zip(geometry.data_symbol_positions.iter()) {
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
    const MOCK_DATA_POSITIONS: [usize; 4] = [2, 5, 6, 7];
    const MOCK_GEOMETRY: FrameGeometry = FrameGeometry {
        sample_rate_hz: 4_000,
        symbol_samples: 160,
        tone_spacing_hz: 12.5,
        message_symbols: 8,
        sync_block_starts: &MOCK_SYNC_BLOCKS,
        costas_pattern: &MOCK_COSTAS,
        data_symbol_positions: &MOCK_DATA_POSITIONS,
        hop_samples: 20,
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
        name: "mock",
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
        assert_eq!(MOCK_SPEC.start_sample_from_dt(0.0), 1_000);
    }
}
