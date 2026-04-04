use serde::Deserialize;
use std::path::Path;

use crate::AppError;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub station: StationConfig,
    pub tx: TxConfig,
    pub queue: QueueConfig,
    pub fsm: FsmConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StationConfig {
    pub our_call: String,
    pub our_grid: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TxConfig {
    pub base_freq_hz: f32,
    pub drive_level: f32,
    pub playback_channels: usize,
    pub output_device: Option<String>,
    pub power_w: Option<f32>,
    pub tx_freq_min_hz: f32,
    pub tx_freq_max_hz: f32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueueConfig {
    pub auto_add_direct_calls_default: bool,
    pub ignore_direct_calls_from_recently_worked_default: bool,
    pub cq_enabled_default: bool,
    pub cq_percent_default: u8,
    pub use_compound_rr73_handoff_default: bool,
    pub no_message_retry_delay_seconds_default: u64,
    pub no_forward_retry_delay_seconds_default: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FsmConfig {
    pub rr73_enabled: bool,
    pub timeout_seconds: u64,
    pub send_grid: RetryThresholds,
    pub send_sig: RetryThresholds,
    pub send_sig_ack: RetryThresholds,
    pub send_rr73: NoFwdThreshold,
    pub send_rrr: RetryThresholds,
    pub send_73: RetryThresholds,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RetryThresholds {
    pub no_fwd: u32,
    pub no_msg: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NoFwdThreshold {
    pub no_fwd: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LoggingConfig {
    pub fsm_log_path: String,
    pub app_log_path: String,
}

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self, AppError> {
        let contents = std::fs::read_to_string(path)?;
        let mut config: Self = serde_json::from_str(&contents)?;
        config.station.our_call = normalize_call(&config.station.our_call);
        config.station.our_grid = config.station.our_grid.trim().to_uppercase();
        Ok(config)
    }

    pub fn validate_tx_freq_hz(&self, freq_hz: f32) -> bool {
        freq_hz.is_finite()
            && freq_hz >= self.tx.tx_freq_min_hz
            && freq_hz <= self.tx.tx_freq_max_hz
    }

    pub fn clamped_default_tx_freq_hz(&self) -> f32 {
        self.tx
            .base_freq_hz
            .clamp(self.tx.tx_freq_min_hz, self.tx.tx_freq_max_hz)
    }
}

fn normalize_call(value: &str) -> String {
    value.trim().to_uppercase()
}
