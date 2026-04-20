mod mchf;

pub mod audio {
    pub use audiolib::{
        AudioDevice, AudioStreamConfig, CaptureStats, Error, PreparedMonoPlayback,
        PreparedMonoPlaybackWriter, Result, SampleStream, list_input_devices, list_output_devices,
        play_interleaved_samples_i16_until, play_mono_samples, play_mono_samples_until, play_tone,
        prepare_mono_playback, prepare_mono_playback_writer,
    };
}

pub use mchf::{
    DEFAULT_MCHF_AUDIO_HINTS, DEFAULT_MCHF_PORT, MCHF_BAUD_RATE, Mchf, MchfConfig,
    decode_ft817_frequency_hz, decode_ft817_mode, encode_ft817_frequency_hz, encode_ft817_mode,
};

use serialport::{ClearBuffer, SerialPort};
use std::fmt;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::thread;
use std::time::{Duration, Instant};

pub const K3S_BAUD_RATE: u32 = 38_400;
pub const DEFAULT_TIMEOUT: Duration = Duration::from_millis(500);
pub const DEFAULT_K3S_PORT: &str = "/dev/serial/by-id/usb-FTDI_FT232R_USB_UART_AK04X2PO-if00-port0";
pub const DEFAULT_K3S_AUDIO_HINTS: &[&str] = &["CARD=CODEC", "USB Audio CODEC", "DEV=0"];

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("serial error: {0}")]
    Serial(#[from] serialport::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("timed out waiting for response to {command}")]
    Timeout { command: String },
    #[error("radio returned busy/limited-access for {command}")]
    Busy { command: String },
    #[error("unexpected response to {command}: {response}")]
    UnexpectedResponse { command: String, response: String },
    #[error("invalid frequency {0} Hz")]
    InvalidFrequency(u64),
    #[error("unsupported operation: {operation}")]
    Unsupported { operation: String },
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RigKind {
    K3s,
    Mchf,
}

impl RigKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::K3s => "k3s",
            Self::Mchf => "mchf",
        }
    }

    pub fn audio_hints(self) -> &'static [&'static str] {
        match self {
            Self::K3s => DEFAULT_K3S_AUDIO_HINTS,
            Self::Mchf => DEFAULT_MCHF_AUDIO_HINTS,
        }
    }
}

impl fmt::Display for RigKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for RigKind {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "k3s" | "k3" => Ok(Self::K3s),
            "mchf" => Ok(Self::Mchf),
            other => Err(format!("unsupported rig kind: {other}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Lsb,
    Usb,
    Cw,
    Fm,
    Am,
    Data,
    CwRev,
    DataRev,
}

impl Mode {
    pub fn code(self) -> u8 {
        match self {
            Self::Lsb => 1,
            Self::Usb => 2,
            Self::Cw => 3,
            Self::Fm => 4,
            Self::Am => 5,
            Self::Data => 6,
            Self::CwRev => 7,
            Self::DataRev => 9,
        }
    }

    pub fn from_code(code: u8) -> Option<Self> {
        Some(match code {
            1 => Self::Lsb,
            2 => Self::Usb,
            3 => Self::Cw,
            4 => Self::Fm,
            5 => Self::Am,
            6 => Self::Data,
            7 => Self::CwRev,
            9 => Self::DataRev,
            _ => return None,
        })
    }
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            Self::Lsb => "lsb",
            Self::Usb => "usb",
            Self::Cw => "cw",
            Self::Fm => "fm",
            Self::Am => "am",
            Self::Data => "data",
            Self::CwRev => "cw-rev",
            Self::DataRev => "data-rev",
        };
        f.write_str(text)
    }
}

impl FromStr for Mode {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "lsb" => Ok(Self::Lsb),
            "usb" => Ok(Self::Usb),
            "cw" => Ok(Self::Cw),
            "fm" => Ok(Self::Fm),
            "am" => Ok(Self::Am),
            "data" => Ok(Self::Data),
            "cw-rev" | "cw_rev" | "cwrev" => Ok(Self::CwRev),
            "data-rev" | "data_rev" | "datarev" => Ok(Self::DataRev),
            other => Err(format!("unsupported mode: {other}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Band {
    M160,
    M80,
    M60,
    M40,
    M30,
    M20,
    M17,
    M15,
    M12,
    M10,
    M6,
    Xvtr(u8),
}

impl Band {
    pub fn number(self) -> u8 {
        match self {
            Self::M160 => 0,
            Self::M80 => 1,
            Self::M60 => 2,
            Self::M40 => 3,
            Self::M30 => 4,
            Self::M20 => 5,
            Self::M17 => 6,
            Self::M15 => 7,
            Self::M12 => 8,
            Self::M10 => 9,
            Self::M6 => 10,
            Self::Xvtr(index) => 15 + index,
        }
    }

    pub fn from_number(number: u8) -> Option<Self> {
        Some(match number {
            0 => Self::M160,
            1 => Self::M80,
            2 => Self::M60,
            3 => Self::M40,
            4 => Self::M30,
            5 => Self::M20,
            6 => Self::M17,
            7 => Self::M15,
            8 => Self::M12,
            9 => Self::M10,
            10 => Self::M6,
            16..=24 => Self::Xvtr(number - 15),
            _ => return None,
        })
    }

    pub fn from_frequency_hz(frequency_hz: u64) -> Option<Self> {
        Some(match frequency_hz {
            1_800_000..=2_000_000 => Self::M160,
            3_500_000..=4_000_000 => Self::M80,
            5_000_000..=5_600_000 => Self::M60,
            7_000_000..=7_300_000 => Self::M40,
            10_000_000..=10_200_000 => Self::M30,
            14_000_000..=14_400_000 => Self::M20,
            18_000_000..=18_300_000 => Self::M17,
            21_000_000..=21_500_000 => Self::M15,
            24_800_000..=25_000_000 => Self::M12,
            28_000_000..=29_800_000 => Self::M10,
            50_000_000..=54_000_000 => Self::M6,
            _ => return None,
        })
    }
}

impl fmt::Display for Band {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::M160 => f.write_str("160m"),
            Self::M80 => f.write_str("80m"),
            Self::M60 => f.write_str("60m"),
            Self::M40 => f.write_str("40m"),
            Self::M30 => f.write_str("30m"),
            Self::M20 => f.write_str("20m"),
            Self::M17 => f.write_str("17m"),
            Self::M15 => f.write_str("15m"),
            Self::M12 => f.write_str("12m"),
            Self::M10 => f.write_str("10m"),
            Self::M6 => f.write_str("6m"),
            Self::Xvtr(index) => write!(f, "xvtr{index}"),
        }
    }
}

impl FromStr for Band {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let value = s.trim().to_ascii_lowercase();
        let normalized = value.strip_suffix('m').unwrap_or(&value);
        match normalized {
            "160" => Ok(Self::M160),
            "80" => Ok(Self::M80),
            "60" => Ok(Self::M60),
            "40" => Ok(Self::M40),
            "30" => Ok(Self::M30),
            "20" => Ok(Self::M20),
            "17" => Ok(Self::M17),
            "15" => Ok(Self::M15),
            "12" => Ok(Self::M12),
            "10" => Ok(Self::M10),
            "6" => Ok(Self::M6),
            _ => {
                if let Some(rest) = normalized.strip_prefix("xvtr") {
                    let index: u8 = rest.parse().map_err(|_| format!("unsupported band: {s}"))?;
                    if (1..=9).contains(&index) {
                        return Ok(Self::Xvtr(index));
                    }
                }
                Err(format!("unsupported band: {s}"))
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Antenna {
    Ant1,
    Ant2,
}

impl Antenna {
    pub fn code(self) -> u8 {
        match self {
            Self::Ant1 => 1,
            Self::Ant2 => 2,
        }
    }

    pub fn from_code(code: u8) -> Option<Self> {
        Some(match code {
            1 => Self::Ant1,
            2 => Self::Ant2,
            _ => return None,
        })
    }
}

impl fmt::Display for Antenna {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ant1 => f.write_str("ant1"),
            Self::Ant2 => f.write_str("ant2"),
        }
    }
}

impl FromStr for Antenna {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "1" | "ant1" | "antenna1" => Ok(Self::Ant1),
            "2" | "ant2" | "antenna2" => Ok(Self::Ant2),
            other => Err(format!("unsupported antenna: {other}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignalLevel {
    pub coarse: u16,
    pub high_res: Option<u16>,
    pub bar_graph: Option<u8>,
    pub receiving: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BarGraphReading {
    pub level: u8,
    pub receiving: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxMeterMode {
    Rf,
    Alc,
}

impl TxMeterMode {
    fn code(self) -> u8 {
        match self {
            Self::Rf => 0,
            Self::Alc => 1,
        }
    }

    fn from_code(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::Rf),
            1 => Some(Self::Alc),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RigState {
    pub frequency_hz: u64,
    pub mode: Mode,
    pub band: Band,
    pub antenna: Antenna,
    pub signal: SignalLevel,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RigPowerSetting {
    pub id: String,
    pub label: String,
    pub nominal_watts: Option<f32>,
}

impl RigPowerSetting {
    pub fn new(
        id: impl Into<String>,
        label: impl Into<String>,
        nominal_watts: Option<f32>,
    ) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            nominal_watts,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RigPowerState {
    Continuous {
        current_watts: Option<f32>,
        min_watts: f32,
        max_watts: f32,
    },
    Discrete {
        current_id: Option<String>,
        current_label: Option<String>,
        settings: Vec<RigPowerSetting>,
        can_set: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct RigTelemetry {
    pub rx_s_meter: Option<f32>,
    pub tx_forward_power_w: Option<f32>,
    pub tx_swr: Option<f32>,
    pub tx_alc: Option<f32>,
    pub bar_graph: Option<u8>,
}

#[derive(Debug, Clone)]
pub struct RigSnapshot {
    pub kind: RigKind,
    pub frequency_hz: u64,
    pub mode: Mode,
    pub band: Band,
    pub transmitting: bool,
    pub telemetry: RigTelemetry,
    pub power: RigPowerState,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RigPowerRequest {
    ContinuousWatts(f32),
    SettingId(String),
}

#[derive(Debug, Clone)]
pub struct DetectedRig {
    pub kind: RigKind,
    pub port_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct RigConnectionConfig {
    pub kind: RigKind,
    pub port_path: Option<PathBuf>,
    pub timeout: Duration,
}

impl RigConnectionConfig {
    pub fn default_for(kind: RigKind) -> Self {
        Self {
            kind,
            port_path: None,
            timeout: DEFAULT_TIMEOUT,
        }
    }
}

#[derive(Debug, Clone)]
pub struct K3sConfig {
    pub port_path: PathBuf,
    pub baud_rate: u32,
    pub timeout: Duration,
    pub rts: bool,
    pub dtr: bool,
}

impl Default for K3sConfig {
    fn default() -> Self {
        let default_path = if Path::new(DEFAULT_K3S_PORT).exists() {
            PathBuf::from(DEFAULT_K3S_PORT)
        } else {
            PathBuf::from("/dev/ttyUSB0")
        };
        Self {
            port_path: default_path,
            baud_rate: K3S_BAUD_RATE,
            timeout: DEFAULT_TIMEOUT,
            rts: false,
            dtr: false,
        }
    }
}

pub struct K3s {
    port: Box<dyn SerialPort>,
    timeout: Duration,
}

impl K3s {
    pub fn connect(config: K3sConfig) -> Result<Self> {
        let mut port = serialport::new(
            config.port_path.to_string_lossy().into_owned(),
            config.baud_rate,
        )
        .timeout(Duration::from_millis(50))
        .dtr_on_open(config.dtr)
        .open()?;
        // On the attached K3S/FTDI interface, asserting RTS mutes receive audio and likely keys
        // a PTT-related control path. Keep both modem-control outputs low by default for RX-only
        // CAT use unless a caller explicitly overrides them in K3sConfig.
        port.write_request_to_send(config.rts)?;
        port.write_data_terminal_ready(config.dtr)?;
        Ok(Self {
            port,
            timeout: config.timeout,
        })
    }

    pub fn get_frequency_hz(&mut self) -> Result<u64> {
        let response = self.query("FA;", &["FA"])?;
        parse_prefixed_u64("FA", &response)
    }

    pub fn set_frequency_hz(&mut self, frequency_hz: u64) -> Result<()> {
        if frequency_hz > 99_999_999_999 {
            return Err(Error::InvalidFrequency(frequency_hz));
        }
        self.send_set(&format!("FA{frequency_hz:011};"))?;
        Ok(())
    }

    pub fn get_mode(&mut self) -> Result<Mode> {
        let response = self.query("MD;", &["MD"])?;
        let code = parse_prefixed_u8("MD", &response)?;
        Mode::from_code(code).ok_or_else(|| Error::UnexpectedResponse {
            command: "MD;".to_string(),
            response,
        })
    }

    pub fn set_mode(&mut self, mode: Mode) -> Result<()> {
        self.send_set(&format!("MD{};", mode.code()))
    }

    pub fn get_band(&mut self) -> Result<Band> {
        let response = self.query("BN;", &["BN"])?;
        let code = parse_prefixed_u8("BN", &response)?;
        Band::from_number(code).ok_or_else(|| Error::UnexpectedResponse {
            command: "BN;".to_string(),
            response,
        })
    }

    pub fn set_band(&mut self, band: Band) -> Result<()> {
        self.send_set(&format!("BN{:02};", band.number()))?;
        thread::sleep(Duration::from_millis(350));
        Ok(())
    }

    pub fn get_antenna(&mut self) -> Result<Antenna> {
        let response = self.query("AN;", &["AN"])?;
        let code = parse_prefixed_u8("AN", &response)?;
        Antenna::from_code(code).ok_or_else(|| Error::UnexpectedResponse {
            command: "AN;".to_string(),
            response,
        })
    }

    pub fn set_antenna(&mut self, antenna: Antenna) -> Result<()> {
        self.send_set(&format!("AN{};", antenna.code()))
    }

    pub fn get_signal_level(&mut self) -> Result<SignalLevel> {
        let high_res = self
            .query("SMH;", &["SMH"])
            .ok()
            .and_then(|rsp| parse_prefixed_u16("SMH", &rsp).ok());
        let coarse_rsp = self.query("SM;", &["SM"])?;
        let coarse = parse_prefixed_u16("SM", &coarse_rsp)?;
        let bar_graph = self.get_bar_graph()?;
        Ok(SignalLevel {
            coarse,
            high_res,
            bar_graph: Some(bar_graph.level),
            receiving: bar_graph.receiving,
        })
    }

    pub fn get_bar_graph(&mut self) -> Result<BarGraphReading> {
        let bar_rsp = self.query("BG;", &["BG"])?;
        let (level, receiving) = parse_bg(&bar_rsp)?;
        Ok(BarGraphReading { level, receiving })
    }

    pub fn get_configured_power_w(&mut self) -> Result<f32> {
        let response = self.query("PC;", &["PC"])?;
        parse_power_control(&response)
    }

    pub fn set_configured_power_w(&mut self, watts: f32) -> Result<()> {
        if !(0.1..=110.0).contains(&watts) {
            return Err(Error::UnexpectedResponse {
                command: "PC;".to_string(),
                response: format!("unsupported power setting {watts}"),
            });
        }
        let command = if watts < 10.0 {
            let tenths = (watts * 10.0).round() as u16;
            format!("PC{tenths:03}0;")
        } else {
            let whole = watts.round() as u16;
            format!("PC{whole:03}1;")
        };
        self.send_set(&command)
    }

    pub fn set_tx_meter_mode(&mut self, mode: TxMeterMode) -> Result<()> {
        self.send_set(&format!("TM{};", mode.code()))
    }

    pub fn get_tx_meter_mode(&mut self) -> Result<TxMeterMode> {
        let response = self.query("TM;", &["TM"])?;
        let code = parse_prefixed_u8("TM", &response)?;
        TxMeterMode::from_code(code).ok_or_else(|| Error::UnexpectedResponse {
            command: "TM;".to_string(),
            response,
        })
    }

    pub fn is_transmitting(&mut self) -> Result<bool> {
        let response = self.query("TQ;", &["TQ"])?;
        let code = parse_prefixed_u8("TQ", &response)?;
        match code {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Error::UnexpectedResponse {
                command: "TQ;".to_string(),
                response,
            }),
        }
    }

    pub fn enter_tx(&mut self) -> Result<()> {
        self.send_set("TX;")
    }

    pub fn enter_rx(&mut self) -> Result<()> {
        self.send_set("RX;")
    }

    pub fn get_last_tx_swr(&mut self) -> Result<f32> {
        let response = self.query("SW;", &["SW"])?;
        let tenths = parse_prefixed_u16("SW", &response)?;
        Ok(tenths as f32 / 10.0)
    }

    pub fn get_tx_output_power_w(&mut self) -> Result<f32> {
        let response = self.query("PO;", &["PO"])?;
        let tenths = parse_prefixed_u16("PO", &response)?;
        Ok(tenths as f32 / 10.0)
    }

    pub fn read_state(&mut self) -> Result<RigState> {
        Ok(RigState {
            frequency_hz: self.get_frequency_hz()?,
            mode: self.get_mode()?,
            band: self.get_band()?,
            antenna: self.get_antenna()?,
            signal: self.get_signal_level()?,
        })
    }

    fn send_set(&mut self, command: &str) -> Result<()> {
        self.port.clear(ClearBuffer::All)?;
        self.port.write_all(command.as_bytes())?;
        self.port.flush()?;
        thread::sleep(Duration::from_millis(25));
        Ok(())
    }

    fn query(&mut self, command: &str, expected_prefixes: &[&str]) -> Result<String> {
        self.port.clear(ClearBuffer::All)?;
        self.port.write_all(command.as_bytes())?;
        self.port.flush()?;

        let deadline = Instant::now() + self.timeout;
        let mut current = Vec::new();
        let mut byte = [0_u8; 1];

        while Instant::now() < deadline {
            match self.port.read(&mut byte) {
                Ok(1) => {
                    current.push(byte[0]);
                    if byte[0] == b';' {
                        let frame = String::from_utf8_lossy(&current).into_owned();
                        current.clear();
                        if frame == "?;" {
                            return Err(Error::Busy {
                                command: command.to_string(),
                            });
                        }
                        if expected_prefixes
                            .iter()
                            .any(|prefix| frame.starts_with(prefix))
                        {
                            return Ok(frame);
                        }
                    }
                }
                Ok(0) => {}
                Ok(_) => {}
                Err(err) if err.kind() == std::io::ErrorKind::TimedOut => {}
                Err(err) => return Err(Error::Io(err)),
            }
        }

        Err(Error::Timeout {
            command: command.to_string(),
        })
    }
}

pub enum Rig {
    K3s(K3s),
    Mchf(Mchf),
}

impl Rig {
    pub fn connect(config: RigConnectionConfig) -> Result<Self> {
        let port_path = config
            .port_path
            .unwrap_or_else(|| default_port_for_kind(config.kind));
        match config.kind {
            RigKind::K3s => Ok(Self::K3s(K3s::connect(K3sConfig {
                port_path,
                timeout: config.timeout,
                ..K3sConfig::default()
            })?)),
            RigKind::Mchf => Ok(Self::Mchf(Mchf::connect(MchfConfig {
                port_path,
                timeout: config.timeout,
                ..MchfConfig::default()
            })?)),
        }
    }

    pub fn kind(&self) -> RigKind {
        match self {
            Self::K3s(_) => RigKind::K3s,
            Self::Mchf(_) => RigKind::Mchf,
        }
    }

    pub fn get_frequency_hz(&mut self) -> Result<u64> {
        match self {
            Self::K3s(rig) => rig.get_frequency_hz(),
            Self::Mchf(rig) => rig.get_frequency_hz(),
        }
    }

    pub fn set_frequency_hz(&mut self, frequency_hz: u64) -> Result<()> {
        match self {
            Self::K3s(rig) => rig.set_frequency_hz(frequency_hz),
            Self::Mchf(rig) => rig.set_frequency_hz(frequency_hz),
        }
    }

    pub fn get_mode(&mut self) -> Result<Mode> {
        match self {
            Self::K3s(rig) => rig.get_mode(),
            Self::Mchf(rig) => rig.get_mode(),
        }
    }

    pub fn set_mode(&mut self, mode: Mode) -> Result<()> {
        match self {
            Self::K3s(rig) => rig.set_mode(mode),
            Self::Mchf(rig) => rig.set_mode(mode),
        }
    }

    pub fn get_band(&mut self) -> Result<Band> {
        match self {
            Self::K3s(rig) => rig.get_band(),
            Self::Mchf(rig) => rig.get_band(),
        }
    }

    pub fn is_transmitting(&mut self) -> Result<bool> {
        match self {
            Self::K3s(rig) => rig.is_transmitting(),
            Self::Mchf(rig) => rig.is_transmitting(),
        }
    }

    pub fn enter_tx(&mut self) -> Result<()> {
        match self {
            Self::K3s(rig) => rig.enter_tx(),
            Self::Mchf(rig) => rig.enter_tx(),
        }
    }

    pub fn enter_rx(&mut self) -> Result<()> {
        match self {
            Self::K3s(rig) => rig.enter_rx(),
            Self::Mchf(rig) => rig.enter_rx(),
        }
    }

    pub fn power_state(&mut self) -> Result<RigPowerState> {
        match self {
            Self::K3s(rig) => Ok(RigPowerState::Continuous {
                current_watts: rig.get_configured_power_w().ok(),
                min_watts: 0.1,
                max_watts: 110.0,
            }),
            Self::Mchf(rig) => rig.power_state(),
        }
    }

    pub fn apply_power_request(&mut self, request: &RigPowerRequest) -> Result<()> {
        match (self, request) {
            (Self::K3s(rig), RigPowerRequest::ContinuousWatts(watts)) => {
                rig.set_configured_power_w(*watts)
            }
            (Self::Mchf(rig), RigPowerRequest::SettingId(setting_id)) => {
                rig.set_power_setting(setting_id)
            }
            (Self::K3s(_), RigPowerRequest::SettingId(_)) => Err(Error::Unsupported {
                operation: "discrete power setting on K3S".to_string(),
            }),
            (Self::Mchf(_), RigPowerRequest::ContinuousWatts(_)) => Err(Error::Unsupported {
                operation: "continuous watt power request on mcHF".to_string(),
            }),
        }
    }

    pub fn read_telemetry(&mut self) -> Result<RigTelemetry> {
        match self {
            Self::K3s(rig) => Ok(RigTelemetry {
                rx_s_meter: None,
                tx_forward_power_w: rig.get_tx_output_power_w().ok(),
                tx_swr: rig.get_last_tx_swr().ok(),
                tx_alc: None,
                bar_graph: rig.get_bar_graph().ok().map(|reading| reading.level),
            }),
            Self::Mchf(rig) => rig.read_telemetry(),
        }
    }

    pub fn read_snapshot(&mut self) -> Result<RigSnapshot> {
        let frequency_hz = self.get_frequency_hz()?;
        let mode = self.get_mode()?;
        let band = self.get_band().or_else(|_| {
            Band::from_frequency_hz(frequency_hz).ok_or(Error::UnexpectedResponse {
                command: "band_from_frequency".to_string(),
                response: format!("unsupported frequency {frequency_hz}"),
            })
        })?;
        let transmitting = self.is_transmitting()?;
        let telemetry = self.read_telemetry()?;
        let power = self.power_state()?;
        Ok(RigSnapshot {
            kind: self.kind(),
            frequency_hz,
            mode,
            band,
            transmitting,
            telemetry,
            power,
        })
    }
}

pub fn detect_attached_rigs() -> Vec<DetectedRig> {
    let mut detected = Vec::new();
    let serial_dir = Path::new("/dev/serial/by-id");
    let Ok(entries) = std::fs::read_dir(serial_dir) else {
        return detected;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        let lower = name.to_ascii_lowercase();
        let kind = if lower.contains("mchf") {
            Some(RigKind::Mchf)
        } else if lower.contains("ft232")
            || lower.contains("elecraft")
            || lower.contains("ak04x2po")
        {
            Some(RigKind::K3s)
        } else {
            None
        };
        if let Some(kind) = kind {
            detected.push(DetectedRig {
                kind,
                port_path: path,
            });
        }
    }
    detected.sort_by(|left, right| left.port_path.cmp(&right.port_path));
    detected
}

pub fn default_port_for_kind(kind: RigKind) -> PathBuf {
    match kind {
        RigKind::K3s => {
            if Path::new(DEFAULT_K3S_PORT).exists() {
                PathBuf::from(DEFAULT_K3S_PORT)
            } else {
                PathBuf::from("/dev/ttyUSB0")
            }
        }
        RigKind::Mchf => {
            if Path::new(DEFAULT_MCHF_PORT).exists() {
                PathBuf::from(DEFAULT_MCHF_PORT)
            } else {
                PathBuf::from("/dev/ttyACM0")
            }
        }
    }
}

pub fn resolve_rig_kind(explicit_kind: Option<RigKind>) -> Result<RigKind> {
    if let Some(kind) = explicit_kind {
        return Ok(kind);
    }
    resolve_rig_kind_from_detected(detect_attached_rigs().iter().map(|entry| entry.kind))
}

fn resolve_rig_kind_from_detected(
    detected_kinds: impl IntoIterator<Item = RigKind>,
) -> Result<RigKind> {
    let mut kinds = detected_kinds.into_iter().collect::<Vec<_>>();
    kinds.sort_by_key(|kind| kind.as_str().to_string());
    kinds.dedup();
    match kinds.as_slice() {
        [kind] => Ok(*kind),
        [] => Err(Error::Unsupported {
            operation: "no supported rigs detected".to_string(),
        }),
        _ => Err(Error::Unsupported {
            operation: "multiple supported rigs detected; configure rig.kind explicitly"
                .to_string(),
        }),
    }
}

fn detect_audio_device_with_predicate(
    devices: Vec<audio::AudioDevice>,
    missing_message: &str,
    mut predicate: impl FnMut(&audio::AudioDevice, &str) -> bool,
) -> audio::Result<audio::AudioDevice> {
    let mut best = None;
    for device in devices {
        let desc = device.description.as_deref().unwrap_or_default();
        let looks_like_match = predicate(&device, desc);
        let looks_like_pcm = device.spec.starts_with("plughw:") || device.spec.starts_with("hw:");
        if looks_like_match && looks_like_pcm {
            if device.spec.starts_with("plughw:") {
                return Ok(device);
            }
            best = Some(device);
        }
    }

    best.ok_or_else(|| audio::Error::CaptureInit(missing_message.to_string()))
}

pub fn detect_audio_device_for_rig(
    kind: RigKind,
    override_spec: Option<&str>,
) -> audio::Result<audio::AudioDevice> {
    if let Some(spec) = override_spec {
        return Ok(audio::AudioDevice {
            name: spec.to_string(),
            spec: spec.to_string(),
            description: None,
        });
    }
    detect_audio_device_with_predicate(
        audio::list_input_devices()?,
        match kind {
            RigKind::K3s => "K3S audio capture device not found",
            RigKind::Mchf => "mcHF audio capture device not found",
        },
        |device, desc| match kind {
            RigKind::K3s => {
                let looks_like_codec = device.spec.contains("CARD=CODEC")
                    || device.name.contains("USB Audio CODEC")
                    || desc.contains("USB Audio CODEC");
                looks_like_codec && device.spec.contains("DEV=0")
            }
            RigKind::Mchf => {
                let looks_like_mchf = device.spec.contains("CARD=mchf")
                    || device.name.contains("USB Interface mchf")
                    || desc.contains("USB Interface mchf")
                    || device.name.contains("mchf")
                    || desc.contains("mchf");
                looks_like_mchf && device.spec.contains("DEV=0")
            }
        },
    )
}

pub fn detect_audio_output_device_for_rig(
    kind: RigKind,
    override_spec: Option<&str>,
) -> audio::Result<audio::AudioDevice> {
    if let Some(spec) = override_spec {
        return Ok(audio::AudioDevice {
            name: spec.to_string(),
            spec: spec.to_string(),
            description: None,
        });
    }

    detect_audio_device_with_predicate(
        audio::list_output_devices()?,
        match kind {
            RigKind::K3s => "K3S audio playback device not found",
            RigKind::Mchf => "mcHF audio playback device not found",
        },
        |device, desc| match kind {
            RigKind::K3s => {
                let looks_like_codec = device.spec.contains("CARD=CODEC")
                    || device.name.contains("USB Audio CODEC")
                    || desc.contains("USB Audio CODEC");
                looks_like_codec && device.spec.contains("DEV=0")
            }
            RigKind::Mchf => {
                let looks_like_mchf = device.spec.contains("CARD=mchf")
                    || device.name.contains("USB Interface mchf")
                    || desc.contains("USB Interface mchf")
                    || device.name.contains("mchf")
                    || desc.contains("mchf");
                looks_like_mchf && device.spec.contains("DEV=0")
            }
        },
    )
}

pub fn detect_k3s_audio_device(override_spec: Option<&str>) -> audio::Result<audio::AudioDevice> {
    detect_audio_device_for_rig(RigKind::K3s, override_spec)
}

pub fn detect_k3s_audio_output_device(
    override_spec: Option<&str>,
) -> audio::Result<audio::AudioDevice> {
    detect_audio_output_device_for_rig(RigKind::K3s, override_spec)
}

fn parse_prefixed_u64(prefix: &str, response: &str) -> Result<u64> {
    let value = response
        .strip_prefix(prefix)
        .and_then(|tail| tail.strip_suffix(';'))
        .ok_or_else(|| Error::UnexpectedResponse {
            command: format!("{prefix};"),
            response: response.to_string(),
        })?;
    value.parse().map_err(|_| Error::UnexpectedResponse {
        command: format!("{prefix};"),
        response: response.to_string(),
    })
}

fn parse_prefixed_u16(prefix: &str, response: &str) -> Result<u16> {
    let value = response
        .strip_prefix(prefix)
        .and_then(|tail| tail.strip_suffix(';'))
        .ok_or_else(|| Error::UnexpectedResponse {
            command: format!("{prefix};"),
            response: response.to_string(),
        })?;
    value.parse().map_err(|_| Error::UnexpectedResponse {
        command: format!("{prefix};"),
        response: response.to_string(),
    })
}

fn parse_prefixed_u8(prefix: &str, response: &str) -> Result<u8> {
    let value = response
        .strip_prefix(prefix)
        .and_then(|tail| tail.strip_suffix(';'))
        .ok_or_else(|| Error::UnexpectedResponse {
            command: format!("{prefix};"),
            response: response.to_string(),
        })?;
    value.parse().map_err(|_| Error::UnexpectedResponse {
        command: format!("{prefix};"),
        response: response.to_string(),
    })
}

fn parse_bg(response: &str) -> Result<(u8, bool)> {
    let body = response
        .strip_prefix("BG")
        .and_then(|tail| tail.strip_suffix(';'))
        .ok_or_else(|| Error::UnexpectedResponse {
            command: "BG;".to_string(),
            response: response.to_string(),
        })?;
    if body.len() != 3 {
        return Err(Error::UnexpectedResponse {
            command: "BG;".to_string(),
            response: response.to_string(),
        });
    }
    let level: u8 = body[..2].parse().map_err(|_| Error::UnexpectedResponse {
        command: "BG;".to_string(),
        response: response.to_string(),
    })?;
    let receiving = matches!(&body[2..], "R");
    Ok((level, receiving))
}

fn parse_power_control(response: &str) -> Result<f32> {
    let body = response
        .strip_prefix("PC")
        .and_then(|tail| tail.strip_suffix(';'))
        .ok_or_else(|| Error::UnexpectedResponse {
            command: "PC;".to_string(),
            response: response.to_string(),
        })?;
    let watts = match body.len() {
        3 => body.parse::<u16>().map(|watts| watts as f32).map_err(|_| {
            Error::UnexpectedResponse {
                command: "PC;".to_string(),
                response: response.to_string(),
            }
        })?,
        4 => {
            let value = body[..3]
                .parse::<u16>()
                .map_err(|_| Error::UnexpectedResponse {
                    command: "PC;".to_string(),
                    response: response.to_string(),
                })?;
            let range = body.as_bytes()[3];
            match range {
                b'0' => value as f32 / 10.0,
                b'1' => value as f32,
                _ => {
                    return Err(Error::UnexpectedResponse {
                        command: "PC;".to_string(),
                        response: response.to_string(),
                    });
                }
            }
        }
        _ => {
            return Err(Error::UnexpectedResponse {
                command: "PC;".to_string(),
                response: response.to_string(),
            });
        }
    };
    Ok(watts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_modes() {
        assert_eq!("data".parse::<Mode>().unwrap(), Mode::Data);
        assert_eq!("cw-rev".parse::<Mode>().unwrap(), Mode::CwRev);
        assert_eq!(Mode::DataRev.code(), 9);
    }

    #[test]
    fn parses_bands() {
        assert_eq!("20m".parse::<Band>().unwrap(), Band::M20);
        assert_eq!("xvtr3".parse::<Band>().unwrap(), Band::Xvtr(3));
        assert_eq!(Band::M6.number(), 10);
    }

    #[test]
    fn parses_protocol_fields() {
        assert_eq!(
            parse_prefixed_u64("FA", "FA00014074000;").unwrap(),
            14_074_000
        );
        assert_eq!(parse_prefixed_u8("BN", "BN05;").unwrap(), 5);
        assert_eq!(parse_bg("BG00R;").unwrap(), (0, true));
        assert_eq!(parse_power_control("PC050;").unwrap(), 50.0);
        assert_eq!(parse_power_control("PC0500;").unwrap(), 5.0);
        assert_eq!(parse_power_control("PC0501;").unwrap(), 50.0);
        assert_eq!(parse_prefixed_u8("TM", "TM1;").unwrap(), 1);
        assert_eq!(parse_prefixed_u8("TQ", "TQ0;").unwrap(), 0);
        assert_eq!(parse_prefixed_u16("SW", "SW015;").unwrap(), 15);
        assert_eq!(parse_prefixed_u16("PO", "PO050;").unwrap(), 50);
    }

    #[test]
    fn derives_band_from_frequency() {
        assert_eq!(Band::from_frequency_hz(14_074_000), Some(Band::M20));
        assert_eq!(Band::from_frequency_hz(7_047_500), Some(Band::M40));
        assert_eq!(Band::from_frequency_hz(999_999), None);
    }

    #[test]
    fn parses_rig_kind() {
        assert_eq!("k3s".parse::<RigKind>().unwrap(), RigKind::K3s);
        assert_eq!("mchf".parse::<RigKind>().unwrap(), RigKind::Mchf);
    }

    #[test]
    fn resolve_rig_kind_from_detected_fails_closed_when_ambiguous() {
        assert!(matches!(
            resolve_rig_kind_from_detected([RigKind::K3s, RigKind::Mchf]),
            Err(Error::Unsupported { .. })
        ));
        assert!(matches!(
            resolve_rig_kind_from_detected(std::iter::empty()),
            Err(Error::Unsupported { .. })
        ));
        assert_eq!(
            resolve_rig_kind_from_detected([RigKind::Mchf]).unwrap(),
            RigKind::Mchf
        );
    }
}
