use crate::{
    Band, DEFAULT_TIMEOUT, Error, Mode, Result, RigPowerSetting, RigPowerState, RigTelemetry,
};
use serialport::{ClearBuffer, DataBits, FlowControl, Parity, SerialPort, StopBits};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

pub const MCHF_BAUD_RATE: u32 = 38_400;
pub const DEFAULT_MCHF_PORT: &str = "/dev/serial/by-id/usb-UHSDR_Community__based_on_STM_Drivers__USB_Interface_mchf_00000000002A-if00";
pub const DEFAULT_MCHF_AUDIO_HINTS: &[&str] = &["USB Interface mchf", "CARD=mchf", "mchf", "DEV=0"];
const FT817_EEPROM_READ: u8 = 0xbb;
const FT817_EEPROM_WRITE: u8 = 0xbc;
const FT817_GET_FREQ_MODE: u8 = 0x03;
const FT817_SET_FREQ: u8 = 0x01;
const FT817_SET_MODE: u8 = 0x07;
const FT817_PTT_ON: u8 = 0x08;
const FT817_PTT_OFF: u8 = 0x88;
const FT817_READ_TX_STATE: u8 = 0xbd;
const FT817_READ_RX_STATE: u8 = 0xe7;
const FT817_PTT_STATE: u8 = 0xf7;
const UHSDR_CONFIG_ADDR_PREFIX: u16 = 0x8000;
const UHSDR_EEPROM_TX_POWER_LEVEL: u16 = 12;
const UHSDR_EEPROM_PTT_RTS_ENABLE: u16 = 409;
const UHSDR_EEPROM_DIGI_MODE_CONF: u16 = 389;

#[derive(Debug, Clone)]
pub struct MchfConfig {
    pub port_path: PathBuf,
    pub baud_rate: u32,
    pub timeout: Duration,
}

impl Default for MchfConfig {
    fn default() -> Self {
        let default_path = if Path::new(DEFAULT_MCHF_PORT).exists() {
            PathBuf::from(DEFAULT_MCHF_PORT)
        } else {
            PathBuf::from("/dev/ttyACM0")
        };
        Self {
            port_path: default_path,
            baud_rate: MCHF_BAUD_RATE,
            timeout: DEFAULT_TIMEOUT,
        }
    }
}

pub struct Mchf {
    port: Box<dyn SerialPort>,
    timeout: Duration,
}

impl Mchf {
    pub fn connect(config: MchfConfig) -> Result<Self> {
        let port = serialport::new(
            config.port_path.to_string_lossy().into_owned(),
            config.baud_rate,
        )
        .timeout(Duration::from_millis(50))
        .data_bits(DataBits::Eight)
        .parity(Parity::None)
        .stop_bits(StopBits::Two)
        .flow_control(FlowControl::None)
        .open()?;
        Ok(Self {
            port,
            timeout: config.timeout,
        })
    }

    pub fn get_frequency_hz(&mut self) -> Result<u64> {
        let rsp = self.query([0, 0, 0, 0, FT817_GET_FREQ_MODE], 5)?;
        decode_ft817_frequency_hz(&rsp[0..4])
    }

    pub fn set_frequency_hz(&mut self, frequency_hz: u64) -> Result<()> {
        let bytes = encode_ft817_frequency_hz(frequency_hz)?;
        self.command([bytes[0], bytes[1], bytes[2], bytes[3], FT817_SET_FREQ], 1)?;
        Ok(())
    }

    pub fn get_mode(&mut self) -> Result<Mode> {
        let rsp = self.query([0, 0, 0, 0, FT817_GET_FREQ_MODE], 5)?;
        decode_ft817_mode(rsp[4])
    }

    pub fn set_mode(&mut self, mode: Mode) -> Result<()> {
        let code = encode_ft817_mode(mode)?;
        self.command([code, 0, 0, 0, FT817_SET_MODE], 1)?;
        Ok(())
    }

    pub fn get_band(&mut self) -> Result<Band> {
        Band::from_frequency_hz(self.get_frequency_hz()?).ok_or(Error::UnexpectedResponse {
            command: "band_from_frequency".to_string(),
            response: "unsupported frequency".to_string(),
        })
    }

    pub fn is_transmitting(&mut self) -> Result<bool> {
        let rsp = self.query([0, 0, 0, 0, FT817_PTT_STATE], 1)?;
        Ok(rsp[0] != 0xff)
    }

    pub fn enter_tx(&mut self) -> Result<()> {
        self.command([0, 0, 0, 0, FT817_PTT_ON], 1)?;
        Ok(())
    }

    pub fn enter_rx(&mut self) -> Result<()> {
        self.command([0, 0, 0, 0, FT817_PTT_OFF], 1)?;
        Ok(())
    }

    pub fn read_telemetry(&mut self) -> Result<RigTelemetry> {
        let rx_rsp = self.query([0, 0, 0, 0, FT817_READ_RX_STATE], 1)?;
        let ptt_rsp = self.query([0, 0, 0, 0, FT817_PTT_STATE], 1)?;
        let tx_rsp = if ptt_rsp[0] == 0xff {
            None
        } else {
            Some(self.query([0, 0, 0, 0, FT817_READ_TX_STATE], 2)?)
        };
        let rx_s_meter = Some(rx_rsp[0] as f32);
        let (tx_forward_power_w, tx_swr, tx_alc) = if let Some(tx_rsp) = tx_rsp {
            (
                Some(((tx_rsp[0] >> 4) & 0x0f) as f32),
                Some((tx_rsp[0] & 0x0f) as f32),
                Some(((tx_rsp[1] >> 4) & 0x0f) as f32),
            )
        } else {
            (None, None, None)
        };
        Ok(RigTelemetry {
            rx_s_meter,
            tx_forward_power_w,
            tx_swr,
            tx_alc,
            bar_graph: rx_s_meter.map(|value| value.round().clamp(0.0, 15.0) as u8),
        })
    }

    pub fn power_state(&mut self) -> Result<RigPowerState> {
        let current = self.read_config_u16(UHSDR_EEPROM_TX_POWER_LEVEL).ok();
        let current_idx = current.map(|value| value as u8);
        let current_setting = current_idx.and_then(mchf_power_setting_by_index);
        Ok(RigPowerState::Discrete {
            current_id: current_setting
                .as_ref()
                .map(|setting| setting.id.to_string()),
            current_label: current_setting
                .as_ref()
                .map(|setting| setting.label.to_string()),
            settings: mchf_power_settings(),
            can_set: false,
        })
    }

    pub fn set_power_setting(&mut self, setting_id: &str) -> Result<()> {
        let _ = setting_id;
        Err(Error::Unsupported {
            operation: "set mcHF power setting over CAT".to_string(),
        })
    }

    pub fn read_config_u16(&mut self, address: u16) -> Result<u16> {
        self.read_eeprom_u16(UHSDR_CONFIG_ADDR_PREFIX | address)
    }

    pub fn write_config_u16(&mut self, address: u16, value: u16) -> Result<()> {
        self.write_eeprom_u16(UHSDR_CONFIG_ADDR_PREFIX | address, value)
    }

    pub fn read_eeprom_u16(&mut self, address: u16) -> Result<u16> {
        let rsp = self.query(
            [
                (address >> 8) as u8,
                (address & 0xff) as u8,
                0,
                0,
                FT817_EEPROM_READ,
            ],
            2,
        )?;
        Ok(u16::from_le_bytes([rsp[0], rsp[1]]))
    }

    pub fn write_eeprom_u16(&mut self, address: u16, value: u16) -> Result<()> {
        let [low, high] = value.to_le_bytes();
        self.command(
            [
                (address >> 8) as u8,
                (address & 0xff) as u8,
                low,
                high,
                FT817_EEPROM_WRITE,
            ],
            1,
        )?;
        Ok(())
    }

    pub fn ensure_rts_ptt_enabled(&mut self) -> Result<()> {
        self.write_config_u16(UHSDR_EEPROM_PTT_RTS_ENABLE, 1)
    }

    pub fn set_digital_mode_index(&mut self, value: u16) -> Result<()> {
        self.write_config_u16(UHSDR_EEPROM_DIGI_MODE_CONF, value)
    }

    fn command(&mut self, frame: [u8; 5], response_len: usize) -> Result<Vec<u8>> {
        self.port.clear(ClearBuffer::All)?;
        self.port.write_all(&frame)?;
        self.port.flush()?;
        if response_len == 0 {
            return Ok(Vec::new());
        }
        self.read_exact_response(hex_cmd(frame), response_len)
    }

    fn query(&mut self, frame: [u8; 5], response_len: usize) -> Result<Vec<u8>> {
        self.command(frame, response_len)
    }

    fn read_exact_response(&mut self, command: String, response_len: usize) -> Result<Vec<u8>> {
        let deadline = Instant::now() + self.timeout;
        let mut response = vec![0_u8; response_len];
        let mut read = 0usize;
        while read < response_len && Instant::now() < deadline {
            match self.port.read(&mut response[read..]) {
                Ok(0) => {}
                Ok(n) => read += n,
                Err(err) if err.kind() == std::io::ErrorKind::TimedOut => {}
                Err(err) => return Err(Error::Io(err)),
            }
        }
        if read == response_len {
            thread::sleep(Duration::from_millis(25));
            Ok(response)
        } else {
            Err(Error::Timeout { command })
        }
    }
}

fn hex_cmd(frame: [u8; 5]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}{:02x}",
        frame[0], frame[1], frame[2], frame[3], frame[4]
    )
}

pub fn mchf_power_settings() -> Vec<RigPowerSetting> {
    vec![
        RigPowerSetting::new("full", "Full", None),
        RigPowerSetting::new("5w", "5 W", Some(5.0)),
        RigPowerSetting::new("2w", "2 W", Some(2.0)),
        RigPowerSetting::new("1w", "1 W", Some(1.0)),
        RigPowerSetting::new("0.5w", "0.5 W", Some(0.5)),
    ]
}

pub fn mchf_power_setting_by_index(index: u8) -> Option<RigPowerSetting> {
    Some(match index {
        0 => RigPowerSetting::new("full", "Full", None),
        1 => RigPowerSetting::new("2w", "2 W", Some(2.0)),
        2 => RigPowerSetting::new("1w", "1 W", Some(1.0)),
        3 => RigPowerSetting::new("0.5w", "0.5 W", Some(0.5)),
        _ => return None,
    })
}

pub fn encode_ft817_frequency_hz(frequency_hz: u64) -> Result<[u8; 4]> {
    if frequency_hz > 99_999_990 {
        return Err(Error::InvalidFrequency(frequency_hz));
    }
    let mut value = frequency_hz / 10;
    let mut digits = [0u8; 8];
    for idx in (0..8).rev() {
        digits[idx] = (value % 10) as u8;
        value /= 10;
    }
    Ok([
        (digits[0] << 4) | digits[1],
        (digits[2] << 4) | digits[3],
        (digits[4] << 4) | digits[5],
        (digits[6] << 4) | digits[7],
    ])
}

pub fn decode_ft817_frequency_hz(bytes: &[u8]) -> Result<u64> {
    if bytes.len() != 4 {
        return Err(Error::UnexpectedResponse {
            command: "ft817_get_freq".to_string(),
            response: format!("expected 4 bytes, got {}", bytes.len()),
        });
    }
    let mut value = 0u64;
    for &byte in bytes {
        value = value * 10 + ((byte >> 4) & 0x0f) as u64;
        value = value * 10 + (byte & 0x0f) as u64;
    }
    Ok(value * 10)
}

pub fn encode_ft817_mode(mode: Mode) -> Result<u8> {
    Ok(match mode {
        Mode::Lsb => 0,
        Mode::Usb => 1,
        Mode::Cw => 2,
        Mode::CwRev => 3,
        Mode::Am => 4,
        Mode::Fm => 8,
        Mode::Data | Mode::DataRev => 0x0a,
    })
}

pub fn decode_ft817_mode(code: u8) -> Result<Mode> {
    match code {
        0 => Ok(Mode::Lsb),
        1 => Ok(Mode::Usb),
        2 => Ok(Mode::Cw),
        3 => Ok(Mode::CwRev),
        4 => Ok(Mode::Am),
        8 | 0x88 => Ok(Mode::Fm),
        0x0a | 0x0c => Ok(Mode::Data),
        _ => Err(Error::UnexpectedResponse {
            command: "ft817_mode".to_string(),
            response: format!("unsupported mode code {code:#x}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ft817_frequency_round_trip() {
        let encoded = encode_ft817_frequency_hz(14_074_000).unwrap();
        assert_eq!(encoded, [0x01, 0x40, 0x74, 0x00]);
        assert_eq!(decode_ft817_frequency_hz(&encoded).unwrap(), 14_074_000);
    }

    #[test]
    fn ft817_mode_mapping_supports_data() {
        assert_eq!(encode_ft817_mode(Mode::Data).unwrap(), 0x0a);
        assert_eq!(decode_ft817_mode(0x0a).unwrap(), Mode::Data);
        assert_eq!(decode_ft817_mode(0x0c).unwrap(), Mode::Data);
    }

    #[test]
    fn mchf_power_step_mapping() {
        assert_eq!(
            mchf_power_setting_by_index(1).unwrap().nominal_watts,
            Some(2.0)
        );
        assert_eq!(mchf_power_setting_by_index(0).unwrap().label, "Full");
    }
}
