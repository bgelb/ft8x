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
}

pub type Result<T> = std::result::Result<T, Error>;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RigState {
    pub frequency_hz: u64,
    pub mode: Mode,
    pub band: Band,
    pub antenna: Antenna,
    pub signal: SignalLevel,
}

#[derive(Debug, Clone)]
pub struct K3sConfig {
    pub port_path: PathBuf,
    pub baud_rate: u32,
    pub timeout: Duration,
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
        }
    }
}

pub struct K3s {
    port: Box<dyn SerialPort>,
    timeout: Duration,
}

impl K3s {
    pub fn connect(config: K3sConfig) -> Result<Self> {
        let mut port = serialport::new(config.port_path.to_string_lossy().into_owned(), config.baud_rate)
            .timeout(Duration::from_millis(50))
            .open()?;
        // On the attached K3S/FTDI interface, asserting RTS mutes receive audio and likely keys
        // a PTT-related control path. Force both modem-control outputs low for RX-only CAT use.
        port.write_request_to_send(false)?;
        port.write_data_terminal_ready(false)?;
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
        let high_res = self.query("SMH;", &["SMH"]).ok().and_then(|rsp| parse_prefixed_u16("SMH", &rsp).ok());
        let coarse_rsp = self.query("SM;", &["SM"])?;
        let coarse = parse_prefixed_u16("SM", &coarse_rsp)?;
        let bar_rsp = self.query("BG;", &["BG"])?;
        let (bar_graph, receiving) = parse_bg(&bar_rsp)?;
        Ok(SignalLevel {
            coarse,
            high_res,
            bar_graph: Some(bar_graph),
            receiving,
        })
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
                        if expected_prefixes.iter().any(|prefix| frame.starts_with(prefix)) {
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
        assert_eq!(parse_prefixed_u64("FA", "FA00014074000;").unwrap(), 14_074_000);
        assert_eq!(parse_prefixed_u8("BN", "BN05;").unwrap(), 5);
        assert_eq!(parse_bg("BG00R;").unwrap(), (0, true));
    }
}
