use std::path::Path;
use std::sync::OnceLock;

use num_complex::Complex32;
use thiserror::Error;

use crate::crc::{crc13_ft2, crc14_ft8};
use crate::message::{GridReport, ReplyWord, hash_callsign};
use crate::modes::{Mode, populate_channel_symbols};
use crate::modes::ft4::FT4_RVEC;
use crate::protocol::{
    CALL_NTOKENS, CALL_STANDARD_BASE, FIELD_DAY_SECTIONS, FTX_DXPEDITION_LAYOUT,
    FTX_EU_VHF_LAYOUT, FTX_FIELD_DAY_LAYOUT, FTX_MESSAGE_BITS, FTX_MESSAGE_KIND_EU_VHF,
    FTX_MESSAGE_KIND_NONSTANDARD, FTX_MESSAGE_KIND_RTTY_CONTEST,
    FTX_MESSAGE_KIND_STANDARD_SLASH_R, FTX_NONSTANDARD_LAYOUT, FTX_RTTY_CONTEST_LAYOUT,
    FTX_STANDARD_LAYOUT, RTTY_MULTIPLIERS, alphabet27_index, alphabet36_index,
    alphabet37_index, alphabet38_index, digit10_index, gray_decode_tone3, gray_encode_3bits,
    write_bit_field,
};
use crate::wave::{AudioBuffer, DecoderError, write_wav};

const MESSAGE_BITS: usize = FTX_MESSAGE_BITS;
const INFO_BITS: usize = 91;
const FT2_INFO_BITS: usize = 90;
const GFSK_BT: f32 = 2.0;

#[derive(Debug, Clone)]
pub struct EncodedFrame {
    pub mode: Mode,
    pub message_bits: [u8; MESSAGE_BITS],
    pub codeword_bits: Vec<u8>,
    pub data_symbols: Vec<u8>,
    pub channel_symbols: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct SynthesizedTxMessage {
    pub frame: EncodedFrame,
    pub audio: AudioBuffer,
    pub rendered_text: String,
}

#[derive(Debug, Clone)]
pub struct WaveformOptions {
    pub mode: Mode,
    pub base_freq_hz: f32,
    pub start_seconds: f32,
    pub total_seconds: f32,
    pub amplitude: f32,
}

#[derive(Debug, Clone)]
pub enum TxDirectedPayload {
    Blank,
    Grid(String),
    Signal(i16),
    SignalWithAck(i16),
    Reply(ReplyWord),
}

#[derive(Debug, Clone)]
pub enum TxMessage {
    Cq {
        my_call: String,
        my_grid: Option<String>,
    },
    Directed {
        my_call: String,
        peer_call: String,
        payload: TxDirectedPayload,
    },
    DxpeditionCompound {
        finished_call: String,
        next_call: String,
        my_call: String,
        report_db: i16,
    },
    FieldDay {
        first_call: String,
        second_call: String,
        acknowledge: bool,
        transmitter_count: u8,
        class: char,
        section: String,
    },
    RttyContest {
        tu: bool,
        first_call: String,
        second_call: String,
        acknowledge: bool,
        report: u16,
        exchange: TxRttyExchange,
    },
    EuVhf {
        first_hashed_call: String,
        second_hashed_call: String,
        acknowledge: bool,
        report: u8,
        serial: u16,
        grid6: String,
    },
}

#[derive(Debug, Clone)]
pub enum TxRttyExchange {
    Multiplier(String),
    Serial(u16),
}

impl Default for WaveformOptions {
    fn default() -> Self {
        Self {
            mode: Mode::Ft8,
            base_freq_hz: 1_000.0,
            start_seconds: 0.5,
            total_seconds: 15.0,
            amplitude: 0.8,
        }
    }
}

#[derive(Debug, Error)]
pub enum EncodeError {
    #[error("unsupported standard callsign: {0}")]
    UnsupportedCallsign(String),
    #[error("unsupported grid or report: {0}")]
    UnsupportedInfo(String),
    #[error("unsupported token: {0}")]
    UnsupportedToken(String),
    #[error("unsupported mode feature: {0}")]
    UnsupportedModeFeature(String),
    #[error("waveform too short for FT8 frame")]
    WaveformTooShort,
}

pub fn encode_standard_message_for_mode(
    mode: Mode,
    first: &str,
    second: &str,
    acknowledge: bool,
    info: &GridReport,
) -> Result<EncodedFrame, EncodeError> {
    let mut message_bits = [0u8; MESSAGE_BITS];
    write_bit_field(
        &mut message_bits,
        FTX_STANDARD_LAYOUT.first_call,
        u64::from(encode_c28(first)?),
    );
    write_bit_field(&mut message_bits, FTX_STANDARD_LAYOUT.first_suffix, 0);
    write_bit_field(
        &mut message_bits,
        FTX_STANDARD_LAYOUT.second_call,
        u64::from(encode_c28(second)?),
    );
    write_bit_field(&mut message_bits, FTX_STANDARD_LAYOUT.second_suffix, 0);
    write_bit_field(
        &mut message_bits,
        FTX_STANDARD_LAYOUT.acknowledge,
        acknowledge as u64,
    );
    write_bit_field(
        &mut message_bits,
        FTX_STANDARD_LAYOUT.info,
        u64::from(encode_g15(info)?),
    );
    write_bit_field(
        &mut message_bits,
        FTX_STANDARD_LAYOUT.kind,
        u64::from(FTX_MESSAGE_KIND_STANDARD_SLASH_R),
    );
    build_frame_for_mode(mode, message_bits)
}

pub fn encode_standard_message(
    first: &str,
    second: &str,
    acknowledge: bool,
    info: &GridReport,
) -> Result<EncodedFrame, EncodeError> {
    encode_standard_message_for_mode(Mode::Ft8, first, second, acknowledge, info)
}

pub fn synthesize_tx_message(
    message: &TxMessage,
    options: &WaveformOptions,
) -> Result<SynthesizedTxMessage, EncodeError> {
    let (frame, rendered_text) = match message {
        TxMessage::DxpeditionCompound {
            finished_call,
            next_call,
            my_call,
            report_db,
        } => {
            if options.mode != Mode::Ft8 {
                return Err(EncodeError::UnsupportedModeFeature(
                    "specialized DXpedition messages are only wired for ft8 so far".to_string(),
                ));
            }
            (
                encode_dxpedition_message(finished_call, next_call, my_call, *report_db)?,
                format!(
                    "{} RR73; {} <{}> {:+03}",
                    finished_call.trim().to_uppercase(),
                    next_call.trim().to_uppercase(),
                    my_call.trim().to_uppercase(),
                    report_db
                ),
            )
        }
        TxMessage::FieldDay {
            first_call,
            second_call,
            acknowledge,
            transmitter_count,
            class,
            section,
        } => {
            if options.mode != Mode::Ft8 {
                return Err(EncodeError::UnsupportedModeFeature(
                    "field-day messages are only wired for ft8 so far".to_string(),
                ));
            }
            (
                encode_field_day_message(
                    first_call,
                    second_call,
                    *acknowledge,
                    *transmitter_count,
                    *class,
                    section,
                )?,
                render_field_day_message(
                    first_call,
                    second_call,
                    *acknowledge,
                    *transmitter_count,
                    *class,
                    section,
                ),
            )
        }
        TxMessage::RttyContest {
            tu,
            first_call,
            second_call,
            acknowledge,
            report,
            exchange,
        } => {
            if options.mode != Mode::Ft8 {
                return Err(EncodeError::UnsupportedModeFeature(
                    "rtty contest messages are only wired for ft8 so far".to_string(),
                ));
            }
            (
                encode_rtty_contest_message(
                    *tu,
                    first_call,
                    second_call,
                    *acknowledge,
                    *report,
                    exchange,
                )?,
                render_rtty_contest_message(
                    *tu,
                    first_call,
                    second_call,
                    *acknowledge,
                    *report,
                    exchange,
                ),
            )
        }
        TxMessage::EuVhf {
            first_hashed_call,
            second_hashed_call,
            acknowledge,
            report,
            serial,
            grid6,
        } => {
            if options.mode != Mode::Ft8 {
                return Err(EncodeError::UnsupportedModeFeature(
                    "eu-vhf messages are only wired for ft8 so far".to_string(),
                ));
            }
            (
                encode_eu_vhf_message(
                    first_hashed_call,
                    second_hashed_call,
                    *acknowledge,
                    *report,
                    *serial,
                    grid6,
                )?,
                render_eu_vhf_message(
                    first_hashed_call,
                    second_hashed_call,
                    *acknowledge,
                    *report,
                    *serial,
                    grid6,
                ),
            )
        }
        _ => {
            let (first, second, acknowledge, info, rendered_text) = tx_message_fields(message);
            (
                encode_standard_message_for_mode(options.mode, &first, &second, acknowledge, &info)?,
                rendered_text,
            )
        }
    };
    let audio = synthesize_rectangular_waveform(&frame, options)?;
    Ok(SynthesizedTxMessage {
        frame,
        audio,
        rendered_text,
    })
}

pub fn encode_dxpedition_message(
    finished_call: &str,
    next_call: &str,
    my_call: &str,
    report_db: i16,
) -> Result<EncodedFrame, EncodeError> {
    let mut message_bits = [0u8; MESSAGE_BITS];
    write_bit_field(
        &mut message_bits,
        FTX_DXPEDITION_LAYOUT.completed_call,
        u64::from(encode_c28(finished_call)?),
    );
    write_bit_field(
        &mut message_bits,
        FTX_DXPEDITION_LAYOUT.next_call,
        u64::from(encode_c28(next_call)?),
    );
    write_bit_field(
        &mut message_bits,
        FTX_DXPEDITION_LAYOUT.hashed_call10,
        hash_callsign(&my_call.trim().to_uppercase(), 10),
    );
    write_bit_field(
        &mut message_bits,
        FTX_DXPEDITION_LAYOUT.report5,
        dxpedition_report_bits(report_db) as u64,
    );
    write_bit_field(&mut message_bits, FTX_DXPEDITION_LAYOUT.subtype, 1);
    write_bit_field(&mut message_bits, FTX_DXPEDITION_LAYOUT.kind, 0);
    build_frame_for_mode(Mode::Ft8, message_bits)
}

pub fn encode_field_day_message(
    first_call: &str,
    second_call: &str,
    acknowledge: bool,
    transmitter_count: u8,
    class: char,
    section: &str,
) -> Result<EncodedFrame, EncodeError> {
    if !(1..=32).contains(&transmitter_count) {
        return Err(EncodeError::UnsupportedInfo(format!(
            "field day transmitters: {transmitter_count}"
        )));
    }
    let class_upper = class.to_ascii_uppercase();
    if !('A'..='H').contains(&class_upper) {
        return Err(EncodeError::UnsupportedInfo(format!(
            "field day class: {class}"
        )));
    }
    let section_index = FIELD_DAY_SECTIONS
        .iter()
        .position(|candidate| *candidate == section.trim().to_uppercase())
        .ok_or_else(|| EncodeError::UnsupportedInfo(section.to_string()))?;

    let mut message_bits = [0u8; MESSAGE_BITS];
    write_bit_field(
        &mut message_bits,
        FTX_FIELD_DAY_LAYOUT.first_call,
        u64::from(encode_c28(first_call)?),
    );
    write_bit_field(
        &mut message_bits,
        FTX_FIELD_DAY_LAYOUT.second_call,
        u64::from(encode_c28(second_call)?),
    );
    write_bit_field(
        &mut message_bits,
        FTX_FIELD_DAY_LAYOUT.acknowledge,
        acknowledge as u64,
    );
    let (subtype, transmitter_offset) = if transmitter_count <= 16 {
        (3u64, u64::from(transmitter_count - 1))
    } else {
        (4u64, u64::from(transmitter_count - 17))
    };
    write_bit_field(
        &mut message_bits,
        FTX_FIELD_DAY_LAYOUT.transmitter_offset,
        transmitter_offset,
    );
    write_bit_field(
        &mut message_bits,
        FTX_FIELD_DAY_LAYOUT.class,
        u64::from(class_upper as u8 - b'A'),
    );
    write_bit_field(
        &mut message_bits,
        FTX_FIELD_DAY_LAYOUT.section,
        u64::try_from(section_index + 1).expect("section fits"),
    );
    write_bit_field(&mut message_bits, FTX_FIELD_DAY_LAYOUT.subtype, subtype);
    write_bit_field(&mut message_bits, FTX_FIELD_DAY_LAYOUT.kind, 0);
    build_frame_for_mode(Mode::Ft8, message_bits)
}

pub fn encode_rtty_contest_message(
    tu: bool,
    first_call: &str,
    second_call: &str,
    acknowledge: bool,
    report: u16,
    exchange: &TxRttyExchange,
) -> Result<EncodedFrame, EncodeError> {
    let report_bits = encode_rtty_report_bits(report);
    let exchange_bits = match exchange {
        TxRttyExchange::Multiplier(value) => {
            let index = RTTY_MULTIPLIERS
                .iter()
                .position(|candidate| *candidate == value.trim().to_uppercase())
                .ok_or_else(|| EncodeError::UnsupportedInfo(value.clone()))?;
            u16::try_from(8001 + index).expect("rtty multiplier fits")
        }
        TxRttyExchange::Serial(value) => *value,
    };

    let mut message_bits = [0u8; MESSAGE_BITS];
    write_bit_field(&mut message_bits, FTX_RTTY_CONTEST_LAYOUT.tu, tu as u64);
    write_bit_field(
        &mut message_bits,
        FTX_RTTY_CONTEST_LAYOUT.first_call,
        u64::from(encode_c28(first_call)?),
    );
    write_bit_field(
        &mut message_bits,
        FTX_RTTY_CONTEST_LAYOUT.second_call,
        u64::from(encode_c28(second_call)?),
    );
    write_bit_field(
        &mut message_bits,
        FTX_RTTY_CONTEST_LAYOUT.acknowledge,
        acknowledge as u64,
    );
    write_bit_field(
        &mut message_bits,
        FTX_RTTY_CONTEST_LAYOUT.report,
        u64::from(report_bits),
    );
    write_bit_field(
        &mut message_bits,
        FTX_RTTY_CONTEST_LAYOUT.exchange,
        u64::from(exchange_bits),
    );
    write_bit_field(
        &mut message_bits,
        FTX_RTTY_CONTEST_LAYOUT.kind,
        u64::from(FTX_MESSAGE_KIND_RTTY_CONTEST),
    );
    build_frame_for_mode(Mode::Ft8, message_bits)
}

pub fn encode_eu_vhf_message(
    first_hashed_call: &str,
    second_hashed_call: &str,
    acknowledge: bool,
    report: u8,
    serial: u16,
    grid6: &str,
) -> Result<EncodedFrame, EncodeError> {
    if !(52..=59).contains(&report) {
        return Err(EncodeError::UnsupportedInfo(format!(
            "eu vhf report: {report}"
        )));
    }
    let mut message_bits = [0u8; MESSAGE_BITS];
    write_bit_field(
        &mut message_bits,
        FTX_EU_VHF_LAYOUT.hashed_call12,
        hash_callsign(&strip_hash_wrapper(first_hashed_call), 12),
    );
    write_bit_field(
        &mut message_bits,
        FTX_EU_VHF_LAYOUT.hashed_call22,
        hash_callsign(&strip_hash_wrapper(second_hashed_call), 22),
    );
    write_bit_field(
        &mut message_bits,
        FTX_EU_VHF_LAYOUT.acknowledge,
        acknowledge as u64,
    );
    write_bit_field(
        &mut message_bits,
        FTX_EU_VHF_LAYOUT.report,
        u64::from(report - 52),
    );
    write_bit_field(
        &mut message_bits,
        FTX_EU_VHF_LAYOUT.serial,
        u64::from(serial.min(2047)),
    );
    write_bit_field(
        &mut message_bits,
        FTX_EU_VHF_LAYOUT.grid6,
        u64::from(encode_grid6(grid6, false)?),
    );
    write_bit_field(
        &mut message_bits,
        FTX_EU_VHF_LAYOUT.kind,
        u64::from(FTX_MESSAGE_KIND_EU_VHF),
    );
    build_frame_for_mode(Mode::Ft8, message_bits)
}

pub fn encode_nonstandard_message(
    hashed_callsign: &str,
    plain_callsign: &str,
    hashed_is_second: bool,
    reply: ReplyWord,
    cq: bool,
) -> Result<EncodedFrame, EncodeError> {
    let mut message_bits = [0u8; MESSAGE_BITS];
    write_bit_field(
        &mut message_bits,
        FTX_NONSTANDARD_LAYOUT.hashed_call,
        hash_callsign(&hashed_callsign.trim().to_uppercase(), 12),
    );
    write_bit_field(
        &mut message_bits,
        FTX_NONSTANDARD_LAYOUT.plain_call,
        encode_c58(plain_callsign)? as u64,
    );
    write_bit_field(
        &mut message_bits,
        FTX_NONSTANDARD_LAYOUT.hashed_is_second,
        hashed_is_second as u64,
    );
    write_bit_field(
        &mut message_bits,
        FTX_NONSTANDARD_LAYOUT.reply,
        encode_r2(reply) as u64,
    );
    write_bit_field(&mut message_bits, FTX_NONSTANDARD_LAYOUT.cq, cq as u64);
    write_bit_field(
        &mut message_bits,
        FTX_NONSTANDARD_LAYOUT.kind,
        u64::from(FTX_MESSAGE_KIND_NONSTANDARD),
    );
    build_frame_for_mode(Mode::Ft8, message_bits)
}

fn build_frame_for_mode(
    mode: Mode,
    message_bits: [u8; MESSAGE_BITS],
) -> Result<EncodedFrame, EncodeError> {
    match mode {
        Mode::Ft8 | Mode::Ft4 => build_ftx_frame(mode, message_bits),
        Mode::Ft2 => build_ft2_frame(message_bits),
    }
}

fn build_ftx_frame(mode: Mode, message_bits: [u8; MESSAGE_BITS]) -> Result<EncodedFrame, EncodeError> {
    let spec = mode.spec();
    let wire_message_bits = if mode == Mode::Ft4 {
        std::array::from_fn(|index| message_bits[index] ^ FT4_RVEC[index])
    } else {
        message_bits
    };
    let crc = crc14_ft8(&wire_message_bits);
    let mut info_bits = vec![0u8; spec.coding.info_bits];
    info_bits[..MESSAGE_BITS].copy_from_slice(&wire_message_bits);
    info_bits[MESSAGE_BITS..].copy_from_slice(&crc);

    let parity_rows = generator_rows();
    let mut codeword_bits = vec![0u8; spec.coding.codeword_bits];
    codeword_bits[..spec.coding.info_bits].copy_from_slice(&info_bits);
    for (row_index, row) in parity_rows.iter().enumerate() {
        let parity = row
            .iter()
            .zip(info_bits.iter())
            .fold(0u8, |acc, (tap, bit)| acc ^ (*tap & *bit));
        codeword_bits[spec.coding.info_bits + row_index] = parity;
    }

    let mut data_symbols = vec![0u8; spec.geometry.data_symbol_positions.len()];
    for (symbol_index, chunk) in codeword_bits
        .chunks_exact(spec.coding.bits_per_symbol)
        .enumerate()
    {
        data_symbols[symbol_index] = match mode {
            Mode::Ft8 => bits_to_tone(chunk),
            Mode::Ft4 => bits_to_ft4_tone(chunk),
            Mode::Ft2 => unreachable!(),
        };
    }

    let mut channel_symbols = vec![0u8; spec.geometry.message_symbols];
    populate_channel_symbols(&mut channel_symbols, &spec.geometry, &data_symbols)
        .expect("ftx channel layout");

    Ok(EncodedFrame {
        mode,
        message_bits,
        codeword_bits,
        data_symbols,
        channel_symbols,
    })
}

fn build_ft2_frame(message_bits: [u8; MESSAGE_BITS]) -> Result<EncodedFrame, EncodeError> {
    let spec = Mode::Ft2.spec();
    let crc = crc13_ft2(&message_bits);
    let mut info_bits = vec![0u8; spec.coding.info_bits];
    info_bits[..MESSAGE_BITS].copy_from_slice(&message_bits);
    info_bits[MESSAGE_BITS..].copy_from_slice(&crc);

    let parity_rows = ft2_generator_rows();
    let mut codeword_bits = vec![0u8; spec.coding.codeword_bits];
    codeword_bits[..spec.coding.info_bits].copy_from_slice(&info_bits);
    for (row_index, row) in parity_rows.iter().enumerate() {
        let parity = row
            .iter()
            .zip(info_bits.iter())
            .fold(0u8, |acc, (tap, bit)| acc ^ (*tap & *bit));
        codeword_bits[spec.coding.info_bits + row_index] = parity;
    }

    let data_symbols = codeword_bits.clone();
    let mut channel_symbols = vec![0u8; spec.geometry.message_symbols];
    populate_channel_symbols(&mut channel_symbols, &spec.geometry, &data_symbols)
        .expect("ft2 channel layout");

    Ok(EncodedFrame {
        mode: Mode::Ft2,
        message_bits,
        codeword_bits,
        data_symbols,
        channel_symbols,
    })
}

pub fn synthesize_rectangular_waveform(
    frame: &EncodedFrame,
    options: &WaveformOptions,
) -> Result<AudioBuffer, EncodeError> {
    let spec = frame.mode.spec();
    let sample_rate_hz = spec.geometry.sample_rate_hz;
    let total_samples = (options.total_seconds * sample_rate_hz as f32).round() as usize;
    let start_sample = (options.start_seconds * sample_rate_hz as f32).round() as usize;
    let frame_samples = spec.geometry.frame_samples();
    if start_sample + frame_samples > total_samples {
        return Err(EncodeError::WaveformTooShort);
    }

    let mut samples = vec![0.0f32; total_samples];
    let reference =
        synthesize_channel_reference_for_mode(frame.mode, &frame.channel_symbols, options.base_freq_hz);
    for (offset, sample) in reference.iter().enumerate() {
        samples[start_sample + offset] = options.amplitude * sample.re;
    }

    Ok(AudioBuffer {
        sample_rate_hz,
        samples,
    })
}

pub fn synthesize_channel_reference_for_mode(
    mode: Mode,
    channel_symbols: &[u8],
    base_freq_hz: f32,
) -> Vec<Complex32> {
    match mode {
        Mode::Ft8 | Mode::Ft4 => synthesize_gfsk_reference(mode, channel_symbols, base_freq_hz),
        Mode::Ft2 => synthesize_ft2_reference(channel_symbols, base_freq_hz),
    }
}

pub fn synthesize_channel_reference(
    channel_symbols: &[u8],
    base_freq_hz: f32,
) -> Vec<Complex32> {
    synthesize_channel_reference_for_mode(Mode::Ft8, channel_symbols, base_freq_hz)
}

fn synthesize_gfsk_reference(mode: Mode, channel_symbols: &[u8], base_freq_hz: f32) -> Vec<Complex32> {
    let spec = mode.spec();
    let nsym = spec.geometry.message_symbols;
    let nsps = spec.geometry.symbol_samples;
    let pulse = gfsk_frequency_pulse(GFSK_BT, nsps);
    let mut dphi = vec![0.0f32; (nsym + 2) * nsps];
    let dphi_peak = 2.0 * std::f32::consts::PI / nsps as f32;
    for (symbol_index, tone) in channel_symbols.iter().copied().enumerate() {
        let start = symbol_index * nsps;
        for offset in 0..(3 * nsps) {
            dphi[start + offset] += dphi_peak * pulse[offset] * tone as f32;
        }
    }
    for offset in 0..(2 * nsps) {
        dphi[offset] += dphi_peak * channel_symbols[0] as f32 * pulse[nsps + offset];
        dphi[nsym * nsps + offset] += dphi_peak * channel_symbols[nsym - 1] as f32 * pulse[offset];
    }

    let mut phase = 0.0f32;
    let carrier_step = 2.0 * std::f32::consts::PI * base_freq_hz / spec.geometry.sample_rate_hz as f32;
    let mut reference = vec![Complex32::new(0.0, 0.0); nsym * nsps];
    for (index, sample) in reference.iter_mut().enumerate() {
        *sample = Complex32::new(phase.cos(), phase.sin());
        phase = (phase + carrier_step + dphi[index + nsps]).rem_euclid(2.0 * std::f32::consts::PI);
    }

    let ramp_samples = (nsps as f32 / 8.0).round() as usize;
    for offset in 0..ramp_samples {
        let phase = std::f32::consts::PI * offset as f32 / (2.0 * ramp_samples as f32);
        let start_gain = phase.sin().powi(2);
        let end_gain = phase.cos().powi(2);
        reference[offset] *= start_gain;
        let tail_index = reference.len() - ramp_samples + offset;
        reference[tail_index] *= end_gain;
    }
    reference
}

fn synthesize_ft2_reference(channel_symbols: &[u8], base_freq_hz: f32) -> Vec<Complex32> {
    let spec = Mode::Ft2.spec();
    let nsps = spec.geometry.symbol_samples as f32;
    let sample_rate_hz = spec.geometry.sample_rate_hz as f32;
    let hmod = 0.8f32;
    let twopi = 2.0 * std::f32::consts::PI;
    let mut phase = 0.0f32;
    let mut reference = vec![Complex32::new(0.0, 0.0); channel_symbols.len() * spec.geometry.symbol_samples];
    for (symbol_index, tone) in channel_symbols.iter().copied().enumerate() {
        let delta =
            twopi * (base_freq_hz / sample_rate_hz + (hmod / 2.0) * (2.0 * tone as f32 - 1.0) / nsps);
        for offset in 0..spec.geometry.symbol_samples {
            let index = symbol_index * spec.geometry.symbol_samples + offset;
            reference[index] = Complex32::new(phase.cos(), phase.sin());
            phase = (phase + delta).rem_euclid(twopi);
        }
    }
    reference
}

fn gfsk_frequency_pulse(bt: f32, nsps: usize) -> Vec<f32> {
    let c = std::f32::consts::PI * (2.0 / std::f32::consts::LN_2).sqrt();
    (0..(3 * nsps))
        .map(|index| {
            let t = (index as f32 + 1.0 - 1.5 * nsps as f32) / nsps as f32;
            0.5 * (erf_approx(c * bt * (t + 0.5)) - erf_approx(c * bt * (t - 0.5)))
        })
        .collect()
}

fn erf_approx(x: f32) -> f32 {
    let sign = x.signum();
    let x = x.abs();
    let t = 1.0 / (1.0 + 0.3275911 * x);
    let y = 1.0
        - (((((1.061_405_4 * t - 1.453_152_1) * t) + 1.421_413_8) * t - 0.284_496_72) * t
            + 0.254_829_6)
            * t
            * (-x * x).exp();
    sign * y
}

pub fn channel_symbols_from_codeword_bits(
    codeword_bits: &[u8],
) -> Option<Vec<u8>> {
    channel_symbols_from_codeword_bits_for_mode(Mode::Ft8, codeword_bits)
}

pub fn channel_symbols_from_codeword_bits_for_mode(mode: Mode, codeword_bits: &[u8]) -> Option<Vec<u8>> {
    let spec = mode.spec();
    if codeword_bits.len() < spec.coding.codeword_bits {
        return None;
    }

    let mut data_symbols = vec![0u8; spec.geometry.data_symbol_positions.len()];
    for (symbol_index, chunk) in codeword_bits[..spec.coding.codeword_bits]
        .chunks_exact(spec.coding.bits_per_symbol)
        .enumerate()
    {
        data_symbols[symbol_index] = match mode {
            Mode::Ft8 => bits_to_tone(chunk),
            Mode::Ft4 => bits_to_ft4_tone(chunk),
            Mode::Ft2 => chunk[0],
        };
    }

    let mut channel_symbols = vec![0u8; spec.geometry.message_symbols];
    populate_channel_symbols(&mut channel_symbols, &spec.geometry, &data_symbols)
        .expect("channel layout");
    Some(channel_symbols)
}

pub fn write_rectangular_standard_wav(
    path: impl AsRef<Path>,
    first: &str,
    second: &str,
    acknowledge: bool,
    info: &GridReport,
    options: &WaveformOptions,
) -> Result<EncodedFrame, EncodeError> {
    let frame = encode_standard_message_for_mode(options.mode, first, second, acknowledge, info)?;
    let audio = synthesize_rectangular_waveform(&frame, options)?;
    write_wav(path, &audio).map_err(map_wave_error)?;
    Ok(frame)
}

pub fn parse_standard_info(text: &str) -> Result<GridReport, EncodeError> {
    let upper = text.trim().to_uppercase();
    if upper.is_empty() {
        return Ok(GridReport::Blank);
    }
    if upper == "RRR" {
        return Ok(GridReport::Reply(ReplyWord::Rrr));
    }
    if upper == "RR73" {
        return Ok(GridReport::Reply(ReplyWord::Rr73));
    }
    if upper == "73" {
        return Ok(GridReport::Reply(ReplyWord::SeventyThree));
    }
    if is_grid4(&upper) {
        return Ok(GridReport::Grid(upper));
    }
    if let Ok(report) = upper.parse::<i16>() {
        if (-50..=49).contains(&report) {
            return Ok(GridReport::Signal(report));
        }
    }
    Err(EncodeError::UnsupportedInfo(text.to_string()))
}

fn tx_message_fields(message: &TxMessage) -> (String, String, bool, GridReport, String) {
    match message {
        TxMessage::Cq { my_call, my_grid } => (
            "CQ".to_string(),
            my_call.clone(),
            false,
            my_grid
                .as_ref()
                .map(|grid| GridReport::Grid(grid.trim().to_uppercase()))
                .unwrap_or(GridReport::Blank),
            my_grid
                .as_ref()
                .map(|grid| format!("CQ {} {}", my_call.trim(), grid.trim().to_uppercase()))
                .unwrap_or_else(|| format!("CQ {}", my_call.trim())),
        ),
        TxMessage::Directed {
            my_call,
            peer_call,
            payload,
        } => {
            let (acknowledge, info) = tx_payload_fields(payload);
            let rendered = render_standard_message(peer_call, my_call, acknowledge, &info);
            (
                peer_call.clone(),
                my_call.clone(),
                acknowledge,
                info,
                rendered,
            )
        }
        TxMessage::DxpeditionCompound { .. }
        | TxMessage::FieldDay { .. }
        | TxMessage::RttyContest { .. }
        | TxMessage::EuVhf { .. } => {
            panic!("specialized FT8 messages bypass standard field rendering")
        }
    }
}

fn tx_payload_fields(payload: &TxDirectedPayload) -> (bool, GridReport) {
    match payload {
        TxDirectedPayload::Blank => (false, GridReport::Blank),
        TxDirectedPayload::Grid(locator) => (false, GridReport::Grid(locator.clone())),
        TxDirectedPayload::Signal(db) => (false, GridReport::Signal(*db)),
        TxDirectedPayload::SignalWithAck(db) => (true, GridReport::Signal(*db)),
        TxDirectedPayload::Reply(word) => (false, GridReport::Reply(*word)),
    }
}

fn render_standard_message(
    first: &str,
    second: &str,
    acknowledge: bool,
    info: &GridReport,
) -> String {
    let trailing = render_standard_info(acknowledge, info);
    if trailing.is_empty() {
        format!("{} {}", first.trim(), second.trim())
    } else {
        format!("{} {} {}", first.trim(), second.trim(), trailing)
    }
}

fn render_standard_info(acknowledge: bool, info: &GridReport) -> String {
    match info {
        GridReport::Blank => {
            if acknowledge {
                "R".to_string()
            } else {
                String::new()
            }
        }
        GridReport::Grid(locator) => {
            if acknowledge {
                format!("R {}", locator)
            } else {
                locator.clone()
            }
        }
        GridReport::Signal(db) => {
            let value = format!("{db:+03}");
            if acknowledge {
                format!("R{value}")
            } else {
                value
            }
        }
        GridReport::Reply(ReplyWord::Blank) => String::new(),
        GridReport::Reply(ReplyWord::Rrr) => "RRR".to_string(),
        GridReport::Reply(ReplyWord::Rr73) => "RR73".to_string(),
        GridReport::Reply(ReplyWord::SeventyThree) => "73".to_string(),
    }
}

fn render_field_day_message(
    first_call: &str,
    second_call: &str,
    acknowledge: bool,
    transmitter_count: u8,
    class: char,
    section: &str,
) -> String {
    if acknowledge {
        format!(
            "{} {} R {}{} {}",
            first_call.trim(),
            second_call.trim(),
            transmitter_count,
            class.to_ascii_uppercase(),
            section.trim().to_uppercase()
        )
    } else {
        format!(
            "{} {} {}{} {}",
            first_call.trim(),
            second_call.trim(),
            transmitter_count,
            class.to_ascii_uppercase(),
            section.trim().to_uppercase()
        )
    }
}

fn render_rtty_contest_message(
    tu: bool,
    first_call: &str,
    second_call: &str,
    acknowledge: bool,
    report: u16,
    exchange: &TxRttyExchange,
) -> String {
    let mut parts = Vec::new();
    if tu {
        parts.push("TU;".to_string());
    }
    parts.push(first_call.trim().to_uppercase());
    parts.push(second_call.trim().to_uppercase());
    if acknowledge {
        parts.push("R".to_string());
    }
    parts.push(format!("{report:03}"));
    parts.push(match exchange {
        TxRttyExchange::Multiplier(value) => value.trim().to_uppercase(),
        TxRttyExchange::Serial(value) => format!("{value:04}"),
    });
    parts.join(" ")
}

fn render_eu_vhf_message(
    first_hashed_call: &str,
    second_hashed_call: &str,
    acknowledge: bool,
    report: u8,
    serial: u16,
    grid6: &str,
) -> String {
    let first = format!("<{}>", strip_hash_wrapper(first_hashed_call));
    let second = format!("<{}>", strip_hash_wrapper(second_hashed_call));
    let exchange = format!("{report:02}{:04}", serial.min(2047));
    if acknowledge {
        format!(
            "{first} {second} R {exchange} {}",
            grid6.trim().to_uppercase()
        )
    } else {
        format!(
            "{first} {second} {exchange} {}",
            grid6.trim().to_uppercase()
        )
    }
}

fn map_wave_error(error: DecoderError) -> EncodeError {
    match error {
        DecoderError::Wav(source) => EncodeError::UnsupportedInfo(source.to_string()),
        DecoderError::UnsupportedFormat(message) => EncodeError::UnsupportedInfo(message),
    }
}

fn bits_to_ft4_tone(bits: &[u8]) -> u8 {
    match [bits[0], bits[1]] {
        [0, 0] => 0,
        [0, 1] => 1,
        [1, 1] => 2,
        [1, 0] => 3,
        _ => unreachable!(),
    }
}

fn generator_rows() -> &'static Vec<[u8; INFO_BITS]> {
    static ROWS: OnceLock<Vec<[u8; INFO_BITS]>> = OnceLock::new();
    ROWS.get_or_init(|| {
        include_str!("../data/generator.dat")
            .lines()
            .filter_map(|line| {
                let trimmed = line.trim();
                if trimmed.len() != INFO_BITS
                    || !trimmed.bytes().all(|byte| matches!(byte, b'0' | b'1'))
                {
                    return None;
                }
                let mut row = [0u8; INFO_BITS];
                for (index, byte) in trimmed.bytes().enumerate() {
                    row[index] = u8::from(byte == b'1');
                }
                Some(row)
            })
            .collect()
    })
}

fn ft2_generator_rows() -> &'static Vec<[u8; FT2_INFO_BITS]> {
    static ROWS: OnceLock<Vec<[u8; FT2_INFO_BITS]>> = OnceLock::new();
    ROWS.get_or_init(|| {
        const HEX_ROWS: [&str; 38] = [
            "a08ea80879050a5e94da994",
            "59f3b48040ca089c81ee880",
            "e4070262802e31b7b17d3dc",
            "95cbcbaf032dc3d960bacc8",
            "c4d79b5dcc21161a254ffbc",
            "93fde9cdbf2622a70868424",
            "e73b888bb1b01167379ba28",
            "45a0d0a0f39a7ad2439949c",
            "759acef19444bcad79c4964",
            "71eb4dddf4f5ed9e2ea17e0",
            "80f0ad76fb247d6b4ca8d38",
            "184fff3aa1b82dc66640104",
            "ca4e320bb382ed14cbb1094",
            "52514447b90e25b9e459e28",
            "dd10c1666e071956bd0df38",
            "99c332a0b792a2da8ef1ba8",
            "7bd9f688e7ed402e231aaac",
            "00fcad76eb647d6a0ca8c38",
            "6ac8d0499c43b02eed78d70",
            "2c2c764baf795b4788db010",
            "0e907bf9e280d2624823dd0",
            "b857a6e315afd8c1c925e64",
            "8deb58e22d73a141cae3778",
            "22d3cb80d92d6ac132dfe08",
            "754763877b28c187746855c",
            "1d1bb7cf6953732e04ebca4",
            "2c65e0ea4466ab9f5e1deec",
            "6dc530ca37fc916d1f84870",
            "49bccbbee152355be7ac984",
            "e8387f3f4367cf45a150448",
            "8ce25e03d67d51091c81884",
            "b798012ffa40a93852752c8",
            "2e43307933adfca37adc3c8",
            "ca06e0a42ca1ec782d6c06c",
            "c02b762927556a7039e638c",
            "4a3e9b7d08b6807f8619fac",
            "45e8030f68997bb68544424",
            "7e79362c16773efc6482e30",
        ];
        HEX_ROWS
            .iter()
            .map(|value| {
                let mut row = [0u8; FT2_INFO_BITS];
                let mut out = 0usize;
                for ch in value.bytes() {
                    let nibble = match ch {
                        b'0'..=b'9' => ch - b'0',
                        b'a'..=b'f' => 10 + ch - b'a',
                        b'A'..=b'F' => 10 + ch - b'A',
                        _ => unreachable!(),
                    };
                    let bits = if out + 4 > FT2_INFO_BITS {
                        2
                    } else {
                        4
                    };
                    for shift in (4 - bits..4).rev() {
                        row[out] = u8::from(((nibble >> shift) & 1) != 0);
                        out += 1;
                    }
                }
                row
            })
            .collect()
    })
}

fn encode_c28(text: &str) -> Result<u32, EncodeError> {
    let upper = text.trim().to_uppercase();
    match upper.as_str() {
        "DE" => Ok(0),
        "QRZ" => Ok(1),
        "CQ" => Ok(2),
        _ => {
            if let Some(rest) = upper.strip_prefix("CQ ") {
                if rest.len() == 3 && rest.chars().all(|ch| ch.is_ascii_digit()) {
                    let suffix = rest
                        .parse::<u32>()
                        .map_err(|_| EncodeError::UnsupportedToken(text.to_string()))?;
                    return Ok(3 + suffix);
                }
                if (1..=4).contains(&rest.len()) && rest.chars().all(|ch| ch.is_ascii_uppercase()) {
                    let mut packed = [' '; 4];
                    let offset = 4 - rest.len();
                    for (index, ch) in rest.chars().enumerate() {
                        packed[offset + index] = ch;
                    }
                    let mut encoded = 0u32;
                    for ch in packed {
                        encoded = encoded * 27 + u32::from(alphabet27_index(ch).unwrap_or(0));
                    }
                    return Ok(1003 + encoded);
                }
                return Err(EncodeError::UnsupportedToken(text.to_string()));
            }
            encode_callsign_or_hash(&upper)
        }
    }
}

fn encode_callsign_or_hash(callsign: &str) -> Result<u32, EncodeError> {
    let normalized = wsjtx_pack28_workaround(callsign);
    if normalized.is_empty() {
        return Err(EncodeError::UnsupportedCallsign(callsign.to_string()));
    }
    if let Some(encoded) = encode_standard_callsign(&normalized) {
        return Ok(CALL_STANDARD_BASE + encoded);
    }
    Ok(CALL_NTOKENS + hash_callsign(&normalized, 22) as u32)
}

fn wsjtx_pack28_workaround(callsign: &str) -> String {
    let upper = callsign.trim().to_uppercase();
    if upper.starts_with("3DA0") && upper.len() >= 7 {
        return format!("3D0{}", &upper[4..7]);
    }
    let bytes = upper.as_bytes();
    if bytes.len() >= 4 && bytes[0] == b'3' && bytes[1] == b'X' && bytes[2].is_ascii_uppercase() {
        let tail_end = upper.len().min(6);
        return format!("Q{}", &upper[2..tail_end]);
    }
    upper
}

fn encode_standard_callsign(callsign: &str) -> Option<u32> {
    let chars: Vec<char> = callsign.chars().collect();
    let digit_indices: Vec<usize> = chars
        .iter()
        .enumerate()
        .filter_map(|(index, ch)| ch.is_ascii_digit().then_some(index))
        .collect();
    let &digit_index = digit_indices.last()?;
    let area_index = digit_index + 1;
    if !(2..=3).contains(&area_index) {
        return None;
    }

    let mut digits_before = 0usize;
    let mut letters_before = 0usize;
    for ch in &chars[..digit_index] {
        if ch.is_ascii_digit() {
            digits_before += 1;
        }
        if ch.is_ascii_uppercase() {
            letters_before += 1;
        }
    }
    let letters_after = chars[digit_index + 1..]
        .iter()
        .filter(|ch| ch.is_ascii_uppercase())
        .count();
    if letters_before == 0 || digits_before >= digit_index || letters_after > 3 {
        return None;
    }

    let trimmed_len = if area_index == 2 { 5 } else { 6 };
    if chars.len() > trimmed_len {
        return None;
    }
    let body: String = chars.iter().collect();
    let raw: String = if area_index == 2 {
        format!(" {:<5}", body)
    } else {
        format!("{body:<6}")
    };
    encode_packed_standard_callsign(&raw)
}

fn encode_packed_standard_callsign(callsign: &str) -> Option<u32> {
    let padded: Vec<char> = callsign.chars().collect();
    if padded.len() != 6 {
        return None;
    }

    let i1 = alphabet37_index(padded[0])?;
    let i2 = alphabet36_index(padded[1])?;
    let i3 = digit10_index(padded[2])?;
    let i4 = alphabet27_index(padded[3])?;
    let i5 = alphabet27_index(padded[4])?;
    let i6 = alphabet27_index(padded[5])?;

    Some((((((i1 * 36) + i2) * 10 + i3) * 27 + i4) * 27 + i5) * 27 + i6)
}

fn encode_g15(info: &GridReport) -> Result<u16, EncodeError> {
    match info {
        GridReport::Grid(grid) if is_grid4(grid) => {
            let chars: Vec<char> = grid.chars().collect();
            let value = (((chars[0] as u16 - b'A' as u16) * 18 + (chars[1] as u16 - b'A' as u16))
                * 10
                + (chars[2] as u16 - b'0' as u16))
                * 10
                + (chars[3] as u16 - b'0' as u16);
            Ok(value)
        }
        GridReport::Blank => Ok(32_401),
        GridReport::Reply(ReplyWord::Rrr) => Ok(32_402),
        // WSJT-X compatibility: RR73 is sent using the overloaded grid-space codepoint.
        GridReport::Reply(ReplyWord::Rr73) => Ok(32_373),
        GridReport::Reply(ReplyWord::SeventyThree) => Ok(32_404),
        GridReport::Reply(ReplyWord::Blank) => Ok(32_401),
        GridReport::Signal(report) if (-50..=49).contains(report) => {
            Ok((32_435i32 + i32::from(*report)) as u16)
        }
        _ => Err(EncodeError::UnsupportedInfo(format!("{info:?}"))),
    }
}

fn encode_r2(reply: ReplyWord) -> u8 {
    match reply {
        ReplyWord::Blank => 0,
        ReplyWord::Rrr => 1,
        ReplyWord::Rr73 => 2,
        ReplyWord::SeventyThree => 3,
    }
}

fn dxpedition_report_bits(report_db: i16) -> u8 {
    (((i32::from(report_db) + 30) / 2).clamp(0, 31)) as u8
}

fn encode_rtty_report_bits(report: u16) -> u8 {
    (((i32::from(report) - 509) / 10) - 2).clamp(0, 7) as u8
}

fn encode_grid6(grid6: &str, allow_blank_tail: bool) -> Result<u32, EncodeError> {
    let mut normalized = grid6.trim().to_uppercase();
    if normalized.len() == 4 && allow_blank_tail {
        normalized.push_str("  ");
    }
    let chars: Vec<char> = normalized.chars().collect();
    let base = if allow_blank_tail { 25u32 } else { 24u32 };
    if chars.len() != 6
        || !matches!(chars[0], 'A'..='R')
        || !matches!(chars[1], 'A'..='R')
        || !chars[2].is_ascii_digit()
        || !chars[3].is_ascii_digit()
    {
        return Err(EncodeError::UnsupportedInfo(grid6.to_string()));
    }
    let encode_tail = |ch: char| -> Option<u32> {
        if allow_blank_tail && ch == ' ' {
            Some(24)
        } else if ('A'..='X').contains(&ch) {
            Some((ch as u8 - b'A') as u32)
        } else {
            None
        }
    };
    let tail1 =
        encode_tail(chars[4]).ok_or_else(|| EncodeError::UnsupportedInfo(grid6.to_string()))?;
    let tail2 =
        encode_tail(chars[5]).ok_or_else(|| EncodeError::UnsupportedInfo(grid6.to_string()))?;
    Ok(
        (((((chars[0] as u32 - 'A' as u32) * 18 + (chars[1] as u32 - 'A' as u32)) * 10
            + (chars[2] as u32 - '0' as u32))
            * 10
            + (chars[3] as u32 - '0' as u32))
            * base
            + tail1)
            * base
            + tail2,
    )
}

fn strip_hash_wrapper(callsign: &str) -> String {
    let trimmed = callsign.trim();
    if trimmed.starts_with('<') && trimmed.ends_with('>') && trimmed.len() >= 3 {
        trimmed[1..trimmed.len() - 1].trim().to_uppercase()
    } else {
        trimmed.to_uppercase()
    }
}

fn encode_c58(text: &str) -> Result<u128, EncodeError> {
    let mut normalized = text.trim().to_uppercase();
    if normalized.len() > 11 {
        return Err(EncodeError::UnsupportedCallsign(text.to_string()));
    }
    while normalized.len() < 11 {
        normalized.push(' ');
    }
    let mut value = 0u128;
    for ch in normalized.chars() {
        let digit = alphabet38_index(ch)
            .ok_or_else(|| EncodeError::UnsupportedCallsign(text.to_string()))?
            as u128;
        value = value * 38 + digit;
    }
    Ok(value)
}

fn bits_to_tone(bits: &[u8]) -> u8 {
    let triad = [bits[0], bits[1], bits[2]];
    let tone = gray_encode_3bits(triad);
    debug_assert_eq!(gray_decode_tone3(tone), triad);
    tone
}

fn is_grid4(grid: &str) -> bool {
    let bytes = grid.as_bytes();
    bytes.len() == 4
        && matches!(bytes[0], b'A'..=b'R')
        && matches!(bytes[1], b'A'..=b'R')
        && bytes[2].is_ascii_digit()
        && bytes[3].is_ascii_digit()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DecodeOptions;
    use crate::crc::crc14_ft8;
    use crate::decode_pcm;
    use crate::ldpc::ParityMatrix;
    use crate::message::{
        GridReport, HashResolver, StructuredInfoValue, StructuredMessage, unpack_message,
        unpack_message_for_mode,
    };

    #[test]
    fn standard_message_codeword_satisfies_parity() {
        let frame = encode_standard_message(
            "GJ0KYZ",
            "RK9AX",
            false,
            &GridReport::Grid("MO05".to_string()),
        )
        .expect("encode");
        assert!(ParityMatrix::global().parity_ok(&frame.codeword_bits));
    }

    #[test]
    fn ft4_standard_message_uses_ft4_geometry_and_symbolization() {
        let frame = encode_standard_message_for_mode(
            Mode::Ft4,
            "K1ABC",
            "W1XYZ",
            false,
            &GridReport::Grid("FN31".to_string()),
        )
        .expect("encode ft4");
        assert_eq!(frame.mode, Mode::Ft4);
        assert_eq!(frame.codeword_bits.len(), 174);
        assert_eq!(frame.data_symbols.len(), 87);
        assert_eq!(frame.channel_symbols.len(), 103);
        assert!(frame.channel_symbols.iter().all(|tone| *tone <= 3));
        let payload = unpack_message_for_mode(Mode::Ft4, &frame.codeword_bits).expect("payload");
        assert_eq!(payload.to_message(&HashResolver::default()).to_text(), "K1ABC W1XYZ FN31");
        assert_eq!(
            channel_symbols_from_codeword_bits_for_mode(Mode::Ft4, &frame.codeword_bits)
                .expect("channel symbols"),
            frame.channel_symbols
        );

        let audio = synthesize_rectangular_waveform(
            &frame,
            &WaveformOptions {
                mode: Mode::Ft4,
                total_seconds: 7.5,
                ..WaveformOptions::default()
            },
        )
        .expect("ft4 waveform");
        assert_eq!(audio.sample_rate_hz, 12_000);
        assert_eq!(audio.samples.len(), 90_000);
    }

    #[test]
    fn ft2_standard_message_uses_ft2_geometry_and_symbolization() {
        let frame = encode_standard_message_for_mode(
            Mode::Ft2,
            "K1ABC",
            "W1XYZ",
            false,
            &GridReport::Grid("FN31".to_string()),
        )
        .expect("encode ft2");
        assert_eq!(frame.mode, Mode::Ft2);
        assert_eq!(frame.codeword_bits.len(), 128);
        assert_eq!(frame.data_symbols.len(), 128);
        assert_eq!(frame.channel_symbols.len(), 144);
        assert!(frame.channel_symbols.iter().all(|tone| *tone <= 1));
        let payload = unpack_message_for_mode(Mode::Ft2, &frame.codeword_bits).expect("payload");
        assert_eq!(payload.to_message(&HashResolver::default()).to_text(), "K1ABC W1XYZ FN31");
        assert_eq!(
            channel_symbols_from_codeword_bits_for_mode(Mode::Ft2, &frame.codeword_bits)
                .expect("channel symbols"),
            frame.channel_symbols
        );

        let audio = synthesize_rectangular_waveform(
            &frame,
            &WaveformOptions {
                mode: Mode::Ft2,
                total_seconds: 2.5,
                ..WaveformOptions::default()
            },
        )
        .expect("ft2 waveform");
        assert_eq!(audio.sample_rate_hz, 12_000);
        assert_eq!(audio.samples.len(), 30_000);
    }

    #[test]
    fn ft2_standard_message_matches_reference_frame_bits() {
        let frame = encode_standard_message_for_mode(
            Mode::Ft2,
            "K1ABC",
            "W1XYZ",
            false,
            &GridReport::Grid("FN31".to_string()),
        )
        .expect("encode ft2");

        let message_bits: String = frame
            .message_bits
            .iter()
            .map(|bit| char::from(b'0' + *bit))
            .collect();
        assert_eq!(
            message_bits,
            "00001001101111011110001101010000011000000001011001010000000010100001011011001"
        );

        let codeword_bits: String = frame
            .codeword_bits
            .iter()
            .map(|bit| char::from(b'0' + *bit))
            .collect();
        assert_eq!(
            codeword_bits,
            "00001001101111011110001101010000011000000001011001010000000010100001011011001101110101010110101011010010000100001100101010000111"
        );

        let channel_symbols: String = frame
            .channel_symbols
            .iter()
            .map(|symbol| char::from(b'0' + *symbol))
            .collect();
        assert_eq!(
            channel_symbols,
            "000011111111000000001001101111011110001101010000011000000001011001010000000010100001011011001101110101010110101011010010000100001100101010000111"
        );
    }

    #[test]
    fn ft4_blank_cq_matches_reference_frame_bits() {
        let frame = encode_standard_message_for_mode(
            Mode::Ft4,
            "CQ",
            "K1ABC",
            false,
            &GridReport::Blank,
        )
        .expect("encode ft4 blank cq");

        let codeword_bits: String = frame
            .codeword_bits
            .iter()
            .map(|bit| char::from(b'0' + *bit))
            .collect();
        assert_eq!(
            codeword_bits,
            "010010100101111010001001100101001111110101100101011000111100101000011010011000001010110011000000100110010000000111100000000011000000100000101101100101000010110000010100010011"
        );

        let channel_symbols: String = frame
            .channel_symbols
            .iter()
            .map(|symbol| char::from(b'0' + *symbol))
            .collect();
        assert_eq!(
            channel_symbols,
            "0132103311233031311022211311130221023033013313003320200031310001232310000020003003213110032001101023201"
        );
    }

    #[test]
    fn encodes_cq_dx_token_round_trip() {
        let frame = encode_standard_message(
            "CQ DX",
            "R6WA",
            false,
            &GridReport::Grid("LN32".to_string()),
        )
        .expect("encode");
        let payload = unpack_message(&frame.codeword_bits).expect("payload");
        let rendered = payload.to_message(&HashResolver::default());
        assert_eq!(rendered.to_text(), "CQ DX R6WA LN32");
    }

    #[test]
    fn encodes_nonstandard_call_as_hash22() {
        let frame =
            encode_standard_message("CQ", "HF19NY", false, &GridReport::Blank).expect("encode");
        let payload = unpack_message(&frame.codeword_bits).expect("payload");
        let mut resolver = HashResolver::default();
        resolver.insert_callsign("HF19NY");
        let rendered = payload.to_message(&resolver);
        assert_eq!(rendered.to_text(), "CQ HF19NY");
    }

    #[test]
    fn encodes_nonstandard_base_callsign_pair() {
        let frame = encode_standard_message("YO7CGS", "A41ZZ", false, &GridReport::Signal(-11))
            .expect("encode");
        let payload = unpack_message(&frame.codeword_bits).expect("payload");
        let mut resolver = HashResolver::default();
        resolver.insert_callsign("YO7CGS");
        resolver.insert_callsign("A41ZZ");
        let rendered = payload.to_message(&resolver);
        assert_eq!(rendered.to_text(), "YO7CGS A41ZZ -11");
    }

    #[test]
    fn encodes_dxpedition_rr73_message() {
        let frame = encode_dxpedition_message("K1ABC", "W9XYZ", "KH1/KH7Z", -11).expect("encode");
        let payload = unpack_message(&frame.codeword_bits).expect("payload");
        let unresolved = payload.to_message(&HashResolver::default());
        assert_eq!(unresolved.to_text(), "K1ABC RR73; W9XYZ <...> -12");

        let mut resolver = HashResolver::default();
        resolver.insert_callsign("KH1/KH7Z");
        let resolved = payload.to_message(&resolver);
        assert_eq!(resolved.to_text(), "K1ABC RR73; W9XYZ <KH1/KH7Z> -12");
    }

    #[test]
    fn encodes_field_day_message() {
        let frame = encode_field_day_message("WA9XYZ", "KA1ABC", true, 16, 'A', "EMA")
            .expect("encode field day");
        let payload = unpack_message(&frame.codeword_bits).expect("payload");
        let rendered = payload.to_message(&HashResolver::default());
        assert_eq!(rendered.to_text(), "WA9XYZ KA1ABC R 16A EMA");
    }

    #[test]
    fn encodes_rtty_contest_tu_message() {
        let frame = encode_rtty_contest_message(
            true,
            "W9XYZ",
            "K1ABC",
            true,
            579,
            &TxRttyExchange::Multiplier("MA".to_string()),
        )
        .expect("encode rtty");
        let payload = unpack_message(&frame.codeword_bits).expect("payload");
        let rendered = payload.to_message(&HashResolver::default());
        assert_eq!(rendered.to_text(), "TU; W9XYZ K1ABC R 579 MA");
    }

    #[test]
    fn encodes_eu_vhf_hashed_message() {
        let frame = encode_eu_vhf_message("<PA3XYZ>", "<G4ABC/P>", true, 59, 3, "IO91NP")
            .expect("encode eu vhf");
        let payload = unpack_message(&frame.codeword_bits).expect("payload");
        let unresolved = payload.to_message(&HashResolver::default());
        assert_eq!(unresolved.to_text(), "<...> <...> R 590003 IO91NP");

        let mut resolver = HashResolver::default();
        resolver.insert_callsign("PA3XYZ");
        resolver.insert_callsign("G4ABC/P");
        let resolved = payload.to_message(&resolver);
        assert_eq!(resolved.to_text(), "<PA3XYZ> <G4ABC/P> R 590003 IO91NP");
    }

    #[test]
    fn wsjtx_reference_vectors_match_special_message_payloads() {
        let dxped = encode_dxpedition_message("K1ABC", "W9XYZ", "KH1/KH7Z", -11)
            .expect("encode dxpedition");
        assert_eq!(
            bits_to_string(&dxped.message_bits),
            "00001001101111011110001101010000110000101001001110111000001100100101001001000"
        );
        let dxped_payload = payload_from_message_bits(&dxped.message_bits);
        let mut dxped_resolver = HashResolver::default();
        dxped_resolver.insert_callsign("KH1/KH7Z");
        assert_eq!(
            dxped_payload.to_message(&dxped_resolver).to_text(),
            "K1ABC RR73; W9XYZ <KH1/KH7Z> -12"
        );

        let field_day_16 =
            encode_field_day_message("WA9XYZ", "KA1ABC", true, 16, 'A', "EMA").expect("fd16");
        assert_eq!(
            bits_to_string(&field_day_16.message_bits),
            "11100111000010000110110111001001010111000110010100100001111110000001011011000"
        );
        assert_eq!(
            payload_from_message_bits(&field_day_16.message_bits)
                .to_message(&HashResolver::default())
                .to_text(),
            "WA9XYZ KA1ABC R 16A EMA"
        );

        let field_day_32 =
            encode_field_day_message("WA9XYZ", "KA1ABC", true, 32, 'A', "EMA").expect("fd32");
        assert_eq!(
            bits_to_string(&field_day_32.message_bits),
            "11100111000010000110110111001001010111000110010100100001111110000001011100000"
        );
        assert_eq!(
            payload_from_message_bits(&field_day_32.message_bits)
                .to_message(&HashResolver::default())
                .to_text(),
            "WA9XYZ KA1ABC R 32A EMA"
        );

        let rtty_tu = encode_rtty_contest_message(
            true,
            "W9XYZ",
            "K1ABC",
            true,
            579,
            &TxRttyExchange::Multiplier("MA".to_string()),
        )
        .expect("encode rtty tu");
        assert_eq!(
            bits_to_string(&rtty_tu.message_bits),
            "10000110000101001001110111000000010011011110111100011010111011111101010101011"
        );
        assert_eq!(
            payload_from_message_bits(&rtty_tu.message_bits)
                .to_message(&HashResolver::default())
                .to_text(),
            "TU; W9XYZ K1ABC R 579 MA"
        );

        let rtty_serial = encode_rtty_contest_message(
            false,
            "W9XYZ",
            "G8ABC",
            true,
            559,
            &TxRttyExchange::Serial(13),
        )
        .expect("encode rtty serial");
        assert_eq!(
            bits_to_string(&rtty_serial.message_bits),
            "00000110000101001001110111000000010010001111101001111001010110000000001101011"
        );
        assert_eq!(
            payload_from_message_bits(&rtty_serial.message_bits)
                .to_message(&HashResolver::default())
                .to_text(),
            "W9XYZ G8ABC R 559 0013"
        );

        let eu_vhf = encode_eu_vhf_message("<PA3XYZ>", "<G4ABC/P>", true, 59, 3, "IO91NP")
            .expect("encode eu vhf");
        assert_eq!(
            bits_to_string(&eu_vhf.message_bits),
            "11001000101111001000101111100100111111000000000110100010111010110000000111101"
        );
        let eu_vhf_payload = payload_from_message_bits(&eu_vhf.message_bits);
        let mut eu_vhf_resolver = HashResolver::default();
        eu_vhf_resolver.insert_callsign("PA3XYZ");
        eu_vhf_resolver.insert_callsign("G4ABC/P");
        assert_eq!(
            eu_vhf_payload.to_message(&eu_vhf_resolver).to_text(),
            "<PA3XYZ> <G4ABC/P> R 590003 IO91NP"
        );
    }

    #[test]
    fn structured_standard_message_preserves_raw_fields() {
        let frame = encode_standard_message(
            "GJ0KYZ",
            "RK9AX",
            false,
            &GridReport::Grid("MO05".to_string()),
        )
        .expect("encode");
        let payload = unpack_message(&frame.codeword_bits).expect("payload");
        let message = payload.to_message(&HashResolver::default());
        let StructuredMessage::Standard {
            first,
            second,
            info,
            ..
        } = message
        else {
            panic!("expected standard message");
        };

        assert_eq!(first.raw, read_bits(&frame.message_bits, 0, 28) as u32);
        assert_eq!(second.raw, read_bits(&frame.message_bits, 29, 28) as u32);
        assert_eq!(info.raw, read_bits(&frame.message_bits, 59, 15) as u16);
        assert!(matches!(
            info.value,
            StructuredInfoValue::Grid { ref locator } if locator == "MO05"
        ));
    }

    #[test]
    fn rr73_reply_uses_wsjt_compatible_wire_value_and_normalizes_on_decode() {
        let frame =
            encode_standard_message("W5XO", "N1VF", false, &GridReport::Reply(ReplyWord::Rr73))
                .expect("encode rr73");
        let payload = unpack_message(&frame.codeword_bits).expect("payload");
        let message = payload.to_message(&HashResolver::default());
        let StructuredMessage::Standard { info, .. } = message else {
            panic!("expected standard message");
        };

        assert_eq!(info.raw, 32_373);
        assert!(matches!(
            info.value,
            StructuredInfoValue::Reply {
                word: ReplyWord::Rr73
            }
        ));
    }

    #[test]
    #[ignore = "slow end-to-end bringup check"]
    fn encodes_and_decodes_rectangular_standard_message() {
        let frame = encode_standard_message(
            "GJ0KYZ",
            "RK9AX",
            false,
            &GridReport::Grid("MO05".to_string()),
        )
        .expect("encode");
        let audio = synthesize_rectangular_waveform(
            &frame,
            &WaveformOptions {
                base_freq_hz: 1_234.0,
                ..WaveformOptions::default()
            },
        )
        .expect("waveform");
        let report = decode_pcm(
            &audio,
            &DecodeOptions {
                max_candidates: 8,
                max_successes: 2,
                ..DecodeOptions::default()
            },
        )
        .expect("decode");
        let decoded: Vec<_> = report
            .decodes
            .iter()
            .map(|decode| decode.text.as_str())
            .collect();
        assert!(
            decoded.contains(&"GJ0KYZ RK9AX MO05"),
            "decoded messages: {decoded:?}"
        );
    }

    fn read_bits(bits: &[u8], start: usize, len: usize) -> u64 {
        let mut value = 0u64;
        for bit in &bits[start..start + len] {
            value = (value << 1) | (*bit as u64);
        }
        value
    }

    fn bits_to_string(bits: &[u8]) -> String {
        bits.iter()
            .map(|bit| if *bit == 0 { '0' } else { '1' })
            .collect()
    }

    fn payload_from_message_bits(message_bits: &[u8]) -> crate::message::Payload {
        let mut codeword = Vec::with_capacity(91);
        codeword.extend_from_slice(message_bits);
        codeword.extend_from_slice(&crc14_ft8(message_bits));
        unpack_message(&codeword).expect("payload")
    }
}
