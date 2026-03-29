use std::path::Path;
use std::sync::OnceLock;

use num_complex::Complex32;
use thiserror::Error;

use crate::crc::crc14_ft8;
use crate::message::{GridReport, ReplyWord, hash_callsign};
use crate::protocol::{
    CALL_NTOKENS, CALL_STANDARD_BASE, FT8_COSTAS, FT8_MESSAGE_SYMBOLS, FT8_SAMPLE_RATE,
    FT8_SYMBOL_SAMPLES, GRAY_TONES_TO_BITS,
};
use crate::wave::{AudioBuffer, DecoderError, write_wav};

const MESSAGE_BITS: usize = 77;
const INFO_BITS: usize = 91;
const CODEWORD_BITS: usize = 174;
const DATA_SYMBOLS: usize = 58;
const GFSK_BT: f32 = 2.0;

#[derive(Debug, Clone)]
pub struct EncodedFrame {
    pub message_bits: [u8; MESSAGE_BITS],
    pub codeword_bits: [u8; CODEWORD_BITS],
    pub data_symbols: [u8; DATA_SYMBOLS],
    pub channel_symbols: [u8; FT8_MESSAGE_SYMBOLS],
}

#[derive(Debug, Clone)]
pub struct WaveformOptions {
    pub base_freq_hz: f32,
    pub start_seconds: f32,
    pub total_seconds: f32,
    pub amplitude: f32,
}

impl Default for WaveformOptions {
    fn default() -> Self {
        Self {
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
    #[error("waveform too short for FT8 frame")]
    WaveformTooShort,
}

pub fn encode_standard_message(
    first: &str,
    second: &str,
    acknowledge: bool,
    info: &GridReport,
) -> Result<EncodedFrame, EncodeError> {
    let mut message_bits = [0u8; MESSAGE_BITS];
    write_bits(
        &mut message_bits,
        0,
        28,
        u64::from(encode_c28(first)?),
    );
    write_bits(&mut message_bits, 28, 1, 0);
    write_bits(
        &mut message_bits,
        29,
        28,
        u64::from(encode_c28(second)?),
    );
    write_bits(&mut message_bits, 57, 1, 0);
    write_bits(&mut message_bits, 58, 1, acknowledge as u64);
    write_bits(&mut message_bits, 59, 15, u64::from(encode_g15(info)?));
    write_bits(&mut message_bits, 74, 3, 1);
    build_frame(message_bits)
}

pub fn encode_nonstandard_message(
    hashed_callsign: &str,
    plain_callsign: &str,
    hashed_is_second: bool,
    reply: ReplyWord,
    cq: bool,
) -> Result<EncodedFrame, EncodeError> {
    let mut message_bits = [0u8; MESSAGE_BITS];
    write_bits(
        &mut message_bits,
        0,
        12,
        hash_callsign(&hashed_callsign.trim().to_uppercase(), 12),
    );
    write_bits(
        &mut message_bits,
        12,
        58,
        encode_c58(plain_callsign)? as u64,
    );
    write_bits(&mut message_bits, 70, 1, hashed_is_second as u64);
    write_bits(&mut message_bits, 71, 2, encode_r2(reply) as u64);
    write_bits(&mut message_bits, 73, 1, cq as u64);
    write_bits(&mut message_bits, 74, 3, 4);
    build_frame(message_bits)
}

fn build_frame(message_bits: [u8; MESSAGE_BITS]) -> Result<EncodedFrame, EncodeError> {
    let crc = crc14_ft8(&message_bits);
    let mut info_bits = [0u8; INFO_BITS];
    info_bits[..MESSAGE_BITS].copy_from_slice(&message_bits);
    info_bits[MESSAGE_BITS..].copy_from_slice(&crc);

    let parity_rows = generator_rows();
    let mut codeword_bits = [0u8; CODEWORD_BITS];
    codeword_bits[..INFO_BITS].copy_from_slice(&info_bits);
    for (row_index, row) in parity_rows.iter().enumerate() {
        let parity = row
            .iter()
            .zip(info_bits.iter())
            .fold(0u8, |acc, (tap, bit)| acc ^ (*tap & *bit));
        codeword_bits[INFO_BITS + row_index] = parity;
    }

    let mut data_symbols = [0u8; DATA_SYMBOLS];
    for (symbol_index, chunk) in codeword_bits.chunks_exact(3).enumerate() {
        data_symbols[symbol_index] = bits_to_tone(chunk);
    }

    let mut channel_symbols = [0u8; FT8_MESSAGE_SYMBOLS];
    channel_symbols[..7].copy_from_slice(&FT8_COSTAS.map(|tone| tone as u8));
    channel_symbols[36..43].copy_from_slice(&FT8_COSTAS.map(|tone| tone as u8));
    channel_symbols[72..79].copy_from_slice(&FT8_COSTAS.map(|tone| tone as u8));
    channel_symbols[7..36].copy_from_slice(&data_symbols[..29]);
    channel_symbols[43..72].copy_from_slice(&data_symbols[29..]);

    Ok(EncodedFrame {
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
    let total_samples = (options.total_seconds * FT8_SAMPLE_RATE as f32).round() as usize;
    let start_sample = (options.start_seconds * FT8_SAMPLE_RATE as f32).round() as usize;
    let frame_samples = FT8_MESSAGE_SYMBOLS * FT8_SYMBOL_SAMPLES;
    if start_sample + frame_samples > total_samples {
        return Err(EncodeError::WaveformTooShort);
    }

    let mut samples = vec![0.0f32; total_samples];
    let reference = synthesize_channel_reference(&frame.channel_symbols, options.base_freq_hz);
    for (offset, sample) in reference.iter().enumerate() {
        samples[start_sample + offset] = options.amplitude * sample.re;
    }

    Ok(AudioBuffer {
        sample_rate_hz: FT8_SAMPLE_RATE,
        samples,
    })
}

pub fn synthesize_channel_reference(
    channel_symbols: &[u8; FT8_MESSAGE_SYMBOLS],
    base_freq_hz: f32,
) -> Vec<Complex32> {
    let nsym = FT8_MESSAGE_SYMBOLS;
    let nsps = FT8_SYMBOL_SAMPLES;
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
    let carrier_step = 2.0 * std::f32::consts::PI * base_freq_hz / FT8_SAMPLE_RATE as f32;
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
) -> Option<[u8; FT8_MESSAGE_SYMBOLS]> {
    if codeword_bits.len() < CODEWORD_BITS {
        return None;
    }

    let mut data_symbols = [0u8; DATA_SYMBOLS];
    for (symbol_index, chunk) in codeword_bits[..CODEWORD_BITS].chunks_exact(3).enumerate() {
        data_symbols[symbol_index] = bits_to_tone(chunk);
    }

    let mut channel_symbols = [0u8; FT8_MESSAGE_SYMBOLS];
    channel_symbols[..7].copy_from_slice(&FT8_COSTAS.map(|tone| tone as u8));
    channel_symbols[36..43].copy_from_slice(&FT8_COSTAS.map(|tone| tone as u8));
    channel_symbols[72..79].copy_from_slice(&FT8_COSTAS.map(|tone| tone as u8));
    channel_symbols[7..36].copy_from_slice(&data_symbols[..29]);
    channel_symbols[43..72].copy_from_slice(&data_symbols[29..]);
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
    let frame = encode_standard_message(first, second, acknowledge, info)?;
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

fn map_wave_error(error: DecoderError) -> EncodeError {
    match error {
        DecoderError::Wav(source) => EncodeError::UnsupportedInfo(source.to_string()),
        DecoderError::UnsupportedFormat(message) => EncodeError::UnsupportedInfo(message),
    }
}

fn generator_rows() -> &'static Vec<[u8; INFO_BITS]> {
    static ROWS: OnceLock<Vec<[u8; INFO_BITS]>> = OnceLock::new();
    ROWS.get_or_init(|| {
        include_str!("../data/generator.dat")
            .lines()
            .filter_map(|line| {
                let trimmed = line.trim();
                if trimmed.len() != INFO_BITS || !trimmed.bytes().all(|byte| matches!(byte, b'0' | b'1'))
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

fn encode_c28(text: &str) -> Result<u32, EncodeError> {
    let upper = text.trim().to_uppercase();
    match upper.as_str() {
        "DE" => Ok(0),
        "QRZ" => Ok(1),
        "CQ" => Ok(2),
        _ => {
            if let Some(rest) = upper.strip_prefix("CQ ") {
                if rest.len() == 3 && rest.chars().all(|ch| ch.is_ascii_digit()) {
                    let suffix = rest.parse::<u32>().map_err(|_| {
                        EncodeError::UnsupportedToken(text.to_string())
                    })?;
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
    if bytes.len() >= 4
        && bytes[0] == b'3'
        && bytes[1] == b'X'
        && bytes[2].is_ascii_uppercase()
    {
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
    let i3 = digit_index_10(padded[2])?;
    let i4 = alphabet27_index(padded[3])?;
    let i5 = alphabet27_index(padded[4])?;
    let i6 = alphabet27_index(padded[5])?;

    Some((((((i1 * 36) + i2) * 10 + i3) * 27 + i4) * 27 + i5) * 27 + i6)
}

fn encode_g15(info: &GridReport) -> Result<u16, EncodeError> {
    match info {
        GridReport::Grid(grid) if is_grid4(grid) => {
            let chars: Vec<char> = grid.chars().collect();
            let value = (((chars[0] as u16 - b'A' as u16) * 18
                + (chars[1] as u16 - b'A' as u16))
                * 10
                + (chars[2] as u16 - b'0' as u16))
                * 10
                + (chars[3] as u16 - b'0' as u16);
            Ok(value)
        }
        GridReport::Blank => Ok(32_400),
        GridReport::Reply(ReplyWord::Rrr) => Ok(32_402),
        GridReport::Reply(ReplyWord::Rr73) => Ok(32_403),
        GridReport::Reply(ReplyWord::SeventyThree) => Ok(32_404),
        GridReport::Reply(ReplyWord::Blank) => Ok(32_400),
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

fn encode_c58(text: &str) -> Result<u128, EncodeError> {
    let alphabet: Vec<char> = " 0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ/".chars().collect();
    let mut normalized = text.trim().to_uppercase();
    if normalized.len() > 11 {
        return Err(EncodeError::UnsupportedCallsign(text.to_string()));
    }
    while normalized.len() < 11 {
        normalized.push(' ');
    }
    let mut value = 0u128;
    for ch in normalized.chars() {
        let digit = alphabet
            .iter()
            .position(|candidate| *candidate == ch)
            .ok_or_else(|| EncodeError::UnsupportedCallsign(text.to_string()))? as u128;
        value = value * 38 + digit;
    }
    Ok(value)
}

fn bits_to_tone(bits: &[u8]) -> u8 {
    GRAY_TONES_TO_BITS
        .iter()
        .position(|candidate| candidate.as_slice() == bits)
        .expect("valid Gray triad") as u8
}

fn write_bits(bits: &mut [u8], start: usize, len: usize, value: u64) {
    for bit_index in 0..len {
        let shift = len - 1 - bit_index;
        bits[start + bit_index] = ((value >> shift) & 1) as u8;
    }
}

fn is_grid4(grid: &str) -> bool {
    let bytes = grid.as_bytes();
    bytes.len() == 4
        && matches!(bytes[0], b'A'..=b'R')
        && matches!(bytes[1], b'A'..=b'R')
        && bytes[2].is_ascii_digit()
        && bytes[3].is_ascii_digit()
}

fn alphabet37_index(ch: char) -> Option<u32> {
    " 0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        .chars()
        .position(|candidate| candidate == ch)
        .map(|index| index as u32)
}

fn alphabet36_index(ch: char) -> Option<u32> {
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        .chars()
        .position(|candidate| candidate == ch)
        .map(|index| index as u32)
}

fn digit_index_10(ch: char) -> Option<u32> {
    "0123456789"
        .chars()
        .position(|candidate| candidate == ch)
        .map(|index| index as u32)
}

fn alphabet27_index(ch: char) -> Option<u32> {
    " ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        .chars()
        .position(|candidate| candidate == ch)
        .map(|index| index as u32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode_pcm;
    use crate::message::{GridReport, HashResolver, unpack_message};
    use crate::ldpc::ParityMatrix;
    use crate::DecodeOptions;

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
    fn encodes_cq_dx_token_round_trip() {
        let frame = encode_standard_message(
            "CQ DX",
            "R6WA",
            false,
            &GridReport::Grid("LN32".to_string()),
        )
        .expect("encode");
        let payload = unpack_message(&frame.codeword_bits).expect("payload");
        let rendered = payload.render(&HashResolver::default());
        assert_eq!(rendered.text, "CQ DX R6WA LN32");
    }

    #[test]
    fn encodes_nonstandard_call_as_hash22() {
        let frame = encode_standard_message("CQ", "HF19NY", false, &GridReport::Blank)
            .expect("encode");
        let payload = unpack_message(&frame.codeword_bits).expect("payload");
        let mut resolver = HashResolver::default();
        resolver.insert_callsign("HF19NY");
        let rendered = payload.render(&resolver);
        assert_eq!(rendered.text, "CQ HF19NY");
    }

    #[test]
    fn encodes_nonstandard_base_callsign_pair() {
        let frame = encode_standard_message("YO7CGS", "A41ZZ", false, &GridReport::Signal(-11))
            .expect("encode");
        let payload = unpack_message(&frame.codeword_bits).expect("payload");
        let mut resolver = HashResolver::default();
        resolver.insert_callsign("YO7CGS");
        resolver.insert_callsign("A41ZZ");
        let rendered = payload.render(&resolver);
        assert_eq!(rendered.text, "YO7CGS A41ZZ -11");
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
        let decoded: Vec<_> = report.decodes.iter().map(|decode| decode.text.as_str()).collect();
        assert!(
            decoded.contains(&"GJ0KYZ RK9AX MO05"),
            "decoded messages: {decoded:?}"
        );
    }
}
