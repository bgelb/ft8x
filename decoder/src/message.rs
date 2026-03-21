use std::collections::HashMap;

use serde::Serialize;

use crate::crc;
use crate::protocol::{CALL_MAX22, CALL_NTOKENS, CALL_STANDARD_BASE, HASH_MULTIPLIER};

#[derive(Debug, Clone, Serialize)]
pub enum MessageKind {
    FreeText,
    Standard,
    Nonstandard,
    Unsupported,
}

#[derive(Debug, Clone, Serialize)]
pub struct DecodedPayload {
    pub kind: MessageKind,
    pub text: String,
    pub primary_call: Option<String>,
    pub secondary_call: Option<String>,
}

#[derive(Debug, Clone)]
pub enum Payload {
    FreeText(String),
    Standard(StandardMessage),
    Nonstandard(NonstandardMessage),
    Unsupported(u8, Option<u8>),
}

#[derive(Debug, Clone)]
pub struct StandardMessage {
    pub first: CallField,
    pub first_suffix: Option<&'static str>,
    pub second: CallField,
    pub second_suffix: Option<&'static str>,
    pub acknowledge: bool,
    pub info: GridReport,
}

#[derive(Debug, Clone)]
pub struct NonstandardMessage {
    pub hashed: u16,
    pub plain: String,
    pub hashed_is_second: bool,
    pub reply: ReplyWord,
    pub cq: bool,
}

#[derive(Debug, Clone)]
pub enum CallField {
    Token(String),
    Standard(String),
    Hash22(u32),
}

#[derive(Debug, Clone)]
pub enum GridReport {
    Grid(String),
    Signal(i16),
    Blank,
    Reply(ReplyWord),
}

#[derive(Debug, Clone, Copy)]
pub enum ReplyWord {
    Blank,
    Rrr,
    Rr73,
    SeventyThree,
}

#[derive(Default, Debug)]
pub struct HashResolver {
    h10: HashMap<u16, String>,
    h12: HashMap<u16, String>,
    h22: HashMap<u32, String>,
}

impl HashResolver {
    pub fn insert_callsign(&mut self, callsign: &str) {
        if callsign.is_empty() {
            return;
        }
        let normalized = callsign.trim().to_string();
        if normalized.is_empty() {
            return;
        }
        self.h10
            .entry(hash_callsign(&normalized, 10) as u16)
            .or_insert_with(|| normalized.clone());
        self.h12
            .entry(hash_callsign(&normalized, 12) as u16)
            .or_insert_with(|| normalized.clone());
        self.h22
            .entry(hash_callsign(&normalized, 22) as u32)
            .or_insert(normalized);
    }

    pub fn resolve12(&self, hash: u16) -> Option<&str> {
        self.h12.get(&hash).map(String::as_str)
    }

    pub fn resolve22(&self, hash: u32) -> Option<&str> {
        self.h22.get(&hash).map(String::as_str)
    }
}

pub fn unpack_message(codeword: &[u8]) -> Option<Payload> {
    if codeword.len() < 91 {
        return None;
    }
    let message_bits = &codeword[..77];
    let crc_bits = &codeword[77..91];
    if !crc::crc_matches(message_bits, crc_bits) {
        return None;
    }

    let i3 = read_bits(message_bits, 74, 3) as u8;
    match i3 {
        0 => {
            let n3 = read_bits(message_bits, 71, 3) as u8;
            match n3 {
                0 => Some(Payload::FreeText(decode_free_text(read_bits_u128(
                    message_bits,
                    0,
                    71,
                )))),
                _ => Some(Payload::Unsupported(i3, Some(n3))),
            }
        }
        1 => Some(Payload::Standard(StandardMessage {
            first: decode_c28(read_bits(message_bits, 0, 28)),
            first_suffix: if read_bits(message_bits, 28, 1) == 1 {
                Some("/R")
            } else {
                None
            },
            second: decode_c28(read_bits(message_bits, 29, 28)),
            second_suffix: if read_bits(message_bits, 57, 1) == 1 {
                Some("/R")
            } else {
                None
            },
            acknowledge: read_bits(message_bits, 58, 1) == 1,
            info: decode_g15(read_bits(message_bits, 59, 15)),
        })),
        2 => Some(Payload::Standard(StandardMessage {
            first: decode_c28(read_bits(message_bits, 0, 28)),
            first_suffix: if read_bits(message_bits, 28, 1) == 1 {
                Some("/P")
            } else {
                None
            },
            second: decode_c28(read_bits(message_bits, 29, 28)),
            second_suffix: if read_bits(message_bits, 57, 1) == 1 {
                Some("/P")
            } else {
                None
            },
            acknowledge: read_bits(message_bits, 58, 1) == 1,
            info: decode_g15(read_bits(message_bits, 59, 15)),
        })),
        4 => Some(Payload::Nonstandard(NonstandardMessage {
            hashed: read_bits(message_bits, 0, 12) as u16,
            plain: decode_c58(read_bits_u128(message_bits, 12, 58)),
            hashed_is_second: read_bits(message_bits, 70, 1) == 1,
            reply: decode_r2(read_bits(message_bits, 71, 2) as u8),
            cq: read_bits(message_bits, 73, 1) == 1,
        })),
        other => Some(Payload::Unsupported(other, None)),
    }
}

impl Payload {
    pub fn collect_callsigns(&self, resolver: &mut HashResolver) {
        match self {
            Payload::Standard(message) => {
                if let CallField::Standard(callsign) = &message.first {
                    resolver.insert_callsign(callsign);
                }
                if let CallField::Standard(callsign) = &message.second {
                    resolver.insert_callsign(callsign);
                }
            }
            Payload::Nonstandard(message) => {
                resolver.insert_callsign(&message.plain);
            }
            Payload::FreeText(_) | Payload::Unsupported(_, _) => {}
        }
    }

    pub fn render(&self, resolver: &HashResolver) -> DecodedPayload {
        match self {
            Payload::FreeText(text) => DecodedPayload {
                kind: MessageKind::FreeText,
                text: text.trim().to_string(),
                primary_call: None,
                secondary_call: None,
            },
            Payload::Standard(message) => {
                let first = render_call(&message.first, resolver);
                let second = render_call(&message.second, resolver);
                let first_with_suffix = append_suffix(&first, message.first_suffix);
                let second_with_suffix = append_suffix(&second, message.second_suffix);
                let trailing = render_standard_info(message.acknowledge, &message.info);
                let text = if trailing.is_empty() {
                    format!("{first_with_suffix} {second_with_suffix}")
                } else {
                    format!("{first_with_suffix} {second_with_suffix} {trailing}")
                };
                DecodedPayload {
                    kind: MessageKind::Standard,
                    text,
                    primary_call: extract_named_call(&message.first, resolver),
                    secondary_call: extract_named_call(&message.second, resolver),
                }
            }
            Payload::Nonstandard(message) => {
                let hash_text = resolver
                    .resolve12(message.hashed)
                    .map(|callsign| format!("<{callsign}>"))
                    .unwrap_or_else(|| "<...>".to_string());
                let mut parts = Vec::new();
                if message.cq {
                    parts.push("CQ".to_string());
                    parts.push(message.plain.clone());
                } else if message.hashed_is_second {
                    parts.push(message.plain.clone());
                    parts.push(hash_text);
                } else {
                    parts.push(hash_text);
                    parts.push(message.plain.clone());
                }
                let reply = render_reply_word(message.reply);
                if !reply.is_empty() {
                    parts.push(reply.to_string());
                }
                DecodedPayload {
                    kind: MessageKind::Nonstandard,
                    text: parts.join(" "),
                    primary_call: Some(message.plain.clone()),
                    secondary_call: resolver.resolve12(message.hashed).map(ToOwned::to_owned),
                }
            }
            Payload::Unsupported(i3, n3) => DecodedPayload {
                kind: MessageKind::Unsupported,
                text: match n3 {
                    Some(subtype) => format!("<unsupported:{i3}.{subtype}>"),
                    None => format!("<unsupported:{i3}>"),
                },
                primary_call: None,
                secondary_call: None,
            },
        }
    }
}

fn extract_named_call(field: &CallField, resolver: &HashResolver) -> Option<String> {
    match field {
        CallField::Standard(callsign) => Some(callsign.clone()),
        CallField::Hash22(hash) => resolver.resolve22(*hash).map(ToOwned::to_owned),
        CallField::Token(_) => None,
    }
}

fn append_suffix(callsign: &str, suffix: Option<&'static str>) -> String {
    match suffix {
        Some(suffix) => format!("{callsign}{suffix}"),
        None => callsign.to_string(),
    }
}

fn render_call(field: &CallField, resolver: &HashResolver) -> String {
    match field {
        CallField::Token(token) => token.clone(),
        CallField::Standard(callsign) => callsign.clone(),
        CallField::Hash22(hash) => resolver
            .resolve22(*hash)
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| "<...>".to_string()),
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
        GridReport::Reply(reply) => render_reply_word(*reply).to_string(),
        GridReport::Grid(grid) => {
            if acknowledge {
                format!("R {grid}")
            } else {
                grid.clone()
            }
        }
        GridReport::Signal(report) => {
            let body = format!("{report:+03}");
            if acknowledge {
                format!("R{body}")
            } else {
                body
            }
        }
    }
}

fn render_reply_word(reply: ReplyWord) -> &'static str {
    match reply {
        ReplyWord::Blank => "",
        ReplyWord::Rrr => "RRR",
        ReplyWord::Rr73 => "RR73",
        ReplyWord::SeventyThree => "73",
    }
}

fn decode_r2(value: u8) -> ReplyWord {
    match value {
        1 => ReplyWord::Rrr,
        2 => ReplyWord::Rr73,
        3 => ReplyWord::SeventyThree,
        _ => ReplyWord::Blank,
    }
}

fn decode_g15(value: u64) -> GridReport {
    const MAX_GRID4: u64 = 32_400;
    if value < MAX_GRID4 {
        let mut remaining = value;
        let a = ((remaining / (18 * 10 * 10)) as u8 + b'A') as char;
        remaining %= 18 * 10 * 10;
        let b = ((remaining / 100) as u8 + b'A') as char;
        remaining %= 100;
        let c = ((remaining / 10) as u8 + b'0') as char;
        let d = ((remaining % 10) as u8 + b'0') as char;
        return GridReport::Grid(format!("{a}{b}{c}{d}"));
    }

    let report = value - MAX_GRID4;
    match report {
        0 | 1 => GridReport::Blank,
        2 => GridReport::Reply(ReplyWord::Rrr),
        3 => GridReport::Reply(ReplyWord::Rr73),
        4 => GridReport::Reply(ReplyWord::SeventyThree),
        _ => GridReport::Signal(report as i16 - 35),
    }
}

fn decode_c28(value: u64) -> CallField {
    match value {
        0 => CallField::Token("DE".to_string()),
        1 => CallField::Token("QRZ".to_string()),
        2 => CallField::Token("CQ".to_string()),
        3..=1002 => CallField::Token(format!("CQ {:03}", value - 3)),
        1003..=532_443 => {
            let mut remaining = value - 1003;
            let i1 = remaining / (27 * 27 * 27);
            remaining %= 27 * 27 * 27;
            let i2 = remaining / (27 * 27);
            remaining %= 27 * 27;
            let i3 = remaining / 27;
            let i4 = remaining % 27;
            let suffix = format!(
                "{}{}{}{}",
                alphabet27(i1 as usize),
                alphabet27(i2 as usize),
                alphabet27(i3 as usize),
                alphabet27(i4 as usize)
            )
            .trim()
            .to_string();
            CallField::Token(format!("CQ {suffix}"))
        }
        raw if raw >= CALL_STANDARD_BASE as u64 => {
            let mut remaining = raw - CALL_STANDARD_BASE as u64;
            let i1 = remaining / (36 * 10 * 27 * 27 * 27) as u64;
            remaining %= (36 * 10 * 27 * 27 * 27) as u64;
            let i2 = remaining / (10 * 27 * 27 * 27) as u64;
            remaining %= (10 * 27 * 27 * 27) as u64;
            let i3 = remaining / (27 * 27 * 27) as u64;
            remaining %= (27 * 27 * 27) as u64;
            let i4 = remaining / (27 * 27) as u64;
            remaining %= (27 * 27) as u64;
            let i5 = remaining / 27;
            let i6 = remaining % 27;
            let a1 = alphabet37(i1 as usize);
            let a2 = alphabet36(i2 as usize);
            let a3 = digit10(i3 as usize);
            let a4 = alphabet27(i4 as usize);
            let a5 = alphabet27(i5 as usize);
            let a6 = alphabet27(i6 as usize);
            CallField::Standard(format!("{a1}{a2}{a3}{a4}{a5}{a6}").trim().to_string())
        }
        raw if raw >= CALL_NTOKENS as u64 && raw < (CALL_NTOKENS + CALL_MAX22) as u64 => {
            CallField::Hash22(raw as u32 - CALL_NTOKENS)
        }
        _ => CallField::Token("<token>".to_string()),
    }
}

fn decode_c58(value: u128) -> String {
    let alphabet: Vec<char> = " 0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ/".chars().collect();
    let mut remaining = value;
    let mut chars = [' '; 11];
    for slot in (0..11).rev() {
        let digit = (remaining % 38) as usize;
        remaining /= 38;
        chars[slot] = alphabet[digit];
    }
    chars.iter().collect::<String>().trim().to_string()
}

fn decode_free_text(value: u128) -> String {
    let alphabet: Vec<char> = " 0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ+-./?"
        .chars()
        .collect();
    let mut remaining = value;
    let mut chars = [' '; 13];
    for slot in (0..13).rev() {
        let digit = (remaining % 42) as usize;
        remaining /= 42;
        chars[slot] = alphabet[digit];
    }
    chars.iter().collect::<String>().trim().to_string()
}

fn alphabet37(index: usize) -> char {
    " 0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        .chars()
        .nth(index)
        .unwrap()
}

fn alphabet36(index: usize) -> char {
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        .chars()
        .nth(index)
        .unwrap()
}

fn digit10(index: usize) -> char {
    "0123456789".chars().nth(index).unwrap()
}

fn alphabet27(index: usize) -> char {
    " ABCDEFGHIJKLMNOPQRSTUVWXYZ".chars().nth(index).unwrap()
}

fn read_bits(bits: &[u8], start: usize, len: usize) -> u64 {
    let mut value = 0u64;
    for bit in &bits[start..start + len] {
        value = (value << 1) | (*bit as u64);
    }
    value
}

fn read_bits_u128(bits: &[u8], start: usize, len: usize) -> u128 {
    let mut value = 0u128;
    for bit in &bits[start..start + len] {
        value = (value << 1) | (*bit as u128);
    }
    value
}

pub fn hash_callsign(callsign: &str, nbits: u8) -> u64 {
    let alphabet: Vec<char> = " 0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ/".chars().collect();
    let mut padded = callsign.trim().to_uppercase();
    padded.truncate(11);
    while padded.len() < 11 {
        padded.push(' ');
    }

    let mut value = 0u64;
    for ch in padded.chars() {
        let digit = alphabet
            .iter()
            .position(|candidate| *candidate == ch)
            .unwrap_or(0) as u64;
        value = value.wrapping_mul(38).wrapping_add(digit);
    }
    value
        .wrapping_mul(HASH_MULTIPLIER)
        .wrapping_shr((64 - nbits) as u32)
}
