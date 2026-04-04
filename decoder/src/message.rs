use std::collections::HashMap;

use serde::Serialize;

use crate::crc;
use crate::protocol::{
    CALL_MAX22, CALL_NTOKENS, CALL_STANDARD_BASE, FTX_FREE_TEXT_FIELD, FTX_FREE_TEXT_SUBTYPE_FIELD,
    FTX_INFO_BITS, FTX_MESSAGE_BITS, FTX_MESSAGE_KIND_NONSTANDARD,
    FTX_MESSAGE_KIND_STANDARD_SLASH_P, FTX_MESSAGE_KIND_STANDARD_SLASH_R, FTX_NONSTANDARD_LAYOUT,
    FTX_STANDARD_LAYOUT, HASH_MULTIPLIER, alphabet27_char, alphabet36_char, alphabet37_char,
    alphabet38_char, alphabet38_index, alphabet42_char, digit10_char, read_bit_field,
    read_bit_field_u128,
};

#[derive(Debug, Clone, Serialize)]
pub enum MessageKind {
    FreeText,
    Standard,
    Nonstandard,
    Unsupported,
}

#[derive(Debug, Clone, Serialize)]
pub enum CallModifier {
    R,
    P,
}

#[derive(Debug, Clone, Serialize)]
pub enum StructuredCallValue {
    Token {
        token: String,
    },
    StandardCall {
        callsign: String,
    },
    Hash22 {
        hash: u32,
        resolved_callsign: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct StructuredCallField {
    pub raw: u32,
    pub value: StructuredCallValue,
    pub modifier: Option<CallModifier>,
}

#[derive(Debug, Clone, Serialize)]
pub enum StructuredInfoValue {
    Grid { locator: String },
    SignalReport { db: i16 },
    Blank,
    Reply { word: ReplyWord },
}

#[derive(Debug, Clone, Serialize)]
pub struct StructuredInfoField {
    pub raw: u16,
    pub value: StructuredInfoValue,
}

#[derive(Debug, Clone, Serialize)]
pub struct HashedCallField12 {
    pub raw: u16,
    pub resolved_callsign: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PlainCallField58 {
    pub raw: u128,
    pub callsign: String,
}

#[derive(Debug, Clone, Copy)]
pub enum MessageCallField<'a> {
    Standard(&'a StructuredCallField),
    Hashed12(&'a HashedCallField12),
    Plain58(&'a PlainCallField58),
}

#[derive(Debug, Clone, Serialize)]
pub enum StructuredMessage {
    FreeText {
        i3: u8,
        n3: u8,
        raw: u128,
        text: String,
    },
    Standard {
        i3: u8,
        first: StructuredCallField,
        second: StructuredCallField,
        acknowledge: bool,
        info: StructuredInfoField,
    },
    Nonstandard {
        i3: u8,
        hashed_call: HashedCallField12,
        plain_call: PlainCallField58,
        hashed_is_second: bool,
        reply: ReplyWord,
        cq: bool,
    },
    Unsupported {
        i3: u8,
        n3: Option<u8>,
        message_bits: Vec<u8>,
    },
}

impl StructuredMessage {
    pub fn kind(&self) -> MessageKind {
        match self {
            StructuredMessage::FreeText { .. } => MessageKind::FreeText,
            StructuredMessage::Standard { .. } => MessageKind::Standard,
            StructuredMessage::Nonstandard { .. } => MessageKind::Nonstandard,
            StructuredMessage::Unsupported { .. } => MessageKind::Unsupported,
        }
    }

    pub fn to_text(&self) -> String {
        match self {
            StructuredMessage::FreeText { text, .. } => text.trim().to_string(),
            StructuredMessage::Standard {
                first,
                second,
                acknowledge,
                info,
                ..
            } => {
                let first = render_structured_call(first);
                let second = render_structured_call(second);
                let trailing = render_structured_info(*acknowledge, info);
                if trailing.is_empty() {
                    format!("{first} {second}")
                } else {
                    format!("{first} {second} {trailing}")
                }
            }
            StructuredMessage::Nonstandard {
                hashed_call,
                plain_call,
                hashed_is_second,
                reply,
                cq,
                ..
            } => {
                let hash_text = hashed_call
                    .resolved_callsign
                    .as_ref()
                    .map(|callsign| format!("<{callsign}>"))
                    .unwrap_or_else(|| "<...>".to_string());
                let mut parts = Vec::new();
                if *cq {
                    parts.push("CQ".to_string());
                    parts.push(plain_call.callsign.clone());
                } else if *hashed_is_second {
                    parts.push(plain_call.callsign.clone());
                    parts.push(hash_text);
                } else {
                    parts.push(hash_text);
                    parts.push(plain_call.callsign.clone());
                }
                let reply = render_reply_word(*reply);
                if !reply.is_empty() {
                    parts.push(reply.to_string());
                }
                parts.join(" ")
            }
            StructuredMessage::Unsupported { i3, n3, .. } => match n3 {
                Some(subtype) => format!("<unsupported:{i3}.{subtype}>"),
                None => format!("<unsupported:{i3}>"),
            },
        }
    }

    pub fn first_call_field(&self) -> Option<MessageCallField<'_>> {
        match self {
            StructuredMessage::Standard { first, .. } => Some(MessageCallField::Standard(first)),
            StructuredMessage::Nonstandard {
                hashed_call,
                plain_call,
                hashed_is_second,
                ..
            } => {
                if *hashed_is_second {
                    Some(MessageCallField::Plain58(plain_call))
                } else {
                    Some(MessageCallField::Hashed12(hashed_call))
                }
            }
            StructuredMessage::FreeText { .. } | StructuredMessage::Unsupported { .. } => None,
        }
    }

    pub fn second_call_field(&self) -> Option<MessageCallField<'_>> {
        match self {
            StructuredMessage::Standard { second, .. } => Some(MessageCallField::Standard(second)),
            StructuredMessage::Nonstandard {
                hashed_call,
                plain_call,
                hashed_is_second,
                ..
            } => {
                if *hashed_is_second {
                    Some(MessageCallField::Hashed12(hashed_call))
                } else {
                    Some(MessageCallField::Plain58(plain_call))
                }
            }
            StructuredMessage::FreeText { .. } | StructuredMessage::Unsupported { .. } => None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum Payload {
    FreeText(FreeTextMessage),
    Standard(StandardMessage),
    Nonstandard(NonstandardMessage),
    Unsupported(UnsupportedMessage),
}

#[derive(Debug, Clone)]
pub struct FreeTextMessage {
    pub i3: u8,
    pub n3: u8,
    pub raw: u128,
    pub text: String,
}

#[derive(Debug, Clone)]
pub struct UnsupportedMessage {
    pub i3: u8,
    pub n3: Option<u8>,
    pub message_bits: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct StandardMessage {
    pub i3: u8,
    pub first_raw: u32,
    pub first: CallField,
    pub first_modifier: Option<CallModifier>,
    pub second_raw: u32,
    pub second: CallField,
    pub second_modifier: Option<CallModifier>,
    pub acknowledge: bool,
    pub info_raw: u16,
    pub info: GridReport,
}

#[derive(Debug, Clone)]
pub struct NonstandardMessage {
    pub i3: u8,
    pub hashed: u16,
    pub plain_raw: u128,
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

#[derive(Debug, Clone, Copy, Serialize)]
pub enum ReplyWord {
    Blank,
    Rrr,
    Rr73,
    SeventyThree,
}

#[derive(Default, Debug, Clone)]
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
    if codeword.len() < FTX_INFO_BITS {
        return None;
    }
    let message_bits = &codeword[..FTX_MESSAGE_BITS];
    let crc_bits = &codeword[FTX_MESSAGE_BITS..FTX_INFO_BITS];
    if !crc::crc_matches(message_bits, crc_bits) {
        return None;
    }

    let i3 = read_bit_field(message_bits, FTX_STANDARD_LAYOUT.kind) as u8;
    match i3 {
        0 => {
            let n3 = read_bit_field(message_bits, FTX_FREE_TEXT_SUBTYPE_FIELD) as u8;
            match n3 {
                0 => {
                    let raw = read_bit_field_u128(message_bits, FTX_FREE_TEXT_FIELD);
                    Some(Payload::FreeText(FreeTextMessage {
                        i3,
                        n3,
                        raw,
                        text: decode_free_text(raw),
                    }))
                }
                _ => Some(Payload::Unsupported(UnsupportedMessage {
                    i3,
                    n3: Some(n3),
                    message_bits: message_bits.to_vec(),
                })),
            }
        }
        FTX_MESSAGE_KIND_STANDARD_SLASH_R | FTX_MESSAGE_KIND_STANDARD_SLASH_P => {
            let first_raw = read_bit_field(message_bits, FTX_STANDARD_LAYOUT.first_call) as u32;
            let second_raw = read_bit_field(message_bits, FTX_STANDARD_LAYOUT.second_call) as u32;
            let modifier = if i3 == FTX_MESSAGE_KIND_STANDARD_SLASH_R {
                CallModifier::R
            } else {
                CallModifier::P
            };
            let info_raw = read_bit_field(message_bits, FTX_STANDARD_LAYOUT.info) as u16;
            Some(Payload::Standard(StandardMessage {
                i3,
                first_raw,
                first: decode_c28(first_raw as u64),
                first_modifier: (read_bit_field(message_bits, FTX_STANDARD_LAYOUT.first_suffix)
                    == 1)
                    .then_some(modifier.clone()),
                second_raw,
                second: decode_c28(second_raw as u64),
                second_modifier: (read_bit_field(message_bits, FTX_STANDARD_LAYOUT.second_suffix)
                    == 1)
                    .then_some(modifier),
                acknowledge: read_bit_field(message_bits, FTX_STANDARD_LAYOUT.acknowledge) == 1,
                info_raw,
                info: decode_g15(info_raw as u64),
            }))
        }
        FTX_MESSAGE_KIND_NONSTANDARD => Some(Payload::Nonstandard(NonstandardMessage {
            i3,
            hashed: read_bit_field(message_bits, FTX_NONSTANDARD_LAYOUT.hashed_call) as u16,
            plain_raw: read_bit_field_u128(message_bits, FTX_NONSTANDARD_LAYOUT.plain_call),
            plain: decode_c58(read_bit_field_u128(
                message_bits,
                FTX_NONSTANDARD_LAYOUT.plain_call,
            )),
            hashed_is_second: read_bit_field(message_bits, FTX_NONSTANDARD_LAYOUT.hashed_is_second)
                == 1,
            reply: decode_r2(read_bit_field(message_bits, FTX_NONSTANDARD_LAYOUT.reply) as u8),
            cq: read_bit_field(message_bits, FTX_NONSTANDARD_LAYOUT.cq) == 1,
        })),
        other => Some(Payload::Unsupported(UnsupportedMessage {
            i3: other,
            n3: None,
            message_bits: message_bits.to_vec(),
        })),
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
            Payload::FreeText(_) | Payload::Unsupported(_) => {}
        }
    }

    pub fn to_message(&self, resolver: &HashResolver) -> StructuredMessage {
        match self {
            Payload::FreeText(message) => StructuredMessage::FreeText {
                i3: message.i3,
                n3: message.n3,
                raw: message.raw,
                text: message.text.trim().to_string(),
            },
            Payload::Standard(message) => StructuredMessage::Standard {
                i3: message.i3,
                first: structured_call_field(
                    message.first_raw,
                    &message.first,
                    message.first_modifier.clone(),
                    resolver,
                ),
                second: structured_call_field(
                    message.second_raw,
                    &message.second,
                    message.second_modifier.clone(),
                    resolver,
                ),
                acknowledge: message.acknowledge,
                info: structured_info_field(message.info_raw, &message.info),
            },
            Payload::Nonstandard(message) => StructuredMessage::Nonstandard {
                i3: message.i3,
                hashed_call: HashedCallField12 {
                    raw: message.hashed,
                    resolved_callsign: resolver.resolve12(message.hashed).map(ToOwned::to_owned),
                },
                plain_call: PlainCallField58 {
                    raw: message.plain_raw,
                    callsign: message.plain.clone(),
                },
                hashed_is_second: message.hashed_is_second,
                reply: message.reply,
                cq: message.cq,
            },
            Payload::Unsupported(message) => StructuredMessage::Unsupported {
                i3: message.i3,
                n3: message.n3,
                message_bits: message.message_bits.clone(),
            },
        }
    }
}

fn structured_call_field(
    raw: u32,
    field: &CallField,
    modifier: Option<CallModifier>,
    resolver: &HashResolver,
) -> StructuredCallField {
    let value = match field {
        CallField::Token(token) => StructuredCallValue::Token {
            token: token.clone(),
        },
        CallField::Standard(callsign) => StructuredCallValue::StandardCall {
            callsign: callsign.clone(),
        },
        CallField::Hash22(hash) => StructuredCallValue::Hash22 {
            hash: *hash,
            resolved_callsign: resolver.resolve22(*hash).map(ToOwned::to_owned),
        },
    };
    StructuredCallField {
        raw,
        value,
        modifier,
    }
}

fn structured_info_field(raw: u16, info: &GridReport) -> StructuredInfoField {
    let value = match normalize_grid_report(info) {
        GridReport::Grid(locator) => StructuredInfoValue::Grid {
            locator: locator.clone(),
        },
        GridReport::Signal(db) => StructuredInfoValue::SignalReport { db: *db },
        GridReport::Blank => StructuredInfoValue::Blank,
        GridReport::Reply(word) => StructuredInfoValue::Reply { word: *word },
    };
    StructuredInfoField { raw, value }
}

fn apply_modifier(text: String, modifier: &Option<CallModifier>) -> String {
    match modifier {
        Some(CallModifier::R) => format!("{text}/R"),
        Some(CallModifier::P) => format!("{text}/P"),
        None => text,
    }
}

fn render_structured_call(field: &StructuredCallField) -> String {
    let text = match &field.value {
        StructuredCallValue::Token { token } => token.clone(),
        StructuredCallValue::StandardCall { callsign } => callsign.clone(),
        StructuredCallValue::Hash22 {
            resolved_callsign: Some(callsign),
            ..
        } => callsign.clone(),
        StructuredCallValue::Hash22 {
            resolved_callsign: None,
            ..
        } => "<...>".to_string(),
    };
    apply_modifier(text, &field.modifier)
}

fn render_structured_info(acknowledge: bool, info: &StructuredInfoField) -> String {
    match &info.value {
        StructuredInfoValue::Blank => {
            if acknowledge {
                "R".to_string()
            } else {
                String::new()
            }
        }
        StructuredInfoValue::Reply { word } => render_reply_word(*word).to_string(),
        StructuredInfoValue::Grid { locator } => {
            if acknowledge {
                format!("R {locator}")
            } else {
                locator.clone()
            }
        }
        StructuredInfoValue::SignalReport { db } => {
            let body = format!("{db:+03}");
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
        return normalize_grid_report(&GridReport::Grid(format!("{a}{b}{c}{d}"))).clone();
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

fn normalize_grid_report(info: &GridReport) -> &GridReport {
    match info {
        GridReport::Grid(locator) if locator.eq_ignore_ascii_case("RR73") => {
            static RR73_REPLY: GridReport = GridReport::Reply(ReplyWord::Rr73);
            &RR73_REPLY
        }
        _ => info,
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
                alphabet27_char(i1 as usize),
                alphabet27_char(i2 as usize),
                alphabet27_char(i3 as usize),
                alphabet27_char(i4 as usize)
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
            let a1 = alphabet37_char(i1 as usize);
            let a2 = alphabet36_char(i2 as usize);
            let a3 = digit10_char(i3 as usize);
            let a4 = alphabet27_char(i4 as usize);
            let a5 = alphabet27_char(i5 as usize);
            let a6 = alphabet27_char(i6 as usize);
            CallField::Standard(format!("{a1}{a2}{a3}{a4}{a5}{a6}").trim().to_string())
        }
        raw if raw >= CALL_NTOKENS as u64 && raw < (CALL_NTOKENS + CALL_MAX22) as u64 => {
            CallField::Hash22(raw as u32 - CALL_NTOKENS)
        }
        _ => CallField::Token("<token>".to_string()),
    }
}

fn decode_c58(value: u128) -> String {
    let mut remaining = value;
    let mut chars = [' '; 11];
    for slot in chars.iter_mut().rev() {
        let digit = (remaining % 38) as usize;
        remaining /= 38;
        *slot = alphabet38_char(digit);
    }
    chars.iter().collect::<String>().trim().to_string()
}

fn decode_free_text(value: u128) -> String {
    let mut remaining = value;
    let mut chars = [' '; 13];
    for slot in chars.iter_mut().rev() {
        let digit = (remaining % 42) as usize;
        remaining /= 42;
        *slot = alphabet42_char(digit);
    }
    chars.iter().collect::<String>().trim().to_string()
}

pub fn hash_callsign(callsign: &str, nbits: u8) -> u64 {
    let mut padded = callsign.trim().to_uppercase();
    padded.truncate(11);
    while padded.len() < 11 {
        padded.push(' ');
    }

    let mut value = 0u64;
    for ch in padded.chars() {
        let digit = alphabet38_index(ch).unwrap_or(0) as u64;
        value = value.wrapping_mul(38).wrapping_add(digit);
    }
    value
        .wrapping_mul(HASH_MULTIPLIER)
        .wrapping_shr((64 - nbits) as u32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crc::crc14_ft8;
    use crate::encode::{encode_nonstandard_message, encode_standard_message};

    #[test]
    fn structured_standard_to_text_preserves_acknowledge_rules() {
        let first = StructuredCallField {
            raw: 0,
            value: StructuredCallValue::Token {
                token: "K1ABC".to_string(),
            },
            modifier: None,
        };
        let second = StructuredCallField {
            raw: 0,
            value: StructuredCallValue::StandardCall {
                callsign: "W1XYZ".to_string(),
            },
            modifier: None,
        };

        let blank = StructuredMessage::Standard {
            i3: 1,
            first: first.clone(),
            second: second.clone(),
            acknowledge: true,
            info: StructuredInfoField {
                raw: 0,
                value: StructuredInfoValue::Blank,
            },
        };
        assert_eq!(blank.to_text(), "K1ABC W1XYZ R");

        let grid = StructuredMessage::Standard {
            i3: 1,
            first: first.clone(),
            second: second.clone(),
            acknowledge: true,
            info: StructuredInfoField {
                raw: 0,
                value: StructuredInfoValue::Grid {
                    locator: "FN31".to_string(),
                },
            },
        };
        assert_eq!(grid.to_text(), "K1ABC W1XYZ R FN31");

        let signal = StructuredMessage::Standard {
            i3: 1,
            first,
            second,
            acknowledge: true,
            info: StructuredInfoField {
                raw: 0,
                value: StructuredInfoValue::SignalReport { db: -7 },
            },
        };
        assert_eq!(signal.to_text(), "K1ABC W1XYZ R-07");
    }

    #[test]
    fn structured_nonstandard_to_text_preserves_variants() {
        let unresolved = StructuredMessage::Nonstandard {
            i3: 4,
            hashed_call: HashedCallField12 {
                raw: 0,
                resolved_callsign: None,
            },
            plain_call: PlainCallField58 {
                raw: 0,
                callsign: "K1ABC".to_string(),
            },
            hashed_is_second: false,
            reply: ReplyWord::Rrr,
            cq: false,
        };
        assert_eq!(unresolved.to_text(), "<...> K1ABC RRR");

        let resolved_second = StructuredMessage::Nonstandard {
            i3: 4,
            hashed_call: HashedCallField12 {
                raw: 0,
                resolved_callsign: Some("HF19NY".to_string()),
            },
            plain_call: PlainCallField58 {
                raw: 0,
                callsign: "K1ABC".to_string(),
            },
            hashed_is_second: true,
            reply: ReplyWord::Blank,
            cq: false,
        };
        assert_eq!(resolved_second.to_text(), "K1ABC <HF19NY>");

        let cq = StructuredMessage::Nonstandard {
            i3: 4,
            hashed_call: HashedCallField12 {
                raw: 0,
                resolved_callsign: Some("HF19NY".to_string()),
            },
            plain_call: PlainCallField58 {
                raw: 0,
                callsign: "K1ABC".to_string(),
            },
            hashed_is_second: false,
            reply: ReplyWord::Blank,
            cq: true,
        };
        assert_eq!(cq.to_text(), "CQ K1ABC");
    }

    #[test]
    fn ordered_call_fields_preserve_nonstandard_position() {
        let first_hashed = StructuredMessage::Nonstandard {
            i3: 4,
            hashed_call: HashedCallField12 {
                raw: 12,
                resolved_callsign: Some("K1ABC".to_string()),
            },
            plain_call: PlainCallField58 {
                raw: 34,
                callsign: "HF19NY".to_string(),
            },
            hashed_is_second: false,
            reply: ReplyWord::Blank,
            cq: false,
        };
        assert!(matches!(
            first_hashed.first_call_field(),
            Some(MessageCallField::Hashed12(_))
        ));
        assert!(matches!(
            first_hashed.second_call_field(),
            Some(MessageCallField::Plain58(_))
        ));

        let second_hashed = StructuredMessage::Nonstandard {
            i3: 4,
            hashed_call: HashedCallField12 {
                raw: 12,
                resolved_callsign: Some("K1ABC".to_string()),
            },
            plain_call: PlainCallField58 {
                raw: 34,
                callsign: "HF19NY".to_string(),
            },
            hashed_is_second: true,
            reply: ReplyWord::Blank,
            cq: false,
        };
        assert!(matches!(
            second_hashed.first_call_field(),
            Some(MessageCallField::Plain58(_))
        ));
        assert!(matches!(
            second_hashed.second_call_field(),
            Some(MessageCallField::Hashed12(_))
        ));
    }

    #[test]
    fn payload_to_message_preserves_standard_modifiers() {
        let frame =
            encode_standard_message("CQ", "K1ABC", false, &GridReport::Blank).expect("encode");

        let mut message_bits = frame.message_bits.to_vec();
        write_bits(&mut message_bits, 28, 1, 1);
        write_bits(&mut message_bits, 57, 1, 1);
        write_bits(&mut message_bits, 74, 3, 1);
        let payload = payload_from_message_bits(&message_bits);
        assert_eq!(
            payload.to_message(&HashResolver::default()).to_text(),
            "CQ/R K1ABC/R"
        );

        let mut portable_bits = frame.message_bits.to_vec();
        write_bits(&mut portable_bits, 28, 1, 1);
        write_bits(&mut portable_bits, 57, 1, 0);
        write_bits(&mut portable_bits, 74, 3, 2);
        let portable = payload_from_message_bits(&portable_bits);
        assert_eq!(
            portable.to_message(&HashResolver::default()).to_text(),
            "CQ/P K1ABC"
        );
    }

    #[test]
    fn payload_to_message_preserves_nonstandard_hash_positions() {
        let frame = encode_nonstandard_message("K1ABC", "HF19NY", true, ReplyWord::Rr73, false)
            .expect("encode");
        let payload = unpack_message(&frame.codeword_bits).expect("payload");
        let unresolved = payload.to_message(&HashResolver::default());
        assert_eq!(unresolved.to_text(), "HF19NY <...> RR73");

        let mut resolver = HashResolver::default();
        resolver.insert_callsign("K1ABC");
        let resolved = payload.to_message(&resolver);
        assert_eq!(resolved.to_text(), "HF19NY <K1ABC> RR73");
    }

    #[test]
    fn structured_misc_rendering_matches_legacy_contract() {
        let free = StructuredMessage::FreeText {
            i3: 0,
            n3: 0,
            raw: 0,
            text: " HELLO WORLD ".to_string(),
        };
        assert_eq!(free.to_text(), "HELLO WORLD");

        let unsupported = StructuredMessage::Unsupported {
            i3: 5,
            n3: Some(3),
            message_bits: vec![0; 77],
        };
        assert_eq!(unsupported.to_text(), "<unsupported:5.3>");
    }

    fn payload_from_message_bits(message_bits: &[u8]) -> Payload {
        let mut codeword = Vec::with_capacity(91);
        codeword.extend_from_slice(message_bits);
        codeword.extend_from_slice(&crc14_ft8(message_bits));
        unpack_message(&codeword).expect("payload")
    }

    fn write_bits(bits: &mut [u8], start: usize, len: usize, value: u64) {
        for offset in 0..len {
            let shift = len - offset - 1;
            bits[start + offset] = ((value >> shift) & 1) as u8;
        }
    }
}
