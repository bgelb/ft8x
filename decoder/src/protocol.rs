/// FT8/FT4-style packed messages carry 77 payload bits before the CRC.
pub const FTX_MESSAGE_BITS: usize = 77;
/// The 77 payload bits plus the 14-bit CRC fed into the LDPC encoder.
pub const FTX_INFO_BITS: usize = 91;
/// LDPC(174, 91) codeword width used by FT8.
pub const FTX_CODEWORD_BITS: usize = 174;
/// 8-FSK carries three coded bits per payload symbol.
pub const FTX_BITS_PER_SYMBOL: usize = 3;
/// One FT8 frame carries two 87-bit LDPC halves, one per 29-symbol data block.
pub const FTX_CODEWORD_HALF_BITS: usize = FTX_CODEWORD_BITS / 2;
pub const FTX_DATA_SYMBOLS_PER_HALF: usize = FTX_CODEWORD_HALF_BITS / FTX_BITS_PER_SYMBOL;
pub const FTX_DATA_SYMBOLS: usize = FTX_DATA_SYMBOLS_PER_HALF * 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BitField {
    pub start: usize,
    pub len: usize,
}

impl BitField {
    pub const fn end(self) -> usize {
        self.start + self.len
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StandardMessageLayout {
    pub first_call: BitField,
    pub first_suffix: BitField,
    pub second_call: BitField,
    pub second_suffix: BitField,
    pub acknowledge: BitField,
    pub info: BitField,
    pub kind: BitField,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NonstandardMessageLayout {
    pub hashed_call: BitField,
    pub plain_call: BitField,
    pub hashed_is_second: BitField,
    pub reply: BitField,
    pub cq: BitField,
    pub kind: BitField,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DxpeditionMessageLayout {
    pub completed_call: BitField,
    pub next_call: BitField,
    pub hashed_call10: BitField,
    pub report5: BitField,
    pub subtype: BitField,
    pub kind: BitField,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FieldDayMessageLayout {
    pub first_call: BitField,
    pub second_call: BitField,
    pub acknowledge: BitField,
    pub transmitter_offset: BitField,
    pub class: BitField,
    pub section: BitField,
    pub subtype: BitField,
    pub kind: BitField,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RttyContestMessageLayout {
    pub tu: BitField,
    pub first_call: BitField,
    pub second_call: BitField,
    pub acknowledge: BitField,
    pub report: BitField,
    pub exchange: BitField,
    pub kind: BitField,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EuVhfMessageLayout {
    pub hashed_call12: BitField,
    pub hashed_call22: BitField,
    pub acknowledge: BitField,
    pub report: BitField,
    pub serial: BitField,
    pub grid6: BitField,
    pub kind: BitField,
}

/// Packed bit layout for standard FT8 messages inside the 77-bit payload.
pub const FTX_STANDARD_LAYOUT: StandardMessageLayout = StandardMessageLayout {
    first_call: BitField { start: 0, len: 28 },
    first_suffix: BitField { start: 28, len: 1 },
    second_call: BitField { start: 29, len: 28 },
    second_suffix: BitField { start: 57, len: 1 },
    acknowledge: BitField { start: 58, len: 1 },
    info: BitField { start: 59, len: 15 },
    kind: BitField { start: 74, len: 3 },
};

/// Packed bit layout for nonstandard FT8 messages inside the 77-bit payload.
pub const FTX_NONSTANDARD_LAYOUT: NonstandardMessageLayout = NonstandardMessageLayout {
    hashed_call: BitField { start: 0, len: 12 },
    plain_call: BitField { start: 12, len: 58 },
    hashed_is_second: BitField { start: 70, len: 1 },
    reply: BitField { start: 71, len: 2 },
    cq: BitField { start: 73, len: 1 },
    kind: BitField { start: 74, len: 3 },
};

/// Packed bit layout for FT8 DXpedition dual-call messages (i3=0, n3=1).
pub const FTX_DXPEDITION_LAYOUT: DxpeditionMessageLayout = DxpeditionMessageLayout {
    completed_call: BitField { start: 0, len: 28 },
    next_call: BitField { start: 28, len: 28 },
    hashed_call10: BitField { start: 56, len: 10 },
    report5: BitField { start: 66, len: 5 },
    subtype: BitField { start: 71, len: 3 },
    kind: BitField { start: 74, len: 3 },
};

/// Packed bit layout for FT8 ARRL Field Day messages (i3=0, n3=3/4).
pub const FTX_FIELD_DAY_LAYOUT: FieldDayMessageLayout = FieldDayMessageLayout {
    first_call: BitField { start: 0, len: 28 },
    second_call: BitField { start: 28, len: 28 },
    acknowledge: BitField { start: 56, len: 1 },
    transmitter_offset: BitField { start: 57, len: 4 },
    class: BitField { start: 61, len: 3 },
    section: BitField { start: 64, len: 7 },
    subtype: BitField { start: 71, len: 3 },
    kind: BitField { start: 74, len: 3 },
};

/// Packed bit layout for FT8 ARRL RTTY contest / TU messages (i3=3).
pub const FTX_RTTY_CONTEST_LAYOUT: RttyContestMessageLayout = RttyContestMessageLayout {
    tu: BitField { start: 0, len: 1 },
    first_call: BitField { start: 1, len: 28 },
    second_call: BitField { start: 29, len: 28 },
    acknowledge: BitField { start: 57, len: 1 },
    report: BitField { start: 58, len: 3 },
    exchange: BitField { start: 61, len: 13 },
    kind: BitField { start: 74, len: 3 },
};

/// Packed bit layout for FT8 hashed EU VHF contest messages (i3=5).
pub const FTX_EU_VHF_LAYOUT: EuVhfMessageLayout = EuVhfMessageLayout {
    hashed_call12: BitField { start: 0, len: 12 },
    hashed_call22: BitField { start: 12, len: 22 },
    acknowledge: BitField { start: 34, len: 1 },
    report: BitField { start: 35, len: 3 },
    serial: BitField { start: 38, len: 11 },
    grid6: BitField { start: 49, len: 25 },
    kind: BitField { start: 74, len: 3 },
};

pub const FTX_MESSAGE_KIND_STANDARD_SLASH_R: u8 = 1;
pub const FTX_MESSAGE_KIND_STANDARD_SLASH_P: u8 = 2;
pub const FTX_MESSAGE_KIND_RTTY_CONTEST: u8 = 3;
pub const FTX_MESSAGE_KIND_NONSTANDARD: u8 = 4;
pub const FTX_MESSAGE_KIND_EU_VHF: u8 = 5;
pub const FTX_FREE_TEXT_FIELD: BitField = BitField { start: 0, len: 71 };
pub const FTX_FREE_TEXT_SUBTYPE_FIELD: BitField = BitField { start: 71, len: 3 };
/// AP templates constrain the first 29 packed bits plus the 3-bit message-kind field.
pub const FTX_AP_KNOWN_FIELDS: [BitField; 2] = [
    BitField { start: 0, len: 29 },
    BitField { start: 74, len: 3 },
];

/// Read a packed FT8 field as a big-endian integer.
pub fn read_bit_field(bits: &[u8], field: BitField) -> u64 {
    let mut value = 0u64;
    for bit in &bits[field.start..field.end()] {
        value = (value << 1) | (*bit as u64);
    }
    value
}

/// Read a packed FT8 field wider than 64 bits, such as the 58-bit nonstandard callsign.
pub fn read_bit_field_u128(bits: &[u8], field: BitField) -> u128 {
    let mut value = 0u128;
    for bit in &bits[field.start..field.end()] {
        value = (value << 1) | (*bit as u128);
    }
    value
}

/// Write a packed FT8 field using the same big-endian bit order as the on-air payload.
pub fn write_bit_field(bits: &mut [u8], field: BitField, value: u64) {
    for (offset, slot) in bits[field.start..field.end()].iter_mut().enumerate() {
        let shift = field.len - 1 - offset;
        *slot = ((value >> shift) & 1) as u8;
    }
}

/// Copy selected packed-message fields into a 174-slot AP known-bit vector.
pub fn copy_known_message_bits(
    known: &mut [Option<u8>],
    message_bits: &[u8],
    fields: &[BitField],
) -> Option<()> {
    if known.len() < message_bits.len() {
        return None;
    }
    for field in fields {
        if field.end() > message_bits.len() {
            return None;
        }
        for (slot, &bit) in known[field.start..field.end()]
            .iter_mut()
            .zip(message_bits[field.start..field.end()].iter())
        {
            *slot = Some(bit);
        }
    }
    Some(())
}

/// Gray-coded FT8 tone values indexed by tone number.
pub const GRAY_TONES_TO_BITS: [[u8; 3]; 8] = [
    [0, 0, 0],
    [0, 0, 1],
    [0, 1, 1],
    [0, 1, 0],
    [1, 1, 0],
    [1, 0, 0],
    [1, 0, 1],
    [1, 1, 1],
];

pub const CALL_NTOKENS: u32 = 2_063_592;
pub const CALL_MAX22: u32 = 4_194_304;
pub const CALL_STANDARD_BASE: u32 = CALL_NTOKENS + CALL_MAX22;
pub const HASH_MULTIPLIER: u64 = 47_055_833_459;

pub const FIELD_DAY_SECTIONS: &[&str] = &[
    "AB", "AK", "AL", "AR", "AZ", "BC", "CO", "CT", "DE", "EB", "EMA", "ENY", "EPA", "EWA", "GA",
    "GH", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "LAX", "NS", "MB", "MDC", "ME", "MI", "MN",
    "MO", "MS", "MT", "NC", "ND", "NE", "NFL", "NH", "NL", "NLI", "NM", "NNJ", "NNY", "TER", "NTX",
    "NV", "OH", "OK", "ONE", "ONN", "ONS", "OR", "ORG", "PAC", "PR", "QC", "RI", "SB", "SC", "SCV",
    "SD", "SDG", "SF", "SFL", "SJV", "SK", "SNJ", "STX", "SV", "TN", "UT", "VA", "VI", "VT", "WCF",
    "WI", "WMA", "WNY", "WPA", "WTX", "WV", "WWA", "WY", "DX", "PE", "NB",
];

pub const RTTY_MULTIPLIERS: &[&str] = &[
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS",
    "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY",
    "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY", "NB", "NS", "QC", "ON", "MB", "SK", "AB", "BC", "NWT", "NF", "LB", "NU", "YT",
    "PEI", "DC", "DR", "FR", "GD", "GR", "OV", "ZH", "ZL", "X01", "X02", "X03", "X04", "X05",
    "X06", "X07", "X08", "X09", "X10", "X11", "X12", "X13", "X14", "X15", "X16", "X17", "X18",
    "X19", "X20", "X21", "X22", "X23", "X24", "X25", "X26", "X27", "X28", "X29", "X30", "X31",
    "X32", "X33", "X34", "X35", "X36", "X37", "X38", "X39", "X40", "X41", "X42", "X43", "X44",
    "X45", "X46", "X47", "X48", "X49", "X50", "X51", "X52", "X53", "X54", "X55", "X56", "X57",
    "X58", "X59", "X60", "X61", "X62", "X63", "X64", "X65", "X66", "X67", "X68", "X69", "X70",
    "X71", "X72", "X73", "X74", "X75", "X76", "X77", "X78", "X79", "X80", "X81", "X82", "X83",
    "X84", "X85", "X86", "X87", "X88", "X89", "X90", "X91", "X92", "X93", "X94", "X95", "X96",
    "X97", "X98", "X99",
];

const ALPHABET_10: &[u8] = b"0123456789";
const ALPHABET_27: &[u8] = b" ABCDEFGHIJKLMNOPQRSTUVWXYZ";
const ALPHABET_36: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
const ALPHABET_37: &[u8] = b" 0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
const ALPHABET_38: &[u8] = b" 0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ/";
const ALPHABET_42: &[u8] = b" 0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ+-./?";
const GRAY_3BIT_MASK: u8 = (1u8 << FTX_BITS_PER_SYMBOL) - 1;

fn alphabet_char(alphabet: &[u8], index: usize) -> char {
    alphabet[index] as char
}

fn alphabet_index(alphabet: &[u8], ch: char) -> Option<u32> {
    ch.is_ascii()
        .then_some(ch as u8)
        .and_then(|byte| alphabet.iter().position(|candidate| *candidate == byte))
        .map(|index| index as u32)
}

// The pack/unpack code names the concrete FT8 alphabets at the callsite so field bases stay
// obvious without re-embedding the byte tables throughout encode/message code.
pub fn digit10_char(index: usize) -> char {
    alphabet_char(ALPHABET_10, index)
}

pub fn digit10_index(ch: char) -> Option<u32> {
    alphabet_index(ALPHABET_10, ch)
}

pub fn alphabet27_char(index: usize) -> char {
    alphabet_char(ALPHABET_27, index)
}

pub fn alphabet27_index(ch: char) -> Option<u32> {
    alphabet_index(ALPHABET_27, ch)
}

pub fn alphabet36_char(index: usize) -> char {
    alphabet_char(ALPHABET_36, index)
}

pub fn alphabet36_index(ch: char) -> Option<u32> {
    alphabet_index(ALPHABET_36, ch)
}

pub fn alphabet37_char(index: usize) -> char {
    alphabet_char(ALPHABET_37, index)
}

pub fn alphabet37_index(ch: char) -> Option<u32> {
    alphabet_index(ALPHABET_37, ch)
}

pub fn alphabet38_char(index: usize) -> char {
    alphabet_char(ALPHABET_38, index)
}

pub fn alphabet38_index(ch: char) -> Option<u32> {
    alphabet_index(ALPHABET_38, ch)
}

pub fn alphabet42_char(index: usize) -> char {
    alphabet_char(ALPHABET_42, index)
}

/// Convert one 3-bit binary symbol value into the FT8 Gray-coded tone number.
pub fn gray_encode_3bit_value(bits: u8) -> u8 {
    match bits & GRAY_3BIT_MASK {
        0b000 => 0,
        0b001 => 1,
        0b011 => 2,
        0b010 => 3,
        0b110 => 4,
        0b100 => 5,
        0b101 => 6,
        0b111 => 7,
        _ => unreachable!(),
    }
}

/// Convenience wrapper for callers that already hold the triplet as separate bits.
pub fn gray_encode_3bits(bits: [u8; 3]) -> u8 {
    gray_encode_3bit_value((bits[0] << 2) | (bits[1] << 1) | bits[2])
}

/// Inverse of `gray_encode_3bit_value`, used when unpacking FT8 tones back into bits.
pub fn gray_decode_tone3(tone: u8) -> [u8; 3] {
    GRAY_TONES_TO_BITS[tone as usize]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_and_nonstandard_layouts_match_message_width() {
        assert_eq!(FTX_STANDARD_LAYOUT.kind.end(), FTX_MESSAGE_BITS);
        assert_eq!(FTX_NONSTANDARD_LAYOUT.kind.end(), FTX_MESSAGE_BITS);
        assert_eq!(FTX_STANDARD_LAYOUT.first_call.start, 0);
        assert_eq!(FTX_NONSTANDARD_LAYOUT.hashed_call.start, 0);
    }

    #[test]
    fn ap_known_fields_cover_expected_prefix_and_type_bits() {
        let mut known = vec![None; FTX_MESSAGE_BITS];
        let mut message_bits = vec![0u8; FTX_MESSAGE_BITS];
        for (index, slot) in message_bits.iter_mut().enumerate() {
            *slot = (index % 2) as u8;
        }
        copy_known_message_bits(&mut known, &message_bits, &FTX_AP_KNOWN_FIELDS)
            .expect("copy known bits");

        for field in FTX_AP_KNOWN_FIELDS {
            for (slot, &bit) in known[field.start..field.end()]
                .iter()
                .zip(message_bits[field.start..field.end()].iter())
            {
                assert_eq!(*slot, Some(bit));
            }
        }
        for entry in &known[FTX_AP_KNOWN_FIELDS[0].end()..FTX_AP_KNOWN_FIELDS[1].start] {
            assert_eq!(*entry, None);
        }
    }

    #[test]
    fn bit_field_helpers_round_trip() {
        let mut bits = [0u8; FTX_MESSAGE_BITS];
        write_bit_field(&mut bits, FTX_STANDARD_LAYOUT.first_call, 0x1234_567);
        write_bit_field(&mut bits, FTX_STANDARD_LAYOUT.kind, 0b101);
        assert_eq!(
            read_bit_field(&bits, FTX_STANDARD_LAYOUT.first_call),
            0x1234_567
        );
        assert_eq!(read_bit_field(&bits, FTX_STANDARD_LAYOUT.kind), 0b101);
    }

    #[test]
    fn gray_helpers_round_trip_all_tones() {
        for tone in 0..8u8 {
            let bits = gray_decode_tone3(tone);
            assert_eq!(gray_encode_3bits(bits), tone);
            assert_eq!(GRAY_TONES_TO_BITS[tone as usize], bits);
        }
    }

    #[test]
    fn alphabet_helpers_cover_shared_symbol_sets() {
        for (index, ch) in " 0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ".chars().enumerate() {
            assert_eq!(alphabet37_char(index), ch);
            assert_eq!(alphabet37_index(ch), Some(index as u32));
        }
        for (index, ch) in "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ".chars().enumerate() {
            assert_eq!(alphabet36_char(index), ch);
            assert_eq!(alphabet36_index(ch), Some(index as u32));
        }
        for (index, ch) in " ABCDEFGHIJKLMNOPQRSTUVWXYZ".chars().enumerate() {
            assert_eq!(alphabet27_char(index), ch);
            assert_eq!(alphabet27_index(ch), Some(index as u32));
        }
        for (index, ch) in "0123456789".chars().enumerate() {
            assert_eq!(digit10_char(index), ch);
            assert_eq!(digit10_index(ch), Some(index as u32));
        }
        for (index, ch) in " 0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ/".chars().enumerate() {
            assert_eq!(alphabet38_char(index), ch);
            assert_eq!(alphabet38_index(ch), Some(index as u32));
        }
        for (index, ch) in " 0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ+-./?"
            .chars()
            .enumerate()
        {
            assert_eq!(alphabet42_char(index), ch);
        }
        assert_eq!(alphabet27_index('?'), None);
        assert_eq!(alphabet38_index('+'), None);
        assert_eq!(digit10_index('A'), None);
    }

    #[test]
    fn free_text_fields_fit_message_layout() {
        assert_eq!(FTX_FREE_TEXT_FIELD.start, 0);
        assert_eq!(
            FTX_FREE_TEXT_SUBTYPE_FIELD.end(),
            FTX_STANDARD_LAYOUT.kind.start
        );
        assert_eq!(FTX_FREE_TEXT_SUBTYPE_FIELD.len, 3);
        assert_eq!(FTX_CODEWORD_HALF_BITS, 87);
        assert_eq!(FTX_DATA_SYMBOLS_PER_HALF, 29);
        assert_eq!(FTX_BITS_PER_SYMBOL, 3);
        assert_eq!(
            read_bit_field_u128(&[1u8; FTX_MESSAGE_BITS], FTX_FREE_TEXT_FIELD),
            (1u128 << 71) - 1
        );
    }

    #[test]
    fn read_bit_field_u128_reads_nonstandard_plain_call() {
        let mut bits = [0u8; FTX_MESSAGE_BITS];
        bits[FTX_NONSTANDARD_LAYOUT.plain_call.start] = 1;
        bits[FTX_NONSTANDARD_LAYOUT.plain_call.end() - 1] = 1;
        let value = read_bit_field_u128(&bits, FTX_NONSTANDARD_LAYOUT.plain_call);
        assert_eq!(value >> 57, 1);
        assert_eq!(value & 1, 1);
    }

    #[test]
    fn copy_known_message_bits_rejects_short_inputs() {
        assert_eq!(
            copy_known_message_bits(&mut [None; 2], &[0u8; 3], &[BitField { start: 0, len: 3 }]),
            None
        );
    }
}
