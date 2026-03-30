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

pub const FTX_MESSAGE_KIND_STANDARD_SLASH_R: u8 = 1;
pub const FTX_MESSAGE_KIND_STANDARD_SLASH_P: u8 = 2;
pub const FTX_MESSAGE_KIND_NONSTANDARD: u8 = 4;
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
        assert_eq!(read_bit_field(&bits, FTX_STANDARD_LAYOUT.first_call), 0x1234_567);
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
        for (index, ch) in " 0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            .chars()
            .enumerate()
        {
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
        assert_eq!(FTX_FREE_TEXT_SUBTYPE_FIELD.end(), FTX_STANDARD_LAYOUT.kind.start);
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
