pub const FTX_MESSAGE_BITS: usize = 77;
pub const FTX_INFO_BITS: usize = 91;
pub const FTX_CODEWORD_BITS: usize = 174;
pub const FTX_DATA_SYMBOLS: usize = FTX_CODEWORD_BITS / 3;

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

pub const FTX_STANDARD_LAYOUT: StandardMessageLayout = StandardMessageLayout {
    first_call: BitField { start: 0, len: 28 },
    first_suffix: BitField { start: 28, len: 1 },
    second_call: BitField { start: 29, len: 28 },
    second_suffix: BitField { start: 57, len: 1 },
    acknowledge: BitField { start: 58, len: 1 },
    info: BitField { start: 59, len: 15 },
    kind: BitField { start: 74, len: 3 },
};

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
pub const FTX_AP_KNOWN_FIELDS: [BitField; 2] = [
    BitField { start: 0, len: 29 },
    BitField { start: 74, len: 3 },
];

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
        for offset in 0..field.len {
            let index = field.start + offset;
            known[index] = Some(message_bits[index]);
        }
    }
    Some(())
}

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

        for index in 0..29 {
            assert_eq!(known[index], Some(message_bits[index]));
        }
        for index in 29..74 {
            assert_eq!(known[index], None);
        }
        for index in 74..77 {
            assert_eq!(known[index], Some(message_bits[index]));
        }
    }
}
