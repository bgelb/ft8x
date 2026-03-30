pub const FTX_MESSAGE_BITS: usize = 77;
pub const FTX_INFO_BITS: usize = 91;
pub const FTX_CODEWORD_BITS: usize = 174;
pub const FTX_DATA_SYMBOLS: usize = FTX_CODEWORD_BITS / 3;

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
