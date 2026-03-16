pub const FT8_SAMPLE_RATE: u32 = 12_000;
pub const FT8_SYMBOL_SAMPLES: usize = 1_920;
pub const FT8_TONE_SPACING_HZ: f32 = 6.25;
pub const FT8_MESSAGE_SYMBOLS: usize = 79;
pub const FT8_PAYLOAD_SYMBOLS: usize = 58;
pub const FT8_SYNC_SYMBOLS: usize = 21;
pub const FT8_COSTAS: [usize; 7] = [3, 1, 4, 0, 6, 5, 2];
pub const FT8_DATA_POSITIONS: [usize; FT8_PAYLOAD_SYMBOLS] = [
    7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
    31, 32, 33, 34, 35, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61,
    62, 63, 64, 65, 66, 67, 68, 69, 70, 71,
];
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

pub const HOP_SAMPLES: usize = 240;
pub const HOPS_PER_SYMBOL: usize = FT8_SYMBOL_SAMPLES / HOP_SAMPLES;

pub const CALL_NTOKENS: u32 = 2_063_592;
pub const CALL_MAX22: u32 = 4_194_304;
pub const CALL_STANDARD_BASE: u32 = CALL_NTOKENS + CALL_MAX22;
pub const HASH_MULTIPLIER: u64 = 47_055_833_459;

pub fn all_costas_positions() -> [(usize, usize); FT8_SYNC_SYMBOLS] {
    let mut pairs = [(0usize, 0usize); FT8_SYNC_SYMBOLS];
    let mut index = 0usize;
    for block in [0usize, 36, 72] {
        for (offset, tone) in FT8_COSTAS.iter().copied().enumerate() {
            pairs[index] = (block + offset, tone);
            index += 1;
        }
    }
    pairs
}
