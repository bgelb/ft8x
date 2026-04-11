use std::sync::OnceLock;

use crate::protocol::FTX_INFO_BITS;

pub(crate) const FT2_ROW_COUNT: usize = 38;
pub(crate) const FT2_COLUMN_COUNT: usize = 128;
pub(crate) const FT2_INFO_BITS: usize = 90;

pub(crate) const FT2_COLUMN_ROWS: [[usize; 3]; FT2_COLUMN_COUNT] = [
    [20, 33, 35],
    [0, 7, 27],
    [1, 8, 36],
    [2, 6, 18],
    [3, 15, 31],
    [1, 4, 21],
    [5, 12, 24],
    [9, 30, 32],
    [10, 23, 26],
    [11, 14, 22],
    [13, 17, 25],
    [16, 19, 28],
    [16, 29, 33],
    [5, 33, 34],
    [0, 9, 29],
    [2, 17, 22],
    [3, 11, 24],
    [4, 27, 35],
    [6, 13, 20],
    [7, 14, 30],
    [8, 26, 31],
    [10, 18, 34],
    [12, 15, 36],
    [19, 23, 37],
    [20, 21, 25],
    [11, 28, 32],
    [0, 16, 34],
    [1, 27, 29],
    [2, 9, 31],
    [3, 7, 35],
    [4, 18, 28],
    [5, 19, 26],
    [6, 21, 36],
    [8, 10, 32],
    [12, 23, 25],
    [13, 30, 33],
    [14, 15, 24],
    [12, 17, 37],
    [7, 19, 22],
    [0, 31, 32],
    [1, 16, 18],
    [2, 23, 33],
    [3, 6, 37],
    [4, 10, 30],
    [5, 17, 20],
    [8, 14, 35],
    [9, 15, 27],
    [11, 25, 29],
    [13, 26, 28],
    [21, 24, 34],
    [22, 29, 31],
    [3, 10, 36],
    [0, 13, 22],
    [1, 7, 24],
    [2, 12, 26],
    [4, 9, 36],
    [5, 15, 30],
    [6, 14, 17],
    [8, 21, 23],
    [11, 18, 35],
    [16, 25, 37],
    [19, 20, 32],
    [19, 27, 34],
    [3, 28, 33],
    [0, 25, 35],
    [1, 22, 33],
    [2, 8, 37],
    [4, 5, 16],
    [6, 26, 34],
    [7, 13, 31],
    [9, 14, 21],
    [10, 17, 28],
    [11, 12, 27],
    [15, 18, 32],
    [20, 24, 30],
    [23, 29, 36],
    [0, 2, 20],
    [1, 17, 30],
    [3, 5, 8],
    [4, 7, 32],
    [6, 28, 31],
    [9, 12, 18],
    [10, 21, 22],
    [11, 26, 33],
    [13, 14, 29],
    [15, 26, 37],
    [16, 27, 36],
    [19, 24, 25],
    [4, 23, 34],
    [2, 5, 35],
    [0, 11, 30],
    [1, 3, 32],
    [2, 15, 29],
    [0, 1, 23],
    [4, 22, 26],
    [5, 27, 31],
    [6, 16, 35],
    [7, 21, 37],
    [8, 17, 19],
    [9, 20, 28],
    [10, 12, 33],
    [3, 13, 19],
    [10, 29, 37],
    [13, 34, 36],
    [14, 18, 25],
    [2, 27, 28],
    [6, 7, 8],
    [4, 17, 33],
    [12, 14, 16],
    [11, 15, 34],
    [9, 22, 24],
    [18, 20, 36],
    [16, 26, 30],
    [23, 24, 35],
    [0, 17, 18],
    [5, 25, 32],
    [21, 30, 31],
    [2, 19, 21],
    [3, 20, 26],
    [1, 12, 28],
    [5, 6, 11],
    [14, 23, 31],
    [8, 24, 29],
    [22, 36, 37],
    [4, 15, 25],
    [10, 13, 27],
    [32, 35, 37],
    [7, 9, 34],
];

const FT2_GENERATOR_HEX: [&str; FT2_ROW_COUNT] = [
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

pub(crate) fn ftx_generator_rows() -> &'static Vec<[u8; FTX_INFO_BITS]> {
    static ROWS: OnceLock<Vec<[u8; FTX_INFO_BITS]>> = OnceLock::new();
    ROWS.get_or_init(|| {
        include_str!("../data/generator.dat")
            .lines()
            .filter_map(|line| {
                let trimmed = line.trim();
                if trimmed.len() != FTX_INFO_BITS
                    || !trimmed.bytes().all(|byte| matches!(byte, b'0' | b'1'))
                {
                    return None;
                }
                let mut row = [0u8; FTX_INFO_BITS];
                for (index, byte) in trimmed.bytes().enumerate() {
                    row[index] = u8::from(byte == b'1');
                }
                Some(row)
            })
            .collect()
    })
}

pub(crate) fn ft2_generator_rows() -> &'static Vec<[u8; FT2_INFO_BITS]> {
    static ROWS: OnceLock<Vec<[u8; FT2_INFO_BITS]>> = OnceLock::new();
    ROWS.get_or_init(|| {
        FT2_GENERATOR_HEX
            .iter()
            .map(|hex| {
                let mut row = [0u8; FT2_INFO_BITS];
                let mut out = 0usize;
                for ch in hex.bytes() {
                    let nibble = match ch {
                        b'0'..=b'9' => ch - b'0',
                        b'a'..=b'f' => 10 + ch - b'a',
                        b'A'..=b'F' => 10 + ch - b'A',
                        _ => unreachable!(),
                    };
                    let bit_count = if out + 4 > FT2_INFO_BITS { 2 } else { 4 };
                    for shift in (4 - bit_count..4).rev() {
                        row[out] = u8::from(((nibble >> shift) & 1) != 0);
                        out += 1;
                    }
                }
                row
            })
            .collect()
    })
}
