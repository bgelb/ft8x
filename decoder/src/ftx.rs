use crate::coding::ftx_generator_rows;
use crate::crc::crc14_ft8;
use crate::modes::ft4::FT4_RVEC;
use crate::modes::{Mode, populate_channel_symbols};
use crate::protocol::{FTX_MESSAGE_BITS, gray_decode_tone3, gray_encode_3bits};

pub(crate) struct FtxFrameData {
    pub(crate) codeword_bits: Vec<u8>,
    pub(crate) data_symbols: Vec<u8>,
    pub(crate) channel_symbols: Vec<u8>,
}

pub(crate) fn ftx_wire_message_bits(
    mode: Mode,
    message_bits: [u8; FTX_MESSAGE_BITS],
) -> [u8; FTX_MESSAGE_BITS] {
    if mode == Mode::Ft4 {
        std::array::from_fn(|index| message_bits[index] ^ FT4_RVEC[index])
    } else {
        message_bits
    }
}

pub(crate) fn build_frame(mode: Mode, message_bits: [u8; FTX_MESSAGE_BITS]) -> FtxFrameData {
    let spec = mode.spec();
    let wire_message_bits = ftx_wire_message_bits(mode, message_bits);
    let crc = crc14_ft8(&wire_message_bits);
    let mut info_bits = vec![0u8; spec.coding.info_bits];
    info_bits[..FTX_MESSAGE_BITS].copy_from_slice(&wire_message_bits);
    info_bits[FTX_MESSAGE_BITS..].copy_from_slice(&crc);

    let mut codeword_bits = vec![0u8; spec.coding.codeword_bits];
    codeword_bits[..spec.coding.info_bits].copy_from_slice(&info_bits);
    for (row_index, row) in ftx_generator_rows().iter().enumerate() {
        let parity = row
            .iter()
            .zip(info_bits.iter())
            .fold(0u8, |acc, (tap, bit)| acc ^ (*tap & *bit));
        codeword_bits[spec.coding.info_bits + row_index] = parity;
    }

    let data_symbols =
        ftx_data_symbols_from_codeword_bits(mode, &codeword_bits).expect("ftx codeword symbols");
    let mut channel_symbols = vec![0u8; spec.geometry.message_symbols];
    populate_channel_symbols(&mut channel_symbols, &spec.geometry, &data_symbols)
        .expect("ftx channel layout");

    FtxFrameData {
        codeword_bits,
        data_symbols,
        channel_symbols,
    }
}

pub(crate) fn ftx_data_symbols_from_codeword_bits(
    mode: Mode,
    codeword_bits: &[u8],
) -> Option<Vec<u8>> {
    let spec = mode.spec();
    if codeword_bits.len() < spec.coding.codeword_bits {
        return None;
    }

    Some(
        codeword_bits[..spec.coding.codeword_bits]
            .chunks_exact(spec.coding.bits_per_symbol)
            .map(|chunk| match mode {
                Mode::Ft8 => ftx_tone_from_bits(chunk),
                Mode::Ft4 => ft4_tone_from_bits(chunk),
                Mode::Ft2 => unreachable!(),
            })
            .collect(),
    )
}

fn ftx_tone_from_bits(bits: &[u8]) -> u8 {
    let triad = [bits[0], bits[1], bits[2]];
    let tone = gray_encode_3bits(triad);
    debug_assert_eq!(gray_decode_tone3(tone), triad);
    tone
}

fn ft4_tone_from_bits(bits: &[u8]) -> u8 {
    match [bits[0], bits[1]] {
        [0, 0] => 0,
        [0, 1] => 1,
        [1, 1] => 2,
        [1, 0] => 3,
        _ => unreachable!(),
    }
}
