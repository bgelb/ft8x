const CRC14_FT8_POLYNOMIAL: [u8; 15] = [1, 1, 0, 0, 1, 1, 1, 0, 1, 0, 1, 0, 1, 1, 1];
const CRC13_FT2_TRUNCATED_POLYNOMIAL: u16 = 0x15d7;

fn augmented_crc_bits<const WIDTH: usize, const POLY: usize>(
    message_bits: &[u8],
    polynomial: [u8; POLY],
) -> [u8; WIDTH] {
    let mut mc = vec![0u8; message_bits.len() + WIDTH + 5];
    mc[..message_bits.len()].copy_from_slice(message_bits);
    let mut register = vec![0u8; POLY];
    register.copy_from_slice(&mc[..POLY]);

    for i in 0..=(mc.len() - POLY) {
        register[POLY - 1] = mc[i + POLY - 1];
        let leading = register[0];
        for (bit, poly) in register.iter_mut().zip(polynomial) {
            *bit = (*bit + leading * poly) & 1;
        }
        register.rotate_left(1);
    }

    let mut crc = [0u8; WIDTH];
    crc.copy_from_slice(&register[..WIDTH]);
    crc
}

pub fn crc14_ft8(message_bits: &[u8]) -> [u8; 14] {
    assert_eq!(message_bits.len(), 77);
    augmented_crc_bits::<14, 15>(message_bits, CRC14_FT8_POLYNOMIAL)
}

pub fn crc13_ft2(message_bits: &[u8]) -> [u8; 13] {
    assert_eq!(message_bits.len(), 77);
    let mut packed = [0u8; 12];
    for (index, bit) in message_bits.iter().copied().enumerate() {
        packed[index / 8] |= bit << (7 - (index % 8));
    }

    let mut register = 0u16;
    for byte in packed {
        for shift in (0..8).rev() {
            let bit = u16::from((byte >> shift) & 1);
            let leading = u16::from((register & (1 << 12)) != 0);
            register = ((register << 1) & 0x1fff) | bit;
            if leading != 0 {
                register ^= CRC13_FT2_TRUNCATED_POLYNOMIAL;
            }
        }
    }

    let mut crc = [0u8; 13];
    for (index, out) in crc.iter_mut().enumerate() {
        *out = u8::from(((register >> (12 - index)) & 1) != 0);
    }
    crc
}

pub fn crc_matches(message_bits: &[u8], crc_bits: &[u8]) -> bool {
    crc14_ft8(message_bits).as_slice() == crc_bits
}

pub fn crc_matches_ft2(message_bits: &[u8], crc_bits: &[u8]) -> bool {
    crc13_ft2(message_bits).as_slice() == crc_bits
}
