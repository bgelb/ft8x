pub fn crc14_ft8(message_bits: &[u8]) -> [u8; 14] {
    assert_eq!(message_bits.len(), 77);

    let polynomial = [1u8, 1, 0, 0, 1, 1, 1, 0, 1, 0, 1, 0, 1, 1, 1];
    let mut mc = [0u8; 96];
    mc[..77].copy_from_slice(message_bits);
    let mut register = [0u8; 15];
    register.copy_from_slice(&mc[..15]);

    for i in 0..=81 {
        register[14] = mc[i + 14];
        let leading = register[0];
        for (bit, poly) in register.iter_mut().zip(polynomial) {
            *bit = (*bit + leading * poly) & 1;
        }
        register.rotate_left(1);
    }

    let mut crc = [0u8; 14];
    crc.copy_from_slice(&register[..14]);
    crc
}

pub fn crc_matches(message_bits: &[u8], crc_bits: &[u8]) -> bool {
    crc14_ft8(message_bits).as_slice() == crc_bits
}
