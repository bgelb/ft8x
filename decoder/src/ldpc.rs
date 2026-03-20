use std::sync::OnceLock;

use crate::crc;
use crate::protocol::GRAY_TONES_TO_BITS;

const MAX_ITERS: usize = 30;
const OSD_NT: usize = 40;
const OSD_NTHETA: usize = 10;

#[derive(Debug)]
pub struct ParityMatrix {
    rows: Vec<Vec<usize>>,
    row_columns: Vec<Vec<usize>>,
    row_column_slots: Vec<Vec<usize>>,
    column_rows: Vec<Vec<usize>>,
    generator_rows: Vec<Vec<u8>>,
}

impl ParityMatrix {
    fn parse() -> Self {
        let mut columns = Vec::with_capacity(174);
        for line in include_str!("../data/parity.dat").lines() {
            let trimmed = line.trim();
            if trimmed.is_empty()
                || !trimmed
                    .chars()
                    .all(|ch| ch.is_ascii_digit() || ch.is_ascii_whitespace())
            {
                continue;
            }
            let values: Vec<_> = trimmed
                .split_whitespace()
                .filter_map(|value| value.parse::<usize>().ok())
                .collect();
            if values.len() != 3 {
                continue;
            }
            let row0 = values[0] - 1;
            let row1 = values[1] - 1;
            let row2 = values[2] - 1;
            columns.push([row0, row1, row2]);
        }
        assert_eq!(columns.len(), 174);

        let mut rows = vec![Vec::<usize>::new(); 83];
        for (column, row_ids) in columns.iter().enumerate() {
            for row in row_ids {
                rows[*row].push(column);
            }
        }

        let row_columns = rows.clone();
        let mut column_rows = vec![Vec::<usize>::new(); 174];
        for (row_index, row) in rows.iter().enumerate() {
            for &column in row {
                column_rows[column].push(row_index);
            }
        }
        let row_column_slots = row_columns
            .iter()
            .enumerate()
            .map(|(row_index, columns)| {
                columns
                    .iter()
                    .map(|&column| {
                        column_rows[column]
                            .iter()
                            .position(|&stored_row| stored_row == row_index)
                            .expect("column contains row")
                    })
                    .collect::<Vec<_>>()
            })
            .collect();

        let parity_generator: Vec<Vec<u8>> = include_str!("../data/generator.dat")
            .lines()
            .filter_map(|line| {
                let trimmed = line.trim();
                if trimmed.len() != 91 || !trimmed.bytes().all(|byte| matches!(byte, b'0' | b'1')) {
                    return None;
                }
                Some(
                    trimmed
                        .bytes()
                        .map(|byte| u8::from(byte == b'1'))
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
        assert_eq!(parity_generator.len(), 83);

        let mut generator_rows = vec![vec![0u8; 174]; 91];
        for (index, row) in generator_rows.iter_mut().enumerate() {
            row[index] = 1;
            for (parity_index, parity_row) in parity_generator.iter().enumerate() {
                row[91 + parity_index] = parity_row[index];
            }
        }

        Self {
            rows,
            row_columns,
            row_column_slots,
            column_rows,
            generator_rows,
        }
    }

    pub fn global() -> &'static Self {
        static MATRIX: OnceLock<ParityMatrix> = OnceLock::new();
        MATRIX.get_or_init(Self::parse)
    }

    pub fn parity_ok(&self, bits: &[u8]) -> bool {
        self.rows
            .iter()
            .all(|row| row.iter().fold(0u8, |acc, column| acc ^ bits[*column]) == 0)
    }

    #[allow(dead_code)]
    pub fn decode(&self, llrs: &[f32]) -> Option<(Vec<u8>, usize)> {
        self.decode_with_maxosd(llrs, 0)
    }

    #[allow(dead_code)]
    pub fn decode_with_known_bits(
        &self,
        llrs: &[f32],
        known_bits: &[Option<u8>],
    ) -> Option<(Vec<u8>, usize)> {
        self.decode_with_known_bits_and_maxosd(llrs, known_bits, 0)
    }

    pub fn decode_with_maxosd(&self, llrs: &[f32], maxosd: isize) -> Option<(Vec<u8>, usize)> {
        self.decode_bp_osd(llrs, None, maxosd)
    }

    pub fn decode_with_known_bits_and_maxosd(
        &self,
        llrs: &[f32],
        known_bits: &[Option<u8>],
        maxosd: isize,
    ) -> Option<(Vec<u8>, usize)> {
        self.decode_bp_osd(llrs, Some(known_bits), maxosd)
    }

    fn decode_bp_osd(
        &self,
        llrs: &[f32],
        known_bits: Option<&[Option<u8>]>,
        maxosd: isize,
    ) -> Option<(Vec<u8>, usize)> {
        if llrs.len() != 174 || known_bits.is_some_and(|bits| bits.len() != 174) {
            return None;
        }
        let maxosd = maxosd.clamp(-1, 3);
        let mut tov = [[0.0f32; 3]; 174];
        let mut toc = [[0.0f32; 7]; 83];
        let mut tanhtoc = [[0.0f32; 7]; 83];
        let mut zsum = [0.0f32; 174];
        let mut saved_llrs = Vec::<Vec<f32>>::new();
        if maxosd == 0 {
            saved_llrs.push(llrs.to_vec());
        }

        for (row_index, columns) in self.row_columns.iter().enumerate() {
            for (slot, &column) in columns.iter().enumerate() {
                toc[row_index][slot] = llrs[column];
            }
        }

        let mut initial_bits = [0u8; 174];
        for (column, llr) in llrs.iter().copied().enumerate() {
            initial_bits[column] = known_bits
                .and_then(|bits| bits[column])
                .unwrap_or_else(|| u8::from(llr >= 0.0));
        }
        if self.parity_ok(&initial_bits)
            && crc::crc_matches(&initial_bits[..77], &initial_bits[77..91])
        {
            return Some((initial_bits.to_vec(), 0));
        }

        let mut hard_bits = [0u8; 174];
        let mut zn = [0.0f32; 174];
        let mut ncnt = 0isize;
        let mut nclast = 0usize;
        for iteration in 0..=MAX_ITERS {
            for column in 0..174 {
                zn[column] = if known_bits.and_then(|bits| bits[column]).is_some() {
                    llrs[column]
                } else {
                    llrs[column] + tov[column][0] + tov[column][1] + tov[column][2]
                };
            }
            if maxosd > 0 {
                for (acc, value) in zsum.iter_mut().zip(zn.iter().copied()) {
                    *acc += value;
                }
                if iteration > 0 && iteration as isize <= maxosd {
                    saved_llrs.push(zsum.to_vec());
                }
            }

            for (column, value) in zn.iter().copied().enumerate() {
                hard_bits[column] = u8::from(value >= 0.0);
            }
            let ncheck = self
                .rows
                .iter()
                .filter(|row| row.iter().fold(0u8, |acc, &column| acc ^ hard_bits[column]) != 0)
                .count();
            if ncheck == 0 && crc::crc_matches(&hard_bits[..77], &hard_bits[77..91]) {
                return Some((hard_bits.to_vec(), iteration));
            }

            if iteration > 0 {
                let nd = ncheck as isize - nclast as isize;
                if nd < 0 {
                    ncnt = 0;
                } else {
                    ncnt += 1;
                }
                if ncnt >= 5 && iteration >= 10 && ncheck > 15 {
                    break;
                }
            }
            nclast = ncheck;

            for (row_index, columns) in self.row_columns.iter().enumerate() {
                for (slot, &column) in columns.iter().enumerate() {
                    let col_slot = self.row_column_slots[row_index][slot];
                    toc[row_index][slot] = zn[column] - tov[column][col_slot];
                }
            }

            for row_index in 0..83 {
                for slot in 0..self.row_columns[row_index].len() {
                    tanhtoc[row_index][slot] = (-toc[row_index][slot] / 2.0).tanh();
                }
            }

            for column in 0..174 {
                for (slot, &row_index) in self.column_rows[column].iter().enumerate() {
                    let mut product = 1.0f32;
                    for (row_slot, &other_column) in self.row_columns[row_index].iter().enumerate()
                    {
                        if other_column == column {
                            continue;
                        }
                        product *= tanhtoc[row_index][row_slot];
                    }
                    tov[column][slot] = 2.0 * atanh_clamped(-product);
                }
            }
        }

        for (index, llrs) in saved_llrs.iter().enumerate() {
            if let Some(bits) = self.decode_osd_medium(llrs, known_bits) {
                return Some((bits, MAX_ITERS + index + 1));
            }
        }
        None
    }

    fn decode_osd_medium(
        &self,
        llrs: &[f32],
        known_bits: Option<&[Option<u8>]>,
    ) -> Option<Vec<u8>> {
        if llrs.len() != 174 || known_bits.is_some_and(|bits| bits.len() != 174) {
            return None;
        }

        const K: usize = 91;
        const N: usize = 174;
        const MRB_SEARCH_EXTRA: usize = 20;

        let mut indices: Vec<usize> = (0..174).collect();
        indices.sort_by(|left, right| llrs[*right].abs().total_cmp(&llrs[*left].abs()));
        let mut genmrb: Vec<Vec<u8>> = self
            .generator_rows
            .iter()
            .map(|row| indices.iter().map(|&index| row[index]).collect())
            .collect();
        let mut permuted_indices = indices;

        for pivot in 0..K {
            let search_end = (K + MRB_SEARCH_EXTRA).min(N);
            let Some(column) = (pivot..search_end).find(|&column| genmrb[pivot][column] == 1)
            else {
                return None;
            };
            if column != pivot {
                for row in &mut genmrb {
                    row.swap(pivot, column);
                }
                permuted_indices.swap(pivot, column);
            }
            for row in 0..K {
                if row != pivot && genmrb[row][pivot] == 1 {
                    for column in 0..N {
                        genmrb[row][column] ^= genmrb[pivot][column];
                    }
                }
            }
        }

        let hard: Vec<u8> = permuted_indices
            .iter()
            .map(|&index| {
                known_bits
                    .and_then(|bits| bits[index])
                    .unwrap_or_else(|| u8::from(llrs[index] >= 0.0))
            })
            .collect();
        let reliabilities: Vec<f32> = permuted_indices
            .iter()
            .map(|&index| llrs[index].abs())
            .collect();
        let apmask: Vec<bool> = permuted_indices
            .iter()
            .map(|&index| known_bits.and_then(|bits| bits[index]).is_some())
            .collect();

        let mut best_codeword = encode_mrb(&hard[..K], &genmrb);
        let mut best_distance = weighted_distance(&best_codeword, &hard, &reliabilities);

        for first in (0..K).rev() {
            if apmask[first] {
                continue;
            }
            let mut message = hard[..K].to_vec();
            message[first] ^= 1;
            let codeword = encode_mrb(&message, &genmrb);
            let parity_tail = xor_tail(&codeword, &hard, K);
            let nd1kpt = parity_tail
                .iter()
                .take(OSD_NT)
                .map(|&bit| bit as usize)
                .sum::<usize>()
                + 1;
            if nd1kpt <= OSD_NTHETA {
                let distance = weighted_distance(&codeword, &hard, &reliabilities);
                if distance < best_distance {
                    best_distance = distance;
                    best_codeword = codeword.clone();
                }
            }

            for second in (0..first).rev() {
                if apmask[second] {
                    continue;
                }
                let nd2kpt = parity_tail
                    .iter()
                    .zip(genmrb[second][K..].iter())
                    .take(OSD_NT)
                    .map(|(&tail_bit, &basis_bit)| (tail_bit ^ basis_bit) as usize)
                    .sum::<usize>()
                    + 2;
                if nd2kpt <= OSD_NTHETA {
                    let mut message2 = message.clone();
                    message2[second] ^= 1;
                    let codeword2 = encode_mrb(&message2, &genmrb);
                    let distance = weighted_distance(&codeword2, &hard, &reliabilities);
                    if distance < best_distance {
                        best_distance = distance;
                        best_codeword = codeword2;
                    }
                }
            }
        }

        let mut restored = vec![0u8; N];
        for (column, bit) in best_codeword.into_iter().enumerate() {
            restored[permuted_indices[column]] = bit;
        }
        if self.parity_ok(&restored) && crc::crc_matches(&restored[..77], &restored[77..91]) {
            Some(restored)
        } else {
            None
        }
    }

    #[allow(dead_code)]
    pub fn symbol_bit_llrs(tones: &[[f32; 8]]) -> Vec<[f32; 3]> {
        let mut all = Vec::with_capacity(tones.len() * 8);
        for symbol in tones {
            all.extend_from_slice(symbol);
        }
        all.sort_by(|left, right| left.total_cmp(right));
        let noise_floor = all[all.len() / 2].max(1e-6);

        let mut llrs = Vec::with_capacity(tones.len());
        for symbol in tones {
            let mut bit_llrs = [0.0f32; 3];
            for bit_index in 0..3 {
                let mut best_zero = f32::NEG_INFINITY;
                let mut best_one = f32::NEG_INFINITY;
                for (tone, bits) in GRAY_TONES_TO_BITS.iter().enumerate() {
                    let energy = symbol[tone];
                    if bits[bit_index] == 0 {
                        best_zero = best_zero.max(energy);
                    } else {
                        best_one = best_one.max(energy);
                    }
                }
                bit_llrs[bit_index] = ((best_one - best_zero) / noise_floor).clamp(-24.0, 24.0);
            }
            llrs.push(bit_llrs);
        }
        llrs
    }
}

fn atanh_clamped(x: f32) -> f32 {
    let clamped = x.clamp(-0.999_999, 0.999_999);
    0.5 * ((1.0 + clamped) / (1.0 - clamped)).ln()
}

fn encode_mrb(message: &[u8], generator_rows: &[Vec<u8>]) -> Vec<u8> {
    let mut codeword = vec![0u8; 174];
    for (row, bit) in generator_rows.iter().zip(message.iter().copied()) {
        if bit == 0 {
            continue;
        }
        for (slot, value) in codeword.iter_mut().zip(row) {
            *slot ^= *value;
        }
    }
    codeword
}

fn weighted_distance(codeword: &[u8], hard: &[u8], reliabilities: &[f32]) -> f32 {
    codeword
        .iter()
        .zip(hard)
        .zip(reliabilities)
        .map(|((&bit, &hard_bit), &weight)| if bit == hard_bit { 0.0 } else { weight })
        .sum()
}

fn xor_tail(codeword: &[u8], hard: &[u8], start: usize) -> Vec<u8> {
    codeword[start..]
        .iter()
        .zip(&hard[start..])
        .map(|(&left, &right)| left ^ right)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::ParityMatrix;

    #[test]
    fn decodes_all_zero_codeword() {
        let parity = ParityMatrix::global();
        assert!(parity.parity_ok(&vec![0u8; 174]));
        let llrs = vec![-10.0f32; 174];
        let (bits, _) = parity.decode(&llrs).expect("decode");
        assert!(bits.iter().all(|bit| *bit == 0));
        assert!(parity.parity_ok(&bits));
    }
}
