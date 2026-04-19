use std::sync::OnceLock;

use crate::crc;
use crate::protocol::GRAY_TONES_TO_BITS;

const MAX_ITERS: usize = 30;
const OSD_NT: usize = 40;
const THRESHOLDED_OSD_ZERO_MAXOSD_NTHETA: usize = 10;
const THRESHOLDED_OSD_ITERATIVE_NTHETA: usize = 12;
// Preserve the historic piecewise-linear atanh approximation used by the
// generalized 174,91 decoder path before the FT8 regression fix.
const BP_UPDATE_LINEAR_LIMIT: f32 = 0.664;
const BP_UPDATE_LINEAR_SCALE: f32 = 0.83;
const BP_UPDATE_SEGMENT_1_LIMIT: f32 = 0.9217;
const BP_UPDATE_SEGMENT_1_OFFSET: f32 = 0.4064;
const BP_UPDATE_SEGMENT_1_SCALE: f32 = 0.322;
const BP_UPDATE_SEGMENT_2_LIMIT: f32 = 0.9951;
const BP_UPDATE_SEGMENT_2_OFFSET: f32 = 0.8378;
const BP_UPDATE_SEGMENT_2_SCALE: f32 = 0.0524;
const BP_UPDATE_SEGMENT_3_LIMIT: f32 = 0.9998;
const BP_UPDATE_SEGMENT_3_OFFSET: f32 = 0.9914;
const BP_UPDATE_SEGMENT_3_SCALE: f32 = 0.0012;
const BP_UPDATE_CLAMP: f32 = 7.0;

#[derive(Debug, Clone, serde::Serialize)]
pub struct LdpcOsdProbeResult {
    pub input_index: usize,
    pub sorted_indices: Vec<usize>,
    pub ordered_indices: Vec<usize>,
    pub mrb_message_bits: String,
    pub mrb_message_flips: Vec<usize>,
    pub mrb_flips: Vec<usize>,
    pub codeword_bits: Option<String>,
    pub hard_errors: Option<usize>,
    pub weighted_distance: Option<f32>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct LdpcDebugState {
    pub bp_codeword_bits: Option<String>,
    pub bp_iterations: Option<usize>,
    pub saved_llrs: Vec<Vec<f32>>,
    pub osd_results: Vec<LdpcOsdProbeResult>,
}

#[derive(Clone, Copy, Debug)]
struct OsdConfig {
    nord: usize,
    npre1: bool,
    #[allow(dead_code)]
    npre2: bool,
    ntheta: usize,
}

#[derive(Clone, Copy, Debug)]
pub enum BpUpdateRule {
    AtanhClamped,
    PiecewiseLinearAtanh,
}

#[derive(Clone, Copy, Debug)]
pub enum OsdStyle {
    OrderedStatistics {
        norder: usize,
    },
    ThresholdedSearch {
        max_order: usize,
        zero_maxosd_ntheta: usize,
        iterative_ntheta: usize,
    },
}

#[derive(Clone, Copy, Debug)]
pub struct LdpcDecodePolicy {
    pub bp_update_rule: BpUpdateRule,
    pub osd_style: OsdStyle,
}

impl LdpcDecodePolicy {
    pub const fn ordered_statistics(norder: usize) -> Self {
        Self {
            bp_update_rule: BpUpdateRule::AtanhClamped,
            osd_style: OsdStyle::OrderedStatistics { norder },
        }
    }

    pub const fn thresholded_search(max_order: usize) -> Self {
        Self {
            bp_update_rule: BpUpdateRule::AtanhClamped,
            osd_style: OsdStyle::ThresholdedSearch {
                max_order,
                zero_maxosd_ntheta: THRESHOLDED_OSD_ZERO_MAXOSD_NTHETA,
                iterative_ntheta: THRESHOLDED_OSD_ITERATIVE_NTHETA,
            },
        }
    }
}

impl OsdConfig {
    fn from_norder(norder: usize) -> Self {
        match norder.clamp(0, 6) {
            0 | 1 => Self {
                nord: 1,
                npre1: false,
                npre2: false,
                ntheta: 12,
            },
            2 => Self {
                nord: 1,
                npre1: true,
                npre2: false,
                ntheta: 10,
            },
            3 => Self {
                nord: 1,
                npre1: true,
                npre2: true,
                ntheta: 12,
            },
            4 => Self {
                nord: 2,
                npre1: true,
                npre2: true,
                ntheta: 12,
            },
            5 | 6 => Self {
                nord: if norder >= 6 { 4 } else { 3 },
                npre1: true,
                npre2: true,
                ntheta: 12,
            },
            _ => unreachable!(),
        }
    }
}

#[derive(Debug)]
pub struct ParityMatrix {
    rows: Vec<Vec<usize>>,
    row_columns: Vec<Vec<usize>>,
    row_column_slots: Vec<Vec<usize>>,
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
        self.decode_with_policy(llrs, None, maxosd, LdpcDecodePolicy::ordered_statistics(2))
    }

    pub fn decode_with_policy(
        &self,
        llrs: &[f32],
        known_bits: Option<&[Option<u8>]>,
        maxosd: isize,
        policy: LdpcDecodePolicy,
    ) -> Option<(Vec<u8>, usize)> {
        match policy.osd_style {
            OsdStyle::OrderedStatistics { norder } => {
                self.decode_bp_osd(llrs, known_bits, maxosd, norder, policy.bp_update_rule)
            }
            OsdStyle::ThresholdedSearch {
                max_order,
                zero_maxosd_ntheta,
                iterative_ntheta,
            } => self.decode_bp_with_thresholded_osd(
                llrs,
                known_bits,
                maxosd,
                max_order,
                zero_maxosd_ntheta,
                iterative_ntheta,
                policy.bp_update_rule,
            ),
        }
    }

    pub fn decode_with_known_bits_and_maxosd(
        &self,
        llrs: &[f32],
        known_bits: &[Option<u8>],
        maxosd: isize,
    ) -> Option<(Vec<u8>, usize)> {
        self.decode_with_policy(
            llrs,
            Some(known_bits),
            maxosd,
            LdpcDecodePolicy::ordered_statistics(2),
        )
    }

    fn decode_bp_osd(
        &self,
        llrs: &[f32],
        known_bits: Option<&[Option<u8>]>,
        maxosd: isize,
        norder: usize,
        bp_update_rule: BpUpdateRule,
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

            self.update_variable_messages(&tanhtoc, &mut tov, bp_update_rule);
        }

        let osd_config = OsdConfig::from_norder(norder);
        for (index, llrs) in saved_llrs.iter().enumerate() {
            if let Some(bits) = self.decode_osd(llrs, known_bits, osd_config) {
                return Some((bits, MAX_ITERS + index + 1));
            }
        }
        None
    }

    fn decode_bp_with_thresholded_osd(
        &self,
        llrs: &[f32],
        known_bits: Option<&[Option<u8>]>,
        maxosd: isize,
        max_order: usize,
        zero_maxosd_ntheta: usize,
        iterative_ntheta: usize,
        bp_update_rule: BpUpdateRule,
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

            self.update_variable_messages(&tanhtoc, &mut tov, bp_update_rule);
        }

        let ntheta = if maxosd == 0 {
            zero_maxosd_ntheta
        } else {
            iterative_ntheta
        };
        for (index, saved) in saved_llrs.iter().enumerate() {
            if let Some(bits) = self.decode_osd_thresholded(saved, known_bits, max_order, ntheta) {
                return Some((bits, MAX_ITERS + index + 1));
            }
        }
        None
    }

    pub fn debug_bp_osd_state(
        &self,
        llrs: &[f32],
        known_bits: Option<&[Option<u8>]>,
        maxosd: isize,
        norder: usize,
    ) -> Option<LdpcDebugState> {
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

        let mut hard_bits = [0u8; 174];
        let mut zn = [0.0f32; 174];
        let mut ncnt = 0isize;
        let mut nclast = 0usize;
        let mut bp_codeword_bits = None;
        let mut bp_iterations = None;
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
                bp_codeword_bits = Some(bit_string(&hard_bits));
                bp_iterations = Some(iteration);
                break;
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

            self.update_variable_messages(&tanhtoc, &mut tov, BpUpdateRule::AtanhClamped);
        }

        let osd_config = OsdConfig::from_norder(norder);
        let osd_results = saved_llrs
            .iter()
            .enumerate()
            .map(|(index, saved)| self.debug_osd(saved, known_bits, osd_config, index + 1))
            .collect();
        Some(LdpcDebugState {
            bp_codeword_bits,
            bp_iterations,
            saved_llrs,
            osd_results,
        })
    }

    fn decode_osd(
        &self,
        llrs: &[f32],
        known_bits: Option<&[Option<u8>]>,
        config: OsdConfig,
    ) -> Option<Vec<u8>> {
        self.debug_osd(llrs, known_bits, config, 0)
            .codeword_bits
            .map(|bits| bits.bytes().map(|byte| u8::from(byte == b'1')).collect())
    }

    fn update_variable_messages(
        &self,
        tanhtoc: &[[f32; 7]; 83],
        tov: &mut [[f32; 3]; 174],
        bp_update_rule: BpUpdateRule,
    ) {
        for (row_index, columns) in self.row_columns.iter().enumerate() {
            for (row_slot, &column) in columns.iter().enumerate() {
                let mut product = 1.0f32;
                for other_slot in 0..columns.len() {
                    if other_slot == row_slot {
                        continue;
                    }
                    product *= tanhtoc[row_index][other_slot];
                }
                let column_slot = self.row_column_slots[row_index][row_slot];
                tov[column][column_slot] = 2.0 * apply_bp_update_rule(bp_update_rule, -product);
            }
        }
    }

    fn debug_osd(
        &self,
        llrs: &[f32],
        known_bits: Option<&[Option<u8>]>,
        config: OsdConfig,
        input_index: usize,
    ) -> LdpcOsdProbeResult {
        if llrs.len() != 174 || known_bits.is_some_and(|bits| bits.len() != 174) {
            return LdpcOsdProbeResult {
                input_index,
                sorted_indices: Vec::new(),
                ordered_indices: Vec::new(),
                mrb_message_bits: String::new(),
                mrb_message_flips: Vec::new(),
                mrb_flips: Vec::new(),
                codeword_bits: None,
                hard_errors: None,
                weighted_distance: None,
            };
        }

        const K: usize = 91;
        const N: usize = 174;
        const MRB_SEARCH_EXTRA: usize = 20;

        let indices = indexx_descending_by_abs(llrs);
        let sorted_indices = indices.clone();
        let mut genmrb: Vec<Vec<u8>> = self
            .generator_rows
            .iter()
            .map(|row| indices.iter().map(|&index| row[index]).collect())
            .collect();
        let mut permuted_indices = indices;

        for pivot in 0..K {
            let search_end = (K + MRB_SEARCH_EXTRA).min(N);
            if let Some(column) = (pivot..search_end).find(|&column| genmrb[pivot][column] == 1) {
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

        let mut best_message = hard[..K].to_vec();
        let mut best_codeword = encode_mrb(&best_message, &genmrb);
        let mut best_selection_distance = weighted_distance(&best_codeword, &hard, &reliabilities);

        if config.nord == 0 {
            return build_osd_probe_result(
                self,
                &best_message,
                &best_codeword,
                &hard,
                &reliabilities,
                &sorted_indices,
                &permuted_indices,
                input_index,
            );
        }

        for iorder in 1..=config.nord {
            let mut misub = vec![0u8; K];
            misub[(K - iorder)..K].fill(1);
            let mut iflag = K - iorder;
            loop {
                let iend = if iorder == config.nord && !config.npre1 {
                    iflag
                } else {
                    0
                };
                let mut cached_tail = Vec::<u8>::new();
                let mut cached_d1 = 0.0f32;
                for n1 in (iend..=iflag).rev() {
                    let mut mi = misub.clone();
                    mi[n1] = 1;
                    if apmask
                        .iter()
                        .zip(mi.iter())
                        .any(|(&masked, &bit)| masked && bit == 1)
                    {
                        continue;
                    }
                    let mut me = hard[..K].to_vec();
                    for (slot, flip) in me.iter_mut().zip(mi.iter().copied()) {
                        *slot ^= flip;
                    }
                    let codeword = encode_mrb(&me, &genmrb);
                    let parity_tail = if n1 == iflag {
                        cached_tail = xor_tail(&codeword, &hard, K);
                        cached_d1 = weighted_distance(&me, &hard[..K], &reliabilities[..K]);
                        cached_tail.clone()
                    } else {
                        cached_tail
                            .iter()
                            .zip(genmrb[n1][K..].iter())
                            .map(|(&tail_bit, &basis_bit)| tail_bit ^ basis_bit)
                            .collect()
                    };
                    let ndkpt = parity_tail
                        .iter()
                        .take(OSD_NT)
                        .map(|&bit| bit as usize)
                        .sum::<usize>()
                        + if n1 == iflag { 1 } else { 2 };
                    if ndkpt > config.ntheta {
                        continue;
                    }
                    let selection_distance = if n1 == iflag {
                        cached_d1
                            + parity_tail
                                .iter()
                                .zip(reliabilities[K..].iter())
                                .map(|(&bit, &weight)| if bit == 0 { 0.0 } else { weight })
                                .sum::<f32>()
                    } else {
                        cached_d1
                            + if codeword[n1] == hard[n1] {
                                0.0
                            } else {
                                reliabilities[n1]
                            }
                            + parity_tail
                                .iter()
                                .zip(reliabilities[K..].iter())
                                .map(|(&bit, &weight)| if bit == 0 { 0.0 } else { weight })
                                .sum::<f32>()
                    };
                    if selection_distance < best_selection_distance {
                        best_selection_distance = selection_distance;
                        best_message = me;
                        best_codeword = codeword;
                    }
                }
                let Some(next_iflag) = nextpat91(&mut misub, iorder) else {
                    break;
                };
                iflag = next_iflag;
            }
        }

        build_osd_probe_result(
            self,
            &best_message,
            &best_codeword,
            &hard,
            &reliabilities,
            &sorted_indices,
            &permuted_indices,
            input_index,
        )
    }

    fn decode_osd_thresholded(
        &self,
        llrs: &[f32],
        known_bits: Option<&[Option<u8>]>,
        max_order: usize,
        ntheta: usize,
    ) -> Option<Vec<u8>> {
        if llrs.len() != 174 || known_bits.is_some_and(|bits| bits.len() != 174) {
            return None;
        }

        const K: usize = 91;
        const N: usize = 174;
        const PARITY: usize = N - K;
        const MRB_SEARCH_EXTRA: usize = 20;

        let no_known = [None; N];
        let known_bits = known_bits.unwrap_or(&no_known);

        let mut indices = [0usize; N];
        for (index, slot) in indices.iter_mut().enumerate() {
            *slot = index;
        }
        indices.sort_by(|left, right| llrs[*right].abs().total_cmp(&llrs[*left].abs()));

        let mut genmrb = [[0u8; N]; K];
        for (row_index, row) in genmrb.iter_mut().enumerate() {
            for (column, &index) in indices.iter().enumerate() {
                row[column] = self.generator_rows[row_index][index];
            }
        }
        let mut permuted_indices = indices;

        for pivot in 0..K {
            let search_end = (K + MRB_SEARCH_EXTRA).min(N);
            let column = (pivot..search_end).find(|&column| genmrb[pivot][column] == 1)?;
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

        let mut hard = [0u8; N];
        let mut reliabilities = [0.0f32; N];
        let mut apmask = [false; N];
        for (slot, &index) in permuted_indices.iter().enumerate() {
            hard[slot] = known_bits[index].unwrap_or_else(|| u8::from(llrs[index] >= 0.0));
            reliabilities[slot] = llrs[index].abs();
            apmask[slot] = known_bits[index].is_some();
        }

        let mut best_codeword = [0u8; N];
        encode_mrb_array(&hard[..K], &genmrb, &mut best_codeword);
        let mut best_distance = weighted_distance_array(&best_codeword, &hard, &reliabilities);

        for first in (0..K).rev() {
            if apmask[first] {
                continue;
            }
            let mut message = [0u8; K];
            message.copy_from_slice(&hard[..K]);
            message[first] ^= 1;
            let mut codeword = [0u8; N];
            encode_mrb_array(&message, &genmrb, &mut codeword);
            let mut parity_tail = [0u8; PARITY];
            xor_tail_array(&codeword, &hard, &mut parity_tail);
            let nd1kpt = parity_tail
                .iter()
                .take(OSD_NT)
                .map(|&bit| bit as usize)
                .sum::<usize>()
                + 1;
            if nd1kpt <= ntheta {
                let distance = weighted_distance_array(&codeword, &hard, &reliabilities);
                if distance < best_distance {
                    best_distance = distance;
                    best_codeword = codeword;
                }
            }

            if max_order < 2 {
                continue;
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
                if nd2kpt > ntheta {
                    continue;
                }

                let mut message2 = message;
                message2[second] ^= 1;
                let mut codeword2 = [0u8; N];
                encode_mrb_array(&message2, &genmrb, &mut codeword2);
                let distance = weighted_distance_array(&codeword2, &hard, &reliabilities);
                if distance < best_distance {
                    best_distance = distance;
                    best_codeword = codeword2;
                }

                if max_order >= 3 {
                    let mut parity_tail2 = [0u8; PARITY];
                    xor_tail_array(&codeword2, &hard, &mut parity_tail2);
                    for third in (0..second).rev() {
                        if apmask[third] {
                            continue;
                        }
                        let nd3kpt = parity_tail2
                            .iter()
                            .zip(genmrb[third][K..].iter())
                            .take(OSD_NT)
                            .map(|(&tail_bit, &basis_bit)| (tail_bit ^ basis_bit) as usize)
                            .sum::<usize>()
                            + 3;
                        if nd3kpt > ntheta {
                            continue;
                        }

                        let mut message3 = message2;
                        message3[third] ^= 1;
                        let mut codeword3 = [0u8; N];
                        encode_mrb_array(&message3, &genmrb, &mut codeword3);
                        let distance = weighted_distance_array(&codeword3, &hard, &reliabilities);
                        if distance < best_distance {
                            best_distance = distance;
                            best_codeword = codeword3;
                        }
                    }
                }
            }
        }

        let restored = restore_osd_codeword_array(self, &best_codeword, &permuted_indices)?;
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

fn apply_bp_update_rule(rule: BpUpdateRule, value: f32) -> f32 {
    match rule {
        BpUpdateRule::AtanhClamped => atanh_clamped(value),
        BpUpdateRule::PiecewiseLinearAtanh => platanh_approx(value),
    }
}

fn platanh_approx(x: f32) -> f32 {
    let magnitude = x.abs();
    let approx = if magnitude <= BP_UPDATE_LINEAR_LIMIT {
        x / BP_UPDATE_LINEAR_SCALE
    } else if magnitude <= BP_UPDATE_SEGMENT_1_LIMIT {
        x.signum() * ((magnitude - BP_UPDATE_SEGMENT_1_OFFSET) / BP_UPDATE_SEGMENT_1_SCALE)
    } else if magnitude <= BP_UPDATE_SEGMENT_2_LIMIT {
        x.signum() * ((magnitude - BP_UPDATE_SEGMENT_2_OFFSET) / BP_UPDATE_SEGMENT_2_SCALE)
    } else if magnitude <= BP_UPDATE_SEGMENT_3_LIMIT {
        x.signum() * ((magnitude - BP_UPDATE_SEGMENT_3_OFFSET) / BP_UPDATE_SEGMENT_3_SCALE)
    } else {
        x.signum() * BP_UPDATE_CLAMP
    };
    if x == 0.0 { 0.0 } else { approx }
}

fn build_osd_probe_result(
    parity: &ParityMatrix,
    best_message: &[u8],
    codeword: &[u8],
    hard: &[u8],
    reliabilities: &[f32],
    sorted_indices: &[usize],
    permuted_indices: &[usize],
    input_index: usize,
) -> LdpcOsdProbeResult {
    let restored = restore_osd_codeword(parity, codeword, permuted_indices);
    let k = parity.generator_rows.len();
    LdpcOsdProbeResult {
        input_index,
        sorted_indices: sorted_indices.to_vec(),
        ordered_indices: permuted_indices.to_vec(),
        mrb_message_bits: bit_string(best_message),
        mrb_message_flips: best_message
            .iter()
            .zip(hard[..k].iter())
            .enumerate()
            .filter_map(|(index, (&left, &right))| (left != right).then_some(index))
            .collect(),
        mrb_flips: codeword[..k]
            .iter()
            .zip(hard[..k].iter())
            .enumerate()
            .filter_map(|(index, (&left, &right))| (left != right).then_some(index))
            .collect(),
        codeword_bits: restored.as_ref().map(|bits| bit_string(bits)),
        hard_errors: Some(
            codeword
                .iter()
                .zip(hard.iter())
                .filter(|(left, right)| left != right)
                .count(),
        ),
        weighted_distance: Some(weighted_distance(codeword, hard, reliabilities)),
    }
}

fn bit_string(bits: &[u8]) -> String {
    bits.iter()
        .map(|bit| if *bit == 0 { '0' } else { '1' })
        .collect()
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

fn encode_mrb_array<const K: usize, const N: usize>(
    message: &[u8],
    generator_rows: &[[u8; N]; K],
    codeword: &mut [u8; N],
) {
    codeword.fill(0);
    for (row, bit) in generator_rows.iter().zip(message.iter().copied()) {
        if bit == 0 {
            continue;
        }
        for (slot, value) in codeword.iter_mut().zip(row) {
            *slot ^= *value;
        }
    }
}

fn weighted_distance(codeword: &[u8], hard: &[u8], reliabilities: &[f32]) -> f32 {
    codeword
        .iter()
        .zip(hard)
        .zip(reliabilities)
        .map(|((&bit, &hard_bit), &weight)| if bit == hard_bit { 0.0 } else { weight })
        .sum()
}

fn weighted_distance_array<const N: usize>(
    codeword: &[u8; N],
    hard: &[u8; N],
    reliabilities: &[f32; N],
) -> f32 {
    let mut distance = 0.0f32;
    for index in 0..N {
        if codeword[index] != hard[index] {
            distance += reliabilities[index];
        }
    }
    distance
}

fn xor_tail(codeword: &[u8], hard: &[u8], start: usize) -> Vec<u8> {
    codeword[start..]
        .iter()
        .zip(&hard[start..])
        .map(|(&left, &right)| left ^ right)
        .collect()
}

fn xor_tail_array<const N: usize, const K: usize>(
    codeword: &[u8; N],
    hard: &[u8; N],
    tail: &mut [u8; K],
) {
    for index in 0..K {
        tail[index] = codeword[N - K + index] ^ hard[N - K + index];
    }
}

fn restore_osd_codeword(
    parity: &ParityMatrix,
    codeword: &[u8],
    permuted_indices: &[usize],
) -> Option<Vec<u8>> {
    let mut restored = vec![0u8; codeword.len()];
    for (column, bit) in codeword.iter().copied().enumerate() {
        restored[permuted_indices[column]] = bit;
    }
    if parity.parity_ok(&restored) && crc::crc_matches(&restored[..77], &restored[77..91]) {
        Some(restored)
    } else {
        None
    }
}

fn restore_osd_codeword_array<const N: usize>(
    parity: &ParityMatrix,
    codeword: &[u8; N],
    permuted_indices: &[usize; N],
) -> Option<Vec<u8>> {
    let mut restored = vec![0u8; N];
    for (column, bit) in codeword.iter().copied().enumerate() {
        restored[permuted_indices[column]] = bit;
    }
    if parity.parity_ok(&restored) && crc::crc_matches(&restored[..77], &restored[77..91]) {
        Some(restored)
    } else {
        None
    }
}

fn nextpat91(mi: &mut [u8], iorder: usize) -> Option<usize> {
    let mut ind = None;
    for i in 0..(mi.len().saturating_sub(1)) {
        if mi[i] == 0 && mi[i + 1] == 1 {
            ind = Some(i);
        }
    }
    let ind = ind?;
    let mut ms = vec![0u8; mi.len()];
    ms[..ind].copy_from_slice(&mi[..ind]);
    ms[ind] = 1;
    ms[ind + 1] = 0;
    if ind + 1 < mi.len() {
        let nz = iorder.saturating_sub(ms.iter().map(|&bit| bit as usize).sum::<usize>());
        if nz > 0 {
            ms[(mi.len() - nz)..].fill(1);
        }
    }
    mi.copy_from_slice(&ms);
    mi.iter().position(|&bit| bit == 1)
}

fn indexx_descending_by_abs(llrs: &[f32]) -> Vec<usize> {
    const M: usize = 7;
    const NSTACK: usize = 50;

    let n = llrs.len();
    let abs_llrs: Vec<f32> = llrs.iter().map(|value| value.abs()).collect();
    let mut indices: Vec<usize> = (0..n).collect();
    let mut istack = [0usize; NSTACK];
    let mut jstack = 0usize;
    let mut l = 0usize;
    let mut ir = n.saturating_sub(1);

    loop {
        if ir.saturating_sub(l) < M {
            for j in (l + 1)..=ir {
                let indxt = indices[j];
                let a = abs_llrs[indxt];
                let mut i = j - 1;
                loop {
                    if abs_llrs[indices[i]] <= a {
                        break;
                    }
                    indices[i + 1] = indices[i];
                    if i == 0 {
                        i = usize::MAX;
                        break;
                    }
                    i -= 1;
                }
                indices[i.wrapping_add(1)] = indxt;
            }
            if jstack == 0 {
                break;
            }
            ir = istack[jstack - 1];
            l = istack[jstack - 2];
            jstack -= 2;
        } else {
            let k = (l + ir) / 2;
            indices.swap(k, l + 1);

            if abs_llrs[indices[l + 1]] > abs_llrs[indices[ir]] {
                indices.swap(l + 1, ir);
            }
            if abs_llrs[indices[l]] > abs_llrs[indices[ir]] {
                indices.swap(l, ir);
            }
            if abs_llrs[indices[l + 1]] > abs_llrs[indices[l]] {
                indices.swap(l + 1, l);
            }

            let indxt = indices[l];
            let a = abs_llrs[indxt];
            let mut i = l + 1;
            let mut j = ir;

            loop {
                loop {
                    i += 1;
                    if abs_llrs[indices[i]] >= a {
                        break;
                    }
                }
                loop {
                    j -= 1;
                    if abs_llrs[indices[j]] <= a {
                        break;
                    }
                }
                if j < i {
                    break;
                }
                indices.swap(i, j);
            }

            indices[l] = indices[j];
            indices[j] = indxt;
            jstack += 2;
            assert!(
                jstack <= NSTACK,
                "NSTACK too small in indexx_descending_by_abs"
            );
            if ir - i + 1 >= j - l {
                istack[jstack - 1] = ir;
                istack[jstack - 2] = i;
                ir = j.saturating_sub(1);
            } else {
                istack[jstack - 1] = j.saturating_sub(1);
                istack[jstack - 2] = l;
                l = i;
            }
        }
    }

    indices.reverse();
    indices
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
