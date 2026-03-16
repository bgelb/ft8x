use ldpc_toolbox::decoder::arithmetic::Minstarapproxf32;
use ldpc_toolbox::decoder::horizontal_layered;
use ldpc_toolbox::sparse::SparseMatrix as ToolboxSparseMatrix;
use std::sync::OnceLock;

use crate::protocol::GRAY_TONES_TO_BITS;

const MAX_ITERS: usize = 40;
const MIN_SUM_SCALE: f32 = 0.8;

#[derive(Debug)]
pub struct ParityMatrix {
    rows: Vec<Vec<usize>>,
    row_edges: Vec<Vec<usize>>,
    column_edges: Vec<Vec<usize>>,
    edge_columns: Vec<usize>,
    toolbox_matrix: ToolboxSparseMatrix,
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

        let mut row_edges = Vec::with_capacity(rows.len());
        let mut column_edges = vec![Vec::<usize>::new(); 174];
        let mut edge_columns = Vec::new();
        for row in &rows {
            let mut edges = Vec::with_capacity(row.len());
            for &column in row {
                let edge_index = edge_columns.len();
                edge_columns.push(column);
                edges.push(edge_index);
                column_edges[column].push(edge_index);
            }
            row_edges.push(edges);
        }

        let mut toolbox_matrix = ToolboxSparseMatrix::new(83, 174);
        for (row_index, row) in rows.iter().enumerate() {
            toolbox_matrix.insert_row(row_index, row.iter());
        }

        Self {
            rows,
            row_edges,
            column_edges,
            edge_columns,
            toolbox_matrix,
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

    pub fn decode(&self, llrs: &[f32]) -> Option<(Vec<u8>, usize)> {
        self.decode_toolbox(llrs).or_else(|| self.decode_min_sum(llrs))
    }

    fn decode_toolbox(&self, llrs: &[f32]) -> Option<(Vec<u8>, usize)> {
        if llrs.len() != 174 {
            return None;
        }
        let mut decoder =
            horizontal_layered::Decoder::new(self.toolbox_matrix.clone(), Minstarapproxf32::new());
        let llrs_f64: Vec<f64> = llrs.iter().map(|value| -(*value as f64)).collect();
        match decoder.decode(&llrs_f64, MAX_ITERS) {
            Ok(output) => Some((output.codeword, output.iterations)),
            Err(_) => None,
        }
    }

    fn decode_min_sum(&self, llrs: &[f32]) -> Option<(Vec<u8>, usize)> {
        if llrs.len() != 174 {
            return None;
        }
        let edge_count = self.edge_columns.len();
        let mut q = vec![0.0f32; edge_count];
        let mut r = vec![0.0f32; edge_count];

        for (column, edges) in self.column_edges.iter().enumerate() {
            for edge in edges {
                q[*edge] = llrs[column];
            }
        }

        let hard_bits: Vec<u8> = llrs.iter().map(|llr| u8::from(*llr >= 0.0)).collect();
        if self.parity_ok(&hard_bits) {
            return Some((hard_bits, 0));
        }

        let mut hard_bits = vec![0u8; 174];
        for iteration in 0..MAX_ITERS {
            for row_edges in &self.row_edges {
                for (edge_idx, edge) in row_edges.iter().copied().enumerate() {
                    let mut sign = 1.0f32;
                    let mut minimum = f32::INFINITY;
                    for (other_idx, other_edge) in row_edges.iter().copied().enumerate() {
                        if edge_idx == other_idx {
                            continue;
                        }
                        let value = q[other_edge];
                        if value.is_sign_negative() {
                            sign = -sign;
                        }
                        minimum = minimum.min(value.abs());
                    }
                    r[edge] = sign * minimum * MIN_SUM_SCALE;
                }
            }

            for (column, edges) in self.column_edges.iter().enumerate() {
                let posterior =
                    llrs[column] + edges.iter().copied().map(|edge| r[edge]).sum::<f32>();
                hard_bits[column] = u8::from(posterior >= 0.0);
                for edge in edges {
                    let extrinsic = posterior - r[*edge];
                    q[*edge] = extrinsic;
                }
            }

            if self.parity_ok(&hard_bits) {
                return Some((hard_bits, iteration + 1));
            }
        }

        None
    }

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
