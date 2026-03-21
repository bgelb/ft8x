# Recall Gap Investigation Notes (2026-03-21)

This note captures the useful findings from the March 21 investigation round so they are not lost between experiments.

## Stable Baseline

- Best production baseline from this round remains:
  - Rust: [artifacts/results/rust-20260321T072024Z/summary.csv](/Users/bgelb/ft8-regr/artifacts/results/rust-20260321T072024Z/summary.csv)
  - WSJT-X: [artifacts/results/20260321T065809Z/summary.csv](/Users/bgelb/ft8-regr/artifacts/results/20260321T065809Z/summary.csv)
- Corpus-level gap at that baseline:
  - `medium`: Rust `381/1/37` vs WSJT-X `390/0/28`
  - `deepest`: Rust `392/1/26` vs WSJT-X `404/0/14`
- Runtime at that baseline is already competitive:
  - `medium` about `0.60s/sample`
  - `deepest` about `0.76s/sample`

## Kept Production Change

- Keep the narrowed `try_candidate` behavior in [decoder/src/decoder.rs](/Users/bgelb/ft8-regr/decoder/src/decoder.rs):
  - refine only the candidate's own coarse `(dt, freq)`
  - do not run the extra non-WSJT-X `±4 bins / ±2 frames` neighborhood sweep
- This is the change that produced the large runtime win without a large additional recall collapse.

## New Tooling Added

- `debug-standard-candidate` CLI in [decoder/src/main.rs](/Users/bgelb/ft8-regr/decoder/src/main.rs)
- `debug_candidate_truth_wav_file` export in [decoder/src/lib.rs](/Users/bgelb/ft8-regr/decoder/src/lib.rs)
- truth-seeded per-pass diagnostics in [decoder/src/decoder.rs](/Users/bgelb/ft8-regr/decoder/src/decoder.rs)
- These tools are for probing exact truth coordinates and rendered standard messages.

## Important Findings

### 1. Remaining misses are mixed search-side and downstream

- Not all remaining WSJT-X-only misses are downstream failures.
- Some misses have a very nearby coarse candidate and still fail later.
- Some misses do not have a nearby first-pass candidate at all.

### 2. Nearby-candidate, downstream-failure examples

These already have a close coarse candidate at the fast baseline and therefore should not be attacked with broad candidate-search tweaks first:

- `medium`:
  - `websdr_test11 | K3ZK IK2ZDT RR73`
  - `websdr_test12 | K1GUY NA4RR EM61`
  - `websdr_test12 | KE0EE N1RDN R-18`
  - `websdr_test6 | CQ IK2YCW JN55`
  - `websdr_test7 | EA8PP JH0INP PM96`
- `deepest`:
  - `websdr_test1 | CQ EA1HTF IN52`
  - `websdr_test1 | EY8MM YB1BML 73`
  - `websdr_test10 | PY1NMG LU1CFU -04`
  - `websdr_test11 | K3ZK IK2ZDT RR73`
  - `websdr_test11 | LZ3CQ K8JDC R-08`
  - `websdr_test12 | KE0EE N1RDN R-18`
  - `websdr_test4 | CQ HF19NY`
  - `websdr_test4 | DL2HRE SP2EWQ +06`
  - `websdr_test8 | DL8FBD LZ2KV -16`
  - `websdr_test9 | K3ZK IK2ZDT RR73`

### 3. Search-side examples

These did not have a nearby coarse candidate at the fast baseline and probably need `sync8`/candidate-admission work:

- `medium`:
  - `191111_110630 | CQ NT6Q DM13`
  - `191111_110630 | <...> OT4B R-14`
  - `websdr_test1 | EY8MM YB1BML 73`
  - `websdr_test1 | R2ATW IZ0VLL -16`
  - `websdr_test3 | <...> YP4XMAS`
- `deepest`:
  - `191111_110700 | <...> DF1XG JO53`
  - `websdr_test3 | <...> YP4XMAS`
  - `websdr_test4 | UT7IS SV8EUB -12`

### 4. One concrete OSD clue

- `websdr_test12 | K1GUY NA4RR EM61` is especially useful.
- On the truth-seeded probe, the best regular pass is relatively close to truth and the message decodes only when using the stronger debug `regular-osd3-4` path.
- This strongly suggests at least part of the remaining gap is in BP/OSD fidelity rather than candidate search.

### 5. Some seeded misses are still far from truth

- `websdr_test11 | K3ZK IK2ZDT RR73`
- `websdr_test12 | KE0EE N1RDN R-18`
- `websdr_test7 | CU2DX R2DQA KO96`

These remain far enough from truth at exact coordinates that stronger OSD alone is unlikely to fix them. They point back toward refinement, symbol extraction, or bit-metric fidelity.

## Experiments Tried And Rejected

- Changing `deepest` OSD scheduling to match guessed WSJT-X behavior (`2/2` or `0/2`) did not improve corpus recall and worsened it in measured runs.
- Reworking `sync8` collection to stage a WSJT-X-like 1000-entry pre-candidate pool before de-duplication did not improve recall in the current implementation.
- Both of those experiments should be treated as negative results unless revisited with a more faithful surrounding implementation.

## Highest-Value Next Steps

1. Improve downstream decode fidelity for near-candidate misses before doing more broad search tuning.
2. Inspect BP/OSD behavior against the exact WSJT-X `decode174_91` / `osd174_91` flow, especially for cases like `K1GUY NA4RR EM61`.
3. Inspect refinement and bitmetric generation for truth-seeded-but-still-failing misses like `K3ZK IK2ZDT RR73`, `KE0EE N1RDN R-18`, and `CU2DX R2DQA KO96`.
4. Only after that, revisit search-side misses with tighter instrumentation around candidate admission, not coarse search rewrites by intuition.
