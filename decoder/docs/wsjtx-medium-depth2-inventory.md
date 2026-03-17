# WSJT-X 2.7.0 Medium (Depth 2) Inventory

This is the feature inventory for the exact benchmark path used by this repo when it runs the `medium` profile against WSJT-X 2.7.0.

The harness definition is:

- `medium` in [config/sources.json](/Users/bgelb/ft8-regr/config/sources.json) maps to `depth = 2`.
- The harness invokes `jt9` as `jt9 -8 -d 2 ... <sample.wav>` in [src/ft8_regr/core.py](/Users/bgelb/ft8-regr/src/ft8_regr/core.py).

## Harness-Level Settings

For file-based FT8 runs through `jt9`, the relevant defaults come from [jt9.f90](/Users/bgelb/wsjtx-2.7.0/wsjtx/lib/jt9.f90):

- FT8 mode: `-8`
- Depth: `-d 2`
- `lft8apon = .true.`
- `lapcqonly = .false.`
- `napwid = 75`
- `n2pass = 2`
- `nQSOProg = 0` unless the CLI passes `-Q`
- default `mycall = 'K1ABC'`, `hiscall = 'W9XYZ'` unless overridden
- default `nrxfreq = 1500`, `flow = 200`, `fhigh = 4000`

Important consequence: the medium benchmark path does use FT8 AP machinery in `ft8b`, even though the harness processes one WAV file at a time.

## FT8 Medium Decode Stack

The medium/depth-2 path uses the following major mechanisms:

1. Early file-input decode schedule in [jt9.f90](/Users/bgelb/wsjtx-2.7.0/wsjtx/lib/jt9.f90)
   - For file input in FT8 mode, `jt9` explicitly runs the decoder at `nzhsym = 41`, then `47`, then `50`.
   - This is not just a single full-file decode.

2. Early-decode bookkeeping and subtraction in [ft8_decode.f90](/Users/bgelb/wsjtx-2.7.0/wsjtx/lib/ft8_decode.f90)
   - `MAXCAND = 600`
   - `MAX_EARLY = 200`
   - At `nzhsym = 41`, it stores early decodes.
   - At `nzhsym = 47`, it subtracts stored early decodes from the partial waveform.
   - At `nzhsym = 50`, it subtracts remaining early decodes from the full waveform.

3. Outer decode passes in [ft8_decode.f90](/Users/bgelb/wsjtx-2.7.0/wsjtx/lib/ft8_decode.f90)
   - `npass = 3` for `ndepth = 2`
   - `syncmin = 1.6` for depth 2
   - `lsubtract = .true.` on all passes
   - pass 3 only runs if pass 2 added more decodes

4. Coarse candidate search in [sync8.f90](/Users/bgelb/wsjtx-2.7.0/wsjtx/lib/ft8/sync8.f90)
   - 3840-point FFT per symbol window
   - quarter-symbol stepping (`NSTEP = NSPS/4`)
   - 3.125 Hz oversampled frequency bins
   - ratio-style Costas scoring (`sync_abc`, `sync_bc`)
   - 40th-percentile baseline normalization
   - near-duplicate suppression at roughly `4 Hz` and `0.04 s`

5. Candidate refinement front-end in [ft8b.f90](/Users/bgelb/wsjtx-2.7.0/wsjtx/lib/ft8/ft8b.f90), [ft8_downsample.f90](/Users/bgelb/wsjtx-2.7.0/wsjtx/lib/ft8/ft8_downsample.f90), and [sync8d.f90](/Users/bgelb/wsjtx-2.7.0/wsjtx/lib/ft8/sync8d.f90)
   - 200 Hz complex baseband
   - 32 samples/symbol
   - coarse time peaking around the candidate
   - residual frequency peaking over `-2.5 Hz .. +2.5 Hz` in `0.5 Hz` steps
   - final time peaking over a smaller window

6. Bit-metric generation in [ft8b.f90](/Users/bgelb/wsjtx-2.7.0/wsjtx/lib/ft8/ft8b.f90)
   - four passes of regular soft metrics:
   - `nsym = 1`
   - `nsym = 2`
   - `nsym = 3`
   - bit-by-bit normalized `nsym = 1`

7. AP inside `ft8b` in [ft8b.f90](/Users/bgelb/wsjtx-2.7.0/wsjtx/lib/ft8/ft8b.f90)
   - For `nQSOProgress = 0`, the AP pass list is `(1, 2, 0, 0)`
   - In practice that means:
   - AP pass type 1: `CQ ??? ???`
   - AP pass type 2: `MyCall ??? ???`
   - Since the harness does not override `mycall`, the default `K1ABC` is the `MyCall` used on this path

8. LDPC + OSD in [decode174_91.f90](/Users/bgelb/wsjtx-2.7.0/wsjtx/lib/ft8/decode174_91.f90) and [osd174_91.f90](/Users/bgelb/wsjtx-2.7.0/wsjtx/lib/ft8/osd174_91.f90)
   - BP max iterations: `30`
   - early stop when parity-check progress stalls
   - for `depth = 2`:
   - `maxosd = 0`, so OSD runs once on the original channel LLRs
   - `norder = 2`
   - in `osd174_91`, `ndeep = 2` means:
   - `nord = 1`
   - `npre1 = 1`
   - `npre2 = 0`
   - `nt = 40`
   - `ntheta = 10`

9. Waveform subtraction in [subtractft8.f90](/Users/bgelb/wsjtx-2.7.0/wsjtx/lib/ft8/subtractft8.f90)
   - subtracts using a smooth complex envelope, not a single constant per symbol

10. Validity and false-decode gating in [ft8b.f90](/Users/bgelb/wsjtx-2.7.0/wsjtx/lib/ft8/ft8b.f90)
   - rejects invalid `(i3, n3)` combinations
   - rejects all-zero codeword
   - applies low-sync / low-SNR bail-out logic

## Not Material For This Harness Comparison

- The `ft8_a7` path in [ft8_decode.f90](/Users/bgelb/wsjtx-2.7.0/wsjtx/lib/ft8_decode.f90) depends on prior-period saved decodes in the same process. The benchmark harness launches a fresh `jt9` process per sample, so this path should not materially contribute here.
- Depth-3 behavior such as `maxosd = 2` near `nfqso` is not part of medium/depth-2.

## Current Rust Alignment

Currently matched or close:

- 200 Hz complex-baseband refinement
- `sync8`-style coarse candidate collection
- waveform-domain subtraction
- multi-pass outer search
- CQ AP

Current mismatches versus WSJT-X medium:

1. Early `41/47/50` file-input schedule is missing.
2. OSD is not medium-faithful yet.
   - The current Rust OSD is stronger and structurally different from WSJT-X medium.
   - This is currently "cheating" relative to the requested constraint.
3. AP coverage is incomplete.
   - WSJT-X medium also runs `MyCall ??? ???` AP for `nQSOProgress = 0`.
4. Some false-decode gates and message-screening behavior are still simplified.

## Immediate Work Plan

To satisfy the "no cheating" constraint, the Rust decoder should be brought in line in this order:

1. Replace the current Rust OSD with a medium-faithful `maxosd = 0`, `norder = 2`, `ndeep = 2` path.
2. Implement the `41/47/50` early decode and subtraction schedule.
3. Add the remaining medium AP pass type(s) that are actually active on this harness path.
4. Only then tune constants and implementation details within those same mechanisms.
