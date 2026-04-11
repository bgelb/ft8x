# Decoder SNR Notes

This decoder reports mode-specific SNR values that are intended to track the stock WSJT-X / `wsjt-x_improved` display value, not a single cross-mode physical SNR definition.

The implementation target is practical parity:

- FT8, FT4, and FT2 should stay within about `+/- 1 dB` of the stock decoder on deterministic synthesized examples.
- CI uses a small weak-signal synthesized regression set in [data/snr_regressions.json](/Users/bgelb/ft8-regr-snr/decoder/data/snr_regressions.json).
- The CI set only includes cases that both stock and the Rust decoder successfully decode.
- If a future weak example is stock-decoded but not Rust-decoded, keep it as a debug seed rather than forcing it into the CI SNR parity gate.

## FT8

FT8 reported SNR follows the stock file-decoder `xsnr2` path.

The steps are:

1. Build the FT8 spectrum baseline from the full input window.
2. Downsample the accepted candidate to baseband at the candidate frequency.
3. Reconstruct the decoded FT8 tone sequence from the decoded bits.
4. Measure the decoded-signal energy from the reconstructed tone positions.
5. Convert that signal energy and the baseline estimate into the displayed dB value.

### FT8 baseline

The baseline matches the stock `get_spectrum_baseline` / `baseline` path:

- Use `3840`-sample FFTs with `50%` overlap.
- Apply the stock Nuttall window normalization, including the `1/300` amplitude scale.
- Accumulate power spectra over the full decode window.
- Convert the selected frequency span to dB.
- Fit the lower envelope with the same 10-segment, degree-4 polynomial baseline used by WSJT-X.
- Apply the stock `+0.65 dB` offset to the fitted baseline.

The Rust helper for this is `ft8_spectrum_baseline_db(...)` in [search.rs](/Users/bgelb/ft8-regr-snr/decoder/src/decoder/search.rs).

### FT8 displayed SNR

After a successful decode:

- Recreate the channel symbols from the decoded codeword bits.
- Extract the complex tone amplitudes for all `79` FT8 symbols.
- Sum the power at the decoded tone for each symbol:

`xsig = sum |tone(decoded_symbol)|^2`

- Convert the fitted baseline at the candidate bin back to linear power:

`xbase = 10^((baseline_db - 40) / 10)`

- Apply the stock file-decoder formula:

`arg = xsig / xbase / 3.0e6 - 1`

`snr_db = 10*log10(max(arg, 0.001)) - 27`

- Clamp to the stock floor of `-24 dB`.

This logic lives in `ft8_reported_snr_db(...)` in [session.rs](/Users/bgelb/ft8-regr-snr/decoder/src/decoder/session.rs).

## FT4

FT4 reported SNR is not derived from the final refined-symbol metrics.

It follows the stock FT4 display path:

1. Run the coarse FT4 candidate search.
2. Take the candidate peak score from the coarse search.
3. Convert that coarse score to the displayed SNR.

### FT4 coarse candidate score

The FT4 coarse search follows the stock `getcandidates4` shape:

- Use `2304`-sample FFTs stepped every `576` samples.
- Apply the stock Nuttall window and `1/300` scale.
- Average symbol spectra across the FT4 search window.
- Smooth with the same 15-bin moving average.
- Divide by the FT4 polynomial baseline estimate.
- Detect local maxima above threshold and interpolate the peak.

The resulting peak value is the `candidate(2)` quantity in stock FT4.

The Rust search implementation is in [search.rs](/Users/bgelb/ft8-regr-snr/decoder/src/decoder/search.rs).

### FT4 displayed SNR

For a successful decode, the displayed SNR uses the stock conversion:

`snr_linear = candidate_score - 1`

`snr_db = 10*log10(snr_linear) - 14.8`

- If `candidate_score <= 1`, use the stock floor.
- Clamp to the stock minimum of `-21 dB`.

This logic lives in `ft4_reported_snr_db(...)` in [session.rs](/Users/bgelb/ft8-regr-snr/decoder/src/decoder/session.rs).

Important detail: FT4 still needs stock-like normalization of the downsampled complex baseband during refinement so that decode behavior lines up with stock, but that normalization is separate from the displayed-SNR formula itself. The displayed FT4 SNR comes from the coarse-search peak, not the final bitmetrics stage.

## FT2

FT2 uses a much rougher reported-SNR estimate than FT8 or FT4.

It follows the stock FT2 display rule directly:

`snr_db = 10*log10(best_sync^2) - 115`

where `best_sync` is the winning FT2 sync/correlation metric for the accepted candidate.

This is intentionally a rough estimator:

- It is derived from the sync score, not from a fitted baseline model.
- It does not behave like the FT8 or FT4 display scale.
- On deterministic single-signal synthesized examples, it tends to stay optimistic until the decode is close to failing.

The Rust FT2 path already matches this stock rule in [ft2.rs](/Users/bgelb/ft8-regr-snr/decoder/src/decoder/ft2.rs).

## Regression Coverage

The SNR regression gate is implemented in [snr_regressions.rs](/Users/bgelb/ft8-regr-snr/decoder/tests/snr_regressions.rs).

The test flow is:

1. Read the checked-in case list from [data/snr_regressions.json](/Users/bgelb/ft8-regr-snr/decoder/data/snr_regressions.json).
2. Synthesize each message in memory with the normal encoder path.
3. Scale the signal by the checked-in gain.
4. Add deterministic pseudo-Gaussian noise from a fixed seed.
5. Decode with the Rust decoder.
6. Compare the reported SNR against the checked-in stock value with a `+/- 1 dB` tolerance.

The current checked-in set is intentionally small and weak-signal focused:

- FT8 includes examples near `-1`, `-7`, and `-19 dB`.
- FT4 includes examples near `0`, `-10`, and `-17 dB`.
- FT2 includes examples near `+1`, `-4`, and `-6 dB`.

FT8 and FT4 currently have stable synthesized cases near `-20 dB` that both decoders handle. FT2 did not produce equally stable stock-decoded single-signal cases that low in the current sweep, so the checked-in FT2 weak range stops earlier. If better FT2 weak cases are found later and both decoders reliably decode them, they should be added to the same manifest.

To regenerate local WAVs for stock re-measurement, run the ignored helper test in [snr_regressions.rs](/Users/bgelb/ft8-regr-snr/decoder/tests/snr_regressions.rs). It writes the current synthesized case set to `decoder/target/snr-regression-wavs/`.
