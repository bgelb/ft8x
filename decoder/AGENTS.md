# FT8 Decoder Mission

- Build a clean-room FT8 decoder in Rust under `decoder/`.
- Keep the design library-first, with CLI and file-based harnesses around the library API.
- Avoid porting or translating WSJT-X or other implementations directly into Rust.
- Prefer first-principles work based on published FT8 protocol and decoding descriptions.
- Use the existing regression datasets and harness to benchmark against WSJT-X 2.7.0.
- The target is not just functional decoding: it must approach WSJT-X 2.7.0 in decode performance and runtime.
- Synthetic waveform generators are acceptable and encouraged for bringup, but reference validation should include WSJT-X 2.7.0 decoding those generated waveforms.
- When adding dependencies, prefer mature, high-quality crates with a performance benefit.
- Selective WSJT-X source inspection is allowed when it unblocks progress, but use it to understand detector structure and tradeoffs, not as a line-by-line template.
- Treat WSJT-X constants as hypotheses, not truth. Any adopted threshold, scale factor, search range, or normalization constant should have one of:
  - a protocol or signal-model explanation,
  - a clean empirical validation against synthetic and corpus data,
  - or a named/tunable configuration point in the Rust implementation.
- Treat corpus "truth" as a high-quality decoder consensus label set, not ground truth from the air interface. When Rust produces an apparent FP that is stable, plausible, or supported by multiple decoders, investigate it as a possible real decode rather than assuming the Rust path regressed.

## Profile Contract

- `medium` and `deepest` are WSJT-X compatibility targets, not open-ended optimization profiles.
- For those profiles, prefer feature-for-feature alignment with the corresponding WSJT-X decode path before adding custom search, AP, or pruning behavior.
- If a change would make `medium` or `deepest` materially diverge from WSJT-X behavior, either:
  - gate it behind a new profile, or
  - make the divergence explicit and temporary, with a clear plan to restore compatibility.
- Use `unlimited` for non-WSJT-X experiments, aggressive recall tricks, or any strategy whose purpose is to beat WSJT-X rather than match its feature set.
- When benchmarking profile changes, compare like-for-like:
  - `medium` against WSJT-X medium semantics
  - `deepest` against WSJT-X deepest semantics
  - `unlimited` against the best available reference, without compatibility constraints

## Decoder Code Contract

### Do

- Route geometry, layout, and timing semantics through `ModeSpec`, `FrameGeometry`, and shared layout helpers instead of scattering literals through decoder code.
- Keep internal indexing zero-based. If an external paper or reference algorithm is 1-based, translate it once at the boundary and keep the rest of the code zero-based.
- Confine raw slice/index math to small named helpers or hot numeric kernels. Orchestration code should work in terms of named fields, windows, and typed geometry.
- Add derived constants or helpers when a protocol shape matters. Examples: symbol-group starts, bit-field ranges, valid baseband windows, taper reach, and sync spans.
- Prefer named conversion helpers over arrays or maps as the public face of an algorithm. Tables are fine internally, but callers should use `gray_*`, `alphabet_*`, `read_bit_field`, or similar helpers.
- Preserve computation order in numeric kernels unless parity and performance are both revalidated.
- Run `cargo test`, `cargo build --release`, and the full `medium` regression after each material decoder cleanup.

### Don't

- Don’t add direct `FT8_` references to shared decoder submodules. FT8-specific constants belong in the FT8 mode definition or FT8 wrapper layer.
- Don’t add raw FT8 timing arithmetic like `dt + 0.5`, `start - 0.5`, or coarse-lag half-step math outside mode/helper code.
- Don’t duplicate message, codeword, or channel-symbol layout logic across encode, decode, and message-render paths.
- Don’t rewrite hot DSP, LDPC, subtraction, or FFT loops into iterator-heavy forms just for style. Keep hot loops explicit when that is the clearest and safest representation of the computation.

## Future Modes

- Future FT4 or FT2 support should start by adding a new mode spec, mode tuning, and wrapper entrypoints.
- Shared decoder kernels should remain mode-parameterized and must not assume FT8 symbol counts, stage counts, Costas block locations, or hardcoded bit offsets.
- If a future mode needs new machinery, add it behind the mode boundary rather than re-entangling shared decoder modules with FT8-specific assumptions.

## Investigation Notes

- Prefer corpus-level A/B runs over intuition. Several plausible-looking changes were neutral or harmful, and the harness results made that obvious quickly.
- Optimize primarily for recall against the labeled corpus and WSJT-X profile peers, but interpret FP deltas carefully. A lower FP count is good, yet a higher FP count is not automatically bad if the extra decodes look physically plausible or repeatedly show up across decoders/runs.
- After any structural decoder change, inspect worst samples individually and determine whether misses are absent from the candidate list or present-but-failing downstream. That split has been the fastest way to localize the real bottleneck.
- Compare against the exact WSJT-X path being targeted, not a guessed profile. For FT8 this matters because `quick`, `medium`, and `deepest` differ materially, and single-file `jt9` runs still enable more machinery than they first appear to.
- When borrowing ideas from WSJT-X, inspect the whole local mechanism around them. Reading only one routine in isolation led to a bad early `sync8` port; reading `sync8`, `ft8_decode`, `ft8b`, `sync8d`, and `ft8_downsample` together was much more effective.
- Candidate-budget and subtraction changes can produce large recall jumps without changing message priors. Use those before reaching for AP or other rescue-style mechanisms.
- If a missed truth already appears as a strong coarse candidate, stop tuning candidate search and move deeper into refinement, soft metrics, or LDPC/OSD.
- Keep synthetic waveform validation in place, but do not trust it as a proxy for corpus readiness. The corpus failures have mostly been dense-scene weak-signal interactions, not legality of generated signals.
- For stubborn misses, probe the exact truth `dt/freq` through a dedicated candidate-debug path before changing algorithms. That distinguishes “candidate exists but the decoder cannot recover it” from “the search/refinement handoff is dropping it.”
- When a miss decodes at its exact truth coordinates but not in the normal run, inspect boundary handling first. This directly exposed the negative-`dt` coarse-candidate rejection bug, which was worth a large recall jump on the corpus.
- After reaching the mid-90% recall range, run the truth-seeded debug probe across all remaining false negatives and classify them explicitly:
  - `seeded-hit`: coarse search / candidate admission problem
  - `seeded-none`: downstream extraction or BP/OSD fidelity problem
  This quickly showed that only 1 of the remaining 18 truth misses was an easy search-side recovery, and stopped several low-value search tweaks.
