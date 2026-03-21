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

## Investigation Notes

- Prefer corpus-level A/B runs over intuition. Several plausible-looking changes were neutral or harmful, and the harness results made that obvious quickly.
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
