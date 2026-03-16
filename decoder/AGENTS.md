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
