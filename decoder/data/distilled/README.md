# Distilled Multi-Mode Regression Sets

These fixtures preserve the historically failing FT4 and FT2 mixed-scene samples that were used
to close WSJT-X parity, plus a matching set of control samples from the same larger corpora.

- `ft4-mixed-medium/`
- `ft4-mixed-deepest/`
- `ft2-mixed-medium/`
- `ft2-mixed-deepest/`

Each directory contains:

- `manifest.json`: expected decode set for each WAV
- `wav/`: copied regression samples

These sets are intentionally small enough to run as routine Rust tests. The larger 5k/20k parity
corpora remain manual validation assets under `/private/tmp` and are not part of the checked-in
test suite.
