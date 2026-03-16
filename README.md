# FT8 WSJT-X Regression Prototype

This repository now contains a prototype benchmark harness for historical FT8 decoding across official WSJT-X macOS releases.

What it does:

- discovers official WSJT-X releases from SourceForge and targets the official macOS DMGs
- downloads and extracts `wsjtx.app`, then runs `jt9 -8`
- records the actual `jt9` binary architecture for each release
- records whether each run is native or Rosetta-translated on Apple Silicon
- benchmarks all configured decode-depth profiles:
  - `quick` = `-d 1`
  - `medium` = `-d 2`
  - `deepest` = `-d 3`
- discovers FT8 sample corpora from GitHub
- treats sample sets as either:
  - `scored`: `.wav` plus companion `.txt` truth files
  - `unscored`: `.wav` only
- generates JSON, CSV, and a static HTML report

## Included sources

- Releases: SourceForge `wsjt` file index
- Scored samples: `kgoba/ft8_lib` `test/wav`
- Unscored samples: `d9394/ft8_test_wave`

## Usage

From the repo root:

```bash
PYTHONPATH=src python3 -m ft8_regr.cli discover
```

Download a subset of releases:

```bash
PYTHONPATH=src python3 -m ft8_regr.cli sync-releases --versions 1.8.0 2.7.0
```

Download a small sample subset:

```bash
PYTHONPATH=src python3 -m ft8_regr.cli sync-samples --datasets kgoba-ft8-lib d9394-ft8-test-wave --sample-limit 3
```

Run the prototype benchmark and emit the report:

```bash
PYTHONPATH=src python3 -m ft8_regr.cli run \
  --versions 1.8.0 2.7.0 \
  --datasets kgoba-ft8-lib d9394-ft8-test-wave \
  --sample-limit 3
```

Re-render the latest report:

```bash
PYTHONPATH=src python3 -m ft8_regr.cli report
```

The main outputs land under:

- `artifacts/discovery/`
- `artifacts/releases/`
- `artifacts/samples/`
- `artifacts/results/latest/`
- `artifacts/reports/latest/index.html`

## Current scoring rule

The prototype uses `unique-message` matching for scored datasets. That is intentionally conservative for a first pass because:

- SNR and DT vary slightly between decoders
- reported audio frequencies can differ by 1-2 Hz
- the `kgoba` truth files appear to be produced by a different decoder implementation

This makes the first report useful for broad improvement/regression tracking, but not yet for exact line-by-line parity checks.

## Apple Silicon note

This development machine is `arm64`. The official macOS releases inspected so far, including `1.8.0` and `2.7.0`, ship an `x86_64` `jt9` binary inside `wsjtx.app`. On an Apple Silicon Mac with Rosetta installed, those binaries still execute, so the framework now records:

- host architecture
- release binary architecture
- execution mode: `native`, `rosetta`, or `incompatible`

## Known gaps

- release verification is lightweight and happens at download time
- DMG extraction currently copies the full app bundle for each release
- scoring does not yet support fuzzy frequency matching or duplicate-message handling
- only official WSJT-X is included for now
