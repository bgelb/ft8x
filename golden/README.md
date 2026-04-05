# Golden Regression Snapshots

This directory holds static, git-tracked copies of important regression runs.

Current layout:

- `wsjtx-vs-version/`

Snapshot directories use this naming scheme:

- `<results-source-commit>__<platform>__<cpu-arch>__<os-version>__<cpu-brand>__<run-id>/`

Each snapshot contains:

- `metadata.json` with the run context and host details
- `results/` copied from `artifacts/results/<run-id>/`
- `report/` copied from `artifacts/reports/<run-id>/`

These snapshots are intended to be immutable once checked in.
