# Golden Regression Snapshots

This directory holds static, git-tracked copies of important regression runs.

Browsable indexes for the saved snapshots are generated at:

- [../index.html](../index.html)
- [index.html](index.html)

Current layout:

- `wsjtx-vs-version/`

Snapshot directories use this naming scheme:

- `<results-source-commit>__<platform>__<cpu-arch>__<os-version>__<cpu-brand>__<run-id>/`

Each snapshot contains:

- `metadata.json` with the run context and host details
- `results/` copied from `artifacts/results/<run-id>/`
- `report/` copied from `artifacts/reports/<run-id>/`

These snapshots are intended to be immutable once checked in.

Create a new snapshot with:

```bash
python3 scripts/archive_golden_run.py --snapshot-kind wsjtx-vs-version
```

Useful flags:

- `results_path`: archive a specific `artifacts/results/<run-id>` directory instead of `latest`
- `--report-path`: point at a matching report directory when it is not the default sibling path
- `--golden-root`: write snapshots somewhere else for verification before copying into the repo
- `--source-ref` / `--source-commit`: record the branch and commit when the results came from a different checkout or worktree
- `--refresh-indexes-only`: rebuild `../index.html` and `index.html` from the checked-in snapshots without archiving a new run
- `--dry-run`: print the resolved snapshot path and metadata without copying files
- `--force`: replace an existing snapshot directory
