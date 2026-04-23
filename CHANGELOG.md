# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- `coerce_timestamps` public helper in `onelake_client.tables` for safely downcasting `timestamp[ns]` columns to `timestamp[us]`
- CI job that enforces `CHANGELOG.md` updates on user-facing PRs (skip with `chore` or `documentation` label)
- Code-review instructions (`.github/instructions/code-review.instructions.md`) covering changelog, docs, Rich markup, and pyarrow conventions

### Fixed

- Data preview crash on Delta tables with nanosecond-precision timestamps — `timestamp[ns]` columns are now safely downcast to `timestamp[us]`, with out-of-range values replaced by null (#19)

## [0.3.0] - 2026-04-20

### Changed

- Copy UX now uses a single `y` copy menu with four URI targets: HTTPS named, HTTPS GUID, ABFSS named, ABFSS GUID.
- Path display now uses breadcrumb format (`Workspace / Item / path`) instead of `onelake://` display strings.
- Help (`?`) now opens a full-screen overlay and footer visibility can be toggled with `Ctrl+F`.
- Vim-style navigation shortcuts (`j/k/g/G`, `h/l`) are documented and surfaced consistently.

### Fixed

- Clipboard copy now supports platform-native command paths across macOS, Windows, and Linux with graceful fallback.
- Delta history loading now uses bounded parallelism to reduce latency on high-version tables.
- Parquet fallback preview now enforces memory-safe file-size limits.
- Release publishing workflow now runs lint + unit tests before building and publishing to PyPI.
- Documentation and splash hints are synchronized with current keybindings and URI behavior.

## [0.2.0b1] - 2026-04-07

### Added

- `pip install onelake-tui` — PyPI packaging with full project metadata (classifiers, URLs, keywords)
- `pip install --pre onelake-tui` — pre-release install path for beta builds
- `--version` CLI flag (`onelake-tui --version`)
- PEP 561 `py.typed` markers for type-checker compatibility
- PyPI publish workflow via GitHub Actions (OIDC trusted publishers)

### Fixed

- Crash when highlighting schema folders (e.g. `Tables/dbo`) in the tree — now shows informative message instead
- Crash from Rust panics in the deltalake library on certain tables — Delta metadata loading now runs in a subprocess
- Username sometimes missing from status bar — added identity resolution fallback
- TUI screenshot in README documentation

## [0.1.0] - 2026-04-06

### Added

- Three-panel TUI: workspace picker → item list → DFS tree + detail preview
- Animated OneLake splash art with shimmer effect
- Live `/` search for workspace filtering
- Rich file preview: Markdown, JSON (NDJSON), CSV, Parquet (pyarrow), Avro (fastavro), syntax-highlighted code
- Delta table tabbed detail: schema, data preview, transaction history, CDF
- Schema-aware table detection for mirrored DB (`Tables/schema/table`) and lakehouse (`Tables/table`)
- Expandable table nodes — browse raw `_delta_log/` and parquet files
- Human-readable `onelake://` paths throughout the UI
- Multi-environment support via `--env` flag (PROD, MSIT, DXT, DAILY)
- 3-line status bar with keyboard shortcuts
- Standalone async `onelake_client` library covering Fabric REST, OneLake DFS, and Delta/Iceberg metadata APIs
- Dual-scope `DefaultAzureCredential` auth (Fabric + DFS tokens)
- CI workflow with Python 3.11/3.12/3.13 matrix, ruff lint/format, pytest
