# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.2.0b1] - 2026-04-07

### Added

- `pip install onelake-tui` — PyPI packaging with full project metadata (classifiers, URLs, keywords)
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
