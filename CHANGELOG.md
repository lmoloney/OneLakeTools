# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Fixed

- Token acquisition no longer blocks the async event loop — `FabricClient` and `DfsClient` now use async header methods that wrap blocking credential calls in `asyncio.to_thread()`
- 401 responses now trigger token cache invalidation via `on_auth_error` callbacks wired through `request_with_retry` and pagination helpers — previously this recovery path was dead code
- `read_file(max_bytes=...)` now enforces the size limit via a HEAD request before downloading the body, preventing large files from being fully buffered in memory
- `read_file_stream` now uses the shared `raise_for_status` error mapping — 403 correctly raises `PermissionDeniedError` (was `AuthenticationError`), 429 raises `RateLimitError` (was `ApiError`)
- Tree child-loading no longer cancels across unrelated nodes — removed `exclusive=True` from the `load_children` work group and added staleness guards to prevent empty folder nodes after rapid expansion
- `IcebergTableReader` now uses environment-aware catalog and blob host URLs instead of hardcoded PROD endpoints — non-PROD rings (MSIT, DXT, DAILY) now hit the correct Iceberg endpoints
- Updated SECURITY.md supported versions table (0.2.x → 0.4.x)
- Data preview on Windows no longer fails with `'No time zone found with key UTC'` — added `tzdata` dependency for Windows where the OS lacks a system timezone database
- CDF preview no longer fails on tables where Change Data Feed was enabled after creation — auto-retries with the latest version and offers a "Search Earlier Versions" button to discover the full CDF-available range

### Added

- `fabric_headers_async()` and `dfs_headers_async()` methods on `OneLakeAuth` for non-blocking token acquisition
- `max_items` parameter on `list_workspaces()` and `list_items()` to cap API results for large tenants
- `iceberg_catalog_url` and `iceberg_blob_host` fields on `FabricEnvironment` with per-ring values
- History tab now shows a **Configuration** column with table property changes (e.g. `delta.enableChangeDataFeed=true`) extracted from `metaData` actions in the Delta log

### Changed

- Workspace and item lookups in TUI widgets now use O(1) dict indexes instead of O(n) linear scans

## [0.4.0] - 2026-05-05

### Fixed

- Delta table metadata via friendly-name paths (workspace name + `DisplayName.Type`) now works with service principal auth — `DeltaTableReader` auto-resolves friendly names to GUIDs via Fabric REST API before calling delta-rs, which uses the Azure Blob API protocol that doesn't resolve OneLake friendly names for SP tokens (#32)

### Added

- Comprehensive fixture-based test suite — 138 new tests exercising real Delta log parsing, Parquet schema introspection, path encoding, tree widget edge cases, and snapshot regression detection
- Committed test fixtures: 5 Delta tables (`basic_table`, `partitioned_table`, `column_mapping_v2`, `cdf_enabled`, `unicode_paths`) and 3 Parquet files (`all_types`, `nested_structs`, `dictionary_encoded`)
- `syrupy` snapshot tests for Delta metadata and Parquet schema regression detection
- Registered pytest markers (`integration`, `iceberg`, `slow`) in `pyproject.toml`
- Integration test CI workflow (`integration.yml`) — runs on push to `main`, nightly, and manual dispatch with OIDC-authenticated Fabric access
- Expanded live integration tests from 6 to 11 — workspace filtering, item field validation, DFS subdirectory browsing, lakehouse properties, Delta column assertions
- `coerce_timestamps` public helper in `onelake_client.tables` for safely downcasting `timestamp[ns]` columns to `timestamp[us]`
- CI job that enforces `CHANGELOG.md` updates on user-facing PRs (skip with `chore` or `documentation` label)
- Code-review instructions (`.github/instructions/code-review.instructions.md`) covering changelog, docs, Rich markup, and pyarrow conventions
- Manifest-driven integration test configuration (`fabric-test-env.json`) replacing environment-variable-only approach — auto-detected from `tests/integration/`, with env-var and XDG fallback
- 5 new integration test files: DFS browsing, Delta tables (10 tables), file operations, schema lakehouse, warehouse + Iceberg
- 3 new TUI graphical test files: navigation flow, file previews, table metadata views
- Enhanced format parser tests covering CSV, JSON, Markdown, and Parquet edge cases
- Deletion vector table provisioned in test environment for protocol-level coverage
- Both GUID and friendly-name DFS addressing modes tested across integration suite
- `nested_types.parquet` test fixture uploaded with struct, array, and map columns

### Fixed

- `Column.metadata` type changed from `dict[str, str]` to `dict[str, Any]` — Delta column mapping stores non-string metadata values (e.g. `delta.columnMapping.id` is an int) (#25)
- Data preview crash on Delta tables with nanosecond-precision timestamps — `timestamp[ns]` columns are now safely downcast to `timestamp[us]` with sub-microsecond precision truncated; timestamps outside year 0001–9999 are defensively nullified (#19)

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
