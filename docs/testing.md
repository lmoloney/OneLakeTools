# Testing Guide

Comprehensive testing overview for OneLakeTools — 787 tests across 34 files.

## Test Architecture

OneLakeTools uses three test layers:

| Layer | Location | Fabric needed? | Speed |
|-------|----------|----------------|-------|
| **Unit** | `tests/` | No — fully mocked | ~80s |
| **Fixture** | `tests/` (committed Delta/Parquet) | No — real file formats, no network | Included in unit |
| **Integration** | `tests/integration/` | Yes — live workspace | ~5min |

**Snapshot regression** (syrupy) is used within the unit layer to detect unintended changes to Delta metadata and Parquet schema rendering.

### Test Counts

| Category | Tests | Files |
|----------|-------|-------|
| Unit tests | 617 | 25 |
| Integration tests | 170 | 9 |
| Snapshot tests | 8 | (included in unit) |
| **Total** | **787** | **34** |

## Running Tests

```bash
cd TUI
uv sync --extra dev

# Unit tests only (fast, no credentials needed)
uv run pytest --ignore=tests/integration

# Integration tests (requires az login + populated Fabric workspace)
uv run pytest tests/integration/ -v

# Everything (requires az login)
uv run pytest

# Update snapshots after changing fixture data or rendering logic
uv run pytest --snapshot-update

# Run a specific test file
uv run pytest tests/test_delta_reader.py -v

# Lint and format
uv run ruff check src/ tests/
uv run ruff format src/ tests/
```

## Unit Tests

### Client Library

- **auth** — token acquisition, dual-scope management, credential caching
- **HTTP** — httpx client factory, retry logic, DFS pagination
- **DFS** — `list_paths()`, `read_file()`, `get_properties()`, `exists()` with mocked responses
- **Fabric API** — `list_workspaces()`, `list_items()`, `get_lakehouse()` with pagination
- **Delta reader** — schema parsing, version tracking, partition detection, file listing
- **Iceberg reader** — metadata extraction from Iceberg table format
- **Models** — Pydantic v2 serialization, alias mapping, edge cases

### TUI Widgets

- **Smoke tests** — app mounts without crashing
- **Navigation flow** — workspace → item → tree selection chain
- **File previews** — Markdown, JSON (including NDJSON), CSV, Parquet rendering
- **Table metadata views** — Delta schema, data preview, history tabs
- **Tree edge cases** — schema folders, empty directories, deep nesting
- **Path encoding** — special characters, unicode, URL encoding

### Format Parser Tests

Enhanced coverage for edge cases in format rendering:
- **CSV** — empty files, single-column, BOM markers, mixed delimiters
- **JSON** — NDJSON streams, deeply nested objects, unicode keys
- **Markdown** — tables, code blocks, frontmatter
- **Parquet** — nested types (struct, array, map), dictionary encoding, all primitive types

### Committed Fixtures

**Delta tables** (5):

| Fixture | Purpose |
|---------|---------|
| `basic_table` | Simple schema, single commit |
| `partitioned_table` | Partition columns and pruning |
| `column_mapping_v2` | Column mapping mode=name (v2 protocol) |
| `cdf_enabled` | Change Data Feed with `_change_type` column |
| `unicode_paths` | Non-ASCII characters in paths and values |

**Parquet files** (3):

| Fixture | Purpose |
|---------|---------|
| `all_types` | Every primitive Arrow type |
| `nested_structs` | Struct and list nesting |
| `dictionary_encoded` | Dictionary-encoded string columns |

Additional: `nested_types.parquet` with struct, array, and map columns.

### Snapshot Tests

8 syrupy snapshots capture Delta metadata and Parquet schema output. These detect regressions when:
- Delta log parsing logic changes
- Parquet schema rendering changes
- deltalake or pyarrow library upgrades alter output format

Update snapshots after intentional changes:
```bash
uv run pytest --snapshot-update
```

## Integration Tests

### Prerequisites

1. `az login` with access to the test Fabric workspace
2. `fabric-test-env.json` manifest (committed in-repo or user-local)

### Test Environment Configuration

Integration tests are configured via a JSON manifest rather than individual environment variables.

**Search order:**
1. `ONELAKE_TEST_ENV_FILE` environment variable (explicit path)
2. `tests/integration/fabric-test-env.json` (in-repo, auto-detected)
3. `~/.config/onelaketools/fabric-test-env.json` (user-local)
4. Individual environment variables (legacy fallback)

### Test Environment

**Workspace:** `OneLakeTools-Test`

**Items:**

| Item | Type | Contents |
|------|------|----------|
| `olt_lakehouse_simple` | Lakehouse | 16 Delta tables + 8 files |
| `olt_lakehouse_schema` | Lakehouse | 4 tables across 2 schemas (dbo, analytics) |
| `olt-warehouse` | Warehouse | 2 tables + Iceberg metadata |
| `olt_mirror_standard` | MirroredDatabase | 3 tables in dbo (GenericMirror) |
| `olt_mirror_cdf` | MirroredDatabase | 3 tables in hr (GenericMirror + CDF) |

### Delta Tables in `olt_lakehouse_simple`

| Table | Features Covered |
|-------|-----------------|
| `customers` | Basic schema, single commit |
| `orders` | Multi-commit (overwrite + append) |
| `données_client` | Unicode table name and column names |
| `all_data_types` | 14+ columns: int, long, float, double, decimal, string, boolean, date, timestamp, binary, struct, array, map |
| `partitioned_sales` | Partitioned by sale_year + sale_month |
| `cdf_tracking` | Change Data Feed enabled, INSERT→UPDATE→DELETE |
| `optimized_events` | OPTIMIZE + ZORDER by event_type |
| `high_version` | 16 commits, checkpoint at version 10 |
| `timestamp_edge_cases` | TIMESTAMP + TIMESTAMP_NTZ + DATE |
| `deletion_vector_demo` | Deletion vectors (protocol v3, readerFeatures) |
| `schema_evolution_add` | ADD COLUMN across commits (3→4 columns) |
| `schema_evolution_type` | Type widening property (typeWidening-preview feature) |
| `column_mapping_id` | Column mapping mode=id, RENAME COLUMN |
| `liquid_clustered` | CLUSTER BY (category, region) |
| `generated_and_checks` | CHECK constraint (age > 0) |
| `large_customers` | Large table (420 MB, 9.9M rows, 15 cols) for size-limit and streaming tests |

### Integration Test Coverage

- **Workspace/item listing** — enumerate workspaces, filter by name, validate item fields
- **DFS browsing** — root dirs, table discovery, file existence, unicode/special paths, GUID + friendly-name modes
- **File operations** — read CSV/JSON/Markdown/Parquet via DFS, get properties, error cases
- **Delta metadata** — schema, version, partitions, file listing for all 16 tables
- **Delta protocol** — reader/writer versions, features, total_rows, warnings
- **Schema folders** — two-level `Tables/SCHEMA/table` layout in `olt_lakehouse_schema`
- **Warehouse + Iceberg** — Audit/ dir, Iceberg metadata reader (namespaces, tables, schema)
- **Mirrored databases** — DFS structure, schema folders, Delta metadata for both mirrors
- **CDF read** — change data feed extraction via `read_cdf()` with arro3 table handling
- **Deletion vectors** — protocol v3, reader features, proactive warnings, schema access
- **Schema evolution** — ADD COLUMN, column mapping mode=id with RENAME
- **Clustering** — liquid clustering metadata and writer features
- **Streaming** — `read_file_stream()` chunk reassembly, small chunks, 404 errors, unicode paths
- **Size enforcement** — `read_file(max_bytes)` HEAD-first check, boundary cases, FileTooLargeError
- **Pagination limits** — `max_items` truncation on `list_workspaces()` and `list_items()`
- **Large files** — 81 MB standalone parquet (streaming, max_bytes rejection) and 420 MB / 9.9M-row Delta table (metadata, read_sample, list_files)

### DFS Addressing Modes

Both GUID-based and friendly-name-based addressing are tested:

- **GUID mode:** `GET /{workspaceGUID}?resource=filesystem&directory={itemGUID}/...`
- **Friendly-name mode:** `GET /{workspaceName}?resource=filesystem&directory={itemName}.{itemType}/...`

> **Important:** You cannot mix modes — a GUID workspace with a friendly-name item path (or vice versa) returns errors.

### Large File Test Data

`large_customers` (Delta table, 420 MB) and `large_customers.parquet` (standalone, 81 MB) are copies of data from the `Demos/precooked_standard_table` in workspace `e1b5da95-2f32-4f5a-8f45-e9634cd2affb`. They test `read_file(max_bytes)` HEAD enforcement, `read_file_stream` on real-size data, and Delta metadata loading on large tables.

> **TODO:** Replace with Spark-provisioned deterministic test data. The current approach is a shortcut copy — the data is not regenerated on each test run and could drift if the source table changes.

## Known Delta Protocol Gaps

Honest assessment of remaining gaps, based on a Delta protocol audit. Items marked ✅ have been addressed.

### Addressed

- ✅ **Protocol version extraction** — `DeltaTableInfo` now exposes `reader_version`, `writer_version`, `reader_features`, `writer_features` via `dt.protocol()`.
- ✅ **Column mapping mode=id** — tested via local fixture (`column_mapping_id`) and live Fabric table with RENAME COLUMN.
- ✅ **Liquid clustering metadata** — TUI displays "Clustered by" from properties. Live `liquid_clustered` table tested.
- ✅ **Deletion vector warnings** — proactive warning banner shown when `deletionVectors` in `reader_features`, before any error occurs.
- ✅ **Total row count** — extracted from `numRecords` in add actions, displayed in Schema tab.
- ✅ **In-commit timestamps** — history tab prefers `inCommitTimestamp` over `commitInfo.timestamp`.
- ✅ **Schema evolution (ADD COLUMN)** — live `schema_evolution_add` table and local fixture with multi-commit log.

### Remaining Gaps

1. **Type widening** — `ALTER COLUMN TYPE` is not supported on Fabric Spark, so we can't create a live test table. Local warning detection is tested but no live validation exists.

2. **V2 checkpoints** (UUID-named) — local fixture tests graceful error handling, but no live Fabric table with V2 checkpoints is available for end-to-end testing.

3. **Schema evolution (RENAME/DROP COLUMN)** — RENAME tested via `column_mapping_id`, but DROP COLUMN has no test coverage.

4. **Per-file statistics** — `numRecords` is summed for `total_rows`, but min/max/nullCount per column are not extracted or displayed.

5. **Generated columns** — `GENERATED ALWAYS AS` not supported on Fabric Spark. CHECK constraints work and are tested.

### Structural Recommendation

The core `dt.protocol()` extraction is now implemented. Remaining work is incremental:
- Extract min/max stats from add actions for data profiling
- Display individual file statistics in a future "Files" tab
- Handle DROP COLUMN via column mapping fixtures

## TUI Graphical Test Coverage

The TUI graphical tests use Textual's `run_test()` with mocked clients. Current coverage:

### Table Exploration (fully tested)
- ✅ Schema tab: columns, version, files, size, partitions, description, protocol version, row count, reader/writer features, clustering columns, proactive warnings, column metadata, schema-qualified names
- ✅ Data tab: load button, sample rendering, reader-feature fallback, network error
- ✅ History tab: commit rendering, empty log, in-commit timestamp preference, error handling
- ✅ CDF tab: presence/absence, data rendering with arro3 values, empty result, error handling
- ✅ Error states: deletion vector warning, minimum reader version fallback

### File Previews (core formats tested)
- ✅ Markdown, CSV, JSON, NDJSON, Parquet, Python, SQL, YAML, binary hex dump
- ✅ Edge cases: empty file, oversized file, network error

### Not Yet Tested (lower risk — shared code paths)
The following syntax-highlighted formats use the same `TextArea` rendering path as `.py`/`.sql`/`.yaml` and are not individually tested:
- `.avro` (separate `_preview_avro` handler — **medium risk**)
- `.xml`, `.html`, `.js`, `.ts`, `.sh`, `.toml`, `.ini`, `.txt`, `.log`
- `.r`, `.scala`, `.java`, `.cs`, `.cpp`, `.c`, `.rs`, `.go`, `.rb`

The `.avro` handler is a distinct code path (`_preview_avro`) and should be tested when Avro fixtures are available. The syntax-highlighted text formats all flow through the same `TextArea(language=lexer)` constructor — testing `.py`, `.sql`, and `.yaml` provides sufficient coverage of this path.

## Complete Test Inventory

**787 tests** across 34 files (780 passed, 7 xpassed on last full run).

### Summary by Category

| Category | Tests | Files | Description |
|----------|-------|-------|-------------|
| Client Library | 344 | 12 | Auth, HTTP, DFS, Fabric API, Delta reader, Iceberg, models, paths, error handling, edge cases |
| TUI Graphical | 113 | 6 | Widget smoke tests, navigation flow, file previews, table views, widget loading |
| Fixture / Snapshot | 108 | 3 | Delta log parsing, Parquet introspection, syrupy regression snapshots |
| Integration | 170 | 9 | Live Fabric workspace: DFS browsing, Delta tables, files, schema lakehouse, warehouse, mirrors, protocol, streaming, large files |
| Other | 52 | 4 | App interactions, detail features, copy menu, sprite |
| **Total** | **787** | **34** | |

### Unit Tests

| File | Tests | Category | Purpose |
|------|-------|----------|---------|
| [test_delta_reader.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/test_delta_reader.py) | 83 | Client Library | DeltaTableReader, protocol extraction, subprocess isolation, timestamp coercion, warnings |
| [test_delta_fixtures.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/test_delta_fixtures.py) | 72 | Fixture | Real Delta log parsing: basic, partitioned, column mapping (v2+id), CDF, unicode, schema evolution, V2 checkpoint, inline DV, reader features |
| [test_path_roundtrips.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/test_path_roundtrips.py) | 55 | Client Library | URI building, path encoding (HTTPS/ABFSS named+GUID), special characters |
| [test_http.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/test_http.py) | 53 | Client Library | Retry logic, exception mapping, pagination, client factory, on_auth_error callback |
| [test_tui_table_views.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/test_tui_table_views.py) | 29 | TUI Graphical | Table metadata tabs: schema, data, history, CDF. Regression tests for arro3 + reader version |
| [test_parquet_fixtures.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/test_parquet_fixtures.py) | 28 | Fixture | Parquet schema introspection, complex types, coerce_timestamps idempotency |
| [test_app_interactions.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/test_app_interactions.py) | 25 | Other | OneLakeApp event chains, action methods, panel switching |
| [test_auth.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/test_auth.py) | 24 | Client Library | OneLakeAuth, token caching, JWT parsing, credential types, async headers |
| [test_detail_preview.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/test_detail_preview.py) | 23 | TUI Graphical | File preview error states, CSV/JSON/Markdown edge cases, binary hex dump |
| [test_detail_features.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/test_detail_features.py) | 23 | Other | Avro preview, Delta features, DV/reader-version error detection |
| [test_tui_navigation_flow.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/test_tui_navigation_flow.py) | 22 | TUI Graphical | End-to-end workspace→item→tree→detail, keyboard nav, copy menu, refresh |
| [test_robustness.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/test_robustness.py) | 22 | Client Library | Very long names, special chars, malformed inputs |
| [test_error_handling.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/test_error_handling.py) | 22 | Client Library | Error propagation: DFS, Fabric, pagination, Delta subprocess |
| [test_edge_cases.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/test_edge_cases.py) | 22 | Client Library | Unicode handling, empty responses, null values, max_bytes enforcement |
| [test_iceberg_reader.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/test_iceberg_reader.py) | 20 | Client Library | IcebergTableReader, schema extraction, Iceberg metadata |
| [test_dfs_client.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/test_dfs_client.py) | 14 | Client Library | DfsClient: list_paths, read_file, read_file_stream, get_properties, exists, max_bytes |
| [test_tui_smoke.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/test_tui_smoke.py) | 14 | TUI Graphical | Widget mount-without-crashing, rapid navigation stability |
| [test_widget_loading.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/test_widget_loading.py) | 13 | TUI Graphical | StatusBar, workspace/item loading, environment display |
| [test_tui_file_previews.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/test_tui_file_previews.py) | 12 | TUI Graphical | Per-format preview rendering: MD, CSV, JSON, NDJSON, Parquet, Python, SQL, YAML, hex, errors |
| [test_tree_edge_cases.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/test_tree_edge_cases.py) | 11 | Client Library | OneLakeTree: table detection, schema folders, sort order, error handling |
| [test_fabric_client.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/test_fabric_client.py) | 10 | Client Library | FabricClient: list_workspaces, list_items, get_lakehouse, 401 recovery |
| [test_models.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/test_models.py) | 8 | Client Library | Pydantic models: camelCase parsing, Column.metadata typing, DeltaTableInfo |
| [test_detail_snapshots.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/test_detail_snapshots.py) | 8 | Snapshot | Syrupy regression detection for Delta metadata + Parquet schema output |
| [test_copy_menu.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/test_copy_menu.py) | 3 | Other | CopyFormatMenu modal, format selection |
| [test_sprite.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/test_sprite.py) | 1 | Other | OneLake block-art logo widget |

### Integration Tests

| File | Tests | Purpose |
|------|-------|---------|
| [test_delta_tables.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/integration/test_delta_tables.py) | 42 | Per-table metadata for all 16 tables + read_sample, list_files, read_cdf, DV details |
| [test_file_operations.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/integration/test_file_operations.py) | 22 | Read CSV/JSON/Markdown/Parquet via DFS, content validation, properties, error cases |
| [test_delta_protocol.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/integration/test_delta_protocol.py) | 21 | Protocol versions, reader/writer features, total_rows, warnings, schema evolution, column mapping, clustering |
| [test_new_features.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/integration/test_new_features.py) | 20 | Streaming, max_bytes enforcement, max_items pagination, large file (81 MB parquet + 420 MB Delta) |
| [test_live.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/integration/test_live.py) | 17 | Workspace/item listing, DFS GUID + friendly-name modes, Delta metadata |
| [test_dfs_browsing.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/integration/test_dfs_browsing.py) | 16 | Root dirs, table discovery, file existence, unicode/special paths, addressing modes |
| [test_mirrored_db.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/integration/test_mirrored_db.py) | 13 | Both mirrors: DFS structure, schema folders, Delta metadata, CDF property |
| [test_warehouse.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/integration/test_warehouse.py) | 10 | Warehouse DFS structure (Audit/), Iceberg metadata reader |
| [test_schema_lakehouse.py](https://github.com/lmoloney/OneLakeTools/blob/main/TUI/tests/integration/test_schema_lakehouse.py) | 9 | Two-level schema folder detection (dbo + analytics), delta log presence |
