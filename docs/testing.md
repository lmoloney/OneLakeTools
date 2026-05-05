# Testing Guide

Comprehensive testing overview for OneLakeTools — 762 tests across 32 files.

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
| Unit tests | 612 | 26 |
| Integration tests | 150 | 8 |
| Snapshot tests | 8 | (included in unit) |
| **Total** | **762** | **34** |

## Running Tests

```bash
cd TUI
uv sync --extra dev

# Unit tests only (fast, no credentials needed)
uv run pytest

# Integration tests (requires az login + populated Fabric workspace)
uv run pytest tests/integration/ -v

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
| `olt_lakehouse_simple` | Lakehouse | 10 Delta tables + 7 files |
| `olt_lakehouse_schema` | Lakehouse | 4 tables across 2 schemas |
| `olt-warehouse` | Warehouse | 2 tables + Iceberg |
| 2 mirrored DBs | Mirrored DB | Not yet ready for testing |

### Delta Tables in `olt_lakehouse_simple`

| Table | Features Covered |
|-------|-----------------|
| `basic_table` | Simple schema, single commit |
| `multi_commit_table` | Multiple commits, version history |
| `unicode_table` | Unicode column names and values |
| `all_types_table` | All supported Delta data types |
| `partitioned_table` | Partition columns |
| `cdf_enabled_table` | Change Data Feed (CDF) |
| `optimized_table` | OPTIMIZE + ZORDER |
| `checkpoint_table` | Checkpoint files in delta log |
| `timestamp_ntz_table` | TIMESTAMP_NTZ type |
| `deletion_vector_table` | Deletion vectors (protocol feature) |

### Integration Test Coverage

- **Workspace/item listing** — enumerate workspaces, filter by name, validate item fields
- **DFS browsing** — list paths, expand subdirectories, verify file/folder metadata
- **File operations** — read file content, get file properties (size, timestamps, content type)
- **Delta metadata** — schema, version, partition info, file listing for all 10 tables
- **Schema folders** — two-level `Tables/SCHEMA/table` layout in `olt_lakehouse_schema`
- **Warehouse + Iceberg** — table listing and Iceberg metadata in `olt-warehouse`
- **CDF read** — change data feed extraction via `read_cdf()`
- **Deletion vectors** — verify tables with deletion vectors load correctly

### DFS Addressing Modes

Both GUID-based and friendly-name-based addressing are tested:

- **GUID mode:** `GET /{workspaceGUID}?resource=filesystem&directory={itemGUID}/...`
- **Friendly-name mode:** `GET /{workspaceName}?resource=filesystem&directory={itemName}.{itemType}/...`

> **Important:** You cannot mix modes — a GUID workspace with a friendly-name item path (or vice versa) returns errors.

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
