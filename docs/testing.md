# Testing Guide

Comprehensive testing overview for OneLakeTools — 653 tests across 32 files.

## Test Architecture

OneLakeTools uses three test layers:

| Layer | Location | Fabric needed? | Speed |
|-------|----------|----------------|-------|
| **Unit** | `tests/` | No — fully mocked | ~60s |
| **Fixture** | `tests/` (committed Delta/Parquet) | No — real file formats, no network | Included in unit |
| **Integration** | `tests/integration/` | Yes — live workspace | ~5min |

**Snapshot regression** (syrupy) is used within the unit layer to detect unintended changes to Delta metadata and Parquet schema rendering.

### Test Counts

| Category | Tests | Files |
|----------|-------|-------|
| Unit tests | 545 | 26 |
| Integration tests | 108 | 6 |
| Snapshot tests | 8 | (included in unit) |
| **Total** | **653** | **32** |

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

Honest assessment of what is **not** yet tested, based on a Delta protocol audit. These represent future work priorities.

### Top Gaps

1. **Protocol version extraction** — `minReaderVersion`, `readerFeatures` are never read from the delta log. The `deltalake` library exposes `dt.protocol()` but we don't call it.

2. **Column mapping mode=id** — only `mode=name` is tested (via `column_mapping_v2` fixture). The `mode=id` path is an untested code path that could silently produce incorrect column names.

3. **Type widening** — no guard against schema corruption when a column type changes between commits (e.g., `int → long`). Reading with a stale schema could truncate data.

4. **V2 checkpoints** (UUID-named) — the checkpoint reader only handles classic `00000000000000000010.checkpoint.parquet` naming. UUID-named V2 checkpoints (`_last_checkpoint` pointing to `{uuid}.checkpoint.parquet`) are untested.

5. **Schema evolution** (ADD/RENAME/DROP COLUMN) — zero test coverage for schema changes between Delta versions. The metadata reader always uses the latest schema.

6. **Per-file statistics** — `add` action stats (min/max/nullCount per column) are never extracted or displayed. These are useful for query planning and data profiling.

7. **Liquid clustering metadata** — `clusteringColumns` in Delta table metadata are not exposed. Tables using liquid clustering show no clustering info.

### Structural Recommendation

Add a `dt.protocol()` call to the metadata extraction script and expose `reader_version`, `writer_version`, `reader_features`, and `writer_features` on `DeltaTableInfo`. This single change enables fixes for gaps 1, 2, and 4 by making protocol-level information visible to both the UI and tests.
