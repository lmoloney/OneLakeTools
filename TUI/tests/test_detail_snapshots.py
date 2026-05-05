"""Snapshot tests — capture DeltaTableInfo and Parquet schema outputs for regression detection.

Uses syrupy for snapshot assertions. Update snapshots with:
    uv run pytest tests/test_detail_snapshots.py --snapshot-update
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
from deltalake import DeltaTable

from onelake_client.tables import coerce_timestamps
from onelake_client.tables.delta import _schema_to_columns

FIXTURES_DIR = Path(__file__).parent / "fixtures"
DELTA_DIR = FIXTURES_DIR / "delta"
PARQUET_DIR = FIXTURES_DIR / "parquet"


# ── Helpers ──────────────────────────────────────────────────────────────


def _delta_metadata_snapshot(table_path: str) -> dict:
    """Extract a serializable metadata dict from a local Delta table."""
    dt = DeltaTable(table_path)
    schema = dt.schema()
    columns = _schema_to_columns(schema)
    metadata = dt.metadata()

    return {
        "name": metadata.name or "",
        "version": dt.version(),
        "num_files": len(dt.file_uris()),
        "partition_columns": list(metadata.partition_columns),
        "properties": dict(metadata.configuration) if metadata.configuration else {},
        "columns": [
            {
                "name": c.name,
                "type": c.type,
                "nullable": c.nullable,
                "has_metadata": c.metadata is not None and len(c.metadata) > 0,
            }
            for c in columns
        ],
    }


def _parquet_schema_snapshot(path: Path) -> dict:
    """Extract a serializable schema dict from a Parquet file."""
    pf = pq.ParquetFile(path)
    schema = pf.schema_arrow
    return {
        "num_columns": len(schema),
        "num_rows": pf.metadata.num_rows,
        "fields": [
            {
                "name": schema.field(i).name,
                "type": str(schema.field(i).type),
                "nullable": schema.field(i).nullable,
            }
            for i in range(len(schema))
        ],
    }


# ── Delta snapshot tests ────────────────────────────────────────────────


class TestDeltaSnapshots:
    """Snapshot the metadata output from Delta fixture parsing."""

    def test_basic_table_metadata(self, snapshot):
        result = _delta_metadata_snapshot(str(DELTA_DIR / "basic_table"))
        assert result == snapshot

    def test_partitioned_table_metadata(self, snapshot):
        result = _delta_metadata_snapshot(str(DELTA_DIR / "partitioned_table"))
        assert result == snapshot

    def test_cdf_enabled_metadata(self, snapshot):
        result = _delta_metadata_snapshot(str(DELTA_DIR / "cdf_enabled"))
        assert result == snapshot

    def test_unicode_paths_metadata(self, snapshot):
        result = _delta_metadata_snapshot(str(DELTA_DIR / "unicode_paths"))
        assert result == snapshot


# ── Parquet snapshot tests ──────────────────────────────────────────────


class TestParquetSnapshots:
    """Snapshot the schema output from Parquet fixture introspection."""

    def test_all_types_schema(self, snapshot):
        result = _parquet_schema_snapshot(PARQUET_DIR / "all_types.parquet")
        assert result == snapshot

    def test_nested_structs_schema(self, snapshot):
        result = _parquet_schema_snapshot(PARQUET_DIR / "nested_structs.parquet")
        assert result == snapshot

    def test_dictionary_encoded_schema(self, snapshot):
        result = _parquet_schema_snapshot(PARQUET_DIR / "dictionary_encoded.parquet")
        assert result == snapshot


# ── coerce_timestamps snapshot ──────────────────────────────────────────


class TestCoerceTimestampsSnapshot:
    """Snapshot the schema after timestamp coercion to catch type changes."""

    def test_coerced_schema(self, snapshot):
        table = pq.read_table(PARQUET_DIR / "all_types.parquet")
        coerced = coerce_timestamps(table)
        schema_info = {
            "fields": [
                {
                    "name": coerced.schema.field(i).name,
                    "type": str(coerced.schema.field(i).type),
                }
                for i in range(coerced.num_columns)
            ],
        }
        assert schema_info == snapshot
