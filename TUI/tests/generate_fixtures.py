#!/usr/bin/env python3
"""Generate test fixtures for Delta logs and Parquet files.

Run:
    cd TUI
    uv run python tests/generate_fixtures.py

Produces deterministic fixtures in tests/fixtures/.
"""

from __future__ import annotations

import shutil
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import write_deltalake

FIXTURES_DIR = Path(__file__).parent / "fixtures"
DELTA_DIR = FIXTURES_DIR / "delta"
PARQUET_DIR = FIXTURES_DIR / "parquet"


def _clean_and_create(path: Path) -> Path:
    """Remove existing fixture and create empty directory."""
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


# ── Delta fixtures ──────────────────────────────────────────────────────


def generate_basic_table() -> None:
    """Simple table with common types — baseline for schema extraction."""
    table_path = str(_clean_and_create(DELTA_DIR / "basic_table"))
    data = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "name": pa.array(["Alice", "Bob", "Charlie"], type=pa.string()),
            "score": pa.array([95.5, 87.3, 92.1], type=pa.float64()),
            "active": pa.array([True, False, True], type=pa.bool_()),
        }
    )
    write_deltalake(table_path, data, mode="overwrite", name="customers")


def generate_partitioned_table() -> None:
    """Table with partition columns."""
    table_path = str(_clean_and_create(DELTA_DIR / "partitioned_table"))
    data = pa.table(
        {
            "region": pa.array(["us-east", "eu-west", "us-east", "eu-west"], type=pa.string()),
            "year": pa.array([2024, 2024, 2025, 2025], type=pa.int32()),
            "revenue": pa.array([1000.0, 2000.0, 1500.0, 2500.0], type=pa.float64()),
        }
    )
    write_deltalake(
        table_path,
        data,
        mode="overwrite",
        partition_by=["region", "year"],
        name="sales",
    )


def generate_column_mapping_v2() -> None:
    """Hand-craft a Delta log with column mapping mode=name.

    delta-rs requires field-level mapping annotations that are hard to
    set from the Python side, so we write the _delta_log JSON directly.
    This is perfectly human-readable and reviewable.
    """
    import json

    table_dir = _clean_and_create(DELTA_DIR / "column_mapping_v2")
    log_dir = table_dir / "_delta_log"
    log_dir.mkdir()

    commit = {
        "protocol": {
            "minReaderVersion": 2,
            "minWriterVersion": 5,
        },
    }
    metadata = {
        "metaData": {
            "id": "col-mapping-test-id",
            "name": "users",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": json.dumps(
                {
                    "type": "struct",
                    "fields": [
                        {
                            "name": "user_id",
                            "type": "long",
                            "nullable": False,
                            "metadata": {
                                "delta.columnMapping.id": 1,
                                "delta.columnMapping.physicalName": "col-uid-1",
                            },
                        },
                        {
                            "name": "email",
                            "type": "string",
                            "nullable": True,
                            "metadata": {
                                "delta.columnMapping.id": 2,
                                "delta.columnMapping.physicalName": "col-email-2",
                            },
                        },
                    ],
                }
            ),
            "partitionColumns": [],
            "configuration": {
                "delta.columnMapping.mode": "name",
                "delta.columnMapping.maxColumnId": "2",
            },
            "createdTime": 1700000000000,
        }
    }
    add = {
        "add": {
            "path": "part-00000.parquet",
            "size": 1024,
            "partitionValues": {},
            "modificationTime": 1700000000000,
            "dataChange": True,
        }
    }

    log_file = log_dir / "00000000000000000000.json"
    with open(log_file, "w") as f:
        for entry in [commit, metadata, add]:
            f.write(json.dumps(entry) + "\n")


def generate_cdf_enabled() -> None:
    """Table with Change Data Feed enabled."""
    table_path = str(_clean_and_create(DELTA_DIR / "cdf_enabled"))
    data = pa.table(
        {
            "order_id": pa.array([100, 101], type=pa.int64()),
            "amount": pa.array([49.99, 129.50], type=pa.float64()),
        }
    )
    write_deltalake(
        table_path,
        data,
        mode="overwrite",
        name="orders",
        configuration={"delta.enableChangeDataFeed": "true"},
    )


def generate_unicode_paths() -> None:
    """Table with unicode characters for testing path handling."""
    table_path = str(_clean_and_create(DELTA_DIR / "unicode_paths"))
    data = pa.table(
        {
            "données_id": pa.array([1, 2], type=pa.int64()),
            "名前": pa.array(["田中", "鈴木"], type=pa.string()),
        }
    )
    write_deltalake(table_path, data, mode="overwrite", name="données_client")


# ── Parquet fixtures ────────────────────────────────────────────────────


def generate_all_types_parquet() -> None:
    """Parquet file with all common column types."""
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    data = pa.table(
        {
            "int_col": pa.array([1, 2, None], type=pa.int64()),
            "float_col": pa.array([1.1, 2.2, 3.3], type=pa.float64()),
            "string_col": pa.array(["hello", "world", None], type=pa.string()),
            "bool_col": pa.array([True, False, True], type=pa.bool_()),
            "date_col": pa.array(
                [date(2024, 1, 1), date(2024, 6, 15), date(2024, 12, 31)],
                type=pa.date32(),
            ),
            "timestamp_us_col": pa.array(
                [
                    datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
                    datetime(2024, 6, 15, 8, 30, 0, tzinfo=UTC),
                    datetime(2024, 12, 31, 23, 59, 59, tzinfo=UTC),
                ],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "timestamp_ns_col": pa.array(
                [1704067200000000000, 1718438400000000000, 1735689599000000000],
                type=pa.timestamp("ns"),
            ),
            "decimal_col": pa.array(
                [Decimal("123.45"), Decimal("-67.89"), Decimal("0.01")],
                type=pa.decimal128(10, 2),
            ),
        }
    )
    pq.write_table(data, PARQUET_DIR / "all_types.parquet")


def generate_nested_structs_parquet() -> None:
    """Parquet file with complex nested types."""
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    data = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "address": pa.array(
                [
                    {"street": "123 Main St", "city": "Springfield"},
                    {"street": "456 Oak Ave", "city": "Shelbyville"},
                ],
                type=pa.struct([pa.field("street", pa.string()), pa.field("city", pa.string())]),
            ),
            "tags": pa.array(
                [["admin", "user"], ["user"]],
                type=pa.list_(pa.string()),
            ),
            "metadata": pa.array(
                [
                    [("role", "admin"), ("level", "5")],
                    [("role", "user"), ("level", "1")],
                ],
                type=pa.map_(pa.string(), pa.string()),
            ),
        }
    )
    pq.write_table(data, PARQUET_DIR / "nested_structs.parquet")


def generate_dictionary_encoded_parquet() -> None:
    """Parquet file with dictionary-encoded string column."""
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    categories = pa.array(
        ["A", "B", "A", "C", "B", "A", "C", "B"], type=pa.string()
    ).dictionary_encode()
    data = pa.table(
        {
            "id": pa.array(range(8), type=pa.int64()),
            "category": categories,
            "value": pa.array([10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0]),
        }
    )
    pq.write_table(data, PARQUET_DIR / "dictionary_encoded.parquet")


# ── Main ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Generating Delta fixtures...")
    generate_basic_table()
    print("  ✓ basic_table")
    generate_partitioned_table()
    print("  ✓ partitioned_table")
    generate_column_mapping_v2()
    print("  ✓ column_mapping_v2")
    generate_cdf_enabled()
    print("  ✓ cdf_enabled")
    generate_unicode_paths()
    print("  ✓ unicode_paths")

    print("\nGenerating Parquet fixtures...")
    generate_all_types_parquet()
    print("  ✓ all_types.parquet")
    generate_nested_structs_parquet()
    print("  ✓ nested_structs.parquet")
    generate_dictionary_encoded_parquet()
    print("  ✓ dictionary_encoded.parquet")

    print("\nDone! All fixtures generated in tests/fixtures/")
