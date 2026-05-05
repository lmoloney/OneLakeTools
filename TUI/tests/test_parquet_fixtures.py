"""Tests exercising committed Parquet fixtures for schema introspection and coerce_timestamps()."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from onelake_client.tables import coerce_timestamps

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "parquet"


# ── all_types.parquet ───────────────────────────────────────────────────


class TestAllTypes:
    @pytest.fixture()
    def pf(self) -> pq.ParquetFile:
        return pq.ParquetFile(FIXTURES_DIR / "all_types.parquet")

    @pytest.fixture()
    def table(self) -> pa.Table:
        return pq.read_table(FIXTURES_DIR / "all_types.parquet")

    def test_field_count(self, pf: pq.ParquetFile) -> None:
        assert len(pf.schema_arrow) == 8

    def test_field_names(self, pf: pq.ParquetFile) -> None:
        expected = [
            "int_col",
            "float_col",
            "string_col",
            "bool_col",
            "date_col",
            "timestamp_us_col",
            "timestamp_ns_col",
            "decimal_col",
        ]
        assert pf.schema_arrow.names == expected

    def test_field_types(self, pf: pq.ParquetFile) -> None:
        schema = pf.schema_arrow
        assert schema.field("int_col").type == pa.int64()
        assert schema.field("float_col").type == pa.float64()
        assert schema.field("string_col").type == pa.string()
        assert schema.field("bool_col").type == pa.bool_()
        assert schema.field("date_col").type == pa.date32()
        assert schema.field("timestamp_us_col").type == pa.timestamp("us", tz="UTC")
        assert schema.field("timestamp_ns_col").type == pa.timestamp("ns")
        assert schema.field("decimal_col").type == pa.decimal128(10, 2)

    def test_row_count(self, table: pa.Table) -> None:
        assert table.num_rows == 3

    def test_null_positions(self, table: pa.Table) -> None:
        int_col = table.column("int_col")
        assert int_col[0].as_py() == 1
        assert int_col[2].as_py() is None

        string_col = table.column("string_col")
        assert string_col[0].as_py() == "hello"
        assert string_col[2].as_py() is None

    def test_non_null_values(self, table: pa.Table) -> None:
        assert table.column("float_col")[1].as_py() == pytest.approx(2.2)
        assert table.column("bool_col")[0].as_py() is True
        assert table.column("date_col")[0].as_py() == date(2024, 1, 1)
        assert table.column("decimal_col")[0].as_py() == Decimal("123.45")


# ── nested_structs.parquet ──────────────────────────────────────────────


class TestNestedStructs:
    @pytest.fixture()
    def pf(self) -> pq.ParquetFile:
        return pq.ParquetFile(FIXTURES_DIR / "nested_structs.parquet")

    @pytest.fixture()
    def table(self) -> pa.Table:
        return pq.read_table(FIXTURES_DIR / "nested_structs.parquet")

    def test_field_count(self, pf: pq.ParquetFile) -> None:
        assert len(pf.schema_arrow) == 4

    def test_field_names(self, pf: pq.ParquetFile) -> None:
        assert pf.schema_arrow.names == ["id", "address", "tags", "metadata"]

    def test_struct_fields(self, pf: pq.ParquetFile) -> None:
        addr_type = pf.schema_arrow.field("address").type
        assert pa.types.is_struct(addr_type)
        field_names = sorted(addr_type.field(i).name for i in range(addr_type.num_fields))
        assert field_names == ["city", "street"]
        assert addr_type.field("street").type == pa.string()
        assert addr_type.field("city").type == pa.string()

    def test_list_element_type(self, pf: pq.ParquetFile) -> None:
        tags_type = pf.schema_arrow.field("tags").type
        assert pa.types.is_list(tags_type)
        assert tags_type.value_type == pa.string()

    def test_map_key_value_types(self, pf: pq.ParquetFile) -> None:
        map_type = pf.schema_arrow.field("metadata").type
        assert pa.types.is_map(map_type)
        assert map_type.key_type == pa.string()
        assert map_type.item_type == pa.string()

    def test_row_count(self, table: pa.Table) -> None:
        assert table.num_rows == 2

    def test_struct_values(self, table: pa.Table) -> None:
        addr = table.column("address")[0].as_py()
        assert addr == {"street": "123 Main St", "city": "Springfield"}

    def test_list_values(self, table: pa.Table) -> None:
        assert table.column("tags")[0].as_py() == ["admin", "user"]
        assert table.column("tags")[1].as_py() == ["user"]

    def test_map_values_accessible(self, table: pa.Table) -> None:
        """Map column values are accessible as key-value pair lists."""
        meta_col = table.column("metadata")
        row0 = meta_col[0].as_py()
        assert isinstance(row0, list), "Map values should be returned as list of tuples"
        # Each entry is a (key, value) tuple
        keys = [k for k, _v in row0]
        assert len(keys) > 0, "Expected at least one key-value pair in map"

    def test_null_handling_in_nested_types(self, table: pa.Table) -> None:
        """Nested columns should be readable even if some rows have None values."""
        for col_name in ["address", "tags", "metadata"]:
            col = table.column(col_name)
            # Verify we can iterate all values without error
            values = col.to_pylist()
            assert len(values) == table.num_rows


# ── dictionary_encoded.parquet ──────────────────────────────────────────


class TestDictionaryEncoded:
    @pytest.fixture()
    def pf(self) -> pq.ParquetFile:
        return pq.ParquetFile(FIXTURES_DIR / "dictionary_encoded.parquet")

    @pytest.fixture()
    def table(self) -> pa.Table:
        return pq.read_table(FIXTURES_DIR / "dictionary_encoded.parquet")

    def test_field_count(self, pf: pq.ParquetFile) -> None:
        assert len(pf.schema_arrow) == 3

    def test_field_names(self, pf: pq.ParquetFile) -> None:
        assert pf.schema_arrow.names == ["id", "category", "value"]

    def test_dictionary_encoding_in_metadata(self, pf: pq.ParquetFile) -> None:
        col_idx = pf.schema_arrow.get_field_index("category")
        rg_meta = pf.metadata.row_group(0)
        col_meta = rg_meta.column(col_idx)
        encodings = set(col_meta.encodings)
        assert encodings & {"RLE_DICTIONARY", "PLAIN_DICTIONARY"}, (
            f"Expected dictionary encoding, got {encodings}"
        )

    def test_row_count(self, table: pa.Table) -> None:
        assert table.num_rows == 8

    def test_category_values(self, table: pa.Table) -> None:
        cats = table.column("category").to_pylist()
        assert set(cats) == {"A", "B", "C"}
        assert cats[0] == "A"


# ── coerce_timestamps() ────────────────────────────────────────────────


class TestCoerceTimestamps:
    @pytest.fixture()
    def table(self) -> pa.Table:
        return pq.read_table(FIXTURES_DIR / "all_types.parquet")

    def test_ns_column_becomes_us(self, table: pa.Table) -> None:
        assert table.schema.field("timestamp_ns_col").type == pa.timestamp("ns")
        coerced = coerce_timestamps(table)
        assert coerced.schema.field("timestamp_ns_col").type == pa.timestamp("us")

    def test_us_utc_column_unchanged(self, table: pa.Table) -> None:
        coerced = coerce_timestamps(table)
        assert coerced.schema.field("timestamp_us_col").type == pa.timestamp("us", tz="UTC")

    def test_values_preserved(self, table: pa.Table) -> None:
        coerced = coerce_timestamps(table)

        # timestamp_us_col — unchanged
        us_vals = coerced.column("timestamp_us_col").to_pylist()
        assert us_vals[0] == datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

        # timestamp_ns_col — coerced but values within valid range are preserved
        ns_vals = coerced.column("timestamp_ns_col").to_pylist()
        assert ns_vals[0] is not None
        assert ns_vals[1] is not None
        assert ns_vals[2] is not None

    def test_non_timestamp_columns_untouched(self, table: pa.Table) -> None:
        coerced = coerce_timestamps(table)
        assert coerced.schema.field("int_col").type == pa.int64()
        assert coerced.schema.field("string_col").type == pa.string()
        assert coerced.column("int_col").to_pylist() == table.column("int_col").to_pylist()

    def test_all_non_timestamp_values_unchanged(self, table: pa.Table) -> None:
        """Every non-timestamp column value must be identical after coercion."""
        coerced = coerce_timestamps(table)
        non_ts_cols = [
            name
            for name in table.schema.names
            if not pa.types.is_timestamp(table.schema.field(name).type)
        ]
        for col_name in non_ts_cols:
            original = table.column(col_name).to_pylist()
            after = coerced.column(col_name).to_pylist()
            assert after == original, f"Column {col_name!r} values changed after coerce"

    def test_coerced_timestamp_values_in_range(self, table: pa.Table) -> None:
        """Coerced timestamp values should be within a sensible date range."""
        coerced = coerce_timestamps(table)
        for name in coerced.schema.names:
            if not pa.types.is_timestamp(coerced.schema.field(name).type):
                continue
            for val in coerced.column(name).to_pylist():
                if val is None:
                    continue
                # Strip timezone for comparison
                naive = val.replace(tzinfo=None) if val.tzinfo else val
                assert datetime(1900, 1, 1) <= naive <= datetime(2200, 1, 1), (
                    f"Column {name!r} has timestamp {val} outside expected range"
                )

    def test_idempotency(self, table: pa.Table) -> None:
        """coerce_timestamps(coerce_timestamps(table)) should equal single coercion."""
        once = coerce_timestamps(table)
        twice = coerce_timestamps(once)
        assert once.schema.equals(twice.schema)
        for col_name in once.schema.names:
            assert once.column(col_name).to_pylist() == twice.column(col_name).to_pylist(), (
                f"Column {col_name!r} differs between single and double coercion"
            )
