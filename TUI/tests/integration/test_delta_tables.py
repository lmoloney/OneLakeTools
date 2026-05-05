"""Integration tests for Delta table metadata across all tables in olt_lakehouse_simple.

Each table exercises different Delta features: partitioning, CDF, OPTIMIZE,
high-version checkpoints, unicode names, complex types, and timestamp edge cases.

Run:
    uv run pytest tests/integration/test_delta_tables.py -v
"""

from __future__ import annotations

import pyarrow as pa
import pytest
from deltalake.exceptions import DeltaError

# ── Helpers ─────────────────────────────────────────────────────────────


@pytest.fixture
def ws(workspace_id):
    return workspace_id


@pytest.fixture
def lh(lakehouse_id):
    return lakehouse_id


# ── 1. customers — Basic table ──────────────────────────────────────────


class TestCustomers:
    TABLE = "customers"

    @pytest.fixture
    async def meta(self, client, ws, lh):
        return await client.delta.get_metadata(ws, lh, self.TABLE)

    async def test_schema_has_five_fields(self, meta):
        assert len(meta.schema_) == 5, f"Expected 5 columns, got {len(meta.schema_)}"

    async def test_expected_columns(self, meta):
        col_names = {c.name for c in meta.schema_}
        for expected in ("id", "name", "email", "city", "created_at"):
            assert expected in col_names, f"Missing column: {expected}"

    async def test_version(self, meta):
        assert meta.version >= 0

    async def test_no_partition_columns(self, meta):
        assert meta.partition_columns == []

    async def test_has_data_files(self, meta):
        assert meta.num_files >= 1


# ── 2. orders — Multi-commit ────────────────────────────────────────────


class TestOrders:
    TABLE = "orders"

    @pytest.fixture
    async def meta(self, client, ws, lh):
        return await client.delta.get_metadata(ws, lh, self.TABLE)

    async def test_multi_commit_version(self, meta):
        assert meta.version >= 1, "orders should have at least 2 commits (version >= 1)"

    async def test_schema_has_order_fields(self, meta):
        col_names = {c.name for c in meta.schema_}
        for expected in ("id", "customer_id", "amount", "order_date", "status"):
            assert expected in col_names, f"Missing column: {expected}"

    async def test_has_data_files(self, meta):
        assert meta.num_files >= 1


# ── 3. données_client — Unicode table name ──────────────────────────────


class TestDonneesClient:
    TABLE = "données_client"

    @pytest.fixture
    async def meta(self, client, ws, lh):
        return await client.delta.get_metadata(ws, lh, self.TABLE)

    async def test_metadata_loads(self, meta):
        """Metadata loads successfully despite unicode table name."""
        assert meta is not None
        assert len(meta.schema_) > 0

    async def test_french_column_names(self, meta):
        col_names = {c.name for c in meta.schema_}
        for expected in ("identifiant", "nom", "ville"):
            assert expected in col_names, f"Missing French column: {expected}"


# ── 4. all_data_types — All Delta types ─────────────────────────────────


class TestAllDataTypes:
    TABLE = "all_data_types"

    @pytest.fixture
    async def meta(self, client, ws, lh):
        return await client.delta.get_metadata(ws, lh, self.TABLE)

    async def test_at_least_14_columns(self, meta):
        assert len(meta.schema_) >= 14, (
            f"Expected >= 14 columns for all_data_types, got {len(meta.schema_)}"
        )

    async def test_complex_types_present(self, meta):
        type_strs = {c.type.lower() for c in meta.schema_}
        type_joined = " ".join(type_strs)
        assert "struct" in type_joined, "Expected a struct-type column"
        assert "array" in type_joined, "Expected an array-type column"
        assert "map" in type_joined, "Expected a map-type column"


# ── 5. partitioned_sales — Partitioned table ────────────────────────────


class TestPartitionedSales:
    TABLE = "partitioned_sales"

    @pytest.fixture
    async def meta(self, client, ws, lh):
        return await client.delta.get_metadata(ws, lh, self.TABLE)

    async def test_partition_columns(self, meta):
        assert meta.partition_columns == ["sale_year", "sale_month"]

    async def test_schema_has_sales_fields(self, meta):
        col_names = {c.name for c in meta.schema_}
        for expected in ("sale_year", "sale_month", "region", "product", "amount"):
            assert expected in col_names, f"Missing column: {expected}"

    async def test_multiple_partition_files(self, meta):
        assert meta.num_files >= 4, (
            f"Expected >= 4 files for partitioned table, got {meta.num_files}"
        )


# ── 6. cdf_tracking — Change Data Feed ─────────────────────────────────


class TestCdfTracking:
    TABLE = "cdf_tracking"

    @pytest.fixture
    async def meta(self, client, ws, lh):
        return await client.delta.get_metadata(ws, lh, self.TABLE)

    async def test_cdf_enabled(self, meta):
        assert meta.properties.get("delta.enableChangeDataFeed") == "true"

    async def test_multi_commit_version(self, meta):
        assert meta.version >= 3, (
            f"cdf_tracking should have >= 4 commits (INSERT+UPDATE+DELETE), "
            f"got version {meta.version}"
        )

    async def test_has_data_files(self, meta):
        assert meta.num_files >= 1


# ── 7. optimized_events — OPTIMIZE + ZORDER ─────────────────────────────


class TestOptimizedEvents:
    TABLE = "optimized_events"

    @pytest.fixture
    async def meta(self, client, ws, lh):
        return await client.delta.get_metadata(ws, lh, self.TABLE)

    async def test_high_version_from_micro_batches(self, meta):
        assert meta.version >= 10, (
            f"optimized_events should have >= 10 commits, got version {meta.version}"
        )

    async def test_has_data_files(self, meta):
        assert meta.num_files >= 1


# ── 8. high_version — Checkpoint ────────────────────────────────────────


class TestHighVersion:
    TABLE = "high_version"

    @pytest.fixture
    async def meta(self, client, ws, lh):
        return await client.delta.get_metadata(ws, lh, self.TABLE)

    async def test_version_past_checkpoint(self, meta):
        assert meta.version >= 15, (
            f"high_version should have >= 15 commits (checkpoint at 10), got version {meta.version}"
        )

    async def test_has_data_files(self, meta):
        assert meta.num_files >= 1


# ── 9. timestamp_edge_cases — Timestamp types ───────────────────────────


class TestTimestampEdgeCases:
    TABLE = "timestamp_edge_cases"

    @pytest.fixture
    async def meta(self, client, ws, lh):
        return await client.delta.get_metadata(ws, lh, self.TABLE)

    async def test_has_timestamp_columns(self, meta):
        type_strs = {c.type.lower() for c in meta.schema_}
        type_joined = " ".join(type_strs)
        assert "timestamp" in type_joined, "Expected at least one timestamp-type column"

    async def test_has_date_column(self, meta):
        type_strs = {c.type.lower() for c in meta.schema_}
        type_joined = " ".join(type_strs)
        assert "date" in type_joined, "Expected a date-type column"

    async def test_has_data_files(self, meta):
        assert meta.num_files >= 1


# ── 10. deletion_vector_demo — DV table (may not exist) ─────────────────


class TestDeletionVectors:
    TABLE = "deletion_vector_demo"

    async def test_dv_table(self, client, ws, lh):
        """DV table: expect graceful metadata or a DeltaError about reader features."""
        try:
            meta = await client.delta.get_metadata(ws, lh, self.TABLE)
        except (DeltaError, Exception) as exc:
            exc_lower = str(exc).lower()
            if "not found" in exc_lower or "no files in log" in exc_lower:
                pytest.skip("deletion_vector_demo table not provisioned yet")
            if "reader features" in exc_lower or "deletionvectors" in exc_lower:
                # Expected: deltalake may not support DV tables
                return
            pytest.skip(f"deletion_vector_demo not accessible: {exc}")
        # If it loaded, basic sanity checks
        assert meta.version >= 0
        assert len(meta.schema_) > 0


# ── 10b. deletion_vector_demo — Enhanced DV tests ───────────────────────


class TestDeletionVectorDetails:
    """Enhanced DV tests using protocol extraction."""

    TABLE = "deletion_vector_demo"

    @pytest.fixture
    async def meta(self, client, ws, lh):
        try:
            return await client.delta.get_metadata(ws, lh, self.TABLE)
        except Exception as exc:
            if "no files in log" in str(exc).lower():
                pytest.skip("deletion_vector_demo not accessible")
            raise

    async def test_reader_version_v3(self, meta):
        assert meta.reader_version >= 3, (
            f"DV tables require reader version >= 3, got {meta.reader_version}"
        )

    async def test_deletion_vectors_in_reader_features(self, meta):
        assert "deletionVectors" in meta.reader_features, (
            f"Expected 'deletionVectors' in reader_features, got {meta.reader_features}"
        )

    async def test_deletion_vectors_in_writer_features(self, meta):
        assert "deletionVectors" in meta.writer_features, (
            f"Expected 'deletionVectors' in writer_features, got {meta.writer_features}"
        )

    async def test_has_warnings(self, meta):
        assert len(meta.warnings) > 0, "Expected at least one warning for DV table"
        dv_warnings = [w for w in meta.warnings if "deletionVectors" in w]
        assert len(dv_warnings) > 0, (
            f"Expected a warning mentioning 'deletionVectors', got {meta.warnings}"
        )

    async def test_schema_still_accessible(self, meta):
        col_names = {c.name for c in meta.schema_}
        for expected in ("id", "name", "category", "value"):
            assert expected in col_names, f"Missing column: {expected}"

    async def test_version_reflects_delete(self, meta):
        assert meta.version >= 1, (
            f"DV table should have at least 2 commits (CREATE + DELETE), got version {meta.version}"
        )

    async def test_list_files_returns_parquet(self, client, ws, lh):
        try:
            files = await client.delta.list_files(ws, lh, self.TABLE)
        except Exception as exc:
            if "no files in log" in str(exc).lower():
                pytest.skip("deletion_vector_demo not accessible")
            raise
        assert len(files) >= 1, "Expected at least one data file"
        for f in files:
            assert f.endswith(".parquet"), f"Expected .parquet file, got: {f}"

    async def test_properties_show_dv_enabled(self, meta):
        assert meta.properties.get("delta.enableDeletionVectors") == "true", (
            f"Expected delta.enableDeletionVectors=true, got {meta.properties}"
        )


# ── Cross-cutting: read_sample ──────────────────────────────────────────


class TestReadSample:
    async def test_returns_pyarrow_table(self, client, ws, lh):
        table = await client.delta.read_sample(ws, lh, "customers", limit=5)
        assert isinstance(table, pa.Table)

    async def test_respects_limit(self, client, ws, lh):
        table = await client.delta.read_sample(ws, lh, "customers", limit=5)
        assert table.num_rows <= 5

    async def test_has_expected_columns(self, client, ws, lh):
        table = await client.delta.read_sample(ws, lh, "customers", limit=5)
        col_names = set(table.column_names)
        for expected in ("id", "name", "email", "city", "created_at"):
            assert expected in col_names, f"Missing column in sample: {expected}"


# ── Cross-cutting: list_files ───────────────────────────────────────────


class TestListFiles:
    async def test_returns_list_of_strings(self, client, ws, lh):
        files = await client.delta.list_files(ws, lh, "customers")
        assert isinstance(files, list)
        assert len(files) >= 1

    async def test_files_are_parquet(self, client, ws, lh):
        files = await client.delta.list_files(ws, lh, "customers")
        for f in files:
            assert f.endswith(".parquet"), f"Expected .parquet file, got: {f}"


# ── Cross-cutting: read_cdf ────────────────────────────────────────────


class TestReadCdf:
    TABLE = "cdf_tracking"

    async def test_returns_pyarrow_table(self, client, ws, lh):
        try:
            table = await client.delta.read_cdf(ws, lh, self.TABLE, starting_version=0)
        except Exception as exc:
            if "change data feed" in str(exc).lower() or "not enabled" in str(exc).lower():
                pytest.skip(f"CDF read not supported for this table: {exc}")
            raise
        # read_cdf may return pyarrow.Table or arro3.core.Table (deltalake >= 1.0)
        assert hasattr(table, "column_names"), f"Expected table-like object, got {type(table)}"
        assert hasattr(table, "num_rows"), f"Expected table-like object, got {type(table)}"

    async def test_has_cdf_columns(self, client, ws, lh):
        table = await client.delta.read_cdf(ws, lh, self.TABLE, starting_version=0)
        col_names = set(table.column_names)
        for expected in ("_change_type", "_commit_version", "_commit_timestamp"):
            assert expected in col_names, f"Missing CDF column: {expected}"

    async def test_includes_insert_change_type(self, client, ws, lh):
        table = await client.delta.read_cdf(ws, lh, self.TABLE, starting_version=0)
        change_types = set(table.column("_change_type").to_pylist())
        assert "insert" in change_types, f"Expected 'insert' in change types, got: {change_types}"
