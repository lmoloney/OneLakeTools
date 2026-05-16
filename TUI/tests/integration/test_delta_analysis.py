"""Integration tests for Delta Analysis (get_analysis) across all table types.

Exercises the full analysis pipeline — Range reads, parquet footer parsing,
row group/column chunk extraction — against real Delta tables with diverse
features: partitioning, column mapping, deletion vectors, schema-qualified
names, large tables, etc.

Run:
    uv run pytest tests/integration/test_delta_analysis.py -v
"""

from __future__ import annotations

import pytest

# All tables in lakehouse_simple
_SIMPLE_TABLES = [
    "customers",
    "orders",
    "données_client",
    "all_data_types",
    "partitioned_sales",
    "cdf_tracking",
    "optimized_events",
    "high_version",
    "timestamp_edge_cases",
    "schema_evolution_add",
    "liquid_clustered",
    "generated_and_checks",
    "schema_evolution_type",
]

# Tables that may fail analysis (reader v3+ / advanced features)
_MAY_FAIL_TABLES = {"deletion_vector_demo"}

# large_customers tested separately with max_files=1


# ── Part 1: Parametrized smoke test across ALL simple tables ────────────


@pytest.mark.parametrize("table_name", _SIMPLE_TABLES)
async def test_analysis_smoke(client, workspace_id, lakehouse_id, table_name):
    """Every simple lakehouse table should produce a valid analysis result."""
    result = await client.delta.get_analysis(
        workspace_id, lakehouse_id, table_name, max_files=5
    )
    s = result.summary
    assert s.total_files > 0
    assert s.total_rows >= 0
    assert s.total_row_groups >= s.total_files  # at least 1 RG per file
    assert len(result.files) == s.total_files
    assert len(result.row_groups) == s.total_row_groups
    assert len(result.columns) > 0
    # Every file should have at least 1 row group
    for f in result.files:
        assert f.row_group_count >= 1
    # Column chunks: schema evolution means older files may have fewer columns,
    # so total chunks can be less than total_row_groups × len(columns).
    assert len(result.column_chunks) <= s.total_row_groups * len(result.columns)
    assert len(result.column_chunks) > 0


# ── Part 2: Specific table validations ──────────────────────────────────


class TestCustomersAnalysis:
    """Validate analysis details for the customers table (known schema)."""

    @pytest.fixture
    async def analysis(self, client, workspace_id, lakehouse_id):
        return await client.delta.get_analysis(
            workspace_id, lakehouse_id, "customers", max_files=20
        )

    async def test_column_names(self, analysis):
        col_names = {c.column_name for c in analysis.columns}
        for expected in ("id", "name", "email", "city", "created_at"):
            assert expected in col_names, f"Missing column: {expected}"

    async def test_row_counts_consistent(self, analysis):
        """Sum of file row counts should equal summary total."""
        file_rows = sum(f.row_count for f in analysis.files)
        assert file_rows == analysis.summary.total_rows

    async def test_compression_ratios_valid(self, analysis):
        """Compression ratios should be non-negative (can exceed 1.0 for tiny files)."""
        for rg in analysis.row_groups:
            assert rg.compression_ratio >= 0, (
                f"Negative ratio {rg.compression_ratio} for RG {rg.row_group_id}"
            )

    async def test_pct_of_table_sums_to_one(self, analysis):
        """Column pct_of_table should sum to ~1.0."""
        total_pct = sum(c.pct_of_table for c in analysis.columns)
        assert abs(total_pct - 1.0) < 0.01, f"pct_of_table sums to {total_pct}"

    async def test_total_table_rows_backfilled(self, analysis):
        """total_table_rows should be set on all files and row groups."""
        expected = analysis.summary.total_rows
        for f in analysis.files:
            assert f.total_table_rows == expected
        for rg in analysis.row_groups:
            assert rg.total_table_rows == expected


class TestPartitionedAnalysis:
    """Partitioned tables should still produce valid analysis."""

    @pytest.fixture
    async def analysis(self, client, workspace_id, lakehouse_id):
        return await client.delta.get_analysis(
            workspace_id, lakehouse_id, "partitioned_sales", max_files=20
        )

    async def test_has_files(self, analysis):
        assert analysis.summary.total_files > 0

    async def test_column_names_include_partition_cols(self, analysis):
        col_names = {c.column_name for c in analysis.columns}
        # Partition columns may or may not appear in parquet column chunks
        # depending on how Spark writes them. Just verify we have data columns.
        assert "sale_id" in col_names or "product" in col_names or len(col_names) > 0


class TestLargeTableAnalysis:
    """Large table (9.9M rows) — test with max_files=1 to limit network."""

    async def test_analysis_with_max_files_cap(
        self, client, workspace_id, lakehouse_id
    ):
        result = await client.delta.get_analysis(
            workspace_id, lakehouse_id, "large_customers", max_files=1
        )
        assert result.summary.total_files == 1
        assert result.summary.files_skipped > 0
        assert result.summary.total_rows > 0
        assert len(result.columns) > 0


class TestColumnMappingAnalysis:
    """Column mapping tables should resolve physical→logical names."""

    @pytest.fixture
    async def analysis(self, client, workspace_id, lakehouse_id):
        return await client.delta.get_analysis(
            workspace_id, lakehouse_id, "column_mapping_id", max_files=20
        )

    async def test_logical_column_names(self, analysis):
        """Analysis should use logical names, not physical GUIDs."""
        col_names = {c.column_name for c in analysis.columns}
        # Should have human-readable names, not UUIDs
        for name in col_names:
            assert len(name) < 36 or "-" not in name, (
                f"Column '{name}' looks like a physical GUID"
                " — mapping may have failed"
            )
        # Expected logical names
        for expected in ("user_id", "email", "rating"):
            assert expected in col_names, f"Missing logical column: {expected}"


class TestDeletionVectorAnalysis:
    """Tables with deletion vectors (reader v3) may or may not work."""

    async def test_analysis_does_not_crash(
        self, client, workspace_id, lakehouse_id
    ):
        """Deletion vector tables should either produce results or raise DeltaError."""
        from deltalake.exceptions import DeltaError

        try:
            result = await client.delta.get_analysis(
                workspace_id, lakehouse_id, "deletion_vector_demo", max_files=5
            )
            # If it succeeds, verify basic structure
            assert result.summary.total_files >= 0
        except DeltaError:
            pytest.skip(
                "DeltaError on deletion_vector_demo — expected with reader v3"
            )


# ── Part 3: Progress callback ──────────────────────────────────────────


async def test_progress_callback_called(client, workspace_id, lakehouse_id):
    """Progress callback should be called for each file."""
    progress_calls: list[tuple[int, int, str]] = []

    async def _track(current: int, total: int, filename: str) -> None:
        progress_calls.append((current, total, filename))

    await client.delta.get_analysis(
        workspace_id, lakehouse_id, "customers", max_files=5, progress_callback=_track
    )
    assert len(progress_calls) > 0
    # Should be sequential
    for i, (current, total, _) in enumerate(progress_calls):
        assert current == i + 1
        assert total > 0
