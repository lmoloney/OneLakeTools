"""Integration tests for Delta Analysis on schema-qualified tables.

Schema lakehouses have Tables organized as Tables/SCHEMA/TABLE/ instead of
flat Tables/TABLE/. Analysis must handle the slash in table_name correctly.

Run:
    uv run pytest tests/integration/test_analysis_schema.py -v
"""

from __future__ import annotations

import pytest

# ── Helpers ─────────────────────────────────────────────────────────────


def _lakehouse_id(item: dict) -> str:
    return item["id"]


def _schema_tables(item: dict) -> list[str]:
    """Build list of schema/table names from manifest."""
    tables = []
    for schema, names in item["expected_schemas"].items():
        for name in names:
            tables.append(f"{schema}/{name}")
    return tables


# ── Parametrized smoke test ─────────────────────────────────────────────


@pytest.fixture
def schema_tables(lakehouse_schema_item):
    return _schema_tables(lakehouse_schema_item)


@pytest.mark.parametrize(
    "table_name",
    [
        "dbo/products",
        "dbo/inventory",
        "analytics/daily_summary",
        "analytics/user_activity",
    ],
)
async def test_schema_table_analysis(
    client, workspace_id, lakehouse_schema_item, table_name
):
    """Schema-qualified tables should produce valid analysis results."""
    lh_id = _lakehouse_id(lakehouse_schema_item)
    result = await client.delta.get_analysis(
        workspace_id, lh_id, table_name, max_files=5
    )
    s = result.summary
    assert s.total_files > 0, f"{table_name}: no files found"
    assert s.total_rows >= 0
    assert s.total_row_groups >= 1
    assert len(result.files) == s.total_files
    assert len(result.columns) > 0


async def test_schema_table_row_counts_consistent(
    client, workspace_id, lakehouse_schema_item
):
    """File row counts should sum to total for a schema table."""
    lh_id = _lakehouse_id(lakehouse_schema_item)
    result = await client.delta.get_analysis(
        workspace_id, lh_id, "analytics/daily_summary", max_files=20
    )
    file_rows = sum(f.row_count for f in result.files)
    assert file_rows == result.summary.total_rows


async def test_schema_table_column_chunks_complete(
    client, workspace_id, lakehouse_schema_item
):
    """Column chunks should cover columns × row groups."""
    lh_id = _lakehouse_id(lakehouse_schema_item)
    result = await client.delta.get_analysis(
        workspace_id, lh_id, "analytics/daily_summary", max_files=20
    )
    expected = result.summary.total_row_groups * len(result.columns)
    assert len(result.column_chunks) == expected


async def test_schema_table_compression_ratios(
    client, workspace_id, lakehouse_schema_item
):
    """Compression ratios should be non-negative (can exceed 1.0 for tiny files)."""
    lh_id = _lakehouse_id(lakehouse_schema_item)
    result = await client.delta.get_analysis(
        workspace_id, lh_id, "dbo/products", max_files=10
    )
    for rg in result.row_groups:
        assert rg.compression_ratio >= 0
