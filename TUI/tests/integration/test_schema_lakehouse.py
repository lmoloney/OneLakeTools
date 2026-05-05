"""Integration tests for schema-enabled lakehouses (two-level table hierarchy).

Schema lakehouses have Tables organized as Tables/SCHEMA/TABLE/ instead of
the flat Tables/TABLE/ layout.  This module verifies DFS browsing and Delta
metadata access for that structure.

Requires the ``lakehouse_schema`` manifest entry in fabric-test-env.json.
"""

from __future__ import annotations

import pytest

# ── Helpers ─────────────────────────────────────────────────────────────


def _lakehouse_id(item: dict) -> str:
    return item["id"]


def _expected_schemas(item: dict) -> dict[str, list[str]]:
    return item["expected_schemas"]


# ── Root directory structure ────────────────────────────────────────────


async def test_root_dirs_match_manifest(client, workspace_id, lakehouse_schema_item):
    """Root directories should match expected_root_dirs from the manifest."""
    lh_id = _lakehouse_id(lakehouse_schema_item)
    paths = await client.dfs.list_paths(workspace_id, lh_id)
    dir_names = {p.name.split("/")[-1] for p in paths if p.is_directory}

    expected = set(lakehouse_schema_item["expected_root_dirs"])
    assert expected.issubset(dir_names), (
        f"Missing root dirs: {expected - dir_names} (got {dir_names})"
    )


# ── Schema folder detection ────────────────────────────────────────────


async def test_tables_children_are_schema_folders(client, workspace_id, lakehouse_schema_item):
    """Tables/ children should be schema folder names, all directories."""
    lh_id = _lakehouse_id(lakehouse_schema_item)
    paths = await client.dfs.list_paths(workspace_id, lh_id, directory="Tables")

    expected_schemas = set(_expected_schemas(lakehouse_schema_item))
    child_names = {p.name.split("/")[-1] for p in paths}

    assert expected_schemas.issubset(child_names), (
        f"Missing schemas: {expected_schemas - child_names} (got {child_names})"
    )
    # Every expected schema entry must be a directory
    for p in paths:
        if p.name.split("/")[-1] in expected_schemas:
            assert p.is_directory, f"Expected directory for schema folder: {p.name}"


async def test_no_delta_log_at_tables_level(client, workspace_id, lakehouse_schema_item):
    """Tables/ should contain schema folders, not a _delta_log."""
    lh_id = _lakehouse_id(lakehouse_schema_item)
    paths = await client.dfs.list_paths(workspace_id, lh_id, directory="Tables")
    child_names = {p.name.split("/")[-1] for p in paths}
    assert "_delta_log" not in child_names, (
        "Tables/ should contain schema folders, not a direct _delta_log"
    )


# ── Schema contents ────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "schema_name",
    ["dbo", "analytics"],
)
async def test_schema_contains_expected_tables(
    client, workspace_id, lakehouse_schema_item, schema_name
):
    """Each schema folder should contain the tables declared in the manifest."""
    lh_id = _lakehouse_id(lakehouse_schema_item)
    expected_tables = _expected_schemas(lakehouse_schema_item).get(schema_name)
    if expected_tables is None:
        pytest.skip(f"Schema '{schema_name}' not in manifest")

    paths = await client.dfs.list_paths(workspace_id, lh_id, directory=f"Tables/{schema_name}")
    table_names = {p.name.split("/")[-1] for p in paths if p.is_directory}

    assert set(expected_tables).issubset(table_names), (
        f"Missing tables in {schema_name}: {set(expected_tables) - table_names} (got {table_names})"
    )


# ── Delta log presence ─────────────────────────────────────────────────


async def test_delta_log_exists_dbo_products(client, workspace_id, lakehouse_schema_item):
    """Tables/dbo/products should contain a _delta_log directory."""
    lh_id = _lakehouse_id(lakehouse_schema_item)
    paths = await client.dfs.list_paths(workspace_id, lh_id, directory="Tables/dbo/products")
    child_names = {p.name.split("/")[-1] for p in paths}
    assert "_delta_log" in child_names, (
        f"Expected _delta_log in Tables/dbo/products, got {child_names}"
    )


async def test_delta_log_exists_analytics_daily_summary(
    client, workspace_id, lakehouse_schema_item
):
    """Tables/analytics/daily_summary should contain a _delta_log directory."""
    lh_id = _lakehouse_id(lakehouse_schema_item)
    paths = await client.dfs.list_paths(
        workspace_id, lh_id, directory="Tables/analytics/daily_summary"
    )
    child_names = {p.name.split("/")[-1] for p in paths}
    assert "_delta_log" in child_names, (
        f"Expected _delta_log in Tables/analytics/daily_summary, got {child_names}"
    )


# ── Delta metadata via schema-qualified path ───────────────────────────


async def test_delta_metadata_dbo_products(client, workspace_id, lakehouse_schema_item):
    """Delta metadata for dbo/products should be valid."""
    lh_id = _lakehouse_id(lakehouse_schema_item)
    info = await client.delta.get_metadata(workspace_id, lh_id, "dbo/products")

    assert info.version >= 0
    assert len(info.schema_) > 0, "Schema should have at least one column"
    for col in info.schema_:
        assert col.name, "Column has empty name"
        assert col.type, "Column has empty type"


async def test_delta_metadata_analytics_daily_summary(client, workspace_id, lakehouse_schema_item):
    """Delta metadata for analytics/daily_summary should be valid."""
    lh_id = _lakehouse_id(lakehouse_schema_item)
    info = await client.delta.get_metadata(workspace_id, lh_id, "analytics/daily_summary")

    assert info.version >= 0
    assert len(info.schema_) > 0, "Schema should have at least one column"
    for col in info.schema_:
        assert col.name, "Column has empty name"
        assert col.type, "Column has empty type"
