"""Live integration tests against a real Fabric workspace.

Run locally:
    ONELAKE_TEST_WORKSPACE_ID=<guid> uv run pytest tests/integration/ -v

With lakehouse tests:
    ONELAKE_TEST_WORKSPACE_ID=<guid> \\
    ONELAKE_TEST_LAKEHOUSE_ID=<guid> \\
    ONELAKE_TEST_LAKEHOUSE_NAME=<name> \\
    ONELAKE_TEST_TABLE_NAME=<table> \\
    uv run pytest tests/integration/ -v
"""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("ONELAKE_TEST_WORKSPACE_ID"),
    reason="ONELAKE_TEST_WORKSPACE_ID not set — skipping integration tests",
)


# ── Workspace & Item listing ────────────────────────────────────────────


async def test_list_workspaces(client):
    """Smoke test: can we list workspaces?"""
    workspaces = await client.fabric.list_workspaces()
    assert isinstance(workspaces, list)
    assert len(workspaces) > 0
    assert all(ws.id for ws in workspaces)


async def test_workspace_in_list(client, workspace_id):
    """The test workspace should appear in the workspace listing."""
    workspaces = await client.fabric.list_workspaces()
    ws_ids = {ws.id for ws in workspaces}
    assert workspace_id in ws_ids, (
        f"Test workspace {workspace_id} not found in {len(workspaces)} workspaces"
    )


async def test_list_items(client, workspace_id):
    """List items in the test workspace."""
    items = await client.fabric.list_items(workspace_id)
    assert isinstance(items, list)


async def test_items_have_required_fields(client, workspace_id):
    """All items should have id, displayName, and type."""
    items = await client.fabric.list_items(workspace_id)
    for item in items:
        assert item.id, f"Item missing id: {item}"
        assert item.display_name, f"Item missing display_name: {item}"
        assert item.type, f"Item missing type: {item}"


async def test_list_lakehouses(client, workspace_id):
    """List lakehouses in the test workspace."""
    lakehouses = await client.fabric.list_lakehouses(workspace_id)
    assert isinstance(lakehouses, list)


# ── Lakehouse-specific tests ────────────────────────────────────────────


async def test_get_lakehouse(client, workspace_id, lakehouse_id):
    """Get details of a specific lakehouse."""
    lh = await client.fabric.get_lakehouse(workspace_id, lakehouse_id)
    assert lh.id == lakehouse_id
    assert lh.type == "Lakehouse"
    assert lh.display_name


async def test_lakehouse_properties(client, workspace_id, lakehouse_id):
    """Lakehouse should have OneLake path properties."""
    lh = await client.fabric.get_lakehouse(workspace_id, lakehouse_id)
    assert lh.properties is not None
    assert lh.properties.onelake_tables_path
    assert lh.properties.onelake_files_path


# ── DFS file browsing ───────────────────────────────────────────────────


async def test_list_dfs_root_paths(client, workspace_id, lakehouse_name):
    """Browse files at the root of a lakehouse via DFS."""
    paths = await client.dfs.list_paths(workspace_id, f"{lakehouse_name}.Lakehouse")
    assert isinstance(paths, list)
    # Lakehouses typically have Tables/ and Files/ at minimum
    dir_names = {p.name.split("/")[-1] for p in paths if p.is_directory}
    assert len(dir_names) > 0, "Expected at least one directory at lakehouse root"


async def test_list_dfs_subdirectory(client, workspace_id, lakehouse_name):
    """Browse a subdirectory (Tables/) in a lakehouse."""
    root_paths = await client.dfs.list_paths(workspace_id, f"{lakehouse_name}.Lakehouse")
    # Find the first directory to descend into
    dirs = [p for p in root_paths if p.is_directory]
    if not dirs:
        pytest.skip("No directories in lakehouse root")
    first_dir = dirs[0]
    sub_paths = await client.dfs.list_paths(workspace_id, first_dir.name)
    assert isinstance(sub_paths, list)


# ── Delta table metadata ────────────────────────────────────────────────


async def test_delta_table_metadata(client, workspace_id, lakehouse_name, table_name):
    """Read Delta table metadata from a live table."""
    info = await client.delta.get_metadata(workspace_id, f"{lakehouse_name}.Lakehouse", table_name)
    assert info.version >= 0
    assert len(info.schema_) > 0
    assert info.num_files >= 0


async def test_delta_table_has_columns(client, workspace_id, lakehouse_name, table_name):
    """Delta table schema should have at least one column with name and type."""
    info = await client.delta.get_metadata(workspace_id, f"{lakehouse_name}.Lakehouse", table_name)
    for col in info.schema_:
        assert col.name, "Column has empty name"
        assert col.type, "Column has empty type"
