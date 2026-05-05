"""Live integration tests against a real Fabric workspace.

Run locally (manifest auto-detected from ~/.config/onelaketools/fabric-test-env.json):
    uv run pytest tests/integration/ -v

Or with explicit env vars (CI):
    ONELAKE_TEST_WORKSPACE_ID=<guid> uv run pytest tests/integration/ -v
"""

from __future__ import annotations

import pytest

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


# ── DFS file browsing (GUID paths) ──────────────────────────────────────


async def test_dfs_root_guid_path(client, workspace_id, lakehouse_id):
    """Browse lakehouse root via GUID workspace + GUID item."""
    paths = await client.dfs.list_paths(workspace_id, lakehouse_id)
    assert isinstance(paths, list)
    dir_names = {p.name.split("/")[-1] for p in paths if p.is_directory}
    assert len(dir_names) > 0, "Expected at least one directory at lakehouse root"


async def test_dfs_subdirectory_guid_path(client, workspace_id, lakehouse_id):
    """Browse a subdirectory via GUID paths."""
    root_paths = await client.dfs.list_paths(workspace_id, lakehouse_id)
    dirs = [p for p in root_paths if p.is_directory]
    if not dirs:
        pytest.skip("No directories in lakehouse root")
    sub_paths = await client.dfs.list_paths(workspace_id, dirs[0].name)
    assert isinstance(sub_paths, list)


# ── DFS file browsing (friendly-name paths) ─────────────────────────────


async def test_dfs_root_friendly_path(client, workspace_name, lakehouse_display_path):
    """Browse lakehouse root via workspace name + DisplayName.Type item path."""
    paths = await client.dfs.list_paths(workspace_name, lakehouse_display_path)
    assert isinstance(paths, list)
    dir_names = {p.name.split("/")[-1] for p in paths if p.is_directory}
    assert len(dir_names) > 0, "Expected at least one directory at lakehouse root"


async def test_dfs_subdirectory_friendly_path(client, workspace_name, lakehouse_display_path):
    """Browse a subdirectory via friendly-name paths."""
    root_paths = await client.dfs.list_paths(workspace_name, lakehouse_display_path)
    dirs = [p for p in root_paths if p.is_directory]
    if not dirs:
        pytest.skip("No directories in lakehouse root")
    sub_paths = await client.dfs.list_paths(workspace_name, dirs[0].name)
    assert isinstance(sub_paths, list)


async def test_dfs_guid_and_friendly_return_same_dirs(
    client, workspace_id, lakehouse_id, workspace_name, lakehouse_display_path
):
    """GUID and friendly-name paths should return the same root directories."""
    guid_paths = await client.dfs.list_paths(workspace_id, lakehouse_id)
    named_paths = await client.dfs.list_paths(workspace_name, lakehouse_display_path)

    guid_dirs = {p.name.split("/")[-1] for p in guid_paths if p.is_directory}
    named_dirs = {p.name.split("/")[-1] for p in named_paths if p.is_directory}
    assert guid_dirs == named_dirs, f"Mismatch: GUID={guid_dirs}, named={named_dirs}"


# ── Delta table metadata (GUID paths) ──────────────────────────────────


async def test_delta_metadata_guid_path(client, workspace_id, lakehouse_id, table_name):
    """Read Delta table metadata via GUID workspace + GUID item."""
    info = await client.delta.get_metadata(workspace_id, lakehouse_id, table_name)
    assert info.version >= 0
    assert len(info.schema_) > 0
    assert info.num_files >= 0


async def test_delta_columns_guid_path(client, workspace_id, lakehouse_id, table_name):
    """Delta table schema columns have name and type (GUID path)."""
    info = await client.delta.get_metadata(workspace_id, lakehouse_id, table_name)
    for col in info.schema_:
        assert col.name, "Column has empty name"
        assert col.type, "Column has empty type"


# ── Delta table metadata (friendly-name paths) ─────────────────────────


async def test_delta_metadata_friendly_path(
    client, workspace_name, lakehouse_display_path, table_name
):
    """Read Delta table metadata via workspace name + DisplayName.Type."""
    info = await client.delta.get_metadata(workspace_name, lakehouse_display_path, table_name)
    assert info.version >= 0
    assert len(info.schema_) > 0
    assert info.num_files >= 0


async def test_delta_columns_friendly_path(
    client, workspace_name, lakehouse_display_path, table_name
):
    """Delta table schema columns have name and type (friendly path)."""
    info = await client.delta.get_metadata(workspace_name, lakehouse_display_path, table_name)
    for col in info.schema_:
        assert col.name, "Column has empty name"
        assert col.type, "Column has empty type"


async def test_delta_guid_and_friendly_return_same_schema(
    client, workspace_id, lakehouse_id, workspace_name, lakehouse_display_path, table_name
):
    """GUID and friendly-name paths should return identical schema."""
    guid_info = await client.delta.get_metadata(workspace_id, lakehouse_id, table_name)
    named_info = await client.delta.get_metadata(workspace_name, lakehouse_display_path, table_name)

    guid_cols = [(c.name, c.type) for c in guid_info.schema_]
    named_cols = [(c.name, c.type) for c in named_info.schema_]
    assert guid_cols == named_cols
    assert guid_info.version == named_info.version
