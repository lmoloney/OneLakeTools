"""Live integration tests against a real Fabric workspace.

Run with:
    ONELAKE_TEST_WORKSPACE_ID=<guid> uv run pytest tests/integration/ -v
"""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("ONELAKE_TEST_WORKSPACE_ID"),
    reason="ONELAKE_TEST_WORKSPACE_ID not set — skipping integration tests",
)


async def test_list_workspaces(client):
    """Smoke test: can we list workspaces?"""
    workspaces = await client.fabric.list_workspaces()
    assert isinstance(workspaces, list)
    assert len(workspaces) > 0
    # Verify all workspaces have IDs
    assert all(ws.id for ws in workspaces)


async def test_list_items(client, workspace_id):
    """List items in the test workspace."""
    items = await client.fabric.list_items(workspace_id)
    assert isinstance(items, list)
    # Workspace should have at least something
    print(f"Found {len(items)} items in workspace")
    for item in items[:5]:
        print(f"  {item.type}: {item.display_name} ({item.id})")


async def test_list_lakehouses(client, workspace_id):
    """List lakehouses in the test workspace."""
    lakehouses = await client.fabric.list_lakehouses(workspace_id)
    assert isinstance(lakehouses, list)
    print(f"Found {len(lakehouses)} lakehouses")


async def test_get_lakehouse(client, workspace_id, lakehouse_id):
    """Get details of a specific lakehouse."""
    lh = await client.fabric.get_lakehouse(workspace_id, lakehouse_id)
    assert lh.id == lakehouse_id
    assert lh.type == "Lakehouse"
    print(f"Lakehouse: {lh.display_name}")
    if lh.properties:
        print(f"  Tables path: {lh.properties.onelake_tables_path}")
        print(f"  Files path: {lh.properties.onelake_files_path}")


async def test_list_dfs_paths(client, workspace_id, lakehouse_name):
    """Browse files in a lakehouse via DFS."""
    paths = await client.dfs.list_paths(workspace_id, f"{lakehouse_name}.Lakehouse")
    assert isinstance(paths, list)
    print(f"Found {len(paths)} paths at root")
    for p in paths[:10]:
        kind = "DIR " if p.is_directory else "FILE"
        print(f"  {kind} {p.name} ({p.content_length or 0} bytes)")


async def test_delta_table_metadata(client, workspace_id, lakehouse_name, table_name):
    """Read Delta table metadata."""
    info = await client.delta.get_metadata(workspace_id, f"{lakehouse_name}.Lakehouse", table_name)
    assert info.version >= 0
    assert len(info.schema_) > 0
    print(f"Delta table: {info.name}, version={info.version}, files={info.num_files}")
    print(f"  Size: {info.size_bytes / 1024 / 1024:.1f} MB")
    print(f"  Columns: {[c.name for c in info.schema_]}")
    if info.partition_columns:
        print(f"  Partitioned by: {info.partition_columns}")
