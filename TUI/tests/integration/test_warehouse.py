"""Integration tests for Fabric Warehouse DFS structure and Iceberg metadata.

Warehouses differ from lakehouses:
- Root includes an ``Audit/`` directory (warehouse-specific).
- Tables under ``Tables/dbo/`` have both ``_delta_log/`` AND ``metadata/``
  (Iceberg metadata).
"""

from __future__ import annotations

import pytest

# ── Root directory structure ────────────────────────────────────────────


class TestWarehouseRootStructure:
    """Verify the DFS root of a warehouse contains expected directories."""

    async def test_root_contains_expected_dirs(self, client, workspace_id, warehouse_item):
        """Root listing includes Audit/, Files/, and Tables/."""
        wh_id = warehouse_item["id"]
        paths = await client.dfs.list_paths(workspace_id, wh_id)
        dir_names = {p.name.split("/")[-1] for p in paths if p.is_directory}

        for expected in warehouse_item["expected_root_dirs"]:
            assert expected in dir_names, (
                f"Expected root directory '{expected}' not found; got {dir_names}"
            )

    async def test_audit_directory_present(self, client, workspace_id, warehouse_item):
        """Audit/ is warehouse-specific (lakehouses don't have it)."""
        wh_id = warehouse_item["id"]
        paths = await client.dfs.list_paths(workspace_id, wh_id)
        dir_names = {p.name.split("/")[-1] for p in paths if p.is_directory}
        assert "Audit" in dir_names, "Warehouse root should include Audit/ directory"


# ── Table directory structure ───────────────────────────────────────────


class TestWarehouseTableStructure:
    """Verify schema folders and table sub-directories inside Tables/."""

    async def test_tables_contains_dbo_schema(self, client, workspace_id, warehouse_item):
        """Tables/ should contain the 'dbo' schema folder."""
        wh_id = warehouse_item["id"]
        tables_dir = f"{wh_id}/Tables"
        paths = await client.dfs.list_paths(workspace_id, tables_dir)
        dir_names = {p.name.split("/")[-1] for p in paths if p.is_directory}
        assert "dbo" in dir_names, f"Expected 'dbo' schema folder under Tables/; got {dir_names}"

    async def test_dbo_contains_expected_tables(self, client, workspace_id, warehouse_item):
        """Tables/dbo/ contains expected table directories."""
        wh_id = warehouse_item["id"]
        dbo_dir = f"{wh_id}/Tables/dbo"
        paths = await client.dfs.list_paths(workspace_id, dbo_dir)
        dir_names = {p.name.split("/")[-1] for p in paths if p.is_directory}

        for schema_name, table_names in warehouse_item["expected_schemas"].items():
            if schema_name != "dbo":
                continue
            for table in table_names:
                assert table in dir_names, (
                    f"Expected table '{table}' under Tables/dbo/; got {dir_names}"
                )

    async def test_table_has_delta_log(self, client, workspace_id, warehouse_item):
        """Each expected table directory has a _delta_log subdirectory."""
        wh_id = warehouse_item["id"]
        for table in warehouse_item["expected_schemas"]["dbo"]:
            table_dir = f"{wh_id}/Tables/dbo/{table}"
            paths = await client.dfs.list_paths(workspace_id, table_dir)
            child_names = {p.name.split("/")[-1] for p in paths if p.is_directory}
            assert "_delta_log" in child_names, (
                f"Table '{table}' missing _delta_log/; got {child_names}"
            )

    async def test_table_has_iceberg_metadata(self, client, workspace_id, warehouse_item):
        """Each expected table directory has a metadata subdirectory (Iceberg)."""
        wh_id = warehouse_item["id"]
        for table in warehouse_item["expected_schemas"]["dbo"]:
            table_dir = f"{wh_id}/Tables/dbo/{table}"
            paths = await client.dfs.list_paths(workspace_id, table_dir)
            child_names = {p.name.split("/")[-1] for p in paths if p.is_directory}
            assert "metadata" in child_names, (
                f"Table '{table}' missing metadata/; got {child_names}"
            )


# ── Iceberg metadata reader ────────────────────────────────────────────


@pytest.mark.xfail(reason="Iceberg REST catalog may not be enabled")
class TestWarehouseIceberg:
    """Verify Iceberg catalog access for warehouse tables."""

    async def test_list_namespaces_contains_dbo(self, client, workspace_id, warehouse_item):
        """list_namespaces returns a list containing 'dbo'."""
        wh_id = warehouse_item["id"]
        namespaces = await client.iceberg.list_namespaces(workspace_id, wh_id)
        assert "dbo" in namespaces, f"Expected 'dbo' in namespaces; got {namespaces}"

    async def test_list_tables_contains_expected(self, client, workspace_id, warehouse_item):
        """list_tables for 'dbo' returns expected table names."""
        wh_id = warehouse_item["id"]
        tables = await client.iceberg.list_tables(workspace_id, wh_id, "dbo")
        for expected in warehouse_item["expected_schemas"]["dbo"]:
            assert expected in tables, (
                f"Expected table '{expected}' in Iceberg listing; got {tables}"
            )

    async def test_get_metadata_dim_product_has_schema(self, client, workspace_id, warehouse_item):
        """get_metadata for dim_product returns non-empty schema."""
        wh_id = warehouse_item["id"]
        info = await client.iceberg.get_metadata(workspace_id, wh_id, "dbo", "dim_product")
        assert len(info.schema_) > 0, "dim_product should have at least one column"
        for col in info.schema_:
            assert col.name, "Column has empty name"
            assert col.type, "Column has empty type"

    async def test_get_metadata_fact_sales_has_columns(self, client, workspace_id, warehouse_item):
        """get_metadata for fact_sales returns columns."""
        wh_id = warehouse_item["id"]
        info = await client.iceberg.get_metadata(workspace_id, wh_id, "dbo", "fact_sales")
        assert len(info.schema_) > 0, "fact_sales should have at least one column"
        for col in info.schema_:
            assert col.name, "Column has empty name"
            assert col.type, "Column has empty type"
