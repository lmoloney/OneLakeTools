from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from onelake_client.models.table import Column, DeltaTableInfo

if TYPE_CHECKING:
    from onelake_client.auth import OneLakeAuth

logger = logging.getLogger("onelake_client.tables.delta")


def _build_table_uri(workspace: str, item_path: str, table_name: str, dfs_host: str) -> str:
    """Build an abfss:// URI for a Delta table on OneLake.

    Args:
        workspace: Workspace name or GUID.
        item_path: Item path like "MyLakehouse.Lakehouse".
        table_name: Table name under the Tables/ directory.
        dfs_host: DFS endpoint hostname (varies per ring).

    Returns:
        abfss:// URI pointing to the Delta table.
    """
    return f"abfss://{workspace}@{dfs_host}/{item_path}/Tables/{table_name}"


def _schema_to_columns(schema) -> list[Column]:
    """Convert a deltalake Schema to our Column model."""
    columns: list[Column] = []
    # deltalake >= 0.18: Schema is not directly iterable, use .fields
    fields = schema.fields if hasattr(schema, "fields") else schema
    for field in fields:
        columns.append(
            Column(
                name=field.name,
                type=str(field.type),
                nullable=field.nullable,
                metadata=field.metadata if field.metadata else None,
            )
        )
    return columns


class DeltaTableReader:
    """Reads Delta table metadata from OneLake.

    Uses the `deltalake` library (delta-rs Python bindings) with OneLake
    storage options for authentication.

    Usage:
        auth = OneLakeAuth()
        reader = DeltaTableReader(auth, dfs_host="onelake.dfs.fabric.microsoft.com")
        info = await reader.get_metadata("MyWorkspace", "MyLakehouse.Lakehouse", "customers")
    """

    def __init__(self, auth: OneLakeAuth, dfs_host: str = "onelake.dfs.fabric.microsoft.com"):
        self._auth = auth
        self._dfs_host = dfs_host

    def _get_storage_options(self) -> dict:
        """Get fresh storage options with a current token."""
        return self._auth.storage_options()

    def _load_table_sync(self, uri: str):
        """Synchronously load a DeltaTable (called via to_thread)."""
        from deltalake import DeltaTable

        return DeltaTable(uri, storage_options=self._get_storage_options())

    async def get_metadata(self, workspace: str, item_path: str, table_name: str) -> DeltaTableInfo:
        """Get metadata for a Delta table.

        Args:
            workspace: Workspace name or GUID.
            item_path: Item path like "MyLakehouse.Lakehouse".
            table_name: Table name under Tables/.

        Returns:
            DeltaTableInfo with schema, version, file count, size, etc.
        """
        uri = _build_table_uri(workspace, item_path, table_name, self._dfs_host)

        dt = await asyncio.to_thread(self._load_table_sync, uri)

        schema = _schema_to_columns(dt.schema())
        version = dt.version()
        files = dt.files()
        metadata = dt.metadata()

        # Sum file sizes from add actions
        size_bytes = 0
        add_actions = await asyncio.to_thread(dt.get_add_actions, flatten=True)
        if hasattr(add_actions, "to_pydict"):
            size_dict = add_actions.to_pydict()
            if "size_bytes" in size_dict:
                size_bytes = sum(size_dict["size_bytes"])
            elif "size" in size_dict:
                size_bytes = sum(size_dict["size"])

        return DeltaTableInfo(
            name=metadata.name or table_name,
            schema_=schema,
            version=version,
            num_files=len(files),
            size_bytes=size_bytes,
            partition_columns=list(metadata.partition_columns),
            properties=dict(metadata.configuration) if metadata.configuration else {},
            description=metadata.description,
        )

    async def list_files(self, workspace: str, item_path: str, table_name: str) -> list[str]:
        """List data files in a Delta table.

        Returns:
            List of relative file paths (parquet files).
        """
        uri = _build_table_uri(workspace, item_path, table_name, self._dfs_host)
        dt = await asyncio.to_thread(self._load_table_sync, uri)
        return dt.files()
