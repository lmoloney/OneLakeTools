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
        logger.debug("Loading Delta table: %s", uri)

        dt = await asyncio.to_thread(self._load_table_sync, uri)

        schema = _schema_to_columns(dt.schema())
        version = dt.version()
        files = dt.file_uris()
        metadata = dt.metadata()

        # Sum file sizes from add actions
        size_bytes = 0
        try:
            add_actions = await asyncio.to_thread(dt.get_add_actions, flatten=True)
            if hasattr(add_actions, "to_pydict"):
                # pyarrow Table (older deltalake)
                size_dict = add_actions.to_pydict()
                size_bytes = sum(size_dict.get("size_bytes", size_dict.get("size", [])))
            elif hasattr(add_actions, "column"):
                # arro3 Table (deltalake >= 1.0)
                col_names = add_actions.column_names
                size_key = "size_bytes" if "size_bytes" in col_names else "size"
                if size_key in col_names:
                    size_bytes = sum(add_actions.column(size_key).to_pylist())
        except Exception:
            logger.debug("Could not compute table size from add actions")

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

    async def read_sample(
        self, workspace: str, item_path: str, table_name: str, *, limit: int = 100
    ):
        """Read a sample of rows from a Delta table.

        Args:
            workspace: Workspace name or GUID.
            item_path: Item path like "MyLakehouse.Lakehouse".
            table_name: Table name under Tables/.
            limit: Maximum number of rows to return.

        Returns:
            pyarrow.Table with up to ``limit`` rows.
        """
        uri = _build_table_uri(workspace, item_path, table_name, self._dfs_host)
        logger.debug("Reading sample (%d rows) from: %s", limit, uri)
        dt = await asyncio.to_thread(self._load_table_sync, uri)

        def _head():
            ds = dt.to_pyarrow_dataset()
            return ds.head(limit)

        return await asyncio.to_thread(_head)

    async def read_cdf(
        self,
        workspace: str,
        item_path: str,
        table_name: str,
        *,
        starting_version: int = 0,
        ending_version: int | None = None,
    ):
        """Read Change Data Feed records from a Delta table.

        Args:
            workspace: Workspace name or GUID.
            item_path: Item path.
            table_name: Table name under Tables/.
            starting_version: First version to include.
            ending_version: Last version to include (None = latest).

        Returns:
            pyarrow.Table with CDF records including _change_type,
            _commit_version, and _commit_timestamp columns.
        """
        uri = _build_table_uri(workspace, item_path, table_name, self._dfs_host)
        logger.debug("Reading CDF from: %s (v%d→%s)", uri, starting_version, ending_version)
        dt = await asyncio.to_thread(self._load_table_sync, uri)

        def _load_cdf():
            kwargs: dict = {"starting_version": starting_version}
            if ending_version is not None:
                kwargs["ending_version"] = ending_version
            cdf = dt.load_cdf(**kwargs)
            return cdf.read_all() if hasattr(cdf, "read_all") else cdf

        return await asyncio.to_thread(_load_cdf)

    async def list_files(self, workspace: str, item_path: str, table_name: str) -> list[str]:
        """List data files in a Delta table.

        Returns:
            List of relative file paths (parquet files).
        """
        uri = _build_table_uri(workspace, item_path, table_name, self._dfs_host)
        dt = await asyncio.to_thread(self._load_table_sync, uri)
        return dt.file_uris()
