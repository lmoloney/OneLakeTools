from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from onelake_client.models.table import Column, IcebergTableInfo

if TYPE_CHECKING:
    from onelake_client.auth import OneLakeAuth

logger = logging.getLogger("onelake_client.tables.iceberg")

_ICEBERG_CATALOG_URL = "https://onelake.table.fabric.microsoft.com/iceberg"


class IcebergTableReader:
    """Reads Iceberg table metadata from OneLake via the IRC endpoint.

    Uses the `pyiceberg` library pointed at OneLake's Iceberg REST Catalog.

    Usage:
        auth = OneLakeAuth()
        reader = IcebergTableReader(auth)
        namespaces = await reader.list_namespaces("workspace-guid", "lakehouse-guid")
        tables = await reader.list_tables("workspace-guid", "lakehouse-guid", "dbo")
        info = await reader.get_metadata("workspace-guid", "lakehouse-guid", "dbo", "customers")
    """

    def __init__(self, auth: OneLakeAuth):
        self._auth = auth

    def _build_catalog_sync(self, workspace_id: str, item_id: str):
        """Build a pyiceberg REST catalog pointing at OneLake (sync)."""
        from pyiceberg.catalog import load_catalog

        token = self._auth.get_token(self._auth._env.storage_scope)
        warehouse = f"{workspace_id}/{item_id}"

        return load_catalog(
            "onelake",
            **{
                "uri": _ICEBERG_CATALOG_URL,
                "token": token,
                "warehouse": warehouse,
                "adls.account-name": "onelake",
                "adls.account-host": "onelake.blob.fabric.microsoft.com",
            },
        )

    async def list_namespaces(self, workspace_id: str, item_id: str) -> list[str]:
        """List namespaces (schemas) in a lakehouse via Iceberg catalog.

        Args:
            workspace_id: Workspace GUID.
            item_id: Lakehouse/data item GUID.

        Returns:
            List of namespace names (e.g., ["dbo"]).
        """
        catalog = await asyncio.to_thread(self._build_catalog_sync, workspace_id, item_id)
        namespaces = await asyncio.to_thread(catalog.list_namespaces)
        return [".".join(ns) for ns in namespaces]

    async def list_tables(
        self, workspace_id: str, item_id: str, namespace: str = "dbo"
    ) -> list[str]:
        """List tables in a namespace via Iceberg catalog.

        Args:
            workspace_id: Workspace GUID.
            item_id: Lakehouse/data item GUID.
            namespace: Schema/namespace name (default "dbo").

        Returns:
            List of table names.
        """
        catalog = await asyncio.to_thread(self._build_catalog_sync, workspace_id, item_id)
        tables = await asyncio.to_thread(catalog.list_tables, namespace)
        return [t[1] if isinstance(t, tuple) else str(t) for t in tables]

    async def get_metadata(
        self,
        workspace_id: str,
        item_id: str,
        namespace: str,
        table_name: str,
    ) -> IcebergTableInfo:
        """Get metadata for an Iceberg table.

        Args:
            workspace_id: Workspace GUID.
            item_id: Lakehouse/data item GUID.
            namespace: Schema/namespace name.
            table_name: Table name.

        Returns:
            IcebergTableInfo with schema, snapshot, partitioning, etc.
        """
        catalog = await asyncio.to_thread(self._build_catalog_sync, workspace_id, item_id)
        table = await asyncio.to_thread(catalog.load_table, f"{namespace}.{table_name}")

        columns: list[Column] = []
        for field in table.schema().fields:
            columns.append(
                Column(
                    name=field.name,
                    type=str(field.field_type),
                    nullable=field.optional,
                    comment=field.doc,
                )
            )

        partition_spec: list[dict] = []
        if table.spec() and table.spec().fields:
            for pf in table.spec().fields:
                partition_spec.append(
                    {
                        "source_id": pf.source_id,
                        "field_id": pf.field_id,
                        "transform": str(pf.transform),
                        "name": pf.name,
                    }
                )

        snapshot_id = None
        if table.current_snapshot():
            snapshot_id = table.current_snapshot().snapshot_id

        return IcebergTableInfo(
            name=table_name,
            schema_=columns,
            current_snapshot_id=snapshot_id,
            format_version=(table.metadata.format_version if hasattr(table, "metadata") else 2),
            location=table.location() if hasattr(table, "location") else None,
            partition_spec=partition_spec,
            properties=dict(table.properties) if table.properties else {},
        )
