from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import httpx

from onelake_client._http import create_client, paginate_fabric, request_with_retry
from onelake_client.models import Item, Lakehouse, Workspace

if TYPE_CHECKING:
    from onelake_client.auth import OneLakeAuth
    from onelake_client.environment import FabricEnvironment


class FabricClient:
    """Client for the Microsoft Fabric REST API (control plane).

    Handles workspace and item management operations.
    """

    def __init__(
        self,
        auth: OneLakeAuth,
        *,
        env: FabricEnvironment | None = None,
        client: httpx.AsyncClient | None = None,
    ):
        self._auth = auth
        if env is None:
            from onelake_client.environment import DEFAULT_ENVIRONMENT

            env = DEFAULT_ENVIRONMENT
        self._base_url = env.fabric_api_url
        self._client = client
        self._owns_client = client is None
        self._client_lock = asyncio.Lock()

    async def _get_client(self) -> httpx.AsyncClient:
        async with self._client_lock:
            if self._client is None:
                self._client = create_client(base_url=self._base_url)
                self._owns_client = True
            return self._client

    async def close(self) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def list_workspaces(self) -> list[Workspace]:
        """List all accessible workspaces.

        GET /v1/workspaces — paginated via continuationToken.
        """
        client = await self._get_client()
        headers = self._auth.fabric_headers()
        workspaces: list[Workspace] = []

        async for item in paginate_fabric(client, f"{self._base_url}/workspaces", headers=headers):
            workspaces.append(Workspace.model_validate(item))

        return workspaces

    async def list_items(
        self,
        workspace_id: str,
        *,
        item_type: str | None = None,
    ) -> list[Item]:
        """List items in a workspace, optionally filtered by type.

        GET /v1/workspaces/{workspaceId}/items?type={type}

        Args:
            workspace_id: The workspace GUID.
            item_type: Optional filter — e.g., "Lakehouse", "Warehouse", "Notebook".
        """
        client = await self._get_client()
        headers = self._auth.fabric_headers()
        params: dict[str, str] = {}
        if item_type:
            params["type"] = item_type

        items: list[Item] = []
        async for raw in paginate_fabric(
            client,
            f"{self._base_url}/workspaces/{workspace_id}/items",
            headers=headers,
            params=params,
        ):
            items.append(Item.model_validate(raw))

        return items

    async def list_lakehouses(self, workspace_id: str) -> list[Lakehouse]:
        """List all lakehouses in a workspace.

        GET /v1/workspaces/{workspaceId}/lakehouses
        """
        client = await self._get_client()
        headers = self._auth.fabric_headers()
        lakehouses: list[Lakehouse] = []

        async for raw in paginate_fabric(
            client,
            f"{self._base_url}/workspaces/{workspace_id}/lakehouses",
            headers=headers,
        ):
            lakehouses.append(Lakehouse.model_validate(raw))

        return lakehouses

    async def get_lakehouse(self, workspace_id: str, lakehouse_id: str) -> Lakehouse:
        """Get details of a specific lakehouse.

        GET /v1/workspaces/{workspaceId}/lakehouses/{lakehouseId}
        Returns SQL endpoint info, OneLake paths, and properties.
        """
        client = await self._get_client()
        headers = self._auth.fabric_headers()

        response = await request_with_retry(
            client,
            "GET",
            f"{self._base_url}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}",
            headers=headers,
        )
        return Lakehouse.model_validate(response.json())
