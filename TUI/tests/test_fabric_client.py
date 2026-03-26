"""Tests for FabricClient."""

from __future__ import annotations

import pytest

from onelake_client.environment import PROD
from onelake_client.fabric.client import FabricClient

BASE_URL = PROD.fabric_api_url


@pytest.fixture
def mock_transport():
    """Creates an httpx.MockTransport for testing."""
    return None  # We'll use pytest-httpx instead


async def test_list_workspaces(httpx_mock, auth):
    httpx_mock.add_response(
        url=f"{BASE_URL}/workspaces",
        json={
            "value": [
                {
                    "id": "ws-001",
                    "displayName": "Test Workspace",
                    "description": "A test workspace",
                    "type": "Workspace",
                    "capacityId": "cap-001",
                },
                {
                    "id": "ws-002",
                    "displayName": "Personal",
                    "type": "PersonalWorkspace",
                },
            ]
        },
    )

    client = FabricClient(auth)
    workspaces = await client.list_workspaces()

    assert len(workspaces) == 2
    assert workspaces[0].id == "ws-001"
    assert workspaces[0].display_name == "Test Workspace"
    assert workspaces[0].capacity_id == "cap-001"
    assert workspaces[1].type == "PersonalWorkspace"

    await client.close()


async def test_list_workspaces_pagination(httpx_mock, auth):
    """Test that continuationToken pagination is followed."""
    httpx_mock.add_response(
        url=f"{BASE_URL}/workspaces",
        json={
            "value": [{"id": "ws-001", "displayName": "First", "type": "Workspace"}],
            "continuationToken": "page2token",
        },
    )
    httpx_mock.add_response(
        url=f"{BASE_URL}/workspaces?continuationToken=page2token",
        json={
            "value": [{"id": "ws-002", "displayName": "Second", "type": "Workspace"}],
        },
    )

    client = FabricClient(auth)
    workspaces = await client.list_workspaces()

    assert len(workspaces) == 2
    assert workspaces[0].id == "ws-001"
    assert workspaces[1].id == "ws-002"

    await client.close()


async def test_list_items_with_type_filter(httpx_mock, auth):
    httpx_mock.add_response(
        json={
            "value": [
                {
                    "id": "lh-001",
                    "displayName": "Sales Lakehouse",
                    "type": "Lakehouse",
                    "workspaceId": "ws-001",
                }
            ]
        },
    )

    client = FabricClient(auth)
    items = await client.list_items("ws-001", item_type="Lakehouse")

    assert len(items) == 1
    assert items[0].display_name == "Sales Lakehouse"
    assert items[0].type == "Lakehouse"

    await client.close()


async def test_get_lakehouse(httpx_mock, auth):
    httpx_mock.add_response(
        json={
            "id": "lh-001",
            "displayName": "Sales",
            "type": "Lakehouse",
            "workspaceId": "ws-001",
            "properties": {
                "oneLakeTablesPath": "abfss://ws-001@onelake.dfs.fabric.microsoft.com/lh-001/Tables",
                "oneLakeFilesPath": "abfss://ws-001@onelake.dfs.fabric.microsoft.com/lh-001/Files",
                "sqlEndpointProperties": {
                    "id": "sql-001",
                    "connectionString": "server.database.fabric.microsoft.com",
                    "provisioningStatus": "Success",
                },
            },
        },
    )

    client = FabricClient(auth)
    lh = await client.get_lakehouse("ws-001", "lh-001")

    assert lh.id == "lh-001"
    assert lh.display_name == "Sales"
    assert lh.properties is not None
    assert lh.properties.sql_endpoint_properties.id == "sql-001"

    await client.close()
