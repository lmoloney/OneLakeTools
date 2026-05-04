"""Integration test configuration.

These tests require real Azure/Fabric credentials and a live workspace.

Required environment variables:
    ONELAKE_TEST_WORKSPACE_ID — GUID of a Fabric workspace to test against

Optional:
    ONELAKE_TEST_LAKEHOUSE_ID — GUID of a lakehouse (for lakehouse-specific tests)
    ONELAKE_TEST_LAKEHOUSE_NAME — Display name of the lakehouse
    ONELAKE_TEST_TABLE_NAME — Name of a Delta table to read metadata from

Authentication:
    Local: `az login` (DefaultAzureCredential picks it up)
    CI: Set AZURE_TENANT_ID + AZURE_CLIENT_ID + federated OIDC token
        (via azure/login GitHub Action)
"""

from __future__ import annotations

import os

import pytest

from onelake_client import OneLakeClient

WORKSPACE_ID = os.environ.get("ONELAKE_TEST_WORKSPACE_ID")
LAKEHOUSE_ID = os.environ.get("ONELAKE_TEST_LAKEHOUSE_ID")
LAKEHOUSE_NAME = os.environ.get("ONELAKE_TEST_LAKEHOUSE_NAME")
TABLE_NAME = os.environ.get("ONELAKE_TEST_TABLE_NAME")

# All integration tests get this marker + skip if no workspace configured
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not WORKSPACE_ID,
        reason="ONELAKE_TEST_WORKSPACE_ID not set — skipping integration tests",
    ),
]


def _create_credential():
    """Create the best available credential for the current environment.

    CI (GitHub Actions with OIDC): azure/login sets AZURE_TENANT_ID,
    AZURE_CLIENT_ID, and ACTIONS_ID_TOKEN_REQUEST_URL. DefaultAzureCredential
    picks up the federated token automatically.

    Local: `az login` is used via DefaultAzureCredential.
    """
    from azure.identity import DefaultAzureCredential

    return DefaultAzureCredential()


@pytest.fixture
async def client():
    """Provide a live OneLakeClient with auto-detected credentials."""
    credential = _create_credential()
    async with OneLakeClient(credential=credential) as c:
        yield c


@pytest.fixture
def workspace_id():
    return WORKSPACE_ID


@pytest.fixture
def lakehouse_id():
    if not LAKEHOUSE_ID:
        pytest.skip("ONELAKE_TEST_LAKEHOUSE_ID not set")
    return LAKEHOUSE_ID


@pytest.fixture
def lakehouse_name():
    if not LAKEHOUSE_NAME:
        pytest.skip("ONELAKE_TEST_LAKEHOUSE_NAME not set")
    return LAKEHOUSE_NAME


@pytest.fixture
def table_name():
    if not TABLE_NAME:
        pytest.skip("ONELAKE_TEST_TABLE_NAME not set")
    return TABLE_NAME
