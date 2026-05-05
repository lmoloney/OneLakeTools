"""Integration test configuration.

These tests require real Azure/Fabric credentials and a live workspace.

Configuration (in priority order):
    1. Manifest file: ~/.config/onelaketools/fabric-test-env.json
       (or path in ONELAKE_TEST_ENV_FILE env var)
    2. Environment variables (CI fallback):
       ONELAKE_TEST_WORKSPACE_ID, ONELAKE_TEST_LAKEHOUSE_ID, etc.

Authentication:
    Local: `az login` (DefaultAzureCredential picks it up)
    CI: Set AZURE_TENANT_ID + AZURE_CLIENT_ID + federated OIDC token
        (via azure/login GitHub Action)
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from onelake_client import OneLakeClient

# ── Manifest loading ────────────────────────────────────────────────────

_REPO_MANIFEST = Path(__file__).parent / "fabric-test-env.json"
_USER_MANIFEST = Path.home() / ".config" / "onelaketools" / "fabric-test-env.json"


def _load_manifest() -> dict | None:
    """Load fabric-test-env.json — env override → in-repo → user home.

    The manifest is the single source of truth for the test environment.
    It defines workspace ID, item IDs, expected tables/files, and per-table
    Delta feature metadata. All fixtures (lakehouse_schema_item, warehouse_item,
    etc.) read from this manifest — they are not individually overridable via
    env vars because item IDs must be consistent within a workspace.

    To point at a different test environment entirely, set ONELAKE_TEST_ENV_FILE
    to your own manifest file. Individual env vars (ONELAKE_TEST_WORKSPACE_ID,
    ONELAKE_TEST_LAKEHOUSE_ID, etc.) override the primary lakehouse config only.
    """
    env_path = os.environ.get("ONELAKE_TEST_ENV_FILE")
    for candidate in [
        Path(env_path) if env_path else None,
        _REPO_MANIFEST,
        _USER_MANIFEST,
    ]:
        if candidate and candidate.exists():
            return json.loads(candidate.read_text())
    return None


_MANIFEST = _load_manifest()


def _manifest_item(key: str) -> dict | None:
    """Get an item from the manifest by its key (e.g. 'lakehouse_simple')."""
    if _MANIFEST is None:
        return None
    return _MANIFEST.get("items", {}).get(key)


# ── Resolve config: env vars take priority, then manifest ───────────────

WORKSPACE_ID = os.environ.get("ONELAKE_TEST_WORKSPACE_ID") or (
    _MANIFEST["workspace"]["id"] if _MANIFEST else None
)

_lh_simple = _manifest_item("lakehouse_simple")
LAKEHOUSE_ID = os.environ.get("ONELAKE_TEST_LAKEHOUSE_ID") or (
    _lh_simple["id"] if _lh_simple else None
)
LAKEHOUSE_NAME = os.environ.get("ONELAKE_TEST_LAKEHOUSE_NAME") or (
    _lh_simple["name"] if _lh_simple else None
)

# Default table for single-table tests
TABLE_NAME = os.environ.get("ONELAKE_TEST_TABLE_NAME") or (
    _lh_simple["expected_tables"][0] if _lh_simple and _lh_simple.get("expected_tables") else None
)


def _has_config() -> bool:
    return WORKSPACE_ID is not None


# All integration tests get this marker + skip if no config available
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not _has_config(),
        reason="No manifest or ONELAKE_TEST_WORKSPACE_ID — skipping integration tests",
    ),
]


# ── Credential ──────────────────────────────────────────────────────────


def _create_credential():
    """Create the best available credential for the current environment.

    CI (GitHub Actions with OIDC): azure/login sets AZURE_TENANT_ID,
    AZURE_CLIENT_ID, and ACTIONS_ID_TOKEN_REQUEST_URL. DefaultAzureCredential
    picks up the federated token automatically.

    Local: `az login` is used via DefaultAzureCredential.
    """
    from azure.identity import DefaultAzureCredential

    return DefaultAzureCredential()


# ── Fixtures ────────────────────────────────────────────────────────────


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
def workspace_name():
    """Workspace display name (for friendly-name path tests)."""
    env_name = os.environ.get("ONELAKE_TEST_WORKSPACE_NAME")
    if env_name:
        return env_name
    if not _MANIFEST:
        pytest.skip("No manifest — workspace_name requires manifest")
    return _MANIFEST["workspace"]["name"]


@pytest.fixture
def manifest():
    """Provide the full manifest dict (skip if not available)."""
    if _MANIFEST is None:
        pytest.skip("No fabric-test-env.json manifest found")
    return _MANIFEST


@pytest.fixture
def lakehouse_id():
    if not LAKEHOUSE_ID:
        pytest.skip("No lakehouse_simple in manifest or ONELAKE_TEST_LAKEHOUSE_ID not set")
    return LAKEHOUSE_ID


@pytest.fixture
def lakehouse_name():
    if not LAKEHOUSE_NAME:
        pytest.skip("No lakehouse_simple in manifest or ONELAKE_TEST_LAKEHOUSE_NAME not set")
    return LAKEHOUSE_NAME


@pytest.fixture
def lakehouse_display_path():
    """Item path in DisplayName.Type format (e.g. 'olt_lakehouse_simple.Lakehouse')."""
    if not LAKEHOUSE_NAME:
        pytest.skip("No lakehouse_simple in manifest")
    return f"{LAKEHOUSE_NAME}.Lakehouse"


@pytest.fixture
def table_name():
    if not TABLE_NAME:
        pytest.skip("No table_name configured")
    return TABLE_NAME


@pytest.fixture
def lakehouse_simple_item():
    """The lakehouse_simple manifest entry."""
    item = _manifest_item("lakehouse_simple")
    if not item:
        pytest.skip("lakehouse_simple not in manifest")
    return item


@pytest.fixture
def lakehouse_schema_item():
    """The lakehouse_schema manifest entry."""
    item = _manifest_item("lakehouse_schema")
    if not item:
        pytest.skip("lakehouse_schema not in manifest")
    return item


@pytest.fixture
def warehouse_item():
    """The warehouse manifest entry."""
    item = _manifest_item("warehouse")
    if not item:
        pytest.skip("warehouse not in manifest")
    return item
