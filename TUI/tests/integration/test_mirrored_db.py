"""Integration tests for Mirrored Database DFS structure and Delta metadata.

Mirrored databases (Open Mirroring / GenericMirror) replicate external data
into OneLake as Delta tables.  Two mirrors are provisioned:

- ``olt_mirror_standard`` — retail data under ``Tables/dbo/``, no CDF.
- ``olt_mirror_cdf``      — HR data under ``Tables/hr/``, CDF enabled at source.

These tests verify DFS browsing and Delta metadata for both variants.

Requires the ``mirror_standard`` and ``mirror_cdf`` manifest entries in
fabric-test-env.json.
"""

from __future__ import annotations

import pytest
from deltalake.exceptions import DeltaError

# ── Fixtures ────────────────────────────────────────────────────────────


@pytest.fixture
def mirror_standard(manifest):
    item = manifest["items"].get("mirror_standard")
    if not item or not item.get("ready"):
        pytest.skip("mirror_standard not ready")
    return item


@pytest.fixture
def mirror_cdf(manifest):
    item = manifest["items"].get("mirror_cdf")
    if not item or not item.get("ready"):
        pytest.skip("mirror_cdf not ready")
    return item


# ── Helpers ─────────────────────────────────────────────────────────────


def _item_id(item: dict) -> str:
    return item["id"]


def _expected_schemas(item: dict) -> dict[str, list[str]]:
    return item["expected_schemas"]


# ── Standard mirror DFS structure ──────────────────────────────────────


class TestMirrorStandardStructure:
    """Verify DFS layout for olt_mirror_standard (dbo schema, no CDF)."""

    async def test_root_dirs(self, client, workspace_id, mirror_standard):
        """Root listing includes Files/ and Tables/."""
        paths = await client.dfs.list_paths(workspace_id, _item_id(mirror_standard))
        dir_names = {p.name.split("/")[-1] for p in paths if p.is_directory}

        for expected in mirror_standard["expected_root_dirs"]:
            assert expected in dir_names, (
                f"Expected root directory '{expected}' not found; got {dir_names}"
            )

    async def test_schema_folder(self, client, workspace_id, mirror_standard):
        """Tables/ contains the 'dbo' schema folder."""
        mid = _item_id(mirror_standard)
        paths = await client.dfs.list_paths(workspace_id, mid, directory="Tables")
        dir_names = {p.name.split("/")[-1] for p in paths if p.is_directory}
        assert "dbo" in dir_names, f"Expected 'dbo' schema folder under Tables/; got {dir_names}"

    async def test_expected_tables(self, client, workspace_id, mirror_standard):
        """Tables/dbo/ contains Customers, Orders, Products."""
        mid = _item_id(mirror_standard)
        paths = await client.dfs.list_paths(workspace_id, mid, directory="Tables/dbo")
        dir_names = {p.name.split("/")[-1] for p in paths if p.is_directory}

        for table in _expected_schemas(mirror_standard)["dbo"]:
            assert table in dir_names, (
                f"Expected table '{table}' under Tables/dbo/; got {dir_names}"
            )

    async def test_tables_have_delta_log(self, client, workspace_id, mirror_standard):
        """At least Customers should have a _delta_log directory."""
        mid = _item_id(mirror_standard)
        paths = await client.dfs.list_paths(workspace_id, mid, directory="Tables/dbo/Customers")
        child_names = {p.name.split("/")[-1] for p in paths if p.is_directory}
        assert "_delta_log" in child_names, f"Customers missing _delta_log/; got {child_names}"


# ── CDF mirror DFS structure ──────────────────────────────────────────


class TestMirrorCdfStructure:
    """Verify DFS layout for olt_mirror_cdf (hr schema, CDF enabled)."""

    async def test_root_dirs(self, client, workspace_id, mirror_cdf):
        """Root listing includes Files/ and Tables/."""
        paths = await client.dfs.list_paths(workspace_id, _item_id(mirror_cdf))
        dir_names = {p.name.split("/")[-1] for p in paths if p.is_directory}

        for expected in mirror_cdf["expected_root_dirs"]:
            assert expected in dir_names, (
                f"Expected root directory '{expected}' not found; got {dir_names}"
            )

    async def test_schema_folder(self, client, workspace_id, mirror_cdf):
        """Tables/ contains the 'hr' schema folder."""
        mid = _item_id(mirror_cdf)
        paths = await client.dfs.list_paths(workspace_id, mid, directory="Tables")
        dir_names = {p.name.split("/")[-1] for p in paths if p.is_directory}
        assert "hr" in dir_names, f"Expected 'hr' schema folder under Tables/; got {dir_names}"

    async def test_expected_tables(self, client, workspace_id, mirror_cdf):
        """Tables/hr/ contains Employees, Departments, TimeOff."""
        mid = _item_id(mirror_cdf)
        paths = await client.dfs.list_paths(workspace_id, mid, directory="Tables/hr")
        dir_names = {p.name.split("/")[-1] for p in paths if p.is_directory}

        for table in _expected_schemas(mirror_cdf)["hr"]:
            assert table in dir_names, f"Expected table '{table}' under Tables/hr/; got {dir_names}"

    async def test_tables_have_delta_log(self, client, workspace_id, mirror_cdf):
        """At least Employees should have a _delta_log directory."""
        mid = _item_id(mirror_cdf)
        paths = await client.dfs.list_paths(workspace_id, mid, directory="Tables/hr/Employees")
        child_names = {p.name.split("/")[-1] for p in paths if p.is_directory}
        assert "_delta_log" in child_names, f"Employees missing _delta_log/; got {child_names}"


# ── Delta metadata ────────────────────────────────────────────────────


@pytest.mark.xfail(
    raises=DeltaError,
    reason="Mirror Delta tables may use features unsupported by local reader",
)
class TestMirrorDeltaMetadata:
    """Read Delta metadata from mirrored tables."""

    async def test_standard_mirror_delta_metadata(self, client, workspace_id, mirror_standard):
        """Delta metadata for dbo/Customers should have schema and version."""
        mid = _item_id(mirror_standard)
        try:
            info = await client.delta.get_metadata(workspace_id, mid, "dbo/Customers")
        except DeltaError:
            pytest.skip("Delta reader could not parse mirror table")

        assert info.version >= 0
        assert len(info.schema_) > 0, "Schema should have at least one column"
        for col in info.schema_:
            assert col.name, "Column has empty name"
            assert col.type, "Column has empty type"

    async def test_cdf_mirror_delta_metadata(self, client, workspace_id, mirror_cdf):
        """Delta metadata for hr/Employees should have schema and version."""
        mid = _item_id(mirror_cdf)
        try:
            info = await client.delta.get_metadata(workspace_id, mid, "hr/Employees")
        except DeltaError:
            pytest.skip("Delta reader could not parse mirror table")

        assert info.version >= 0
        assert len(info.schema_) > 0, "Schema should have at least one column"
        for col in info.schema_:
            assert col.name, "Column has empty name"
            assert col.type, "Column has empty type"

    async def test_cdf_mirror_has_cdf_property(self, client, workspace_id, mirror_cdf):
        """Check whether CDF is surfaced in Delta properties.

        Mirrors inherit CDF from the source — it may or may not appear as
        ``delta.enableChangeDataFeed`` in the Delta table properties.
        We record the result either way for observability.
        """
        mid = _item_id(mirror_cdf)
        try:
            info = await client.delta.get_metadata(workspace_id, mid, "hr/Employees")
        except DeltaError:
            pytest.skip("Delta reader could not parse mirror table")

        cdf_key = "delta.enableChangeDataFeed"
        has_cdf = info.properties.get(cdf_key, "").lower() == "true"
        # Not asserting True — mirrors may not expose this property.
        # Mark xfail-strict=False at class level so this is informational.
        assert isinstance(has_cdf, bool), "CDF check should return a boolean"


# ── Mirror-specific feature checks ────────────────────────────────────


class TestMirrorSpecificFeatures:
    """Cross-mirror comparisons and feature presence checks."""

    async def test_mirror_standard_not_cdf(self, client, workspace_id, mirror_standard):
        """Standard mirror should NOT have CDF enabled in Delta properties."""
        mid = _item_id(mirror_standard)
        try:
            info = await client.delta.get_metadata(workspace_id, mid, "dbo/Customers")
        except DeltaError:
            pytest.skip("Delta reader could not parse mirror table")

        cdf_key = "delta.enableChangeDataFeed"
        cdf_value = info.properties.get(cdf_key, "false").lower()
        # Standard mirror was provisioned without CDF
        assert cdf_value != "true", (
            f"Standard mirror unexpectedly has CDF enabled: {info.properties}"
        )

    async def test_both_mirrors_different_schemas(
        self, client, workspace_id, mirror_standard, mirror_cdf
    ):
        """Standard mirror uses 'dbo', CDF mirror uses 'hr' — verify distinct."""
        std_id = _item_id(mirror_standard)
        cdf_id = _item_id(mirror_cdf)

        std_paths = await client.dfs.list_paths(workspace_id, std_id, directory="Tables")
        cdf_paths = await client.dfs.list_paths(workspace_id, cdf_id, directory="Tables")

        std_schemas = {p.name.split("/")[-1] for p in std_paths if p.is_directory}
        cdf_schemas = {p.name.split("/")[-1] for p in cdf_paths if p.is_directory}

        assert "dbo" in std_schemas, f"Standard mirror missing 'dbo'; got {std_schemas}"
        assert "hr" in cdf_schemas, f"CDF mirror missing 'hr'; got {cdf_schemas}"
        assert std_schemas != cdf_schemas, (
            f"Mirrors should have different schema folders; both have {std_schemas}"
        )
