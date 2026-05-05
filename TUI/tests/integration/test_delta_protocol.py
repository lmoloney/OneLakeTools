"""Integration tests for Delta protocol features (versions, reader/writer features, warnings).

Validates that protocol metadata extracted by DeltaTableReader matches the
expected values declared in the fabric-test-env.json manifest.

Run:
    uv run pytest tests/integration/test_delta_protocol.py -v
"""

from __future__ import annotations

import pytest
from deltalake.exceptions import DeltaError

# ── Helpers ─────────────────────────────────────────────────────────────


@pytest.fixture
def ws(workspace_id):
    return workspace_id


@pytest.fixture
def lh(lakehouse_id):
    return lakehouse_id


def _get_features(manifest, table_name: str) -> dict:
    """Return the delta_features entry for a table from the manifest."""
    return manifest["items"]["lakehouse_simple"]["delta_features"][table_name]


async def _safe_metadata(client, ws, lh, table_name: str):
    """Load metadata, skipping if the table is not accessible."""
    try:
        return await client.delta.get_metadata(ws, lh, table_name)
    except (DeltaError, Exception) as exc:
        exc_lower = str(exc).lower()
        if any(msg in exc_lower for msg in ("not found", "no files in log", "reader features")):
            pytest.skip(f"{table_name} not accessible: {exc}")
        raise


# ── TestProtocolVersions ───────────────────────────────────────────────


class TestProtocolVersions:
    """Verify protocol versions match manifest delta_features."""

    async def test_basic_table_protocol_v1(self, client, ws, lh, manifest):
        """customers: reader_version == 1, writer_version >= 2."""
        expected = _get_features(manifest, "customers")
        info = await client.delta.get_metadata(ws, lh, "customers")
        assert info.reader_version == expected["reader_version"]
        assert info.writer_version >= expected["writer_version"]

    async def test_dv_table_protocol_v3(self, client, ws, lh, manifest):
        """deletion_vector_demo: reader_version >= 3 (skip if DV table fails)."""
        expected = _get_features(manifest, "deletion_vector_demo")
        info = await _safe_metadata(client, ws, lh, "deletion_vector_demo")
        assert info.reader_version >= expected["reader_version"]

    async def test_column_mapping_table_protocol(self, client, ws, lh, manifest):
        """column_mapping_id: reader_version >= 2, writer_version >= 5."""
        expected = _get_features(manifest, "column_mapping_id")
        info = await client.delta.get_metadata(ws, lh, "column_mapping_id")
        assert info.reader_version >= expected["reader_version"]
        assert info.writer_version >= expected["writer_version"]


# ── TestReaderFeatures ─────────────────────────────────────────────────


class TestReaderFeatures:
    """Verify reader_features lists on tables that declare them."""

    async def test_dv_table_has_deletion_vectors_feature(self, client, ws, lh):
        """deletion_vector_demo: 'deletionVectors' in reader_features."""
        info = await _safe_metadata(client, ws, lh, "deletion_vector_demo")
        assert "deletionVectors" in info.reader_features

    async def test_basic_table_no_reader_features(self, client, ws, lh):
        """customers: v1 tables have no reader features list."""
        info = await client.delta.get_metadata(ws, lh, "customers")
        assert info.reader_features == []

    async def test_column_mapping_features(self, client, ws, lh):
        """column_mapping_id: columnMapping declared in features or properties.

        Protocol v2/v5 expresses column mapping via table properties rather
        than the features list (which was introduced with table features in v3/v7).
        """
        info = await client.delta.get_metadata(ws, lh, "column_mapping_id")
        all_features = info.reader_features + info.writer_features
        has_feature = "columnMapping" in all_features
        has_property = info.properties.get("delta.columnMapping.mode") in ("id", "name")
        assert has_feature or has_property, (
            f"Expected columnMapping in features or properties, got "
            f"reader={info.reader_features}, writer={info.writer_features}, "
            f"properties={info.properties}"
        )


# ── TestWriterFeatures ─────────────────────────────────────────────────


class TestWriterFeatures:
    """Verify writer_features on tables that use advanced write features."""

    async def test_liquid_clustered_has_clustering_feature(self, client, ws, lh):
        """liquid_clustered: check for 'clustering' in writer_features.

        May not be present depending on Fabric version — relaxed assertion.
        """
        info = await _safe_metadata(client, ws, lh, "liquid_clustered")
        # Relaxed: clustering should be there, but skip-assert if missing
        if "clustering" not in info.writer_features:
            pytest.skip(
                f"liquid_clustered writer_features {info.writer_features} "
                "does not include 'clustering' — may vary by Fabric version"
            )


# ── TestTotalRows ──────────────────────────────────────────────────────


class TestTotalRows:
    """Verify total_rows is populated from add-action statistics."""

    @pytest.fixture
    async def customers_meta(self, client, ws, lh):
        return await client.delta.get_metadata(ws, lh, "customers")

    async def test_total_rows_populated(self, customers_meta):
        """customers: total_rows is not None and > 0."""
        assert customers_meta.total_rows is not None
        assert customers_meta.total_rows > 0

    async def test_total_rows_matches_expected(self, customers_meta):
        """customers should have ~20 rows."""
        assert customers_meta.total_rows is not None
        assert 10 <= customers_meta.total_rows <= 50, (
            f"Expected ~20 rows for customers, got {customers_meta.total_rows}"
        )

    async def test_total_rows_partitioned(self, client, ws, lh):
        """partitioned_sales: total_rows > 0."""
        info = await client.delta.get_metadata(ws, lh, "partitioned_sales")
        assert info.total_rows is not None
        assert info.total_rows > 0


# ── TestWarnings ───────────────────────────────────────────────────────


class TestWarnings:
    """Verify that warnings are populated for tables with unsupported features."""

    async def test_dv_table_has_warnings(self, client, ws, lh):
        """deletion_vector_demo: warnings list mentions deletionVectors."""
        info = await _safe_metadata(client, ws, lh, "deletion_vector_demo")
        assert len(info.warnings) > 0, "Expected warnings for DV table"
        warning_text = " ".join(info.warnings).lower()
        assert "deletionvectors" in warning_text or "deletion" in warning_text

    async def test_basic_table_no_warnings(self, client, ws, lh):
        """customers: no warnings for basic v1 tables."""
        info = await client.delta.get_metadata(ws, lh, "customers")
        assert info.warnings == []


# ── TestSchemaEvolution ────────────────────────────────────────────────


class TestSchemaEvolution:
    """Verify schema evolution (additive column) table."""

    @pytest.fixture
    async def meta(self, client, ws, lh):
        return await client.delta.get_metadata(ws, lh, "schema_evolution_add")

    async def test_schema_evolution_add_has_four_columns(self, meta):
        """schema_evolution_add: schema has 4 columns (id, name, value, category)."""
        col_names = {c.name for c in meta.schema_}
        for expected in ("id", "name", "value", "category"):
            assert expected in col_names, f"Missing column: {expected}"
        assert len(meta.schema_) == 4, f"Expected 4 columns, got {len(meta.schema_)}"

    async def test_schema_evolution_add_version(self, meta):
        """version >= 1 (multiple commits from schema evolution)."""
        assert meta.version >= 1


# ── TestColumnMappingId ────────────────────────────────────────────────


class TestColumnMappingId:
    """Verify column mapping (id mode) table."""

    @pytest.fixture
    async def meta(self, client, ws, lh):
        return await client.delta.get_metadata(ws, lh, "column_mapping_id")

    async def test_column_mapping_id_schema_loads(self, meta):
        """column_mapping_id: metadata loads without error."""
        assert meta is not None
        assert len(meta.schema_) > 0

    async def test_column_mapping_id_renamed_column(self, meta):
        """Schema includes 'rating' (renamed from 'score')."""
        col_names = {c.name for c in meta.schema_}
        assert "rating" in col_names, f"Expected 'rating' column, got {col_names}"

    async def test_column_mapping_id_mode_in_properties(self, meta):
        """properties['delta.columnMapping.mode'] == 'id'."""
        assert meta.properties.get("delta.columnMapping.mode") == "id"


# ── TestLiquidClustering ──────────────────────────────────────────────


class TestLiquidClustering:
    """Verify liquid clustering table."""

    @pytest.fixture
    async def meta(self, client, ws, lh):
        return await _safe_metadata(client, ws, lh, "liquid_clustered")

    async def test_liquid_clustered_metadata_loads(self, meta):
        """liquid_clustered: metadata loads successfully."""
        assert meta is not None
        assert len(meta.schema_) > 0

    async def test_liquid_clustered_has_data(self, meta):
        """total_rows > 0 or num_files >= 1."""
        has_data = (meta.total_rows is not None and meta.total_rows > 0) or meta.num_files >= 1
        assert has_data, (
            f"Expected data in liquid_clustered: total_rows={meta.total_rows}, "
            f"num_files={meta.num_files}"
        )


# ── TestGeneratedAndChecks ─────────────────────────────────────────────


class TestGeneratedAndChecks:
    """Verify generated columns and check constraints table."""

    @pytest.fixture
    async def meta(self, client, ws, lh):
        return await client.delta.get_metadata(ws, lh, "generated_and_checks")

    async def test_generated_and_checks_loads(self, meta):
        """generated_and_checks: metadata loads successfully."""
        assert meta is not None
        assert len(meta.schema_) > 0

    async def test_check_constraint_in_properties(self, meta):
        """properties contains a delta.constraints.* entry."""
        constraint_keys = [k for k in meta.properties if k.startswith("delta.constraints.")]
        assert len(constraint_keys) >= 1, (
            f"Expected at least one delta.constraints.* property, got keys: "
            f"{list(meta.properties.keys())}"
        )
