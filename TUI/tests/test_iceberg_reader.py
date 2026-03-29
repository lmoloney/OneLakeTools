"""Tests for onelake_client.tables.iceberg — IcebergTableReader."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from onelake_client.models.table import IcebergTableInfo
from onelake_client.tables.iceberg import IcebergTableReader

# ── Fixtures ────────────────────────────────────────────────────────────


@pytest.fixture()
def auth():
    from onelake_client.auth import OneLakeAuth
    from tests.conftest import FakeCredential

    return OneLakeAuth(credential=FakeCredential())


def _make_mock_iceberg_field(name, field_type, optional=True, doc=None):
    f = MagicMock()
    f.name = name
    f.field_type = field_type
    f.optional = optional
    f.doc = doc
    return f


def _make_mock_iceberg_table(
    *,
    fields=None,
    snapshot_id=12345,
    format_version=2,
    location="abfss://ws@host/item/Tables/t",
    partition_fields=None,
    properties=None,
):
    """Build a mock pyiceberg Table."""
    table = MagicMock()

    if fields is None:
        fields = [
            _make_mock_iceberg_field("id", "long", optional=False),
            _make_mock_iceberg_field("name", "string", optional=True, doc="Customer name"),
        ]

    schema = MagicMock()
    schema.fields = fields
    table.schema.return_value = schema

    spec = MagicMock()
    if partition_fields:
        pf_mocks = []
        for pf in partition_fields:
            m = MagicMock()
            m.source_id = pf["source_id"]
            m.field_id = pf["field_id"]
            m.transform = pf["transform"]
            m.name = pf["name"]
            pf_mocks.append(m)
        spec.fields = pf_mocks
    else:
        spec.fields = []
    table.spec.return_value = spec

    snapshot = MagicMock()
    snapshot.snapshot_id = snapshot_id
    table.current_snapshot.return_value = snapshot if snapshot_id else None

    table.metadata = MagicMock()
    table.metadata.format_version = format_version
    table.location.return_value = location
    table.properties = properties or {"write.format.default": "parquet"}

    return table


# ── Initialization ──────────────────────────────────────────────────────


class TestIcebergReaderInit:
    """Test basic initialization."""

    def test_stores_auth(self, auth):
        reader = IcebergTableReader(auth)
        assert reader._auth is auth

    def test_uses_public_env_property(self, auth):
        """Verify _build_catalog_sync uses self._auth.env (public property)."""
        reader = IcebergTableReader(auth)
        # Access via the public property — should not raise
        env = reader._auth.env
        assert env.storage_scope == "https://storage.azure.com/.default"


# ── _build_catalog_sync ─────────────────────────────────────────────────


class TestBuildCatalog:
    """Test catalog construction."""

    def test_catalog_passes_token_and_warehouse(self, auth):
        reader = IcebergTableReader(auth)
        with patch("pyiceberg.catalog.load_catalog") as mock_load:
            mock_load.return_value = MagicMock()
            reader._build_catalog_sync("ws-guid", "item-guid")

        mock_load.assert_called_once()
        _, kwargs = mock_load.call_args
        assert kwargs["token"] == "fake-token-12345"
        assert kwargs["warehouse"] == "ws-guid/item-guid"
        assert kwargs["uri"] == "https://onelake.table.fabric.microsoft.com/iceberg"


# ── list_namespaces ─────────────────────────────────────────────────────


class TestListNamespaces:
    """Test namespace listing."""

    @pytest.mark.asyncio()
    async def test_returns_joined_namespace_names(self, auth):
        catalog = MagicMock()
        catalog.list_namespaces.return_value = [("dbo",), ("staging",)]

        reader = IcebergTableReader(auth)
        with patch.object(reader, "_build_catalog_sync", return_value=catalog):
            result = await reader.list_namespaces("ws", "item")

        assert result == ["dbo", "staging"]

    @pytest.mark.asyncio()
    async def test_multi_part_namespace(self, auth):
        catalog = MagicMock()
        catalog.list_namespaces.return_value = [("level1", "level2")]

        reader = IcebergTableReader(auth)
        with patch.object(reader, "_build_catalog_sync", return_value=catalog):
            result = await reader.list_namespaces("ws", "item")

        assert result == ["level1.level2"]

    @pytest.mark.asyncio()
    async def test_empty_namespaces(self, auth):
        catalog = MagicMock()
        catalog.list_namespaces.return_value = []

        reader = IcebergTableReader(auth)
        with patch.object(reader, "_build_catalog_sync", return_value=catalog):
            result = await reader.list_namespaces("ws", "item")

        assert result == []


# ── list_tables ─────────────────────────────────────────────────────────


class TestListTables:
    """Test table listing."""

    @pytest.mark.asyncio()
    async def test_returns_table_names_from_tuples(self, auth):
        catalog = MagicMock()
        catalog.list_tables.return_value = [("dbo", "customers"), ("dbo", "orders")]

        reader = IcebergTableReader(auth)
        with patch.object(reader, "_build_catalog_sync", return_value=catalog):
            result = await reader.list_tables("ws", "item", "dbo")

        assert result == ["customers", "orders"]
        catalog.list_tables.assert_called_once_with("dbo")

    @pytest.mark.asyncio()
    async def test_returns_table_names_from_strings(self, auth):
        """Handle case where list_tables returns plain strings."""
        catalog = MagicMock()
        catalog.list_tables.return_value = ["customers", "orders"]

        reader = IcebergTableReader(auth)
        with patch.object(reader, "_build_catalog_sync", return_value=catalog):
            result = await reader.list_tables("ws", "item")

        assert result == ["customers", "orders"]

    @pytest.mark.asyncio()
    async def test_default_namespace_is_dbo(self, auth):
        catalog = MagicMock()
        catalog.list_tables.return_value = []

        reader = IcebergTableReader(auth)
        with patch.object(reader, "_build_catalog_sync", return_value=catalog):
            await reader.list_tables("ws", "item")

        catalog.list_tables.assert_called_once_with("dbo")


# ── get_metadata ────────────────────────────────────────────────────────


class TestGetMetadata:
    """Test metadata retrieval."""

    @pytest.mark.asyncio()
    async def test_happy_path(self, auth):
        ice_table = _make_mock_iceberg_table()
        catalog = MagicMock()
        catalog.load_table.return_value = ice_table

        reader = IcebergTableReader(auth)
        with patch.object(reader, "_build_catalog_sync", return_value=catalog):
            info = await reader.get_metadata("ws", "item", "dbo", "customers")

        assert isinstance(info, IcebergTableInfo)
        assert info.name == "customers"
        assert len(info.schema_) == 2
        assert info.schema_[0].name == "id"
        assert info.schema_[0].nullable is False
        assert info.schema_[1].comment == "Customer name"
        assert info.current_snapshot_id == 12345
        assert info.format_version == 2

    @pytest.mark.asyncio()
    async def test_partition_spec_extraction(self, auth):
        partition_fields = [
            {"source_id": 1, "field_id": 1000, "transform": "identity", "name": "region"},
            {"source_id": 2, "field_id": 1001, "transform": "month", "name": "order_month"},
        ]
        ice_table = _make_mock_iceberg_table(partition_fields=partition_fields)
        catalog = MagicMock()
        catalog.load_table.return_value = ice_table

        reader = IcebergTableReader(auth)
        with patch.object(reader, "_build_catalog_sync", return_value=catalog):
            info = await reader.get_metadata("ws", "item", "dbo", "sales")

        assert len(info.partition_spec) == 2
        assert info.partition_spec[0]["name"] == "region"
        assert info.partition_spec[1]["transform"] == "month"

    @pytest.mark.asyncio()
    async def test_no_snapshot(self, auth):
        ice_table = _make_mock_iceberg_table(snapshot_id=None)
        catalog = MagicMock()
        catalog.load_table.return_value = ice_table

        reader = IcebergTableReader(auth)
        with patch.object(reader, "_build_catalog_sync", return_value=catalog):
            info = await reader.get_metadata("ws", "item", "dbo", "empty_table")

        assert info.current_snapshot_id is None

    @pytest.mark.asyncio()
    async def test_no_partition_spec(self, auth):
        ice_table = _make_mock_iceberg_table(partition_fields=None)
        catalog = MagicMock()
        catalog.load_table.return_value = ice_table

        reader = IcebergTableReader(auth)
        with patch.object(reader, "_build_catalog_sync", return_value=catalog):
            info = await reader.get_metadata("ws", "item", "dbo", "t")

        assert info.partition_spec == []

    @pytest.mark.asyncio()
    async def test_properties_extracted(self, auth):
        props = {"write.format.default": "parquet", "commit.retry.num-retries": "3"}
        ice_table = _make_mock_iceberg_table(properties=props)
        catalog = MagicMock()
        catalog.load_table.return_value = ice_table

        reader = IcebergTableReader(auth)
        with patch.object(reader, "_build_catalog_sync", return_value=catalog):
            info = await reader.get_metadata("ws", "item", "dbo", "t")

        assert info.properties == props

    @pytest.mark.asyncio()
    async def test_empty_properties(self, auth):
        ice_table = _make_mock_iceberg_table(properties={})
        # Make table.properties falsy
        ice_table.properties = {}
        catalog = MagicMock()
        catalog.load_table.return_value = ice_table

        reader = IcebergTableReader(auth)
        with patch.object(reader, "_build_catalog_sync", return_value=catalog):
            info = await reader.get_metadata("ws", "item", "dbo", "t")

        assert info.properties == {}

    @pytest.mark.asyncio()
    async def test_location_from_table(self, auth):
        ice_table = _make_mock_iceberg_table(location="abfss://ws@host/LH/Tables/t")
        catalog = MagicMock()
        catalog.load_table.return_value = ice_table

        reader = IcebergTableReader(auth)
        with patch.object(reader, "_build_catalog_sync", return_value=catalog):
            info = await reader.get_metadata("ws", "item", "dbo", "t")

        assert info.location == "abfss://ws@host/LH/Tables/t"

    @pytest.mark.asyncio()
    async def test_loads_correct_table_identifier(self, auth):
        """Verify catalog.load_table is called with 'namespace.table_name'."""
        catalog = MagicMock()
        catalog.load_table.return_value = _make_mock_iceberg_table()

        reader = IcebergTableReader(auth)
        with patch.object(reader, "_build_catalog_sync", return_value=catalog):
            await reader.get_metadata("ws", "item", "myschema", "mytable")

        catalog.load_table.assert_called_once_with("myschema.mytable")


# ── Error paths ─────────────────────────────────────────────────────────


class TestIcebergErrors:
    """Test error handling."""

    @pytest.mark.asyncio()
    async def test_catalog_build_failure_propagates(self, auth):
        reader = IcebergTableReader(auth)
        with patch.object(
            reader, "_build_catalog_sync", side_effect=RuntimeError("catalog connect failed")
        ), pytest.raises(RuntimeError, match="catalog connect failed"):
            await reader.list_namespaces("ws", "item")

    @pytest.mark.asyncio()
    async def test_load_table_failure_propagates(self, auth):
        catalog = MagicMock()
        catalog.load_table.side_effect = Exception("table not found")

        reader = IcebergTableReader(auth)
        with (
            patch.object(reader, "_build_catalog_sync", return_value=catalog),
            pytest.raises(Exception, match="table not found"),
        ):
                await reader.get_metadata("ws", "item", "dbo", "missing")

    @pytest.mark.asyncio()
    async def test_list_tables_failure_propagates(self, auth):
        catalog = MagicMock()
        catalog.list_tables.side_effect = PermissionError("403 Forbidden")

        reader = IcebergTableReader(auth)
        with (
            patch.object(reader, "_build_catalog_sync", return_value=catalog),
            pytest.raises(PermissionError, match="403 Forbidden"),
        ):
                await reader.list_tables("ws", "item", "dbo")
