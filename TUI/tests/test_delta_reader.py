"""Tests for onelake_client.tables.delta — DeltaTableReader and helpers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from onelake_client.environment import DAILY, MSIT, PROD
from onelake_client.models.table import DeltaTableInfo
from onelake_client.tables.delta import (
    DeltaTableReader,
    _build_table_uri,
    _schema_to_columns,
)

# ── Fixtures ────────────────────────────────────────────────────────────


def _make_mock_field(name: str, type_str: str, nullable: bool = True, metadata=None):
    """Build a mock deltalake Field."""
    f = MagicMock()
    f.name = name
    f.type = type_str
    f.nullable = nullable
    f.metadata = metadata or {}
    return f


def _make_mock_schema(fields):
    """Build a mock deltalake Schema with .fields attribute."""
    schema = MagicMock()
    schema.fields = fields
    return schema


def _make_mock_metadata(
    name="customers",
    partition_columns=None,
    configuration=None,
    description=None,
):
    """Build a mock deltalake TableMetadata."""
    m = MagicMock()
    m.name = name
    m.partition_columns = partition_columns or []
    m.configuration = configuration or {}
    m.description = description
    return m


# ── _build_table_uri ────────────────────────────────────────────────────


class TestBuildTableUri:
    """Test the abfss:// URI builder."""

    def test_prod_uri(self):
        uri = _build_table_uri("ws-guid", "LH.Lakehouse", "sales", PROD.dfs_host)
        assert uri == "abfss://ws-guid@onelake.dfs.fabric.microsoft.com/LH.Lakehouse/Tables/sales"

    def test_msit_uri(self):
        uri = _build_table_uri("ws", "item", "t", MSIT.dfs_host)
        assert uri == "abfss://ws@msit-onelake.dfs.fabric.microsoft.com/item/Tables/t"

    def test_daily_uri(self):
        uri = _build_table_uri("ws", "item", "t", DAILY.dfs_host)
        assert "daily-onelake.dfs.fabric.microsoft.com" in uri

    def test_table_name_with_schema_prefix(self):
        """Two-level table names (SCHEMA/table) should pass through."""
        uri = _build_table_uri("ws", "item", "dbo/orders", PROD.dfs_host)
        assert uri.endswith("/Tables/dbo/orders")

    def test_special_characters_not_encoded(self):
        """URI builder is a plain string formatter — no encoding."""
        uri = _build_table_uri("My Workspace", "item", "my table", PROD.dfs_host)
        assert "My Workspace" in uri
        assert "my table" in uri


# ── _schema_to_columns ──────────────────────────────────────────────────


class TestSchemaToColumns:
    """Test conversion from deltalake Schema → Column models."""

    def test_basic_fields(self):
        fields = [
            _make_mock_field("id", "long", nullable=False),
            _make_mock_field("name", "string", nullable=True),
        ]
        schema = _make_mock_schema(fields)
        columns = _schema_to_columns(schema)

        assert len(columns) == 2
        assert columns[0].name == "id"
        assert columns[0].type == "long"
        assert columns[0].nullable is False
        assert columns[1].name == "name"
        assert columns[1].nullable is True

    def test_metadata_preserved(self):
        meta = {"delta.generationExpression": "now()"}
        fields = [_make_mock_field("ts", "timestamp", metadata=meta)]
        schema = _make_mock_schema(fields)
        columns = _schema_to_columns(schema)
        assert columns[0].metadata == {"delta.generationExpression": "now()"}

    def test_empty_metadata_becomes_none(self):
        fields = [_make_mock_field("x", "int", metadata={})]
        schema = _make_mock_schema(fields)
        columns = _schema_to_columns(schema)
        assert columns[0].metadata is None

    def test_legacy_iterable_schema(self):
        """Older deltalake versions where Schema is directly iterable (no .fields)."""
        fields = [_make_mock_field("a", "string")]
        # No .fields attribute — schema itself is the iterable
        columns = _schema_to_columns(fields)
        assert len(columns) == 1
        assert columns[0].name == "a"

    def test_empty_schema(self):
        schema = _make_mock_schema([])
        columns = _schema_to_columns(schema)
        assert columns == []


# ── DeltaTableReader.get_metadata ───────────────────────────────────────


class TestGetMetadata:
    """Test get_metadata with mocked DeltaTable."""

    @pytest.fixture()
    def auth(self):
        from onelake_client.auth import OneLakeAuth
        from tests.conftest import FakeCredential

        return OneLakeAuth(credential=FakeCredential())

    def _make_delta_table_mock(
        self,
        *,
        fields=None,
        version=5,
        file_uris=None,
        metadata=None,
        add_actions_style="arro3",
        add_actions_sizes=None,
    ):
        """Build a complete mock DeltaTable."""
        dt = MagicMock()

        if fields is None:
            fields = [
                _make_mock_field("id", "long", nullable=False),
                _make_mock_field("value", "double"),
            ]
        dt.schema.return_value = _make_mock_schema(fields)
        dt.version.return_value = version
        dt.file_uris.return_value = file_uris or [
            "part-00000.parquet",
            "part-00001.parquet",
        ]
        dt.metadata.return_value = metadata or _make_mock_metadata()

        sizes = add_actions_sizes or [1000, 2000]
        actions = MagicMock()

        if add_actions_style == "arro3":
            # arro3 Table — has .column() but not .to_pydict()
            del actions.to_pydict
            actions.column_names = ["path", "size_bytes", "modification_time"]
            size_col = MagicMock()
            size_col.to_pylist.return_value = sizes
            actions.column.return_value = size_col
        elif add_actions_style == "pyarrow":
            # pyarrow Table — has .to_pydict() but not .column()
            del actions.column
            actions.to_pydict.return_value = {"size_bytes": sizes, "path": ["a", "b"]}
        elif add_actions_style == "pyarrow_old_key":
            del actions.column
            actions.to_pydict.return_value = {"size": sizes, "path": ["a", "b"]}

        dt.get_add_actions.return_value = actions
        return dt

    def _patch_reader(self, reader, dt_mock):
        """Patch _load_table_sync to return our mock (avoids local-import issues)."""
        return patch.object(reader, "_load_table_sync", return_value=dt_mock)

    @pytest.mark.asyncio()
    async def test_happy_path_arro3(self, auth):
        dt_mock = self._make_delta_table_mock(add_actions_sizes=[5000, 3000])
        reader = DeltaTableReader(auth)
        with self._patch_reader(reader, dt_mock):
            info = await reader.get_metadata("ws", "LH.Lakehouse", "customers")

        assert isinstance(info, DeltaTableInfo)
        assert info.name == "customers"
        assert info.version == 5
        assert info.num_files == 2
        assert info.size_bytes == 8000
        assert len(info.schema_) == 2
        assert info.schema_[0].name == "id"

    @pytest.mark.asyncio()
    async def test_happy_path_pyarrow(self, auth):
        dt_mock = self._make_delta_table_mock(
            add_actions_style="pyarrow", add_actions_sizes=[100, 200]
        )
        reader = DeltaTableReader(auth)
        with self._patch_reader(reader, dt_mock):
            info = await reader.get_metadata("ws", "item", "t")

        assert info.size_bytes == 300

    @pytest.mark.asyncio()
    async def test_pyarrow_old_size_key(self, auth):
        """Older pyarrow-style add actions use 'size' instead of 'size_bytes'."""
        dt_mock = self._make_delta_table_mock(
            add_actions_style="pyarrow_old_key", add_actions_sizes=[400]
        )
        reader = DeltaTableReader(auth)
        with self._patch_reader(reader, dt_mock):
            info = await reader.get_metadata("ws", "item", "t")

        assert info.size_bytes == 400

    @pytest.mark.asyncio()
    async def test_version_and_files(self, auth):
        dt_mock = self._make_delta_table_mock(
            version=42, file_uris=["a.parquet", "b.parquet", "c.parquet"]
        )
        reader = DeltaTableReader(auth)
        with self._patch_reader(reader, dt_mock):
            info = await reader.get_metadata("ws", "item", "t")

        assert info.version == 42
        assert info.num_files == 3

    @pytest.mark.asyncio()
    async def test_metadata_name_from_delta(self, auth):
        """When Delta metadata has a name, it takes precedence over the argument."""
        meta = _make_mock_metadata(name="actual_name")
        dt_mock = self._make_delta_table_mock(metadata=meta)
        reader = DeltaTableReader(auth)
        with self._patch_reader(reader, dt_mock):
            info = await reader.get_metadata("ws", "item", "arg_name")

        assert info.name == "actual_name"

    @pytest.mark.asyncio()
    async def test_metadata_name_falls_back_to_arg(self, auth):
        """When Delta metadata.name is None, the table_name argument is used."""
        meta = _make_mock_metadata(name=None)
        dt_mock = self._make_delta_table_mock(metadata=meta)
        reader = DeltaTableReader(auth)
        with self._patch_reader(reader, dt_mock):
            info = await reader.get_metadata("ws", "item", "fallback")

        assert info.name == "fallback"

    @pytest.mark.asyncio()
    async def test_partition_columns(self, auth):
        meta = _make_mock_metadata(partition_columns=["year", "month"])
        dt_mock = self._make_delta_table_mock(metadata=meta)
        reader = DeltaTableReader(auth)
        with self._patch_reader(reader, dt_mock):
            info = await reader.get_metadata("ws", "item", "t")

        assert info.partition_columns == ["year", "month"]

    @pytest.mark.asyncio()
    async def test_properties_and_description(self, auth):
        meta = _make_mock_metadata(
            configuration={"delta.autoOptimize.optimizeWrite": "true"},
            description="Sales data",
        )
        dt_mock = self._make_delta_table_mock(metadata=meta)
        reader = DeltaTableReader(auth)
        with self._patch_reader(reader, dt_mock):
            info = await reader.get_metadata("ws", "item", "t")

        assert info.properties == {"delta.autoOptimize.optimizeWrite": "true"}
        assert info.description == "Sales data"

    @pytest.mark.asyncio()
    async def test_custom_dfs_host(self, auth):
        """DeltaTableReader passes the configured DFS host to the URI builder."""
        dt_mock = self._make_delta_table_mock()
        reader = DeltaTableReader(auth, dfs_host=MSIT.dfs_host)
        with self._patch_reader(reader, dt_mock) as mock_load:
            await reader.get_metadata("ws", "item", "t")

        call_args = mock_load.call_args
        uri = call_args[0][0]
        assert MSIT.dfs_host in uri


# ── Size computation error handling ─────────────────────────────────────


class TestSizeComputationErrors:
    """Test the narrowed exception handling during size calculation."""

    @pytest.fixture()
    def auth(self):
        from onelake_client.auth import OneLakeAuth
        from tests.conftest import FakeCredential

        return OneLakeAuth(credential=FakeCredential())

    def _make_dt_with_failing_actions(self, error):
        """Build a DeltaTable mock where get_add_actions raises ``error``."""
        dt = MagicMock()
        dt.schema.return_value = _make_mock_schema([_make_mock_field("x", "int")])
        dt.version.return_value = 0
        dt.file_uris.return_value = []
        dt.metadata.return_value = _make_mock_metadata()
        dt.get_add_actions.side_effect = error
        return dt

    def _patch_reader(self, reader, dt_mock):
        return patch.object(reader, "_load_table_sync", return_value=dt_mock)

    @pytest.mark.asyncio()
    @pytest.mark.parametrize(
        "exc",
        [
            pytest.param(KeyError("size_bytes"), id="KeyError"),
            pytest.param(IndexError("out of range"), id="IndexError"),
            pytest.param(ValueError("bad value"), id="ValueError"),
        ],
    )
    async def test_caught_exceptions_yield_zero_size(self, auth, exc):
        """DeltaError, KeyError, IndexError, ValueError → size_bytes=0."""
        dt_mock = self._make_dt_with_failing_actions(exc)
        reader = DeltaTableReader(auth)
        with self._patch_reader(reader, dt_mock):
            info = await reader.get_metadata("ws", "item", "t")

        assert info.size_bytes == 0

    @pytest.mark.asyncio()
    async def test_delta_error_caught(self, auth):
        """DeltaError (including DeletionVectors) is caught gracefully."""
        from deltalake.exceptions import DeltaError

        dt_mock = self._make_dt_with_failing_actions(
            DeltaError(
                "The table has set these reader features: {'deletionVectors'} "
                "but these are not yet supported by the deltalake reader."
            )
        )
        reader = DeltaTableReader(auth)
        with self._patch_reader(reader, dt_mock):
            info = await reader.get_metadata("ws", "item", "t")

        assert info.size_bytes == 0

    @pytest.mark.asyncio()
    @pytest.mark.parametrize(
        "exc",
        [
            pytest.param(AttributeError("no attribute 'column'"), id="AttributeError"),
            pytest.param(TypeError("unsupported operand"), id="TypeError"),
        ],
    )
    async def test_uncaught_exceptions_propagate(self, auth, exc):
        """AttributeError and TypeError are NOT caught — they propagate."""
        dt_mock = self._make_dt_with_failing_actions(exc)
        reader = DeltaTableReader(auth)
        with self._patch_reader(reader, dt_mock), pytest.raises(type(exc)):
            await reader.get_metadata("ws", "item", "t")


# ── Storage options ─────────────────────────────────────────────────────


class TestStorageOptions:
    """Verify storage options are passed through to DeltaTable."""

    @pytest.fixture()
    def auth(self):
        from onelake_client.auth import OneLakeAuth
        from tests.conftest import FakeCredential

        return OneLakeAuth(credential=FakeCredential())

    @pytest.mark.asyncio()
    async def test_storage_options_forwarded(self, auth):
        dt_mock = MagicMock()
        dt_mock.schema.return_value = _make_mock_schema([])
        dt_mock.version.return_value = 0
        dt_mock.file_uris.return_value = []
        dt_mock.metadata.return_value = _make_mock_metadata()
        actions = MagicMock()
        del actions.to_pydict
        actions.column_names = []
        dt_mock.get_add_actions.return_value = actions

        reader = DeltaTableReader(auth)
        # Spy on _get_storage_options to verify it's called, then test the actual
        # _load_table_sync by patching the deltalake import
        opts = reader._get_storage_options()
        assert opts["account_name"] == "onelake"
        assert opts["azure_storage_token"] == "fake-token-12345"
        assert opts["account_host"] == "onelake.dfs.fabric.microsoft.com"
