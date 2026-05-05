"""Tests for DeltaTableReader against real committed Delta log fixtures.

Complements the mock-based test_delta_reader.py by exercising actual
delta-rs parsing of on-disk Delta tables.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from deltalake import DeltaTable

from onelake_client.auth import OneLakeAuth
from onelake_client.models.table import DeltaTableInfo
from onelake_client.tables.delta import DeltaTableReader

from .conftest import FakeCredential

FIXTURES = Path(__file__).parent / "fixtures" / "delta"


def _make_reader() -> DeltaTableReader:
    auth = OneLakeAuth(credential=FakeCredential())
    reader = DeltaTableReader(auth)
    reader._isolate = False
    return reader


async def _load_info(fixture_name: str, table_name: str = "t") -> DeltaTableInfo:
    """Load DeltaTableInfo from a local fixture via the full reader path."""
    fixture_path = str(FIXTURES / fixture_name)
    reader = _make_reader()

    def _local_load(uri: str) -> DeltaTable:
        return DeltaTable(fixture_path)

    with patch.object(reader, "_load_table_sync", side_effect=_local_load):
        return await reader.get_metadata("ws", "item", table_name)


# ── Basic table ─────────────────────────────────────────────────────────


class TestBasicTable:
    """basic_table: 3 rows — id(int64), name(string), score(float64), active(bool)."""

    @pytest.fixture()
    async def info(self) -> DeltaTableInfo:
        return await _load_info("basic_table")

    @pytest.mark.asyncio()
    async def test_table_name(self, info: DeltaTableInfo) -> None:
        assert info.name == "customers"

    @pytest.mark.asyncio()
    async def test_version_is_zero(self, info: DeltaTableInfo) -> None:
        assert info.version == 0

    @pytest.mark.asyncio()
    async def test_schema_field_names(self, info: DeltaTableInfo) -> None:
        names = [c.name for c in info.schema_]
        assert names == ["id", "name", "score", "active"]

    @pytest.mark.asyncio()
    async def test_schema_field_types(self, info: DeltaTableInfo) -> None:
        types = [c.type for c in info.schema_]
        assert "long" in types[0]
        assert "string" in types[1]
        assert "double" in types[2]
        assert "boolean" in types[3]

    @pytest.mark.asyncio()
    async def test_schema_nullable(self, info: DeltaTableInfo) -> None:
        nullable = [c.nullable for c in info.schema_]
        assert nullable == [True, True, True, True]

    @pytest.mark.asyncio()
    async def test_num_files(self, info: DeltaTableInfo) -> None:
        assert info.num_files == 1

    @pytest.mark.asyncio()
    async def test_size_bytes(self, info: DeltaTableInfo) -> None:
        assert info.size_bytes > 0

    @pytest.mark.asyncio()
    async def test_no_partition_columns(self, info: DeltaTableInfo) -> None:
        assert info.partition_columns == []

    @pytest.mark.asyncio()
    async def test_no_description(self, info: DeltaTableInfo) -> None:
        assert info.description is None


# ── Schema evolution (multi-commit ADD COLUMN) ──────────────────────────


class TestSchemaEvolution:
    """schema_evolution: 2 commits — initial 3 cols, then ADD category."""

    @pytest.fixture()
    def dt(self) -> DeltaTable:
        return DeltaTable(str(FIXTURES / "schema_evolution"))

    def test_version_is_one(self, dt: DeltaTable) -> None:
        assert dt.version() == 1

    def test_schema_has_four_fields(self, dt: DeltaTable) -> None:
        names = [f.name for f in dt.schema().fields]
        assert names == ["id", "name", "value", "category"]

    def test_schema_field_types(self, dt: DeltaTable) -> None:
        types = [str(f.type) for f in dt.schema().fields]
        assert "long" in types[0]
        assert "string" in types[1]
        assert "double" in types[2]
        assert "string" in types[3]

    def test_version_zero_had_three_fields(self, dt: DeltaTable) -> None:
        dt.load_as_version(0)
        names = [f.name for f in dt.schema().fields]
        assert names == ["id", "name", "value"]

    def test_id_not_nullable(self, dt: DeltaTable) -> None:
        assert dt.schema().fields[0].nullable is False

    def test_category_nullable(self, dt: DeltaTable) -> None:
        assert dt.schema().fields[3].nullable is True

    def test_table_name(self, dt: DeltaTable) -> None:
        assert dt.metadata().name == "schema_evolution"

    def test_no_partition_columns(self, dt: DeltaTable) -> None:
        assert list(dt.metadata().partition_columns) == []


# ── Column mapping mode=id ──────────────────────────────────────────────


class TestColumnMappingId:
    """column_mapping_id: mode=id with physical names, 3 columns."""

    @pytest.fixture()
    def dt(self) -> DeltaTable:
        return DeltaTable(str(FIXTURES / "column_mapping_id"))

    def test_table_name(self, dt: DeltaTable) -> None:
        assert dt.metadata().name == "mapped_by_id"

    def test_column_mapping_mode_is_id(self, dt: DeltaTable) -> None:
        config = dict(dt.metadata().configuration)
        assert config["delta.columnMapping.mode"] == "id"

    def test_max_column_id(self, dt: DeltaTable) -> None:
        config = dict(dt.metadata().configuration)
        assert config["delta.columnMapping.maxColumnId"] == "3"

    def test_schema_field_names(self, dt: DeltaTable) -> None:
        names = [f.name for f in dt.schema().fields]
        assert names == ["user_id", "email", "score"]

    def test_user_id_mapping_metadata(self, dt: DeltaTable) -> None:
        meta = dict(dt.schema().fields[0].metadata)
        assert meta["delta.columnMapping.id"] == 1
        assert meta["delta.columnMapping.physicalName"] == "col-uid-1"

    def test_email_mapping_metadata(self, dt: DeltaTable) -> None:
        meta = dict(dt.schema().fields[1].metadata)
        assert meta["delta.columnMapping.id"] == 2
        assert meta["delta.columnMapping.physicalName"] == "col-email-2"

    def test_score_mapping_metadata(self, dt: DeltaTable) -> None:
        meta = dict(dt.schema().fields[2].metadata)
        assert meta["delta.columnMapping.id"] == 3
        assert meta["delta.columnMapping.physicalName"] == "col-score-3"

    def test_version_is_zero(self, dt: DeltaTable) -> None:
        assert dt.version() == 0

    def test_no_partition_columns(self, dt: DeltaTable) -> None:
        assert list(dt.metadata().partition_columns) == []


# ── V2 checkpoint protocol ──────────────────────────────────────────────


class TestV2Checkpoint:
    """v2_checkpoint: protocol v3 with readerFeatures=['v2Checkpoint'].

    delta-rs may or may not fully support v2Checkpoint. If it opens,
    verify protocol; if it fails, verify graceful error.
    """

    def test_opens_and_has_v2checkpoint_feature(self) -> None:
        try:
            dt = DeltaTable(str(FIXTURES / "v2_checkpoint"))
        except Exception as exc:
            assert "v2Checkpoint" in str(exc) or "unsupported" in str(exc).lower()
            return
        p = dt.protocol()
        assert p.min_reader_version >= 3
        assert "v2Checkpoint" in (p.reader_features or [])

    def test_schema_if_parseable(self) -> None:
        try:
            dt = DeltaTable(str(FIXTURES / "v2_checkpoint"))
        except Exception:
            pytest.skip("delta-rs cannot open v2Checkpoint tables")
        names = [f.name for f in dt.schema().fields]
        assert names == ["id", "data"]


# ── Inline deletion vector ──────────────────────────────────────────────


class TestInlineDV:
    """inline_dv: deletionVectors feature with inline DV in add action."""

    def test_opens_and_has_deletion_vectors_feature(self) -> None:
        try:
            dt = DeltaTable(str(FIXTURES / "inline_dv"))
        except Exception as exc:
            assert "deletionVectors" in str(exc) or "unsupported" in str(exc).lower()
            return
        p = dt.protocol()
        assert p.min_reader_version >= 3
        assert "deletionVectors" in (p.reader_features or [])

    def test_schema_if_parseable(self) -> None:
        try:
            dt = DeltaTable(str(FIXTURES / "inline_dv"))
        except Exception:
            pytest.skip("delta-rs cannot open inline DV tables")
        names = [f.name for f in dt.schema().fields]
        assert names == ["id", "value"]

    def test_dv_config(self) -> None:
        try:
            dt = DeltaTable(str(FIXTURES / "inline_dv"))
        except Exception:
            pytest.skip("delta-rs cannot open inline DV tables")
        config = dict(dt.metadata().configuration)
        assert config["delta.enableDeletionVectors"] == "true"


# ── Reader features protocol (multiple features) ───────────────────────


class TestReaderFeaturesProtocol:
    """reader_features: protocol v3 with multiple reader/writer features."""

    @pytest.fixture()
    def dt(self) -> DeltaTable:
        return DeltaTable(str(FIXTURES / "reader_features"))

    def test_min_reader_version(self, dt: DeltaTable) -> None:
        assert dt.protocol().min_reader_version == 3

    def test_min_writer_version(self, dt: DeltaTable) -> None:
        assert dt.protocol().min_writer_version == 7

    def test_reader_features(self, dt: DeltaTable) -> None:
        rf = dt.protocol().reader_features or []
        assert "deletionVectors" in rf
        assert "columnMapping" in rf
        assert "timestampNtz" in rf

    def test_writer_features(self, dt: DeltaTable) -> None:
        wf = dt.protocol().writer_features or []
        assert "deletionVectors" in wf
        assert "columnMapping" in wf
        assert "timestampNtz" in wf
        assert "appendOnly" in wf
        assert "invariants" in wf
        assert "checkConstraints" in wf

    def test_table_name(self, dt: DeltaTable) -> None:
        assert dt.metadata().name == "multi_feature_table"

    def test_column_mapping_config(self, dt: DeltaTable) -> None:
        config = dict(dt.metadata().configuration)
        assert config["delta.columnMapping.mode"] == "name"
        assert config["delta.columnMapping.maxColumnId"] == "2"
        assert config["delta.enableDeletionVectors"] == "true"

    def test_schema_field_names(self, dt: DeltaTable) -> None:
        names = [f.name for f in dt.schema().fields]
        assert names == ["id", "ts"]

    def test_timestamp_ntz_type(self, dt: DeltaTable) -> None:
        ts_field = dt.schema().fields[1]
        assert "timestamp_ntz" in str(ts_field.type)

    def test_version_is_zero(self, dt: DeltaTable) -> None:
        assert dt.version() == 0


# ── Partitioned table ───────────────────────────────────────────────────


class TestPartitionedTable:
    """partitioned_table: partition_by=[region, year], 4 rows."""

    @pytest.fixture()
    async def info(self) -> DeltaTableInfo:
        return await _load_info("partitioned_table")

    @pytest.mark.asyncio()
    async def test_table_name(self, info: DeltaTableInfo) -> None:
        assert info.name == "sales"

    @pytest.mark.asyncio()
    async def test_partition_columns(self, info: DeltaTableInfo) -> None:
        assert info.partition_columns == ["region", "year"]

    @pytest.mark.asyncio()
    async def test_schema_field_names(self, info: DeltaTableInfo) -> None:
        names = [c.name for c in info.schema_]
        assert names == ["region", "year", "revenue"]

    @pytest.mark.asyncio()
    async def test_schema_field_types(self, info: DeltaTableInfo) -> None:
        types = [c.type for c in info.schema_]
        assert "string" in types[0]
        assert "integer" in types[1]
        assert "double" in types[2]

    @pytest.mark.asyncio()
    async def test_has_data_files(self, info: DeltaTableInfo) -> None:
        # Partitioned writes produce multiple files (one per partition combo)
        assert info.num_files >= 1

    @pytest.mark.asyncio()
    async def test_size_bytes(self, info: DeltaTableInfo) -> None:
        assert info.size_bytes > 0

    @pytest.mark.asyncio()
    async def test_version_is_zero(self, info: DeltaTableInfo) -> None:
        assert info.version == 0


# ── Column mapping v2 ──────────────────────────────────────────────────


class TestColumnMappingV2:
    """column_mapping_v2: hand-crafted JSON log with column mapping mode=name.

    No parquet data files — open DeltaTable directly and test schema/metadata
    extraction only.
    """

    @pytest.fixture()
    def dt(self) -> DeltaTable:
        return DeltaTable(str(FIXTURES / "column_mapping_v2"))

    def test_table_name(self, dt: DeltaTable) -> None:
        assert dt.metadata().name == "users"

    def test_schema_field_names(self, dt: DeltaTable) -> None:
        names = [f.name for f in dt.schema().fields]
        assert names == ["user_id", "email"]

    def test_schema_field_types(self, dt: DeltaTable) -> None:
        types = [str(f.type) for f in dt.schema().fields]
        assert "long" in types[0]
        assert "string" in types[1]

    def test_user_id_not_nullable(self, dt: DeltaTable) -> None:
        user_id_field = dt.schema().fields[0]
        assert user_id_field.nullable is False

    def test_email_nullable(self, dt: DeltaTable) -> None:
        email_field = dt.schema().fields[1]
        assert email_field.nullable is True

    def test_column_mapping_mode_in_config(self, dt: DeltaTable) -> None:
        config = dict(dt.metadata().configuration)
        assert config["delta.columnMapping.mode"] == "name"

    def test_column_mapping_max_id_in_config(self, dt: DeltaTable) -> None:
        config = dict(dt.metadata().configuration)
        assert config["delta.columnMapping.maxColumnId"] == "2"

    def test_user_id_mapping_metadata(self, dt: DeltaTable) -> None:
        meta = dict(dt.schema().fields[0].metadata)
        assert meta["delta.columnMapping.id"] == 1
        assert meta["delta.columnMapping.physicalName"] == "col-uid-1"

    def test_email_mapping_metadata(self, dt: DeltaTable) -> None:
        meta = dict(dt.schema().fields[1].metadata)
        assert meta["delta.columnMapping.id"] == 2
        assert meta["delta.columnMapping.physicalName"] == "col-email-2"

    def test_no_partition_columns(self, dt: DeltaTable) -> None:
        assert list(dt.metadata().partition_columns) == []

    def test_version_is_zero(self, dt: DeltaTable) -> None:
        assert dt.version() == 0


# ── CDF enabled ─────────────────────────────────────────────────────────


class TestCdfEnabled:
    """cdf_enabled: delta.enableChangeDataFeed=true, 2 rows."""

    @pytest.fixture()
    async def info(self) -> DeltaTableInfo:
        return await _load_info("cdf_enabled")

    @pytest.mark.asyncio()
    async def test_table_name(self, info: DeltaTableInfo) -> None:
        assert info.name == "orders"

    @pytest.mark.asyncio()
    async def test_cdf_property(self, info: DeltaTableInfo) -> None:
        assert info.properties.get("delta.enableChangeDataFeed") == "true"

    @pytest.mark.asyncio()
    async def test_schema_field_names(self, info: DeltaTableInfo) -> None:
        names = [c.name for c in info.schema_]
        assert names == ["order_id", "amount"]

    @pytest.mark.asyncio()
    async def test_schema_field_types(self, info: DeltaTableInfo) -> None:
        types = [c.type for c in info.schema_]
        assert "long" in types[0]
        assert "double" in types[1]

    @pytest.mark.asyncio()
    async def test_num_files(self, info: DeltaTableInfo) -> None:
        assert info.num_files == 1

    @pytest.mark.asyncio()
    async def test_size_bytes(self, info: DeltaTableInfo) -> None:
        assert info.size_bytes > 0

    @pytest.mark.asyncio()
    async def test_version_is_zero(self, info: DeltaTableInfo) -> None:
        assert info.version == 0


# ── Unicode paths ───────────────────────────────────────────────────────


class TestUnicodePaths:
    """unicode_paths: columns données_id(int64), 名前(string)."""

    @pytest.fixture()
    async def info(self) -> DeltaTableInfo:
        return await _load_info("unicode_paths")

    @pytest.mark.asyncio()
    async def test_table_name(self, info: DeltaTableInfo) -> None:
        assert info.name == "données_client"

    @pytest.mark.asyncio()
    async def test_schema_field_names(self, info: DeltaTableInfo) -> None:
        names = [c.name for c in info.schema_]
        assert names == ["données_id", "名前"]

    @pytest.mark.asyncio()
    async def test_schema_field_types(self, info: DeltaTableInfo) -> None:
        types = [c.type for c in info.schema_]
        assert "long" in types[0]
        assert "string" in types[1]

    @pytest.mark.asyncio()
    async def test_num_files(self, info: DeltaTableInfo) -> None:
        assert info.num_files == 1

    @pytest.mark.asyncio()
    async def test_size_bytes(self, info: DeltaTableInfo) -> None:
        assert info.size_bytes > 0

    @pytest.mark.asyncio()
    async def test_version_is_zero(self, info: DeltaTableInfo) -> None:
        assert info.version == 0

    @pytest.mark.asyncio()
    async def test_no_partition_columns(self, info: DeltaTableInfo) -> None:
        assert info.partition_columns == []
