"""Tests for the Delta analysis engine (get_analysis + helpers)."""

from __future__ import annotations

import io
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from onelake_client.models.table import Column, DeltaTableInfo
from onelake_client.tables.delta import DeltaTableReader, _clean_type_str, _parse_dfs_path

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DFS_HOST = "onelake.dfs.fabric.microsoft.com"
_WS_GUID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


def _make_parquet_bytes(**kwargs) -> bytes:
    """Create a minimal parquet file in memory."""
    table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    buf = io.BytesIO()
    pq.write_table(table, buf, **kwargs)
    return buf.getvalue()


def _make_uri(file_name: str = "part-00000.parquet") -> str:
    return f"abfss://{_WS_GUID}@{_DFS_HOST}/item-guid/Tables/mytable/{file_name}"


@pytest.fixture
def mock_dfs():
    return AsyncMock()


@pytest.fixture
def reader(mock_dfs):
    auth = MagicMock()
    r = DeltaTableReader(
        auth, dfs_host=_DFS_HOST, dfs_client=mock_dfs
    )
    r._isolate = False
    return r


def _default_metadata() -> DeltaTableInfo:
    """Return a DeltaTableInfo with no column mapping."""
    return DeltaTableInfo(
        name="mytable",
        schema_=[
            Column(name="id", type="long"),
            Column(name="name", type="string"),
        ],
        version=1,
        num_files=1,
    )


# ---------------------------------------------------------------------------
# _clean_type_str
# ---------------------------------------------------------------------------


class TestCleanTypeStr:
    def test_plain_string(self):
        assert _clean_type_str("string") == "string"

    def test_primitive_type_string(self):
        assert _clean_type_str('PrimitiveType("string")') == "string"

    def test_primitive_type_int32(self):
        assert _clean_type_str('PrimitiveType("int32")') == "int32"

    def test_non_primitive_passthrough(self):
        assert _clean_type_str("map<string, int>") == "map<string, int>"

    def test_single_quotes(self):
        assert _clean_type_str("PrimitiveType('boolean')") == "boolean"


# ---------------------------------------------------------------------------
# _parse_dfs_path
# ---------------------------------------------------------------------------


class TestParseDfsPath:
    def test_basic(self):
        uri = f"abfss://{_WS_GUID}@{_DFS_HOST}/item/Tables/t/file.parquet"
        ws, path = _parse_dfs_path(uri)
        assert ws == _WS_GUID
        assert path == "item/Tables/t/file.parquet"


# ---------------------------------------------------------------------------
# get_analysis — basic
# ---------------------------------------------------------------------------


async def test_get_analysis_basic(reader, mock_dfs):
    parquet_bytes = _make_parquet_bytes()
    uri = _make_uri()

    with (
        patch.object(reader, "list_files", new_callable=AsyncMock) as mock_list,
        patch.object(reader, "get_metadata", new_callable=AsyncMock) as mock_meta,
    ):
        mock_list.return_value = [uri]
        mock_meta.return_value = _default_metadata()
        mock_dfs.read_file_range.return_value = parquet_bytes

        result = await reader.get_analysis(_WS_GUID, "item-guid", "mytable")

    # Summary
    assert result.summary.total_rows == 3
    assert result.summary.total_files == 1
    assert result.summary.total_row_groups == 1
    assert result.summary.files_skipped == 0

    # Files
    assert len(result.files) == 1
    assert result.files[0].file_name == "part-00000.parquet"
    assert result.files[0].row_count == 3
    assert result.files[0].total_table_rows == 3

    # Row groups
    assert len(result.row_groups) == 1
    assert result.row_groups[0].row_count == 3
    assert result.row_groups[0].compression_ratio > 0

    # Column chunks — 2 columns (id, name)
    assert len(result.column_chunks) == 2
    col_names = {cc.column_name for cc in result.column_chunks}
    assert col_names == {"id", "name"}

    # Columns (aggregated)
    assert len(result.columns) == 2
    agg_names = {c.column_name for c in result.columns}
    assert agg_names == {"id", "name"}
    total_pct = sum(c.pct_of_table for c in result.columns)
    assert abs(total_pct - 1.0) < 0.001


# ---------------------------------------------------------------------------
# get_analysis — column mapping
# ---------------------------------------------------------------------------


async def test_get_analysis_column_mapping(reader, mock_dfs):
    parquet_bytes = _make_parquet_bytes()
    uri = _make_uri()

    # Parquet physical columns are "id" and "name".
    # Simulate column mapping: physical "id" → logical "user_id",
    # physical "name" → logical "user_name".
    mapped_meta = DeltaTableInfo(
        name="mytable",
        schema_=[
            Column(
                name="user_id",
                type="long",
                metadata={"delta.columnMapping.physicalName": "id"},
            ),
            Column(
                name="user_name",
                type="string",
                metadata={"delta.columnMapping.physicalName": "name"},
            ),
        ],
        version=1,
        num_files=1,
    )

    with (
        patch.object(reader, "list_files", new_callable=AsyncMock) as mock_list,
        patch.object(reader, "get_metadata", new_callable=AsyncMock) as mock_meta,
    ):
        mock_list.return_value = [uri]
        mock_meta.return_value = mapped_meta
        mock_dfs.read_file_range.return_value = parquet_bytes

        result = await reader.get_analysis(_WS_GUID, "item-guid", "mytable")

    chunk_names = {cc.column_name for cc in result.column_chunks}
    assert chunk_names == {"user_id", "user_name"}

    col_names = {c.column_name for c in result.columns}
    assert col_names == {"user_id", "user_name"}


# ---------------------------------------------------------------------------
# get_analysis — max_files cap
# ---------------------------------------------------------------------------


async def test_get_analysis_max_files(reader, mock_dfs):
    parquet_bytes = _make_parquet_bytes()
    uris = [_make_uri(f"part-{i:05d}.parquet") for i in range(30)]

    with (
        patch.object(reader, "list_files", new_callable=AsyncMock) as mock_list,
        patch.object(reader, "get_metadata", new_callable=AsyncMock) as mock_meta,
    ):
        mock_list.return_value = uris
        mock_meta.return_value = _default_metadata()
        mock_dfs.read_file_range.return_value = parquet_bytes

        result = await reader.get_analysis(
            _WS_GUID, "item-guid", "mytable", max_files=5
        )

    assert result.summary.files_skipped == 25
    assert result.summary.total_files == 5


# ---------------------------------------------------------------------------
# get_analysis — progress callback
# ---------------------------------------------------------------------------


async def test_get_analysis_progress_callback(reader, mock_dfs):
    parquet_bytes = _make_parquet_bytes()
    uris = [_make_uri(f"part-{i:05d}.parquet") for i in range(3)]
    calls: list[tuple[int, int, str]] = []

    async def on_progress(current: int, total: int, name: str):
        calls.append((current, total, name))

    with (
        patch.object(reader, "list_files", new_callable=AsyncMock) as mock_list,
        patch.object(reader, "get_metadata", new_callable=AsyncMock) as mock_meta,
    ):
        mock_list.return_value = uris
        mock_meta.return_value = _default_metadata()
        mock_dfs.read_file_range.return_value = parquet_bytes

        await reader.get_analysis(
            _WS_GUID, "item-guid", "mytable", progress_callback=on_progress
        )

    assert len(calls) == 3
    assert calls[0] == (1, 3, "part-00000.parquet")
    assert calls[1] == (2, 3, "part-00001.parquet")
    assert calls[2] == (3, 3, "part-00002.parquet")


# ---------------------------------------------------------------------------
# get_analysis — footer overflow (> 64 KB initial tail)
# ---------------------------------------------------------------------------


async def test_get_analysis_footer_overflow(reader, mock_dfs):
    """Verify fallback when footer is larger than the 64 KB initial tail read."""
    parquet_bytes = _make_parquet_bytes()

    # Simulate: first read returns only the last 8 bytes (too small for real footer).
    # The method should detect this and issue a second read for the full tail.
    small_tail = parquet_bytes[-8:]  # Just footer_len + magic
    uri = _make_uri()

    with (
        patch.object(reader, "list_files", new_callable=AsyncMock) as mock_list,
        patch.object(reader, "get_metadata", new_callable=AsyncMock) as mock_meta,
    ):
        mock_list.return_value = [uri]
        mock_meta.return_value = _default_metadata()
        # First call: small tail triggers overflow; second call: full file
        mock_dfs.read_file_range.side_effect = [small_tail, parquet_bytes]

        result = await reader.get_analysis(_WS_GUID, "item-guid", "mytable")

    assert result.summary.total_rows == 3
    assert mock_dfs.read_file_range.call_count == 2


# ---------------------------------------------------------------------------
# get_analysis — no dfs_client raises RuntimeError
# ---------------------------------------------------------------------------


async def test_get_analysis_requires_dfs_client():
    auth = MagicMock()
    reader = DeltaTableReader(auth, dfs_host=_DFS_HOST)
    reader._isolate = False

    with pytest.raises(RuntimeError, match="DfsClient required"):
        await reader.get_analysis(_WS_GUID, "item-guid", "mytable")
