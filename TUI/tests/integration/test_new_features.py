"""Integration tests for new features from the review-findings branch.

Covers:
- read_file_stream() — streaming reads and error handling
- read_file(max_bytes) — HEAD-first size enforcement
- max_items pagination — list_workspaces / list_items truncation
"""

from __future__ import annotations

import pytest

from onelake_client.exceptions import FileTooLargeError, NotFoundError


def _file_path(lakehouse_id: str, relative: str) -> str:
    """Build the full DFS read path: {lakehouse_id}/{relative}."""
    return f"{lakehouse_id}/{relative}"


# ---------------------------------------------------------------------------
# 1. read_file_stream()
# ---------------------------------------------------------------------------


class TestReadFileStream:
    async def test_stream_reassembles_to_full_content(self, client, workspace_id, lakehouse_id):
        """Streaming a file and joining chunks should match read_file output."""
        path = _file_path(lakehouse_id, "Files/sample.csv")

        # Read the whole file for reference
        full = await client.dfs.read_file(workspace_id, path)

        # Stream it and reassemble
        chunks: list[bytes] = []
        async for chunk in client.dfs.read_file_stream(workspace_id, path):
            chunks.append(chunk)
        streamed = b"".join(chunks)

        assert streamed == full
        assert len(chunks) >= 1

    async def test_stream_small_chunk_size(self, client, workspace_id, lakehouse_id):
        """Streaming with a tiny chunk size should still yield all data."""
        path = _file_path(lakehouse_id, "Files/sample.csv")
        full = await client.dfs.read_file(workspace_id, path)

        chunks: list[bytes] = []
        async for chunk in client.dfs.read_file_stream(workspace_id, path, chunk_size=64):
            chunks.append(chunk)
        streamed = b"".join(chunks)

        assert streamed == full
        # With a 64-byte chunk size, a CSV should produce multiple chunks
        if len(full) > 64:
            assert len(chunks) > 1

    async def test_stream_nonexistent_raises_not_found(self, client, workspace_id, lakehouse_id):
        """Streaming a nonexistent file should raise NotFoundError."""
        path = _file_path(lakehouse_id, "Files/does_not_exist_stream_test.txt")
        with pytest.raises(NotFoundError):
            async for _ in client.dfs.read_file_stream(workspace_id, path):
                pass

    async def test_stream_unicode_path(self, client, workspace_id, lakehouse_id):
        """Streaming works with unicode paths."""
        path = _file_path(lakehouse_id, "Files/données/résumé.csv")
        chunks: list[bytes] = []
        async for chunk in client.dfs.read_file_stream(workspace_id, path):
            chunks.append(chunk)
        data = b"".join(chunks)
        text = data.decode("utf-8")
        assert len(text) > 0


# ---------------------------------------------------------------------------
# 2. read_file(max_bytes) — HEAD-first enforcement
# ---------------------------------------------------------------------------


class TestReadFileMaxBytes:
    async def test_max_bytes_accepts_small_file(self, client, workspace_id, lakehouse_id):
        """A file within the max_bytes limit should be returned normally."""
        path = _file_path(lakehouse_id, "Files/sample.csv")
        # 10MB limit — sample.csv is tiny
        data = await client.dfs.read_file(workspace_id, path, max_bytes=10 * 1024 * 1024)
        assert len(data) > 0

    async def test_max_bytes_rejects_with_tiny_limit(self, client, workspace_id, lakehouse_id):
        """A file should be rejected if max_bytes is smaller than the file."""
        path = _file_path(lakehouse_id, "Files/sample.csv")

        # First, find out the actual size
        props = await client.dfs.get_properties(workspace_id, path)
        if props.content_length == 0:
            pytest.skip("sample.csv is 0 bytes — provisioning issue")

        # Set max_bytes to 1 byte — should fail
        with pytest.raises(FileTooLargeError) as exc_info:
            await client.dfs.read_file(workspace_id, path, max_bytes=1)

        assert exc_info.value.size == props.content_length
        assert exc_info.value.max_bytes == 1

    async def test_max_bytes_exact_boundary(self, client, workspace_id, lakehouse_id):
        """A file at exactly max_bytes should be accepted (not >)."""
        path = _file_path(lakehouse_id, "Files/sample.csv")
        props = await client.dfs.get_properties(workspace_id, path)
        if props.content_length == 0:
            pytest.skip("sample.csv is 0 bytes — provisioning issue")

        # Set max_bytes to the exact file size — should succeed
        data = await client.dfs.read_file(
            workspace_id, path, max_bytes=props.content_length
        )
        assert len(data) == props.content_length

    async def test_no_max_bytes_backwards_compat(self, client, workspace_id, lakehouse_id):
        """Without max_bytes, files of any size should be returned."""
        path = _file_path(lakehouse_id, "Files/sample.csv")
        data = await client.dfs.read_file(workspace_id, path)
        assert len(data) > 0


# ---------------------------------------------------------------------------
# 3. max_items pagination
# ---------------------------------------------------------------------------


class TestMaxItemsPagination:
    async def test_list_workspaces_max_items(self, client):
        """list_workspaces(max_items=2) should return at most 2."""
        workspaces = await client.fabric.list_workspaces(max_items=2)
        assert len(workspaces) <= 2
        assert len(workspaces) >= 1  # We know at least 1 workspace exists

    async def test_list_workspaces_max_items_vs_unlimited(self, client):
        """max_items should return a subset of the unlimited result."""
        all_ws = await client.fabric.list_workspaces()
        limited = await client.fabric.list_workspaces(max_items=1)
        assert len(limited) <= len(all_ws)
        assert len(limited) == 1 or len(all_ws) == 0

    async def test_list_items_max_items(self, client, workspace_id):
        """list_items(max_items=3) should return at most 3."""
        items = await client.fabric.list_items(workspace_id, max_items=3)
        assert len(items) <= 3

    async def test_list_items_max_items_vs_unlimited(self, client, workspace_id):
        """max_items should return a subset of the unlimited result."""
        all_items = await client.fabric.list_items(workspace_id)
        if len(all_items) < 2:
            pytest.skip("Need at least 2 items to test truncation")
        limited = await client.fabric.list_items(workspace_id, max_items=1)
        assert len(limited) == 1
        assert len(limited) < len(all_items)

    async def test_list_workspaces_none_returns_all(self, client):
        """max_items=None should return the same as no limit."""
        all_ws = await client.fabric.list_workspaces()
        explicit_none = await client.fabric.list_workspaces(max_items=None)
        assert len(all_ws) == len(explicit_none)


# ---------------------------------------------------------------------------
# 4. Large file tests (81 MB standalone parquet + 420 MB Delta table)
# ---------------------------------------------------------------------------

_LARGE_PARQUET = "Files/large_customers.parquet"
_LARGE_TABLE = "large_customers"
_LARGE_PARQUET_SIZE_MB = 81  # approximate
_MAX_BINARY_BYTES = 50 * 1024 * 1024  # 50 MB — matches detail.py limit


class TestLargeParquetFile:
    """Tests against an 81 MB standalone parquet file in Files/."""

    async def test_large_parquet_exists(self, client, workspace_id, lakehouse_id):
        path = _file_path(lakehouse_id, _LARGE_PARQUET)
        props = await client.dfs.get_properties(workspace_id, path)
        assert props.content_length > _MAX_BINARY_BYTES, (
            f"large_customers.parquet should be >{_MAX_BINARY_BYTES} bytes, "
            f"got {props.content_length}"
        )

    async def test_max_bytes_rejects_large_parquet(self, client, workspace_id, lakehouse_id):
        """HEAD-first check should reject an 81 MB parquet with a 50 MB limit."""
        path = _file_path(lakehouse_id, _LARGE_PARQUET)
        with pytest.raises(FileTooLargeError) as exc_info:
            await client.dfs.read_file(workspace_id, path, max_bytes=_MAX_BINARY_BYTES)
        assert exc_info.value.size > _MAX_BINARY_BYTES
        assert exc_info.value.max_bytes == _MAX_BINARY_BYTES

    async def test_stream_large_parquet(self, client, workspace_id, lakehouse_id):
        """Streaming should return the full file content for a large parquet."""
        path = _file_path(lakehouse_id, _LARGE_PARQUET)
        props = await client.dfs.get_properties(workspace_id, path)

        total = 0
        chunk_count = 0
        async for chunk in client.dfs.read_file_stream(workspace_id, path):
            total += len(chunk)
            chunk_count += 1

        assert total == props.content_length
        assert chunk_count > 1, "Large file should produce multiple chunks"

    @pytest.mark.slow
    async def test_stream_large_parquet_is_valid(self, client, workspace_id, lakehouse_id):
        """Streamed parquet bytes should parse into a valid pyarrow table."""
        import io

        import pyarrow.parquet as pq

        path = _file_path(lakehouse_id, _LARGE_PARQUET)
        chunks: list[bytes] = []
        async for chunk in client.dfs.read_file_stream(workspace_id, path):
            chunks.append(chunk)
        buf = io.BytesIO(b"".join(chunks))
        table = pq.read_table(buf)
        assert table.num_rows > 0
        assert table.num_columns >= 15  # 15 data cols + possible _change_type


class TestLargeDeltaTable:
    """Tests against a 420 MB / 9.9M-row Delta table."""

    async def test_metadata_loads(self, client, workspace_id, lakehouse_id):
        info = await client.delta.get_metadata(workspace_id, lakehouse_id, _LARGE_TABLE)
        assert info.num_files == 2
        assert info.total_rows == 9_900_000
        assert info.size_bytes > 400 * 1024 * 1024  # > 400 MB
        assert len(info.schema_) == 15

    async def test_read_sample_returns_limited_rows(self, client, workspace_id, lakehouse_id):
        """read_sample should return exactly `limit` rows, not all 9.9M."""
        sample = await client.delta.read_sample(
            workspace_id, lakehouse_id, _LARGE_TABLE, limit=50
        )
        assert sample.num_rows == 50
        col_names = (
            sample.column_names
            if hasattr(sample, "column_names")
            else [sample.schema.field(i).name for i in range(sample.num_columns)]
        )
        assert "customer_id" in col_names
        assert "email" in col_names

    async def test_list_files_returns_parquet(self, client, workspace_id, lakehouse_id):
        files = await client.delta.list_files(workspace_id, lakehouse_id, _LARGE_TABLE)
        assert len(files) == 2
        assert all(".parquet" in f for f in files)
