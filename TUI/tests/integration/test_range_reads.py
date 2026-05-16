"""Integration tests for DFS Range read support (read_file_range).

Validates that suffix, offset+length, and offset-only range reads work
against real OneLake DFS files. OneLake may return 200 (full file) instead
of 206 (partial content) per RFC 7233 §4.4 — the client handles both.

Run:
    uv run pytest tests/integration/test_range_reads.py -v
"""

from __future__ import annotations

import struct


async def test_suffix_range_read(client, workspace_id, lakehouse_id):
    """Suffix range should return data (200 or 206)."""
    # Read the last 100 bytes of a known file
    data = await client.dfs.read_file_range(
        workspace_id,
        f"{lakehouse_id}/Files/sample.csv",
        suffix_length=100,
    )
    assert len(data) > 0


async def test_suffix_range_larger_than_file(client, workspace_id, lakehouse_id):
    """Suffix larger than file should return full file content."""
    # Read a small file with a large suffix — should get full file
    full = await client.dfs.read_file(
        workspace_id, f"{lakehouse_id}/Files/sample.csv"
    )
    tail = await client.dfs.read_file_range(
        workspace_id,
        f"{lakehouse_id}/Files/sample.csv",
        suffix_length=1024 * 1024,  # 1MB, likely larger than the file
    )
    # OneLake returns full file for both — content should match
    assert tail == full


async def test_offset_range_read(client, workspace_id, lakehouse_id):
    """Offset+length range should return data."""
    data = await client.dfs.read_file_range(
        workspace_id,
        f"{lakehouse_id}/Files/sample.csv",
        offset=0,
        length=50,
    )
    assert len(data) > 0


async def test_offset_only_range_read(client, workspace_id, lakehouse_id):
    """Offset-only should return from offset to end."""
    data = await client.dfs.read_file_range(
        workspace_id,
        f"{lakehouse_id}/Files/sample.csv",
        offset=0,
    )
    assert len(data) > 0


async def test_range_read_parquet_footer(client, workspace_id, lakehouse_id):
    """Read the tail of a parquet file to verify footer extraction works."""
    # Get a parquet file path from a known table
    files = await client.delta.list_files(workspace_id, lakehouse_id, "customers")
    assert len(files) > 0

    from onelake_client.tables.delta import _parse_dfs_path

    ws_guid, file_path = _parse_dfs_path(files[0])

    tail = await client.dfs.read_file_range(ws_guid, file_path, suffix_length=8192)
    assert len(tail) >= 8

    # Verify parquet magic at end
    assert tail[-4:] == b"PAR1", f"Last 4 bytes: {tail[-4:]!r}"

    # Parse footer length
    footer_len = struct.unpack("<I", tail[-8:-4])[0]
    assert footer_len > 0
    assert footer_len < len(tail), "Footer should fit in the tail buffer"
