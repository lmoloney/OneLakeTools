"""Tests for DetailPanel file preview error states and rendering."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from textual.app import App, ComposeResult
from textual.widgets import DataTable, Markdown, Static, TextArea

from onelake_client.environment import DEFAULT_ENVIRONMENT
from onelake_client.exceptions import FileTooLargeError
from onelake_client.models import PathInfo
from onelake_tui.detail import (
    _MAX_PREVIEW_BYTES,
    DetailPanel,
)
from onelake_tui.nodes import FileNode, TableNode

# ── Helpers ──────────────────────────────────────────────────────────


def _make_mock_client() -> MagicMock:
    """Build a mock OneLakeClient with the minimum surface area."""
    client = MagicMock()
    client.env = DEFAULT_ENVIRONMENT
    client.fabric.list_workspaces = AsyncMock(return_value=[])
    client.fabric.list_items = AsyncMock(return_value=[])
    client.dfs.list_paths = AsyncMock(return_value=[])
    client.dfs.read_file = AsyncMock()
    client.auth.get_identity = MagicMock(return_value="test-user@contoso.com")
    client.close = AsyncMock()
    return client


class _DetailHarness(App):
    """Minimal app that mounts only DetailPanel."""

    def __init__(self, client: MagicMock):
        super().__init__()
        self._client = client

    def compose(self) -> ComposeResult:
        yield DetailPanel(self._client, id="detail")


def _get_widget_text(widget) -> str:
    """Extract plain text from a rendered Textual widget."""
    try:
        line = widget.render_line(0)
        # Strip is iterable of Segments; extract text from each
        return "".join(seg.text for seg in line)
    except Exception:
        return ""


# ── Tests ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_preview_file_too_large():
    """When file size exceeds _MAX_PREVIEW_BYTES, show 'too large' message."""
    client = _make_mock_client()
    app = _DetailHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        detail = app.query_one("#detail", DetailPanel)
        detail._workspace_name = "TestWS"
        detail._item_name = "TestItem"

        # Create a file node with size exceeding _MAX_PREVIEW_BYTES
        file_size = _MAX_PREVIEW_BYTES + 1024 * 1024  # 1MB over the limit
        file_node = FileNode(workspace="ws", path="item/Files/large.txt", size=file_size)

        # Call preview_file (which is @work decorated, so call it directly)
        detail.preview_file(file_node)

        # Wait for workers and debounce
        await pilot.pause()
        await asyncio.sleep(0.3)
        await pilot.pause()

        # Verify a Static widget containing "too large" is mounted
        statics = detail.query(Static)
        static_texts = [_get_widget_text(w) for w in statics]
        assert any("too large" in txt for txt in static_texts), (
            f"Expected 'too large' message. Found: {static_texts}"
        )


@pytest.mark.asyncio
async def test_preview_file_network_error():
    """When dfs.read_file raises an exception, show 'Preview failed' message."""
    client = _make_mock_client()
    client.dfs.read_file = AsyncMock(side_effect=Exception("Connection refused"))
    app = _DetailHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        detail = app.query_one("#detail", DetailPanel)
        detail._workspace_name = "TestWS"
        detail._item_name = "TestItem"

        file_node = FileNode(workspace="ws", path="item/Files/data.txt", size=100)
        detail.preview_file(file_node)

        await pilot.pause()
        await asyncio.sleep(0.3)
        await pilot.pause()

        statics = detail.query(Static)
        static_texts = [_get_widget_text(w) for w in statics]
        assert any("Preview failed" in txt for txt in static_texts), (
            f"Expected 'Preview failed' message. Found: {static_texts}"
        )


@pytest.mark.asyncio
async def test_preview_csv_renders_datatable():
    """When file is CSV, render as a DataTable."""
    client = _make_mock_client()
    csv_data = b"name,age\nAlice,30\nBob,25"
    client.dfs.read_file = AsyncMock(return_value=csv_data)
    app = _DetailHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        detail = app.query_one("#detail", DetailPanel)
        detail._workspace_name = "TestWS"
        detail._item_name = "TestItem"

        file_node = FileNode(workspace="ws", path="item/Files/data.csv", size=len(csv_data))
        detail.preview_file(file_node)

        await pilot.pause()
        await asyncio.sleep(0.3)
        await pilot.pause()

        # Verify a DataTable widget is mounted
        datatables = detail.query(DataTable)
        assert len(datatables) > 0, "Expected DataTable to be mounted for CSV preview"
        table = datatables[0]
        # Verify the table has columns
        assert table.columns, "Expected DataTable to have columns"


@pytest.mark.asyncio
async def test_preview_json_pretty_prints():
    """When file is JSON, render as a TextArea with formatted JSON."""
    client = _make_mock_client()
    json_data = b'{"key":"value"}'
    client.dfs.read_file = AsyncMock(return_value=json_data)
    app = _DetailHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        detail = app.query_one("#detail", DetailPanel)
        detail._workspace_name = "TestWS"
        detail._item_name = "TestItem"

        file_node = FileNode(workspace="ws", path="item/Files/data.json", size=len(json_data))
        detail.preview_file(file_node)

        await pilot.pause()
        await asyncio.sleep(0.3)
        await pilot.pause()

        # Verify a TextArea widget is mounted
        textareas = detail.query(TextArea)
        assert len(textareas) > 0, "Expected TextArea to be mounted for JSON preview"


@pytest.mark.asyncio
async def test_preview_markdown_renders():
    """When file is Markdown, render as a Markdown widget."""
    client = _make_mock_client()
    md_data = b"# Hello\nWorld"
    client.dfs.read_file = AsyncMock(return_value=md_data)
    app = _DetailHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        detail = app.query_one("#detail", DetailPanel)
        detail._workspace_name = "TestWS"
        detail._item_name = "TestItem"

        file_node = FileNode(workspace="ws", path="item/Files/README.md", size=len(md_data))
        detail.preview_file(file_node)

        await pilot.pause()
        await asyncio.sleep(0.3)
        await pilot.pause()

        # Verify a Markdown widget is mounted
        markdowns = detail.query(Markdown)
        assert len(markdowns) > 0, "Expected Markdown to be mounted for .md preview"


@pytest.mark.asyncio
async def test_preview_binary_shows_hex():
    """When file is binary (has null bytes), render as hex dump."""
    client = _make_mock_client()
    binary_data = b"\x00\x01\x02\x03"
    client.dfs.read_file = AsyncMock(return_value=binary_data)
    app = _DetailHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        detail = app.query_one("#detail", DetailPanel)
        detail._workspace_name = "TestWS"
        detail._item_name = "TestItem"

        file_node = FileNode(workspace="ws", path="item/Files/data.bin", size=len(binary_data))
        detail.preview_file(file_node)

        await pilot.pause()
        await asyncio.sleep(0.3)
        await pilot.pause()

        # Verify a Static widget (hex dump) is mounted
        statics = detail.query(Static)
        static_texts = [_get_widget_text(w) for w in statics]
        # Hex dump should mention first 256 bytes or contain hex content
        assert any("Binary file" in txt or "00000000" in txt for txt in static_texts), (
            f"Expected hex dump or binary file message. Found: {static_texts}"
        )


@pytest.mark.asyncio
async def test_preview_ndjson_formats_lines():
    """When file is NDJSON (newline-delimited JSON), format each line."""
    client = _make_mock_client()
    ndjson_data = b'{"a":1}\n{"b":2}'
    client.dfs.read_file = AsyncMock(return_value=ndjson_data)
    app = _DetailHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        detail = app.query_one("#detail", DetailPanel)
        detail._workspace_name = "TestWS"
        detail._item_name = "TestItem"

        file_node = FileNode(workspace="ws", path="item/Files/log.json", size=len(ndjson_data))
        detail.preview_file(file_node)

        await pilot.pause()
        await asyncio.sleep(0.3)
        await pilot.pause()

        # Verify a TextArea is mounted with formatted JSON
        textareas = detail.query(TextArea)
        assert len(textareas) > 0, "Expected TextArea to be mounted for NDJSON preview"


@pytest.mark.asyncio
async def test_read_parquet_fallback_skips_oversized_unknown_and_tries_next():
    """Parquet fallback should keep trying candidates when unknown-size file exceeds max_bytes."""
    client = _make_mock_client()
    detail = DetailPanel(client)
    table = TableNode(workspace="ws", item_path="item-guid", table_name="dbo/table")

    client.dfs.list_paths = AsyncMock(
        return_value=[
            PathInfo(name="item-guid/Tables/dbo/table/huge.parquet", isDirectory=False),
            PathInfo(name="item-guid/Tables/dbo/table/small.parquet", isDirectory=False),
        ]
    )
    client.dfs.read_file = AsyncMock(
        side_effect=[
            FileTooLargeError(size=100 * 1024 * 1024, max_bytes=50 * 1024 * 1024),
            b"parquet-bytes",
        ]
    )

    sample = MagicMock(name="sample")
    row_groups = MagicMock(name="row_groups")
    row_groups.slice.return_value = sample
    parquet_file = MagicMock(name="parquet_file")
    parquet_file.read_row_groups.return_value = row_groups

    with patch("pyarrow.parquet.ParquetFile", return_value=parquet_file):
        result = await detail._read_parquet_fallback(table)

    assert result is sample
    assert client.dfs.read_file.await_count == 2
