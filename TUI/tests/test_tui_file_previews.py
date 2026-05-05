"""Tests for TUI file preview rendering across all supported formats.

Each test mounts a DetailPanel in a harness app, calls preview_file()
with a mock client, and verifies the correct widget is rendered.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from textual.app import App, ComposeResult
from textual.widgets import DataTable, Markdown, Static, TextArea

from onelake_client.environment import DEFAULT_ENVIRONMENT
from onelake_tui.detail import _MAX_PREVIEW_BYTES, DetailPanel
from onelake_tui.nodes import FileNode

# ── Helpers ──────────────────────────────────────────────────────────

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _make_mock_client() -> MagicMock:
    """Build a mock OneLakeClient with the minimum surface area."""
    client = MagicMock()
    client.env = DEFAULT_ENVIRONMENT
    client.fabric.list_workspaces = AsyncMock(return_value=[])
    client.fabric.list_items = AsyncMock(return_value=[])
    client.dfs.list_paths = AsyncMock(return_value=[])
    client.dfs.read_file = AsyncMock()
    client.dfs.get_properties = AsyncMock(side_effect=Exception("not needed"))
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
        return "".join(seg.text for seg in line)
    except Exception:
        return ""


async def _preview_and_wait(pilot, detail: DetailPanel, node: FileNode) -> None:
    """Trigger preview_file and wait for the @work method to complete."""
    detail.preview_file(node)
    await pilot.pause()
    await asyncio.sleep(0.3)
    await pilot.pause()


# ── Markdown preview ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_markdown_preview():
    """Markdown files should render as a Markdown widget."""
    client = _make_mock_client()
    md_bytes = b"# Hello\n\nSome **bold** text\n"
    client.dfs.read_file = AsyncMock(return_value=md_bytes)
    app = _DetailHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        detail = app.query_one("#detail", DetailPanel)
        detail._workspace_name = "TestWS"
        detail._item_name = "TestItem"

        node = FileNode(workspace="w", path="i/Files/readme.md", size=len(md_bytes))
        await _preview_and_wait(pilot, detail, node)

        markdowns = detail.query(Markdown)
        assert len(markdowns) > 0, "Expected Markdown widget to be mounted for .md preview"


# ── CSV preview ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_csv_preview():
    """CSV files should render as a DataTable with correct column count."""
    client = _make_mock_client()
    csv_bytes = b"name,age,city\nAlice,30,NYC\nBob,25,London\n"
    client.dfs.read_file = AsyncMock(return_value=csv_bytes)
    app = _DetailHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        detail = app.query_one("#detail", DetailPanel)
        detail._workspace_name = "TestWS"
        detail._item_name = "TestItem"

        node = FileNode(workspace="w", path="i/Files/data.csv", size=len(csv_bytes))
        await _preview_and_wait(pilot, detail, node)

        tables = detail.query(DataTable)
        assert len(tables) > 0, "Expected DataTable to be mounted for CSV preview"
        assert len(tables[0].columns) == 3, f"Expected 3 columns, got {len(tables[0].columns)}"


# ── JSON preview ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_json_preview():
    """JSON files should render as a TextArea with pretty-printed content."""
    client = _make_mock_client()
    json_bytes = b'{"name": "test", "value": 42}'
    client.dfs.read_file = AsyncMock(return_value=json_bytes)
    app = _DetailHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        detail = app.query_one("#detail", DetailPanel)
        detail._workspace_name = "TestWS"
        detail._item_name = "TestItem"

        node = FileNode(workspace="w", path="i/Files/data.json", size=len(json_bytes))
        await _preview_and_wait(pilot, detail, node)

        textareas = detail.query(TextArea)
        assert len(textareas) > 0, "Expected TextArea to be mounted for JSON preview"
        content = textareas[0].text
        assert '"name"' in content, "Expected pretty-printed JSON with 'name' key"
        assert '"test"' in content, "Expected pretty-printed JSON with 'test' value"


# ── NDJSON preview ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_ndjson_preview():
    """NDJSON (newline-delimited JSON) should format each line."""
    client = _make_mock_client()
    ndjson_bytes = b'{"a":1}\n{"a":2}\n'
    client.dfs.read_file = AsyncMock(return_value=ndjson_bytes)
    app = _DetailHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        detail = app.query_one("#detail", DetailPanel)
        detail._workspace_name = "TestWS"
        detail._item_name = "TestItem"

        node = FileNode(workspace="w", path="i/Files/data.json", size=len(ndjson_bytes))
        await _preview_and_wait(pilot, detail, node)

        textareas = detail.query(TextArea)
        assert len(textareas) > 0, "Expected TextArea to be mounted for NDJSON preview"
        content = textareas[0].text
        # NDJSON falls through to the except branch and formats each line
        assert '"a"' in content, "Expected formatted NDJSON with 'a' key"


# ── Parquet preview ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_parquet_preview():
    """Parquet files should render as DataTable with columns matching schema."""
    client = _make_mock_client()
    parquet_path = FIXTURES_DIR / "parquet" / "all_types.parquet"
    parquet_bytes = parquet_path.read_bytes()
    client.dfs.read_file = AsyncMock(return_value=parquet_bytes)
    app = _DetailHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        detail = app.query_one("#detail", DetailPanel)
        detail._workspace_name = "TestWS"
        detail._item_name = "TestItem"

        node = FileNode(workspace="w", path="i/Files/data.parquet", size=len(parquet_bytes))
        await _preview_and_wait(pilot, detail, node)

        tables = detail.query(DataTable)
        assert len(tables) > 0, "Expected DataTable to be mounted for Parquet preview"
        # The parquet preview mounts a schema DataTable and a data DataTable
        # At least one should have columns
        assert any(len(t.columns) > 0 for t in tables), (
            "Expected DataTable with columns for Parquet schema/data"
        )


# ── Syntax-highlighted preview (.py) ─────────────────────────────────


@pytest.mark.asyncio
async def test_syntax_highlighted_python_preview():
    """Python files should render as a TextArea with syntax highlighting."""
    client = _make_mock_client()
    py_bytes = b"def hello():\n    print('world')\n"
    client.dfs.read_file = AsyncMock(return_value=py_bytes)
    app = _DetailHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        detail = app.query_one("#detail", DetailPanel)
        detail._workspace_name = "TestWS"
        detail._item_name = "TestItem"

        node = FileNode(workspace="w", path="i/Files/script.py", size=len(py_bytes))
        await _preview_and_wait(pilot, detail, node)

        textareas = detail.query(TextArea)
        assert len(textareas) > 0, "Expected TextArea to be mounted for .py preview"
        assert "def hello" in textareas[0].text


# ── SQL preview ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_sql_preview():
    """SQL files should render as a TextArea."""
    client = _make_mock_client()
    sql_bytes = b"SELECT * FROM customers WHERE id = 1;"
    client.dfs.read_file = AsyncMock(return_value=sql_bytes)
    app = _DetailHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        detail = app.query_one("#detail", DetailPanel)
        detail._workspace_name = "TestWS"
        detail._item_name = "TestItem"

        node = FileNode(workspace="w", path="i/Files/query.sql", size=len(sql_bytes))
        await _preview_and_wait(pilot, detail, node)

        textareas = detail.query(TextArea)
        assert len(textareas) > 0, "Expected TextArea to be mounted for .sql preview"
        assert "SELECT" in textareas[0].text


# ── Hex dump preview (binary / unknown) ──────────────────────────────


@pytest.mark.asyncio
async def test_hex_dump_preview():
    """Unknown binary files should render as a hex dump in a Static widget."""
    client = _make_mock_client()
    binary_bytes = b"\x00\x01\x02\xff\xfe" + b"\x00" * 20
    client.dfs.read_file = AsyncMock(return_value=binary_bytes)
    app = _DetailHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        detail = app.query_one("#detail", DetailPanel)
        detail._workspace_name = "TestWS"
        detail._item_name = "TestItem"

        node = FileNode(workspace="w", path="i/Files/unknown.bin", size=len(binary_bytes))
        await _preview_and_wait(pilot, detail, node)

        statics = detail.query(Static)
        static_texts = [_get_widget_text(w) for w in statics]
        assert any("Binary file" in txt or "00000000" in txt for txt in static_texts), (
            f"Expected hex dump or binary file message. Found: {static_texts}"
        )


# ── Large file preview ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_large_file_preview():
    """Files exceeding the size limit should show a 'too large' message."""
    client = _make_mock_client()
    app = _DetailHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        detail = app.query_one("#detail", DetailPanel)
        detail._workspace_name = "TestWS"
        detail._item_name = "TestItem"

        file_size = _MAX_PREVIEW_BYTES + 1024 * 1024  # well over limit
        node = FileNode(workspace="w", path="i/Files/huge.txt", size=file_size)
        await _preview_and_wait(pilot, detail, node)

        statics = detail.query(Static)
        static_texts = [_get_widget_text(w) for w in statics]
        assert any("too large" in txt for txt in static_texts), (
            f"Expected 'too large' message. Found: {static_texts}"
        )

        # read_file should NOT have been called
        client.dfs.read_file.assert_not_awaited()


# ── Empty file preview ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_empty_file_preview():
    """Empty files should be handled gracefully without crashing."""
    client = _make_mock_client()
    client.dfs.read_file = AsyncMock(return_value=b"")
    app = _DetailHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        detail = app.query_one("#detail", DetailPanel)
        detail._workspace_name = "TestWS"
        detail._item_name = "TestItem"

        node = FileNode(workspace="w", path="i/Files/empty.csv", size=0)
        await _preview_and_wait(pilot, detail, node)

        # Should not crash — the panel remains mounted
        assert detail.is_mounted, "DetailPanel should remain mounted for empty file"


# ── Network error during preview ─────────────────────────────────────


@pytest.mark.asyncio
async def test_preview_network_error():
    """When dfs.read_file raises, show 'Preview failed' message."""
    client = _make_mock_client()
    client.dfs.read_file = AsyncMock(side_effect=Exception("Connection refused"))
    app = _DetailHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        detail = app.query_one("#detail", DetailPanel)
        detail._workspace_name = "TestWS"
        detail._item_name = "TestItem"

        node = FileNode(workspace="w", path="i/Files/data.txt", size=100)
        await _preview_and_wait(pilot, detail, node)

        statics = detail.query(Static)
        static_texts = [_get_widget_text(w) for w in statics]
        assert any("Preview failed" in txt for txt in static_texts), (
            f"Expected 'Preview failed' message. Found: {static_texts}"
        )


# ── YAML preview ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_yaml_preview():
    """YAML files should render as a TextArea with syntax highlighting."""
    client = _make_mock_client()
    yaml_bytes = b"name: test\nversion: 1.0\nitems:\n  - one\n  - two\n"
    client.dfs.read_file = AsyncMock(return_value=yaml_bytes)
    app = _DetailHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        detail = app.query_one("#detail", DetailPanel)
        detail._workspace_name = "TestWS"
        detail._item_name = "TestItem"

        node = FileNode(workspace="w", path="i/Files/config.yaml", size=len(yaml_bytes))
        await _preview_and_wait(pilot, detail, node)

        textareas = detail.query(TextArea)
        assert len(textareas) > 0, "Expected TextArea to be mounted for .yaml preview"
        assert "name: test" in textareas[0].text
