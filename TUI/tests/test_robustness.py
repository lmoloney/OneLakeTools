"""Robustness tests for edge-case and malformed inputs."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError
from textual.app import App, ComposeResult

from onelake_client.environment import DEFAULT_ENVIRONMENT
from onelake_client.models import Item, PathInfo, Workspace
from onelake_tui.detail import DetailPanel
from onelake_tui.nodes import FileNode, FolderNode, TableNode
from onelake_tui.status_bar import StatusBar

# ── Model robustness tests ───────────────────────────────────────────


def test_workspace_name_very_long():
    """Verify Workspace handles very long displayName (2000 chars)."""
    long_name = "A" * 2000
    ws = Workspace(id="ws-1", displayName=long_name, type="Workspace")
    assert ws.display_name == long_name
    assert len(ws.display_name) == 2000


def test_item_name_null_bytes():
    """Verify Item handles displayName with null bytes without crash."""
    # Null bytes in strings are valid in Python
    name_with_null = "test\x00name"
    item = Item(id="i-1", displayName=name_with_null, type="Lakehouse")
    assert item.display_name == name_with_null
    assert "\x00" in item.display_name


def test_path_with_special_chars():
    """Verify PathInfo preserves special characters in name."""
    special_name = "data [copy]/file (1).csv"
    path = PathInfo(name=special_name, isDirectory=False, contentLength=42)
    assert path.name == special_name

    # Test brackets and backticks
    brackets_name = "[file]_`code`.txt"
    path2 = PathInfo(name=brackets_name, isDirectory=False)
    assert path2.name == brackets_name


def test_api_response_wrong_type():
    """Verify Workspace rejects wrong type (int instead of str).

    Pydantic v2 is strict about types and does NOT coerce int → str
    for string fields by default. This test verifies the rejection.
    """

    # Pydantic rejects the wrong type
    with pytest.raises(ValidationError) as exc_info:
        Workspace(id="ws", displayName=12345, type="Workspace")  # type: ignore

    # Verify it's a type validation error
    assert "string_type" in str(exc_info.value)


def test_api_response_extra_fields():
    """Verify Workspace ignores extra unknown fields (Pydantic default)."""
    # By default, Pydantic ignores extra fields
    ws = Workspace(
        id="ws",
        displayName="test",
        type="Workspace",
        unknownField="extra",  # type: ignore
    )
    assert ws.id == "ws"
    assert ws.display_name == "test"
    # Extra field should be ignored
    assert not hasattr(ws, "unknownField")


def test_api_response_empty_string_id():
    """Verify Workspace accepts empty string id (not None)."""
    ws = Workspace(id="", displayName="test", type="Workspace")
    assert ws.id == ""
    assert ws.display_name == "test"


def test_deeply_nested_path():
    """Verify PathInfo handles deeply nested paths (100 levels)."""
    deep_path_parts = ["dir"] * 100 + ["file.csv"]
    deep_path = "/".join(deep_path_parts)

    path = PathInfo(name=deep_path, isDirectory=False)
    assert path.name == deep_path
    assert path.name.count("/") == 100


def test_file_size_zero():
    """Verify FileNode accepts size=0."""
    node = FileNode(workspace="ws", path="item/Files/empty.csv", size=0)
    assert node.workspace == "ws"
    assert node.path == "item/Files/empty.csv"
    assert node.size == 0


def test_file_size_negative():
    """Verify FileNode accepts negative size (dataclasses don't validate)."""
    node = FileNode(workspace="ws", path="item/Files/bad.csv", size=-1)
    assert node.size == -1


def test_table_name_with_slashes():
    """Verify TableNode preserves slashes in table_name."""
    table = TableNode(workspace="ws", item_path="item", table_name="schema/sub/table")
    assert table.table_name == "schema/sub/table"
    assert table.table_name.count("/") == 2


# ── StatusBar widget robustness tests ─────────────────────────────


class _StatusBarHarness(App):
    """Minimal app that mounts only StatusBar for testing."""

    def compose(self) -> ComposeResult:
        yield StatusBar(id="status")


@pytest.mark.asyncio
async def test_status_bar_very_long_path():
    """Verify StatusBar truncates very long paths and render doesn't crash.

    Path is truncated to fit in 80 chars, replaced with "…" prefix.
    Verify render() output is valid and reasonably short.
    """
    app = _StatusBarHarness()

    async with app.run_test() as pilot:
        await pilot.pause()
        status = app.query_one("#status", StatusBar)

        # Set a 200-char path
        long_path = "a" * 200
        status.path = long_path

        output = status.render()

        # Should be truncated with "…" prefix
        assert "…" in output or len(output.split("\n")[0]) < 200
        # Render output should not be excessively long
        first_line = output.split("\n")[0]
        assert len(first_line) < 200


@pytest.mark.asyncio
async def test_status_bar_special_chars_in_path():
    """Verify StatusBar handles special chars and Rich markup escaping.

    Path with brackets, braces, etc. should not crash render().
    Rich's escape() function should handle markup chars.
    """
    app = _StatusBarHarness()

    async with app.run_test() as pilot:
        await pilot.pause()
        status = app.query_one("#status", StatusBar)

        # Set path with special chars that could be Rich markup
        special_path = "ws / [special] / file [copy].txt"
        status.path = special_path

        # render() should not crash
        output = status.render()
        assert output is not None
        assert len(output) > 0


@pytest.mark.asyncio
async def test_status_bar_render_with_various_fields():
    """Verify StatusBar render handles various combinations of fields."""
    app = _StatusBarHarness()

    async with app.run_test() as pilot:
        await pilot.pause()
        status = app.query_one("#status", StatusBar)

        # Test with various field combinations
        status.path = "test-workspace / item-name"
        status.item_count = 42
        status.auth_method = "msal"
        status.identity = "user@contoso.com"
        status.env_name = "DEV"

        output = status.render()

        assert "test-workspace / item-name" in output
        assert "42 items" in output
        assert "msal" in output
        assert "user@contoso.com" in output
        assert "DEV" in output


@pytest.mark.asyncio
async def test_status_bar_empty_fields():
    """Verify StatusBar render handles empty/zero fields gracefully."""
    app = _StatusBarHarness()

    async with app.run_test() as pilot:
        await pilot.pause()
        status = app.query_one("#status", StatusBar)

        # Empty/zero values
        status.path = ""
        status.item_count = 0
        status.identity = ""
        status.env_name = ""

        output = status.render()

        # Should not crash and produce valid output
        assert output is not None
        assert len(output) > 0


# ── DetailPanel with edge-case nodes ─────────────────────────────


class _DetailHarness(App):
    """Minimal app for testing DetailPanel with edge-case nodes."""

    def __init__(self, client: MagicMock):
        super().__init__()
        self._client = client

    def compose(self) -> ComposeResult:
        yield DetailPanel(self._client, id="detail")


def _make_mock_client() -> MagicMock:
    """Create a minimal mock OneLakeClient."""
    client = MagicMock()
    client.env = DEFAULT_ENVIRONMENT
    client.dfs.exists = AsyncMock(return_value=False)
    client.close = AsyncMock()
    return client


@pytest.mark.asyncio
async def test_detail_panel_folder_node_special_chars():
    """Verify DetailPanel._show_folder handles special chars in directory name."""
    client = _make_mock_client()
    app = _DetailHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        detail = app.query_one("#detail", DetailPanel)
        detail._workspace_name = "TestWS"
        detail._item_name = "TestItem"

        # FolderNode with special chars in directory
        folder = FolderNode(
            workspace="ws",
            item_path="item",
            directory="Files/[backup]/data (v2)",
        )

        detail.update_for_node(folder)
        await pilot.pause()

        # Should render without crash
        assert detail.is_mounted


@pytest.mark.asyncio
async def test_detail_panel_file_node_very_long_path():
    """Verify DetailPanel._show_file handles very long file paths."""
    client = _make_mock_client()
    app = _DetailHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        detail = app.query_one("#detail", DetailPanel)
        detail._workspace_name = "TestWS"
        detail._item_name = "TestItem"

        # Very long path
        long_path = "item/Files/" + "/".join(["subdir"] * 50) + "/file.txt"
        file_node = FileNode(workspace="ws", path=long_path, size=1024)

        detail.update_for_node(file_node)
        await pilot.pause()

        # Should render without crash
        assert detail.is_mounted


@pytest.mark.asyncio
async def test_detail_panel_table_node_complex_name():
    """Verify DetailPanel._show_table handles complex table names."""
    client = _make_mock_client()
    app = _DetailHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        detail = app.query_one("#detail", DetailPanel)
        detail._workspace_name = "TestWS"
        detail._item_name = "TestItem"

        # Complex table name with special chars
        table = TableNode(
            workspace="ws",
            item_path="item",
            table_name="[dbo].[my_table-v2.0]",
        )

        detail.update_for_node(table)
        await pilot.pause()

        # Should render without crash
        assert detail.is_mounted


@pytest.mark.asyncio
async def test_detail_panel_rapid_node_changes_with_edge_cases():
    """Verify rapid switching between edge-case nodes doesn't crash."""
    client = _make_mock_client()
    app = _DetailHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        detail = app.query_one("#detail", DetailPanel)
        detail._workspace_name = "TestWS"
        detail._item_name = "TestItem"

        # Create nodes with edge cases
        folder_with_special = FolderNode(
            workspace="ws", item_path="item", directory="Files/[archive]"
        )
        file_with_long_path = FileNode(
            workspace="ws",
            path="item/Files/" + "/".join(["d"] * 50) + "/file.csv",
            size=0,
        )
        table_complex = TableNode(
            workspace="ws", item_path="item", table_name="schema/[table]/name"
        )

        # Rapid switching
        for _ in range(5):
            detail.update_for_node(folder_with_special)
            detail.update_for_node(file_with_long_path)
            detail.update_for_node(table_complex)
            detail.update_for_node(None)

        await pilot.pause()

        # Should not crash
        assert detail.is_mounted


# ── Edge cases with pathinfo and filesystem models ─────────────────


def test_path_info_alias_fields():
    """Verify PathInfo correctly aliases camelCase to snake_case."""
    path = PathInfo(
        name="test.txt",
        isDirectory=False,
        contentLength=1024,
    )
    assert path.is_directory is False
    assert path.content_length == 1024

    # Also test populate_by_name with snake_case
    path2 = PathInfo(
        name="test2.txt",
        is_directory=True,
        content_length=2048,
    )
    assert path2.is_directory is True
    assert path2.content_length == 2048


def test_path_info_optional_fields():
    """Verify PathInfo handles optional fields gracefully."""
    path = PathInfo(name="test.txt", isDirectory=False)
    # Optional fields should default to None
    assert path.content_length is None
    assert path.last_modified is None
    assert path.etag is None


def test_workspace_optional_fields():
    """Verify Workspace handles optional fields gracefully."""
    ws = Workspace(id="ws", displayName="test", type="Workspace")
    # Optional fields should default to None
    assert ws.description is None
    assert ws.capacity_id is None
    assert ws.state is None


def test_item_optional_fields():
    """Verify Item handles optional fields gracefully."""
    item = Item(id="i", displayName="test", type="Lakehouse")
    # Optional fields should default to None
    assert item.description is None
    assert item.workspace_id is None
