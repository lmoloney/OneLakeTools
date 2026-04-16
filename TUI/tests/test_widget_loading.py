"""Tests for StatusBar widget and workspace/item loading functionality."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from textual.app import App, ComposeResult

from onelake_client.environment import DEFAULT_ENVIRONMENT
from onelake_client.models import Item, Workspace
from onelake_tui.item_list import ItemList
from onelake_tui.status_bar import StatusBar
from onelake_tui.workspace_picker import WorkspacePicker

# ── Helpers ──────────────────────────────────────────────────────────


def _make_mock_client() -> MagicMock:
    """Build a mock OneLakeClient with the minimum surface area."""
    client = MagicMock()
    client.env = DEFAULT_ENVIRONMENT
    client.fabric.list_workspaces = AsyncMock(return_value=[])
    client.fabric.list_items = AsyncMock(return_value=[])
    client.dfs.list_paths = AsyncMock(return_value=[])
    client.auth.get_identity = MagicMock(return_value="test-user@contoso.com")
    client.close = AsyncMock()
    return client


class _StatusBarHarness(App):
    """Minimal app that mounts only StatusBar for testing."""

    def compose(self) -> ComposeResult:
        yield StatusBar(id="status")


class _PickerHarness(App):
    """Minimal app that mounts only WorkspacePicker."""

    def __init__(self, client: MagicMock):
        super().__init__()
        self._client = client

    def compose(self) -> ComposeResult:
        yield WorkspacePicker(self._client, id="picker")


class _ItemListHarness(App):
    """Minimal app that mounts only ItemList."""

    def __init__(self, client: MagicMock):
        super().__init__()
        self._client = client

    def compose(self) -> ComposeResult:
        yield ItemList(self._client, id="items")


# ── StatusBar tests (using Textual run_test) ────────────────────────


@pytest.mark.asyncio
async def test_status_bar_render_default():
    """Mount a StatusBar in an app harness. Verify render() output contains expected elements."""
    app = _StatusBarHarness()

    async with app.run_test() as pilot:
        await pilot.pause()
        status = app.query_one("#status", StatusBar)

        output = status.render()

        # Verify output contains expected elements: emoji, shortcuts, auth info
        assert "📍" in output, "Status bar should contain location emoji"
        assert "Enter Preview" in output, "Status bar should contain keyboard shortcuts"
        assert "🔑" in output, "Status bar should contain auth emoji"
        # Check that it's a multi-line string (3 lines as per render())
        lines = output.split("\n")
        assert len(lines) == 3, "Status bar should have 3 lines"


@pytest.mark.asyncio
async def test_status_bar_update_path():
    """Mount StatusBar, call update_path(). Verify the path reactive property updated."""
    app = _StatusBarHarness()

    async with app.run_test() as pilot:
        await pilot.pause()
        status = app.query_one("#status", StatusBar)

        new_path = "onelake://MyWS/MyLH"
        status.update_path(new_path)

        # Verify the path property was updated
        assert status.path == new_path
        # Verify it appears in render() output
        assert new_path in status.render()


@pytest.mark.asyncio
async def test_status_bar_long_path_truncated():
    """Set path to a 200-char string. Verify render() output is truncated with '…'."""
    app = _StatusBarHarness()

    async with app.run_test() as pilot:
        await pilot.pause()
        status = app.query_one("#status", StatusBar)

        # Create a long path (>80 chars)
        long_path = "onelake://" + "a" * 200
        status.path = long_path

        output = status.render()
        first_line = output.split("\n")[0]

        # Verify truncation: the path portion (after emoji) should contain "…"
        assert "…" in first_line, "Long path should be truncated with '…' prefix"
        # First line should not be as long as the original path
        assert len(first_line) < len(long_path) - 100


@pytest.mark.asyncio
async def test_status_bar_msit_env_escaped():
    """Set env_name to 'MSIT'. Verify render() contains \\[MSIT] (escaped, not swallowed)."""
    app = _StatusBarHarness()

    async with app.run_test() as pilot:
        await pilot.pause()
        status = app.query_one("#status", StatusBar)

        status.env_name = "MSIT"
        output = status.render()

        # The render() method returns Rich markup, so the environment label
        # must be bracket-escaped to avoid being interpreted as a tag.
        assert "\\[MSIT]" in output, (
            "StatusBar should escape MSIT environment brackets in Rich markup output"
        )


@pytest.mark.asyncio
async def test_status_bar_identity_display():
    """Set identity property. Verify it appears in render() output."""
    app = _StatusBarHarness()

    async with app.run_test() as pilot:
        await pilot.pause()
        status = app.query_one("#status", StatusBar)

        test_identity = "user@contoso.com"
        status.identity = test_identity

        output = status.render()

        # Verify identity appears in the rendered output
        assert test_identity in output, "Identity should appear in render() output"


# ── WorkspacePicker tests ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_workspace_picker_load_populates():
    """Load workspaces via load_workspaces(). Verify the picker has options."""
    client = _make_mock_client()

    # Mock list_workspaces to return 3 Workspace objects
    workspaces = [
        Workspace(id="ws-1", displayName="FabricTest", type="Workspace"),
        Workspace(id="ws-2", displayName="Analytics", type="Workspace"),
        Workspace(id="ws-3", displayName="FabricDemo", type="Workspace"),
    ]
    client.fabric.list_workspaces = AsyncMock(return_value=workspaces)

    app = _PickerHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        picker = app.query_one("#picker", WorkspacePicker)

        # Wait for load_workspaces to complete (it's a @work task)
        await asyncio.sleep(0.3)
        await pilot.pause()

        # Verify that workspaces were loaded
        # Check internal _workspaces list
        assert len(picker._workspaces) == 3, "Should have loaded 3 workspaces"
        assert picker._workspaces[0].display_name == "Analytics"  # sorted case-insensitively
        assert picker._workspaces[1].display_name == "FabricDemo"
        assert picker._workspaces[2].display_name == "FabricTest"


@pytest.mark.asyncio
async def test_workspace_picker_filter_matches():
    """Load workspaces, call filter('Fabric'). Verify return value is match count."""
    client = _make_mock_client()

    workspaces = [
        Workspace(id="ws-1", displayName="FabricTest", type="Workspace"),
        Workspace(id="ws-2", displayName="Analytics", type="Workspace"),
        Workspace(id="ws-3", displayName="FabricDemo", type="Workspace"),
    ]
    client.fabric.list_workspaces = AsyncMock(return_value=workspaces)

    app = _PickerHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        picker = app.query_one("#picker", WorkspacePicker)

        # Wait for load_workspaces to complete
        await asyncio.sleep(0.3)
        await pilot.pause()

        # Filter for "Fabric" — should match 2 workspaces (FabricTest, FabricDemo)
        match_count = picker.filter("Fabric")

        assert match_count == 2, "Filter should return 2 matches for 'Fabric'"
        assert len(picker._filtered) == 2, "Filtered list should have 2 items"
        # Verify the correct items are in _filtered
        filtered_names = {ws.display_name for ws in picker._filtered}
        assert filtered_names == {"FabricTest", "FabricDemo"}


# ── ItemList tests ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_item_list_load_populates():
    """Load items via load_items(). Verify items are populated."""
    client = _make_mock_client()

    # Mock list_items to return 2 Item objects
    items = [
        Item(id="item-1", displayName="MyLakehouse", type="Lakehouse"),
        Item(id="item-2", displayName="MyWarehouse", type="Warehouse"),
    ]
    client.fabric.list_items = AsyncMock(return_value=items)

    app = _ItemListHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        item_list = app.query_one("#items", ItemList)

        # Call load_items() with workspace ID and name
        item_list.load_items("ws-1", "TestWorkspace")

        # Wait for load_items to complete (it's a @work task)
        await asyncio.sleep(0.3)
        await pilot.pause()

        # Verify items were loaded
        assert len(item_list._items) == 2, "Should have loaded 2 items"
        # Items should be sorted by type, then name
        assert item_list._items[0].display_name == "MyLakehouse"
        assert item_list._items[1].display_name == "MyWarehouse"


@pytest.mark.asyncio
async def test_item_list_clear():
    """Load items, then call clear_items(). Verify the list is empty."""
    client = _make_mock_client()

    items = [
        Item(id="item-1", displayName="MyLakehouse", type="Lakehouse"),
        Item(id="item-2", displayName="MyWarehouse", type="Warehouse"),
    ]
    client.fabric.list_items = AsyncMock(return_value=items)

    app = _ItemListHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        item_list = app.query_one("#items", ItemList)

        # Load items
        item_list.load_items("ws-1", "TestWorkspace")
        await asyncio.sleep(0.3)
        await pilot.pause()

        # Verify items are loaded
        assert len(item_list._items) == 2

        # Clear items
        item_list.clear_items()

        # Verify list is empty
        assert len(item_list._items) == 0, "Items list should be empty after clear"
        assert len(item_list._item_cache) == 0, "Item cache should be empty after clear"


# ── Additional StatusBar field tests ─────────────────────────────────


@pytest.mark.asyncio
async def test_status_bar_item_count_display():
    """Verify item_count is displayed in render() output."""
    app = _StatusBarHarness()

    async with app.run_test() as pilot:
        await pilot.pause()
        status = app.query_one("#status", StatusBar)

        status.update_path("onelake://ws/item", item_count=42)

        output = status.render()

        # Verify item count appears in output
        assert "42 items" in output, "Item count should appear in render() output"


@pytest.mark.asyncio
async def test_status_bar_prod_env_no_tag():
    """Verify PROD environment doesn't show env tag."""
    app = _StatusBarHarness()

    async with app.run_test() as pilot:
        await pilot.pause()
        status = app.query_one("#status", StatusBar)

        status.env_name = "PROD"
        output = status.render()

        # For PROD, env_tag should be "PROD" (no brackets)
        assert "PROD" in output
        # Should not have escaped brackets for PROD
        assert "\\[PROD]" not in output


# ── Integration: filter and clear workflow ───────────────────────────


@pytest.mark.asyncio
async def test_workspace_picker_filter_then_clear():
    """Test filter -> clear_filter workflow."""
    client = _make_mock_client()

    workspaces = [
        Workspace(id="ws-1", displayName="FabricTest", type="Workspace"),
        Workspace(id="ws-2", displayName="Analytics", type="Workspace"),
        Workspace(id="ws-3", displayName="FabricDemo", type="Workspace"),
    ]
    client.fabric.list_workspaces = AsyncMock(return_value=workspaces)

    app = _PickerHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        picker = app.query_one("#picker", WorkspacePicker)

        # Wait for load
        await asyncio.sleep(0.3)
        await pilot.pause()

        # Filter
        match_count = picker.filter("Fabric")
        assert match_count == 2

        # Clear filter
        picker.clear_filter()

        # Should be back to all 3 workspaces
        assert len(picker._filtered) == 3, "After clear_filter(), should show all workspaces"


@pytest.mark.asyncio
async def test_item_list_load_different_workspaces():
    """Test loading items for different workspaces (cache behavior)."""
    client = _make_mock_client()

    items_ws1 = [Item(id="i-1", displayName="Item1", type="Lakehouse")]
    items_ws2 = [Item(id="i-2", displayName="Item2", type="Warehouse")]

    client.fabric.list_items = AsyncMock(side_effect=[items_ws1, items_ws2])

    app = _ItemListHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        item_list = app.query_one("#items", ItemList)

        # Load for workspace 1
        item_list.load_items("ws-1", "WS1")
        await asyncio.sleep(0.3)
        await pilot.pause()

        assert len(item_list._items) == 1
        assert item_list._items[0].display_name == "Item1"

        # Load for workspace 2
        item_list.load_items("ws-2", "WS2")
        await asyncio.sleep(0.3)
        await pilot.pause()

        assert len(item_list._items) == 1
        assert item_list._items[0].display_name == "Item2"

        # Cache should have both
        assert len(item_list._item_cache) == 2, "Cache should store items for both workspaces"
