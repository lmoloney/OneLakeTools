"""Tests for OneLakeApp widget interaction chain and action methods."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from textual.app import App
from textual.widgets import Input

from onelake_client.environment import DEFAULT_ENVIRONMENT
from onelake_client.models import Item
from onelake_tui.app import OneLakeApp
from onelake_tui.nodes import FileNode, FolderNode, TableNode
from onelake_tui.tree import OneLakeTree

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


# ── Test helpers: create app harness ─────────────────────────────────


def _create_app_harness() -> tuple[OneLakeApp, MagicMock]:
    """Create an OneLakeApp with mocked client for testing.

    Returns:
        (app, mock_client) tuple for use in async context managers.
    """
    mock_client = _make_mock_client()
    with patch.object(OneLakeApp, "__init__", lambda self, **kw: None):
        app = OneLakeApp.__new__(OneLakeApp)
        App.__init__(app)
        app._env = DEFAULT_ENVIRONMENT
        app.client = mock_client
        app._auth_error = None
    return app, mock_client


# ── 1. test_action_search_shows_input ────────────────────────────────


@pytest.mark.asyncio
async def test_action_search_shows_input():
    """Mount app. Call action_search(). Verify search input is shown."""
    app, _ = _create_app_harness()

    async with app.run_test() as pilot:
        await pilot.pause()
        search = app.query_one("#search-input", Input)

        # Initially hidden
        assert search.display is False

        # Call action_search
        app.action_search()
        assert search.display is True
        assert search.has_focus


# ── 2. test_action_search_escape_hides ───────────────────────────────


@pytest.mark.asyncio
async def test_action_search_escape_hides():
    """Mount app. Show search via action_search(). Simulate Escape key. Verify hidden."""
    app, _ = _create_app_harness()

    async with app.run_test() as pilot:
        await pilot.pause()
        search = app.query_one("#search-input", Input)

        # Show search
        app.action_search()
        assert search.display is True

        # Simulate Escape key
        await pilot.press("escape")
        await pilot.pause()

        # Should be hidden again
        assert search.display is False


# ── 3. test_action_help_notifies ─────────────────────────────────────


@pytest.mark.asyncio
async def test_action_help_notifies():
    """Mount app. Call action_help(). Verify it triggers a notification without crashing."""
    app, _ = _create_app_harness()

    async with app.run_test() as pilot:
        await pilot.pause()

        # Should not raise
        app.action_help()
        await pilot.pause()


# ── 4. test_copy_path_no_selection ───────────────────────────────────


@pytest.mark.asyncio
async def test_copy_path_no_selection():
    """Mount app. Call action_copy_path() with no tree selection.
    Should show a warning notification without crashing.
    """
    app, _ = _create_app_harness()

    async with app.run_test() as pilot:
        await pilot.pause()
        tree = app.query_one("#tree", OneLakeTree)

        # Verify tree has no cursor node selected
        assert tree.cursor_node is None or tree.cursor_node.data is None

        # Call action_copy_path — should show warning but not crash
        app.action_copy_path()
        await pilot.pause()


# ── 5. test_node_to_path_folder ──────────────────────────────────────


@pytest.mark.asyncio
async def test_node_to_path_folder():
    """Mount app. Set tree context. Convert FolderNode to path.
    Verify format: onelake://WsName/ItemName/relative/path
    """
    app, _ = _create_app_harness()

    async with app.run_test() as pilot:
        await pilot.pause()
        tree = app.query_one("#tree", OneLakeTree)

        # Set up tree context
        tree._current_workspace_name = "MyWorkspace"
        tree._current_workspace_id = "ws-guid-123"
        tree._current_item = Item(
            id="item-guid",
            display_name="MyLakehouse",
            type="Lakehouse",
        )

        # Create a FolderNode with a directory path (contains item GUID prefix)
        folder_node = FolderNode(
            workspace="ws-guid-123",
            item_path="item-guid",
            directory="item-guid/Files/subfolder",  # item GUID prefix should be stripped
        )

        result = app._node_to_path(folder_node)
        assert result == "onelake://MyWorkspace/MyLakehouse/Files/subfolder"


# ── 6. test_node_to_path_file ────────────────────────────────────────


@pytest.mark.asyncio
async def test_node_to_path_file():
    """Mount app. Set tree context. Convert FileNode to path.
    Verify format: onelake://WsName/ItemName/path/to/file
    """
    app, _ = _create_app_harness()

    async with app.run_test() as pilot:
        await pilot.pause()
        tree = app.query_one("#tree", OneLakeTree)

        # Set up tree context
        tree._current_workspace_name = "MyWorkspace"
        tree._current_workspace_id = "ws-guid-123"
        tree._current_item = Item(
            id="item-guid",
            display_name="MyLakehouse",
            type="Lakehouse",
        )

        # Create a FileNode with a path containing item GUID prefix
        file_node = FileNode(
            workspace="ws-guid-123",
            path="item-guid/Files/data.csv",  # item GUID prefix should be stripped
            size=2048,
        )

        result = app._node_to_path(file_node)
        assert result == "onelake://MyWorkspace/MyLakehouse/Files/data.csv"


# ── 7. test_node_to_path_table ───────────────────────────────────────


@pytest.mark.asyncio
async def test_node_to_path_table():
    """Mount app. Set tree context. Convert TableNode to path.
    Verify format includes Tables/: onelake://WsName/ItemName/Tables/table_name
    """
    app, _ = _create_app_harness()

    async with app.run_test() as pilot:
        await pilot.pause()
        tree = app.query_one("#tree", OneLakeTree)

        # Set up tree context
        tree._current_workspace_name = "MyWorkspace"
        tree._current_workspace_id = "ws-guid-123"
        tree._current_item = Item(
            id="item-guid",
            display_name="MyLakehouse",
            type="Lakehouse",
        )

        # Create a TableNode
        table_node = TableNode(
            workspace="ws-guid-123",
            item_path="item-guid",
            table_name="dbo/my_table",
        )

        result = app._node_to_path(table_node)
        assert result == "onelake://MyWorkspace/MyLakehouse/Tables/dbo/my_table"


# ── 8. test_node_to_abfss_folder ────────────────────────────────────


@pytest.mark.asyncio
async def test_node_to_abfss_folder():
    """Mount app. Convert FolderNode to ABFSS path.
    Verify format: abfss://ws-guid@host/...
    """
    app, _ = _create_app_harness()

    async with app.run_test() as pilot:
        await pilot.pause()
        tree = app.query_one("#tree", OneLakeTree)

        # Set up tree context
        tree._current_workspace_id = "ws-guid-123"
        tree._current_item = Item(
            id="item-guid",
            display_name="MyLakehouse",
            type="Lakehouse",
        )

        # Create a FolderNode
        folder_node = FolderNode(
            workspace="ws-guid-123",
            item_path="item-guid",
            directory="item-guid/Files/subfolder",
        )

        result = app._node_to_abfss(folder_node)
        host = DEFAULT_ENVIRONMENT.dfs_host
        assert result == f"abfss://ws-guid-123@{host}/item-guid/Files/subfolder"


# ── 9. test_node_to_https_file ──────────────────────────────────────


@pytest.mark.asyncio
async def test_node_to_https_file():
    """Mount app. Convert FileNode to HTTPS URL.
    Verify format: https://host/ws-guid/...
    """
    app, _ = _create_app_harness()

    async with app.run_test() as pilot:
        await pilot.pause()
        tree = app.query_one("#tree", OneLakeTree)

        # Set up tree context
        tree._current_workspace_id = "ws-guid-123"
        tree._current_item = Item(
            id="item-guid",
            display_name="MyLakehouse",
            type="Lakehouse",
        )

        # Create a FileNode
        file_node = FileNode(
            workspace="ws-guid-123",
            path="item-guid/Files/data.csv",
            size=2048,
        )

        result = app._node_to_https(file_node)
        host = DEFAULT_ENVIRONMENT.dfs_host
        assert result == f"https://{host}/ws-guid-123/item-guid/Files/data.csv"


# ── 10. test_node_to_path_none_data ──────────────────────────────────


@pytest.mark.asyncio
async def test_node_to_path_none_data():
    """Mount app. Call _node_to_path() with unsupported data type.
    Verify it returns None without crashing.
    """
    app, _ = _create_app_harness()

    async with app.run_test() as pilot:
        await pilot.pause()

        # Call with unsupported type
        result = app._node_to_path("unsupported")
        assert result is None

        # Also test with an integer
        result = app._node_to_path(42)
        assert result is None

        # Test with None
        result = app._node_to_path(None)
        assert result is None


# ── Additional integration tests ─────────────────────────────────────


@pytest.mark.asyncio
async def test_on_input_changed_filters_picker():
    """Test that on_input_changed triggers workspace picker filter."""
    app, _ = _create_app_harness()

    async with app.run_test() as pilot:
        await pilot.pause()
        search = app.query_one("#search-input", Input)

        # Type in search input (simulate user input)
        search.value = "test"
        app.on_input_changed(type("obj", (), {"input": search, "value": "test"})())
        await pilot.pause()


@pytest.mark.asyncio
async def test_node_to_https_table():
    """Mount app. Convert TableNode to HTTPS URL.
    Verify format includes Tables/: https://host/ws-guid/.../Tables/...
    """
    app, _ = _create_app_harness()

    async with app.run_test() as pilot:
        await pilot.pause()
        tree = app.query_one("#tree", OneLakeTree)

        # Set up tree context
        tree._current_workspace_id = "ws-guid-123"
        tree._current_item = Item(
            id="item-guid",
            display_name="MyLakehouse",
            type="Lakehouse",
        )

        # Create a TableNode
        table_node = TableNode(
            workspace="ws-guid-123",
            item_path="item-guid",
            table_name="dbo/my_table",
        )

        result = app._node_to_https(table_node)
        host = DEFAULT_ENVIRONMENT.dfs_host
        assert result == f"https://{host}/ws-guid-123/item-guid/Tables/dbo/my_table"


@pytest.mark.asyncio
async def test_node_to_abfss_file():
    """Mount app. Convert FileNode to ABFSS path.
    Verify format: abfss://ws-guid@host/path/to/file
    """
    app, _ = _create_app_harness()

    async with app.run_test() as pilot:
        await pilot.pause()
        tree = app.query_one("#tree", OneLakeTree)

        # Set up tree context
        tree._current_workspace_id = "ws-guid-123"
        tree._current_item = Item(
            id="item-guid",
            display_name="MyLakehouse",
            type="Lakehouse",
        )

        # Create a FileNode
        file_node = FileNode(
            workspace="ws-guid-123",
            path="item-guid/Files/data.csv",
            size=2048,
        )

        result = app._node_to_abfss(file_node)
        host = DEFAULT_ENVIRONMENT.dfs_host
        assert result == f"abfss://ws-guid-123@{host}/item-guid/Files/data.csv"


@pytest.mark.asyncio
async def test_node_to_abfss_table():
    """Mount app. Convert TableNode to ABFSS path.
    Verify format includes Tables/: abfss://ws-guid@host/.../Tables/...
    """
    app, _ = _create_app_harness()

    async with app.run_test() as pilot:
        await pilot.pause()
        tree = app.query_one("#tree", OneLakeTree)

        # Set up tree context
        tree._current_workspace_id = "ws-guid-123"
        tree._current_item = Item(
            id="item-guid",
            display_name="MyLakehouse",
            type="Lakehouse",
        )

        # Create a TableNode
        table_node = TableNode(
            workspace="ws-guid-123",
            item_path="item-guid",
            table_name="dbo/my_table",
        )

        result = app._node_to_abfss(table_node)
        host = DEFAULT_ENVIRONMENT.dfs_host
        assert result == f"abfss://ws-guid-123@{host}/item-guid/Tables/dbo/my_table"
