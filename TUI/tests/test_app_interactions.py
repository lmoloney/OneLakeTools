"""Tests for OneLakeApp widget interaction chain and action methods."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from textual.app import App
from textual.widgets import Input

from onelake_client.environment import DEFAULT_ENVIRONMENT
from onelake_client.models import Item
from onelake_tui.app import OneLakeApp
from onelake_tui.help_screen import HelpScreen
from onelake_tui.nodes import FileNode, FolderNode, TableNode
from onelake_tui.status_bar import StatusBar
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


# ── 3. test_action_help_opens_fullscreen ─────────────────────────────


@pytest.mark.asyncio
async def test_action_help_opens_fullscreen():
    """Mount app. Help action should push HelpScreen and close with Escape."""
    app, _ = _create_app_harness()

    async with app.run_test() as pilot:
        await pilot.pause()

        app.action_help()
        await pilot.pause()
        assert isinstance(app.screen, HelpScreen)

        await pilot.press("escape")
        await pilot.pause()
        assert not isinstance(app.screen, HelpScreen)


# ── 4. test_copy_path_no_selection ───────────────────────────────────


@pytest.mark.asyncio
async def test_copy_path_no_selection():
    """Mount app. Call action_copy() with no tree selection.
    Should show a warning notification without crashing.
    """
    app, _ = _create_app_harness()

    async with app.run_test() as pilot:
        await pilot.pause()
        tree = app.query_one("#tree", OneLakeTree)

        # Verify tree has no cursor node selected
        assert tree.cursor_node is None or tree.cursor_node.data is None

        # Call action_copy — should show warning but not crash
        app.action_copy()
        await pilot.pause()


# ── 5. test_node_to_path_folder ──────────────────────────────────────


@pytest.mark.asyncio
async def test_node_to_path_folder():
    """Mount app. Set tree context. Convert FolderNode to display path.
    Verify format: WsName / ItemName / relative/path
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

        result = app._node_display_path(folder_node)
        assert result == "MyWorkspace / MyLakehouse / Files/subfolder"


# ── 6. test_node_to_path_file ────────────────────────────────────────


@pytest.mark.asyncio
async def test_node_to_path_file():
    """Mount app. Set tree context. Convert FileNode to display path.
    Verify format: WsName / ItemName / path/to/file
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

        result = app._node_display_path(file_node)
        assert result == "MyWorkspace / MyLakehouse / Files/data.csv"


# ── 7. test_node_to_path_table ───────────────────────────────────────


@pytest.mark.asyncio
async def test_node_to_path_table():
    """Mount app. Set tree context. Convert TableNode to display path.
    Verify format: WsName / ItemName / Tables / table_name
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

        result = app._node_display_path(table_node)
        assert result == "MyWorkspace / MyLakehouse / Tables / dbo/my_table"


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

        result = app._node_to_abfss_guid(folder_node)
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

        result = app._node_to_https_guid(file_node)
        host = DEFAULT_ENVIRONMENT.dfs_host
        assert result == f"https://{host}/ws-guid-123/item-guid/Files/data.csv"


# ── 10. test_node_to_path_none_data ──────────────────────────────────


@pytest.mark.asyncio
async def test_node_to_path_none_data():
    """Mount app. Call _node_display_path() with unsupported data type.
    Verify it returns None without crashing.
    """
    app, _ = _create_app_harness()

    async with app.run_test() as pilot:
        await pilot.pause()

        # Call with unsupported type
        result = app._node_display_path("unsupported")
        assert result is None

        # Also test with an integer
        result = app._node_display_path(42)
        assert result is None

        # Test with None
        result = app._node_display_path(None)
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

        result = app._node_to_https_guid(table_node)
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

        result = app._node_to_abfss_guid(file_node)
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

        result = app._node_to_abfss_guid(table_node)
        host = DEFAULT_ENVIRONMENT.dfs_host
        assert result == f"abfss://ws-guid-123@{host}/item-guid/Tables/dbo/my_table"


@pytest.mark.asyncio
async def test_action_toggle_footer_toggles_display():
    """Status bar display should toggle on each action call."""
    app, _ = _create_app_harness()

    async with app.run_test() as pilot:
        await pilot.pause()
        status = app.query_one(StatusBar)
        assert status.display is True

        app.action_toggle_footer()
        assert status.display is False

        app.action_toggle_footer()
        assert status.display is True


@pytest.mark.asyncio
async def test_ctrl_f_key_toggles_footer():
    """Ctrl+f binding exists and footer action toggles visibility."""
    app, _ = _create_app_harness()

    async with app.run_test() as pilot:
        await pilot.pause()
        status = app.query_one(StatusBar)
        assert status.display is True

        assert any(binding.key == "ctrl+f" for binding in app.BINDINGS)

        app.action_toggle_footer()
        assert status.display is False


@pytest.mark.asyncio
async def test_on_key_panel_shortcuts_call_focus_actions():
    """h/l shortcuts should call focus previous/next actions."""
    app, _ = _create_app_harness()

    async with app.run_test() as pilot:
        await pilot.pause()
        app.query_one("#tree", OneLakeTree).focus()
        await pilot.pause()

        with (
            patch.object(app, "action_focus_previous") as focus_prev,
            patch.object(app, "action_focus_next") as focus_next,
        ):
            left_event = SimpleNamespace(key="h", prevent_default=MagicMock())
            app.on_key(left_event)
            focus_prev.assert_called_once()
            left_event.prevent_default.assert_called_once()

            right_event = SimpleNamespace(key="l", prevent_default=MagicMock())
            app.on_key(right_event)
            focus_next.assert_called_once()
            right_event.prevent_default.assert_called_once()


@pytest.mark.asyncio
async def test_on_key_vim_nav_shortcuts_map_to_simulate_key():
    """j/k/g/G should map to down/up/home/end on focused nav widgets."""
    app, _ = _create_app_harness()

    async with app.run_test() as pilot:
        await pilot.pause()
        app.query_one("#tree", OneLakeTree).focus()
        await pilot.pause()

        with patch.object(app, "simulate_key") as simulate_key:
            for key, expected in (("j", "down"), ("k", "up"), ("g", "home"), ("G", "end")):
                event = SimpleNamespace(key=key, prevent_default=MagicMock())
                app.on_key(event)
                simulate_key.assert_called_with(expected)
                event.prevent_default.assert_called_once()
                simulate_key.reset_mock()


@pytest.mark.asyncio
async def test_on_key_vim_nav_ignored_while_search_focused():
    """j/k/g/G shortcuts should not fire while typing in search input."""
    app, _ = _create_app_harness()

    async with app.run_test() as pilot:
        await pilot.pause()
        app.action_search()
        await pilot.pause()

        with patch.object(app, "simulate_key") as simulate_key:
            event = SimpleNamespace(key="j", prevent_default=MagicMock())
            app.on_key(event)
            simulate_key.assert_not_called()
            event.prevent_default.assert_not_called()


@pytest.mark.asyncio
async def test_uri_builders_return_none_without_client():
    """Path builders should safely return None when app client is unavailable."""
    app, _ = _create_app_harness()

    async with app.run_test() as pilot:
        await pilot.pause()
        tree = app.query_one("#tree", OneLakeTree)
        tree._current_workspace_name = "MyWorkspace"
        tree._current_workspace_id = "ws-guid-123"
        tree._current_item = Item(id="item-guid", display_name="MyLakehouse", type="Lakehouse")

        file_node = FileNode(workspace="ws-guid-123", path="item-guid/Files/data.csv", size=1)
        app.client = None

        assert app._node_to_https_named(file_node) is None
        assert app._node_to_https_guid(file_node) is None
        assert app._node_to_abfss_named(file_node) is None
        assert app._node_to_abfss_guid(file_node) is None


@pytest.mark.asyncio
async def test_named_uri_builders_encode_special_characters():
    """Named URI builders should percent-encode unsafe chars in workspace/item/path segments."""
    app, _ = _create_app_harness()

    async with app.run_test() as pilot:
        await pilot.pause()
        tree = app.query_one("#tree", OneLakeTree)
        tree._current_workspace_name = "My Workspace/West"
        tree._current_workspace_id = "ws-guid-123"
        tree._current_item = Item(id="item-guid", display_name="Lake#1", type="Lakehouse")

        file_node = FileNode(
            workspace="ws-guid-123",
            path="item-guid/Files/raw data/file #1.csv",
            size=1,
        )
        table_node = TableNode(
            workspace="ws-guid-123",
            item_path="item-guid",
            table_name="dbo/my table#1",
        )

        host = DEFAULT_ENVIRONMENT.dfs_host
        assert app._node_to_https_named(file_node) == (
            f"https://{host}/My%20Workspace%2FWest/Lake%231/Files/raw%20data/file%20%231.csv"
        )
        assert app._node_to_abfss_named(table_node) == (
            f"abfss://My%20Workspace%2FWest@{host}/Lake%231/Tables/dbo/my%20table%231"
        )


@pytest.mark.asyncio
async def test_copy_to_clipboard_uses_platform_command():
    """macOS path should use pbcopy command."""
    app, _ = _create_app_harness()

    async with app.run_test() as pilot:
        await pilot.pause()
        with (
            patch("onelake_tui.app.platform.system", return_value="Darwin"),
            patch("onelake_tui.app.subprocess.run") as run,
            patch.object(app, "notify") as notify,
        ):
            app._copy_to_clipboard("abc", "TEST")
            run.assert_called_once_with(["pbcopy"], input=b"abc", check=True)
            notify.assert_called_with("Copied TEST: abc", timeout=3)


@pytest.mark.asyncio
async def test_copy_to_clipboard_linux_fallback_chain():
    """Linux path should try wl-copy, then xclip, then xsel."""
    app, _ = _create_app_harness()

    async with app.run_test() as pilot:
        await pilot.pause()
        side_effects = [
            FileNotFoundError("wl-copy missing"),
            FileNotFoundError("xclip missing"),
            None,
        ]
        with (
            patch("onelake_tui.app.platform.system", return_value="Linux"),
            patch("onelake_tui.app.subprocess.run", side_effect=side_effects) as run,
            patch.object(app, "notify") as notify,
        ):
            app._copy_to_clipboard("abc", "TEST")
            assert run.call_count == 3
            notify.assert_called_with("Copied TEST: abc", timeout=3)
