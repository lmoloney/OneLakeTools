"""End-to-end TUI navigation flow tests using Textual's run_test() with a fully mocked client."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from textual.app import App
from textual.widgets import Input, OptionList

from onelake_client.environment import DEFAULT_ENVIRONMENT
from onelake_client.models import Item, PathInfo, Workspace
from onelake_tui.app import OneLakeApp
from onelake_tui.detail import DetailPanel
from onelake_tui.item_list import ItemList
from onelake_tui.nodes import FileNode, FolderNode
from onelake_tui.status_bar import StatusBar
from onelake_tui.tree import OneLakeTree
from onelake_tui.workspace_picker import WorkspacePicker

# ── Helpers ──────────────────────────────────────────────────────────


def _make_mock_client() -> MagicMock:
    """Build a mock OneLakeClient with the minimum surface area."""
    client = MagicMock()
    client.env = DEFAULT_ENVIRONMENT
    client.fabric.list_workspaces = AsyncMock(return_value=[])
    client.fabric.list_items = AsyncMock(return_value=[])
    client.dfs.list_paths = AsyncMock(return_value=[])
    client.dfs.read_file = AsyncMock(return_value=b"")
    client.dfs.exists = AsyncMock(return_value=False)
    client.auth.get_identity = MagicMock(return_value="test-user@contoso.com")
    client.close = AsyncMock()
    return client


def _create_app_harness(mock_client: MagicMock | None = None) -> tuple[OneLakeApp, MagicMock]:
    """Create an OneLakeApp with mocked client for testing."""
    client = mock_client or _make_mock_client()
    with patch.object(OneLakeApp, "__init__", lambda self, **kw: None):
        app = OneLakeApp.__new__(OneLakeApp)
        App.__init__(app)
        app._env = DEFAULT_ENVIRONMENT
        app.client = client
        app._auth_error = None
    return app, client


def _make_workspace(name: str, id_: str) -> Workspace:
    return Workspace(id=id_, displayName=name, type="Workspace")


def _make_item(name: str = "TestLH", id_: str = "item-guid", type_: str = "Lakehouse") -> Item:
    return Item(id=id_, displayName=name, type=type_)


def _make_path(name: str, *, is_dir: bool = False, size: int = 0) -> PathInfo:
    return PathInfo(name=name, isDirectory=is_dir, contentLength=size)


async def _settle(pilot, delay: float = 0.3) -> None:
    """Wait for async workers to complete."""
    await pilot.pause()
    await asyncio.sleep(delay)
    await pilot.pause()


# ── TestWorkspaceToItemFlow ──────────────────────────────────────────


class TestWorkspaceToItemFlow:
    """Workspace selection → ItemList loads items."""

    @pytest.mark.asyncio
    async def test_workspace_picker_shows_entries(self):
        """App mounts → WorkspacePicker shows 3 workspace entries."""
        workspaces = [
            _make_workspace("Alpha WS", "ws-1"),
            _make_workspace("Beta WS", "ws-2"),
            _make_workspace("Gamma WS", "ws-3"),
        ]
        client = _make_mock_client()
        client.fabric.list_workspaces = AsyncMock(return_value=workspaces)
        app, _ = _create_app_harness(client)

        async with app.run_test() as pilot:
            await _settle(pilot)

            picker = app.query_one("#picker", WorkspacePicker)
            option_list = picker.query_one("#workspace-list", OptionList)
            assert option_list.option_count == 3

    @pytest.mark.asyncio
    async def test_workspace_selection_loads_items(self):
        """Selecting a workspace triggers ItemList to load items."""
        workspaces = [
            _make_workspace("Alpha WS", "ws-1"),
            _make_workspace("Beta WS", "ws-2"),
            _make_workspace("Gamma WS", "ws-3"),
        ]
        items = [
            _make_item("MyLakehouse", "item-1", "Lakehouse"),
            _make_item("MyWarehouse", "item-2", "Warehouse"),
        ]
        client = _make_mock_client()
        client.fabric.list_workspaces = AsyncMock(return_value=workspaces)
        client.fabric.list_items = AsyncMock(return_value=items)
        app, _ = _create_app_harness(client)

        async with app.run_test() as pilot:
            await _settle(pilot)

            # Explicitly post workspace-selected message to simulate user action
            ws = workspaces[0]
            app.on_workspace_picker_workspace_selected(WorkspacePicker.WorkspaceSelected(ws))
            await _settle(pilot)

            client.fabric.list_items.assert_called_with("ws-1")
            item_list = app.query_one("#items", ItemList)
            option_list = item_list.query_one("#item-option-list", OptionList)
            assert option_list.option_count == 2


# ── TestItemToTreeFlow ───────────────────────────────────────────────


class TestItemToTreeFlow:
    """Item selection → OneLakeTree loads DFS paths."""

    @pytest.mark.asyncio
    async def test_tree_shows_item_root_with_icon(self):
        """Selecting a Lakehouse item shows its name with icon in tree root."""
        client = _make_mock_client()
        client.dfs.list_paths = AsyncMock(
            return_value=[
                _make_path("Tables", is_dir=True),
                _make_path("Files", is_dir=True),
            ]
        )
        app, _ = _create_app_harness(client)

        async with app.run_test() as pilot:
            await pilot.pause()

            tree = app.query_one("#tree", OneLakeTree)
            item = _make_item("SalesLH", "item-lh", "Lakehouse")
            tree.load_item("ws-1", "TestWS", item)
            await _settle(pilot)

            label = str(tree.root.label)
            assert label.startswith("🏠"), f"Expected 🏠 prefix, got: {label!r}"
            assert "SalesLH" in label

    @pytest.mark.asyncio
    async def test_tree_has_tables_and_files_children(self):
        """Lakehouse tree root shows Tables and Files folders."""
        client = _make_mock_client()
        client.dfs.list_paths = AsyncMock(
            return_value=[
                _make_path("Tables", is_dir=True),
                _make_path("Files", is_dir=True),
            ]
        )
        app, _ = _create_app_harness(client)

        async with app.run_test() as pilot:
            await pilot.pause()

            tree = app.query_one("#tree", OneLakeTree)
            item = _make_item("SalesLH", "item-lh", "Lakehouse")
            tree.load_item("ws-1", "TestWS", item)
            await _settle(pilot)

            children = list(tree.root.children)
            assert len(children) == 2
            labels = [str(c.label) for c in children]
            assert any("Files" in lbl for lbl in labels)
            assert any("Tables" in lbl for lbl in labels)


# ── TestTreeExpansion ────────────────────────────────────────────────


class TestTreeExpansion:
    """Folder expansion loads children with correct sort order."""

    @pytest.mark.asyncio
    async def test_expand_folder_loads_children(self):
        """Expanding a Files/ folder loads nested children."""
        client = _make_mock_client()
        client.dfs.list_paths = AsyncMock(
            side_effect=[
                # load_item: top-level
                [_make_path("Files", is_dir=True)],
                # Expand Files/: nested items
                [
                    _make_path("Files/data.csv", size=500),
                    _make_path("Files/subfolder", is_dir=True),
                    _make_path("Files/readme.md", size=100),
                ],
            ]
        )
        app, _ = _create_app_harness(client)

        async with app.run_test() as pilot:
            await pilot.pause()

            tree = app.query_one("#tree", OneLakeTree)
            tree.load_item("ws-1", "TestWS", _make_item())
            await _settle(pilot)

            files_node = list(tree.root.children)[0]
            assert isinstance(files_node.data, FolderNode)
            files_node.expand()
            await _settle(pilot)

            children = list(files_node.children)
            assert len(children) == 3

    @pytest.mark.asyncio
    async def test_folders_first_sort_order(self):
        """Children sorted: folders first (case-insensitive), then files."""
        client = _make_mock_client()
        client.dfs.list_paths = AsyncMock(
            side_effect=[
                [_make_path("Files", is_dir=True)],
                [
                    _make_path("Files/zebra.txt", size=100),
                    _make_path("Files/Alpha", is_dir=True),
                    _make_path("Files/data.csv", size=200),
                    _make_path("Files/beta", is_dir=True),
                ],
            ]
        )
        app, _ = _create_app_harness(client)

        async with app.run_test() as pilot:
            await pilot.pause()

            tree = app.query_one("#tree", OneLakeTree)
            tree.load_item("ws-1", "TestWS", _make_item())
            await _settle(pilot)

            files_node = list(tree.root.children)[0]
            files_node.expand()
            await _settle(pilot)

            children = list(files_node.children)
            assert len(children) == 4

            # Folders first, case-insensitive
            assert isinstance(children[0].data, FolderNode)
            assert "Alpha" in str(children[0].label)
            assert isinstance(children[1].data, FolderNode)
            assert "beta" in str(children[1].label)

            # Files next, case-insensitive
            assert isinstance(children[2].data, FileNode)
            assert "data.csv" in str(children[2].label)
            assert isinstance(children[3].data, FileNode)
            assert "zebra.txt" in str(children[3].label)


# ── TestNodeHighlightUpdatesDetail ───────────────────────────────────


class TestNodeHighlightUpdatesDetail:
    """Highlighting tree nodes updates the DetailPanel."""

    @pytest.mark.asyncio
    async def test_file_node_highlight_shows_properties(self):
        """Highlighting a FileNode updates DetailPanel with file properties."""
        from textual.widgets import Label, Static

        client = _make_mock_client()
        app, _ = _create_app_harness(client)

        async with app.run_test() as pilot:
            await pilot.pause()

            detail = app.query_one("#detail", DetailPanel)
            detail.set_context("TestWS", "TestItem")

            file_node = FileNode(workspace="ws-1", path="item-guid/Files/data.csv", size=2048)
            detail.update_for_node(file_node)
            await _settle(pilot)

            # Check that file emoji and size info are rendered
            labels = detail.query(Label)
            label_texts = [_get_widget_text(w) for w in labels]
            assert any("📄" in txt for txt in label_texts), (
                f"Expected file emoji in labels. Found: {label_texts}"
            )

            statics = detail.query(Static)
            static_texts = [_get_widget_text(w) for w in statics]
            assert any("Size:" in txt for txt in static_texts), (
                f"Expected 'Size:' in statics. Found: {static_texts}"
            )

    @pytest.mark.asyncio
    async def test_folder_node_highlight_shows_path(self):
        """Highlighting a FolderNode shows folder path in DetailPanel."""
        from textual.widgets import Static

        client = _make_mock_client()
        app, _ = _create_app_harness(client)

        async with app.run_test() as pilot:
            await pilot.pause()

            detail = app.query_one("#detail", DetailPanel)
            detail.set_context("TestWS", "TestItem")

            folder_node = FolderNode(
                workspace="ws-1", item_path="item-guid", directory="item-guid/Files/subfolder"
            )
            detail.update_for_node(folder_node)
            await _settle(pilot)

            statics = detail.query(Static)
            static_texts = [_get_widget_text(w) for w in statics]
            assert any("Path:" in txt for txt in static_texts), (
                f"Expected 'Path:' in statics. Found: {static_texts}"
            )


# ── TestFilePreviewOnEnter ───────────────────────────────────────────


class TestFilePreviewOnEnter:
    """Enter on a FileNode triggers file preview in DetailPanel."""

    @pytest.mark.asyncio
    async def test_csv_preview_renders(self):
        """Calling preview_file on a CSV file triggers file read and renders content."""
        csv_content = b"name,age\nAlice,30\nBob,25\n"
        client = _make_mock_client()
        client.dfs.read_file = AsyncMock(return_value=csv_content)
        app, _ = _create_app_harness(client)

        async with app.run_test() as pilot:
            await pilot.pause()

            detail = app.query_one("#detail", DetailPanel)
            detail.set_context("TestWS", "TestItem")

            file_node = FileNode(workspace="ws-1", path="item-guid/Files/data.csv", size=100)
            detail.preview_file(file_node)

            # preview_file is a @work async method — give it time to complete
            for _ in range(5):
                await pilot.pause()
                await asyncio.sleep(0.2)

            # Verify the DFS read was called with correct args
            client.dfs.read_file.assert_called_once_with("ws-1", "item-guid/Files/data.csv")

            # Verify preview content was mounted (children replaced the sprite)
            assert len(detail.children) > 0, "DetailPanel should have children after preview"


# ── TestSearchKeybinding ─────────────────────────────────────────────


class TestSearchKeybinding:
    """Search input behavior via `/` keybinding."""

    @pytest.mark.asyncio
    async def test_slash_shows_search_input(self):
        """Pressing `/` shows the search input and focuses it."""
        app, _ = _create_app_harness()

        async with app.run_test() as pilot:
            await pilot.pause()
            search = app.query_one("#search-input", Input)
            assert search.display is False

            app.action_search()
            assert search.display is True
            assert search.has_focus

    @pytest.mark.asyncio
    async def test_escape_dismisses_search(self):
        """Pressing Escape hides the search input."""
        app, _ = _create_app_harness()

        async with app.run_test() as pilot:
            await pilot.pause()
            search = app.query_one("#search-input", Input)

            app.action_search()
            assert search.display is True

            await pilot.press("escape")
            await pilot.pause()
            assert search.display is False

    @pytest.mark.asyncio
    async def test_search_filters_workspace_list(self):
        """Typing in search input filters workspace list."""
        workspaces = [
            _make_workspace("Alpha WS", "ws-1"),
            _make_workspace("Beta WS", "ws-2"),
            _make_workspace("Gamma WS", "ws-3"),
        ]
        client = _make_mock_client()
        client.fabric.list_workspaces = AsyncMock(return_value=workspaces)
        app, _ = _create_app_harness(client)

        async with app.run_test() as pilot:
            await _settle(pilot)

            picker = app.query_one("#picker", WorkspacePicker)
            count = picker.filter("Alpha")
            assert count == 1

            option_list = picker.query_one("#workspace-list", OptionList)
            assert option_list.option_count == 1


# ── TestVimKeybindings ───────────────────────────────────────────────


class TestVimKeybindings:
    """Vim-style navigation keybindings (j/k/h/l)."""

    @pytest.mark.asyncio
    async def test_j_k_map_to_down_up(self):
        """j/k keys simulate down/up on nav widgets."""
        app, _ = _create_app_harness()

        async with app.run_test() as pilot:
            await pilot.pause()
            tree = app.query_one("#tree", OneLakeTree)
            tree.focus()
            await pilot.pause()

            with patch.object(app, "simulate_key") as simulate_key:
                event = SimpleNamespace(key="j", prevent_default=MagicMock())
                app.on_key(event)
                simulate_key.assert_called_with("down")
                event.prevent_default.assert_called_once()

                simulate_key.reset_mock()
                event = SimpleNamespace(key="k", prevent_default=MagicMock())
                app.on_key(event)
                simulate_key.assert_called_with("up")
                event.prevent_default.assert_called_once()

    @pytest.mark.asyncio
    async def test_h_l_switch_panels(self):
        """h/l keys switch focus between panels."""
        app, _ = _create_app_harness()

        async with app.run_test() as pilot:
            await pilot.pause()
            app.query_one("#tree", OneLakeTree).focus()
            await pilot.pause()

            with (
                patch.object(app, "action_focus_previous") as focus_prev,
                patch.object(app, "action_focus_next") as focus_next,
            ):
                event = SimpleNamespace(key="h", prevent_default=MagicMock())
                app.on_key(event)
                focus_prev.assert_called_once()
                event.prevent_default.assert_called_once()

                event = SimpleNamespace(key="l", prevent_default=MagicMock())
                app.on_key(event)
                focus_next.assert_called_once()
                event.prevent_default.assert_called_once()

    @pytest.mark.asyncio
    async def test_vim_keys_ignored_during_search(self):
        """Vim shortcuts should not fire while search input is focused."""
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


# ── TestCopyMenu ─────────────────────────────────────────────────────


class TestCopyMenu:
    """Copy menu (y keybinding) behavior."""

    @pytest.mark.asyncio
    async def test_copy_no_selection_shows_warning(self):
        """Copy with no tree selection shows warning without crashing."""
        app, _ = _create_app_harness()

        async with app.run_test() as pilot:
            await pilot.pause()
            tree = app.query_one("#tree", OneLakeTree)
            assert tree.cursor_node is None or tree.cursor_node.data is None

            app.action_copy()
            await pilot.pause()

    @pytest.mark.asyncio
    async def test_copy_with_selection_opens_menu(self):
        """Copy with a selected node opens the CopyFormatMenu."""
        from onelake_tui.copy_menu import CopyFormatMenu

        client = _make_mock_client()
        client.dfs.list_paths = AsyncMock(return_value=[_make_path("Files", is_dir=True)])
        app, _ = _create_app_harness(client)

        async with app.run_test() as pilot:
            await pilot.pause()

            tree = app.query_one("#tree", OneLakeTree)
            tree.load_item("ws-1", "TestWS", _make_item())
            await _settle(pilot)

            # Move cursor to the first child (Files folder)
            children = list(tree.root.children)
            assert len(children) >= 1
            tree.select_node(children[0])
            await pilot.pause()

            app.action_copy()
            await pilot.pause()

            assert isinstance(app.screen, CopyFormatMenu)


# ── TestRefresh ──────────────────────────────────────────────────────


class TestRefresh:
    """Refresh action reloads workspaces."""

    @pytest.mark.asyncio
    async def test_refresh_triggers_workspace_reload(self):
        """Pressing r calls action_refresh which refreshes picker, items, and tree."""
        app, client = _create_app_harness()

        async with app.run_test() as pilot:
            await pilot.pause()

            picker = app.query_one("#picker", WorkspacePicker)
            with patch.object(picker, "refresh_workspaces") as refresh_ws:
                app.action_refresh()
                refresh_ws.assert_called_once()

    @pytest.mark.asyncio
    async def test_refresh_clears_item_list(self):
        """Refresh clears the item list."""
        app, _ = _create_app_harness()

        async with app.run_test() as pilot:
            await pilot.pause()

            item_list = app.query_one("#items", ItemList)
            with patch.object(item_list, "clear_items") as clear_items:
                app.action_refresh()
                clear_items.assert_called_once()


# ── TestStatusBarUpdates ─────────────────────────────────────────────


class TestStatusBarUpdates:
    """Status bar reflects navigation state."""

    @pytest.mark.asyncio
    async def test_workspace_selection_updates_subtitle(self):
        """Selecting a workspace updates the app subtitle."""
        workspaces = [_make_workspace("MyWorkspace", "ws-1")]
        client = _make_mock_client()
        client.fabric.list_workspaces = AsyncMock(return_value=workspaces)
        app, _ = _create_app_harness(client)

        async with app.run_test() as pilot:
            await _settle(pilot)

            # Explicitly trigger workspace selection
            app.on_workspace_picker_workspace_selected(
                WorkspacePicker.WorkspaceSelected(workspaces[0])
            )
            await pilot.pause()

            assert "MyWorkspace" in app.sub_title

    @pytest.mark.asyncio
    async def test_item_selection_updates_status_path(self):
        """Selecting an item updates the status bar path."""
        app, _ = _create_app_harness()

        async with app.run_test() as pilot:
            await pilot.pause()

            status = app.query_one(StatusBar)
            item = _make_item("MyLakehouse", "item-1")
            event = ItemList.ItemSelected("ws-1", "TestWS", item)
            app.on_item_list_item_selected(event)
            await _settle(pilot)

            assert "TestWS" in status.path
            assert "MyLakehouse" in status.path


# ── TestFullNavigationChain ──────────────────────────────────────────


class TestFullNavigationChain:
    """Full workspace → item → tree → detail chain."""

    @pytest.mark.asyncio
    async def test_full_chain_workspace_to_detail(self):
        """Complete navigation: workspace → item → tree → detail panel update."""

        workspaces = [_make_workspace("ProdWS", "ws-1")]
        items = [_make_item("DataLake", "item-1", "Lakehouse")]
        dfs_paths = [
            _make_path("Tables", is_dir=True),
            _make_path("Files", is_dir=True),
        ]

        client = _make_mock_client()
        client.fabric.list_workspaces = AsyncMock(return_value=workspaces)
        client.fabric.list_items = AsyncMock(return_value=items)
        client.dfs.list_paths = AsyncMock(return_value=dfs_paths)
        app, _ = _create_app_harness(client)

        async with app.run_test() as pilot:
            await _settle(pilot)

            # Verify workspace loaded
            picker = app.query_one("#picker", WorkspacePicker)
            ws_list = picker.query_one("#workspace-list", OptionList)
            assert ws_list.option_count == 1

            # Verify items loaded for auto-selected workspace
            await _settle(pilot)
            app.query_one("#items", ItemList)

            # Simulate item selection event (as if user highlighted the item)
            event = ItemList.ItemSelected("ws-1", "ProdWS", items[0])
            app.on_item_list_item_selected(event)
            await _settle(pilot)

            # Verify tree loaded
            tree = app.query_one("#tree", OneLakeTree)
            root_label = str(tree.root.label)
            assert "DataLake" in root_label

            children = list(tree.root.children)
            assert len(children) == 2

            # Verify detail context was set
            detail = app.query_one("#detail", DetailPanel)
            assert detail._workspace_name == "ProdWS"
            assert detail._item_name == "DataLake"


# ── Utility ──────────────────────────────────────────────────────────


def _get_widget_text(widget) -> str:
    """Extract plain text from a rendered Textual widget."""
    try:
        line = widget.render_line(0)
        return "".join(seg.text for seg in line)
    except Exception:
        return ""
