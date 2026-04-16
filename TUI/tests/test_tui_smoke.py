"""Smoke tests for onelake_tui widgets — mount without crashing."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from textual.app import App, ComposeResult

from onelake_client.environment import DEFAULT_ENVIRONMENT
from onelake_tui.detail import DetailPanel
from onelake_tui.item_list import ItemList
from onelake_tui.nodes import FolderNode, TableNode
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


class _DetailHarness(App):
    """Minimal app that mounts only DetailPanel."""

    def __init__(self, client: MagicMock):
        super().__init__()
        self._client = client

    def compose(self) -> ComposeResult:
        yield DetailPanel(self._client, id="detail")


# ── 1. App startup smoke ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_app_mounts_without_crashing():
    """OneLakeApp should mount and display the header without real auth."""
    from onelake_tui.app import OneLakeApp

    mock_client = _make_mock_client()
    with patch.object(OneLakeApp, "__init__", lambda self, **kw: None):
        app = OneLakeApp.__new__(OneLakeApp)
        # Manually set the attributes that __init__ normally creates
        App.__init__(app)
        app._env = DEFAULT_ENVIRONMENT
        app.client = mock_client
        app._auth_error = None

    async with app.run_test() as pilot:
        await pilot.pause()
        assert app.title == "OneLake TUI (Unofficial)"


# ── 2. on_unmount with None client ───────────────────────────────────


@pytest.mark.asyncio
async def test_unmount_with_none_client():
    """on_unmount must not crash when self.client is None (v1.8 fix)."""
    from onelake_tui.app import OneLakeApp

    with patch.object(OneLakeApp, "__init__", lambda self, **kw: None):
        app = OneLakeApp.__new__(OneLakeApp)
        App.__init__(app)
        app._env = DEFAULT_ENVIRONMENT
        app.client = None
        app._auth_error = "simulated auth failure"

    async with app.run_test():
        pass  # unmount happens on context-manager exit — should not raise


# ── 3. DetailPanel unmount guard ─────────────────────────────────────


def test_apply_pending_node_returns_early_when_not_mounted():
    """_apply_pending_node should be a no-op before the widget is mounted."""
    client = _make_mock_client()
    panel = DetailPanel(client, id="detail")
    # Not mounted → is_mounted is False
    panel._pending_node = FolderNode(workspace="ws", item_path="item", directory="Files")
    panel._apply_pending_node()  # must not raise


# ── 4. suppress uses NoMatches, not Exception ────────────────────────


def test_no_hardcoded_widget_ids_in_detail_panel_mounts():
    """detail.py must not use id= on widgets mounted into DetailPanel.

    Hardcoded widget IDs on dynamically-mounted children cause DuplicateIds
    crashes when rapid navigation triggers mount before remove_children()
    completes. Use CSS classes instead. IDs inside TabPane/TabbedContent
    children are safe (scoped to a fresh parent each time).
    """
    import ast
    import inspect

    from onelake_tui import detail

    source = inspect.getsource(detail)
    tree = ast.parse(source)

    # Collect methods of DetailPanel that mount widgets directly (not into TabPanes)
    # These are the synchronous _show_* methods and _render_* helpers
    risky_methods = {
        "_show_table",
        "_show_file",
        "_show_folder",
        "_show_placeholder",
        "_render_text",
        "_render_csv",
        "_render_hex",
    }

    violations = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        if node.name not in risky_methods:
            continue
        for child in ast.walk(node):
            if not isinstance(child, ast.Call):
                continue
            # Look for self.mount(..., id="...")
            for kw in child.keywords:
                if kw.arg == "id" and isinstance(kw.value, ast.Constant):
                    violations.append(
                        f"{node.name}() mounts widget with id={kw.value.value!r} "
                        f"at line {child.lineno} — use classes= instead"
                    )

    assert not violations, (
        "Hardcoded widget IDs in DetailPanel mount methods cause DuplicateIds crashes:\n"
        + "\n".join(f"  • {v}" for v in violations)
    )


def test_suppress_uses_nomatches_not_exception():
    """All contextlib.suppress() calls in detail.py should target NoMatches."""
    import ast  # noqa: E401
    import inspect

    from onelake_tui import detail

    source = inspect.getsource(detail)
    tree = ast.parse(source)

    for node in ast.walk(tree):
        if not isinstance(node, ast.With):
            continue
        for item in node.items:
            call = item.context_expr
            if not isinstance(call, ast.Call):
                continue
            func = call.func
            # Match contextlib.suppress(...)
            is_suppress = (
                isinstance(func, ast.Attribute)
                and func.attr == "suppress"
                and isinstance(func.value, ast.Name)
                and func.value.id == "contextlib"
            )
            if not is_suppress:
                continue
            for arg in call.args:
                if isinstance(arg, ast.Name):
                    assert arg.id == "NoMatches", (
                        f"contextlib.suppress should use NoMatches, "
                        f"found {arg.id} at line {node.lineno}"
                    )


# ── 5. WorkspacePicker basics ────────────────────────────────────────


@pytest.mark.asyncio
async def test_workspace_picker_mounts_and_has_filter():
    """WorkspacePicker should mount and expose a filter() method."""
    client = _make_mock_client()
    app = _PickerHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        picker = app.query_one("#picker", WorkspacePicker)
        assert callable(picker.filter)
        # filter on an empty list should return 0
        assert picker.filter("anything") == 0


@pytest.mark.asyncio
async def test_item_list_mounts():
    """ItemList should mount without errors."""
    client = _make_mock_client()
    app = _ItemListHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        item_list = app.query_one("#items", ItemList)
        assert item_list is not None


@pytest.mark.asyncio
async def test_detail_panel_mounts():
    """DetailPanel should mount and show the welcome sprite."""
    client = _make_mock_client()
    app = _DetailHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        detail = app.query_one("#detail", DetailPanel)
        assert detail.is_mounted


# ── 7. Rapid table navigation does not crash (DuplicateIds) ──────────


@pytest.mark.asyncio
async def test_rapid_table_navigation_no_duplicate_ids():
    """Rapidly switching between TableNodes must not raise DuplicateIds.

    This reproduces the crash from GH issue where fast arrow-key navigation
    through table nodes caused _show_table to mount a LoadingIndicator
    before remove_children() had completed, duplicating widget IDs.
    """
    client = _make_mock_client()
    client.dfs.exists = AsyncMock(return_value=False)
    app = _DetailHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        detail = app.query_one("#detail", DetailPanel)
        detail._workspace_name = "TestWS"
        detail._item_name = "TestItem"

        table_a = TableNode(workspace="ws-1", item_path="item-1", table_name="dbo")
        table_b = TableNode(
            workspace="ws-1", item_path="item-1", table_name="dbo/my_table"
        )

        # Simulate rapid navigation: update_node fires multiple times
        # without awaiting removal of the previous widgets
        for _ in range(5):
            detail.update_for_node(table_a)
            detail.update_for_node(table_b)

        # Let debounce timers and workers settle
        await pilot.pause()
        await pilot.pause()

        # If we got here without DuplicateIds, the fix is working
        assert detail.is_mounted


# ── 8. Rapid file highlight does not crash (DuplicateIds) ────────────


@pytest.mark.asyncio
async def test_rapid_file_highlight_no_duplicate_ids():
    """Rapidly switching between FileNodes must not raise DuplicateIds."""
    from onelake_tui.nodes import FileNode

    client = _make_mock_client()
    app = _DetailHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        detail = app.query_one("#detail", DetailPanel)
        detail._workspace_name = "TestWS"
        detail._item_name = "TestItem"

        file_a = FileNode(workspace="ws-1", path="item-1/Files/a.csv", size=100)
        file_b = FileNode(workspace="ws-1", path="item-1/Files/b.json", size=200)

        for _ in range(5):
            detail.update_for_node(file_a)
            detail.update_for_node(file_b)

        await pilot.pause()
        await pilot.pause()
        assert detail.is_mounted


# ── 9. DetailPanel metadata pipeline and rendering ─────────────────────


def _get_widget_text(widget) -> str:
    """Extract plain text from a rendered Textual widget."""
    try:
        line = widget.render_line(0)
        # Strip is iterable of Segments; extract text from each
        return "".join(seg.text for seg in line)
    except Exception:
        return ""


@pytest.mark.asyncio
async def test_show_table_schema_folder_fallback():
    """When a table node represents a schema folder (no _delta_log),
    DetailPanel should show a friendly 'Not a Delta table' message.
    """
    import asyncio

    from textual.widgets import Static

    client = _make_mock_client()
    client.dfs.exists = AsyncMock(return_value=False)
    app = _DetailHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        detail = app.query_one("#detail", DetailPanel)
        detail._workspace_name = "TestWS"
        detail._item_name = "TestItem"

        table_node = TableNode(workspace="ws", item_path="item", table_name="dbo")
        detail.update_for_node(table_node)

        # Wait for debounce (0.15s) and async workers to complete
        await pilot.pause()
        await asyncio.sleep(0.2)
        await pilot.pause()

        # Query for Static widgets and extract their text content
        statics = detail.query(Static)
        static_texts = [_get_widget_text(w) for w in statics]
        assert any("Not a Delta table" in txt for txt in static_texts), (
            f"Expected 'Not a Delta table' message in DetailPanel. "
            f"Found: {static_texts}"
        )


@pytest.mark.asyncio
async def test_show_table_loads_metadata():
    """When a table node has valid Delta metadata (_delta_log exists),
    DetailPanel should load metadata and mount a TabbedContent widget.
    """
    import asyncio

    from textual.widgets import TabbedContent

    from onelake_client.models.table import Column, DeltaTableInfo

    client = _make_mock_client()
    client.dfs.exists = AsyncMock(return_value=True)

    # Create a minimal DeltaTableInfo mock
    delta_info = DeltaTableInfo(
        name="my_table",
        schema_=[Column(name="id", type="long", nullable=False)],
        version=5,
        num_files=10,
        size_bytes=1024000,
        partition_columns=[],
        properties={"delta.minReaderVersion": "1"},
        description="Test table",
    )
    client.delta.get_metadata = AsyncMock(return_value=delta_info)

    app = _DetailHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        detail = app.query_one("#detail", DetailPanel)
        detail._workspace_name = "TestWS"
        detail._item_name = "TestItem"

        table_node = TableNode(workspace="ws", item_path="item", table_name="dbo/my_table")
        detail.update_for_node(table_node)

        # Wait for debounce and async workers to complete
        await pilot.pause()
        await asyncio.sleep(0.2)
        await pilot.pause()
        await pilot.pause()

        # Query for TabbedContent widget (should be mounted on success)
        try:
            tabbed = detail.query_one(TabbedContent)
            assert tabbed is not None
        except Exception as e:
            raise AssertionError(
                f"Expected TabbedContent to be mounted in DetailPanel. Error: {e}"
            ) from e


@pytest.mark.asyncio
async def test_show_folder_renders_path():
    """When a folder node is selected, DetailPanel should render
    the folder icon, name, and path.
    """
    import asyncio

    from textual.widgets import Label, Static

    client = _make_mock_client()
    app = _DetailHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        detail = app.query_one("#detail", DetailPanel)
        detail._workspace_name = "TestWS"
        detail._item_name = "TestItem"

        folder_node = FolderNode(workspace="ws", item_path="item", directory="Files/subfolder")
        detail.update_for_node(folder_node)

        # Wait for debounce and rendering
        await pilot.pause()
        await asyncio.sleep(0.2)
        await pilot.pause()

        # Query for Label with folder emoji
        labels = detail.query(Label)
        label_texts = [_get_widget_text(w) for w in labels]
        assert any("📂" in txt for txt in label_texts), (
            f"Expected folder emoji (📂) in DetailPanel labels. Found: {label_texts}"
        )

        # Query for Static with path info
        statics = detail.query(Static)
        static_texts = [_get_widget_text(w) for w in statics]
        assert any("Path:" in txt for txt in static_texts), (
            f"Expected 'Path:' in DetailPanel statics. Found: {static_texts}"
        )


@pytest.mark.asyncio
async def test_show_file_renders_size():
    """When a file node is selected, DetailPanel should render
    the file icon, name, path, and size.
    """
    import asyncio

    from textual.widgets import Label, Static

    from onelake_tui.nodes import FileNode

    client = _make_mock_client()
    app = _DetailHarness(client)

    async with app.run_test() as pilot:
        await pilot.pause()
        detail = app.query_one("#detail", DetailPanel)
        detail._workspace_name = "TestWS"
        detail._item_name = "TestItem"

        file_node = FileNode(workspace="ws", path="item/Files/data.csv", size=2048)
        detail.update_for_node(file_node)

        # Wait for debounce and rendering
        await pilot.pause()
        await asyncio.sleep(0.2)
        await pilot.pause()

        # Query for Label with file emoji
        labels = detail.query(Label)
        label_texts = [_get_widget_text(w) for w in labels]
        assert any("📄" in txt for txt in label_texts), (
            f"Expected file emoji (📄) in DetailPanel labels. Found: {label_texts}"
        )

        # Query for Static with size info
        statics = detail.query(Static)
        static_texts = [_get_widget_text(w) for w in statics]
        assert any("Size:" in txt for txt in static_texts), (
            f"Expected 'Size:' in DetailPanel statics. Found: {static_texts}"
        )
