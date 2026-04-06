"""Smoke tests for onelake_tui widgets — mount without crashing."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from textual.app import App, ComposeResult

from onelake_client.environment import DEFAULT_ENVIRONMENT
from onelake_tui.detail import DetailPanel
from onelake_tui.item_list import ItemList
from onelake_tui.nodes import FolderNode
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
