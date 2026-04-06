"""Flat workspace picker — filterable OptionList for workspace selection."""

from __future__ import annotations

import logging
import time

from textual import on, work
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.message import Message
from textual.widgets import OptionList
from textual.widgets.option_list import Option

from onelake_client import OneLakeClient
from onelake_client.models import Workspace

logger = logging.getLogger("onelake_tui.workspace_picker")

_CACHE_TTL = 300  # 5 minutes


class WorkspacePicker(Vertical):
    """Flat, filterable list of workspaces in the left panel."""

    class WorkspaceSelected(Message):
        """Posted when a workspace is highlighted in the list."""

        def __init__(self, workspace: Workspace) -> None:
            super().__init__()
            self.workspace = workspace

    DEFAULT_CSS = """
    WorkspacePicker {
        height: 1fr;
    }
    WorkspacePicker OptionList {
        height: 1fr;
        background: $surface-darken-1;
        border-right: tall $primary-background;
        padding: 0 1;
    }
    """

    def __init__(self, client: OneLakeClient, **kwargs) -> None:
        super().__init__(**kwargs)
        self.client = client
        self._workspaces: list[Workspace] = []
        self._filtered: list[Workspace] = []
        self._cache_fetched_at: float = 0.0

    def compose(self) -> ComposeResult:
        yield OptionList(id="workspace-list")

    def on_mount(self) -> None:
        self.load_workspaces()

    @work(exclusive=True, group="load_workspaces")
    async def load_workspaces(self, *, force: bool = False) -> None:
        """Fetch workspaces and populate the option list."""
        option_list = self.query_one("#workspace-list", OptionList)

        # Use cached data if still fresh
        if (
            not force
            and self._workspaces
            and (time.monotonic() - self._cache_fetched_at) < _CACHE_TTL
        ):
            logger.debug("Using cached workspaces (%d items)", len(self._workspaces))
            return

        option_list.clear_options()
        try:
            self.app.notify("Fetching workspaces...", timeout=3)
            workspaces = await self.client.fabric.list_workspaces()
            self._workspaces = sorted(workspaces, key=lambda w: w.display_name.casefold())
            self._filtered = list(self._workspaces)
            self._cache_fetched_at = time.monotonic()
            self._rebuild_options()
            self.app.notify(f"Loaded {len(self._workspaces)} workspaces", timeout=3)
            logger.info("Loaded %d workspaces", len(self._workspaces))
        except Exception as e:
            option_list.add_option(Option(f"❌ Error: {e}", id="error", disabled=True))
            logger.exception("Failed to load workspaces")
            self.app.notify(
                f"Error loading workspaces: {e}",
                severity="error",
                timeout=20,
                markup=False,
            )

    def _rebuild_options(self) -> None:
        """Rebuild the OptionList from the current filtered workspace list."""
        option_list = self.query_one("#workspace-list", OptionList)
        option_list.clear_options()
        for ws in self._filtered:
            option_list.add_option(Option(f"📁 {ws.display_name}", id=ws.id))

    def filter(self, query: str) -> int:
        """Filter workspaces by name substring. Returns match count."""
        q = query.casefold()
        self._filtered = [ws for ws in self._workspaces if q in ws.display_name.casefold()]
        self._rebuild_options()
        # Auto-highlight first match
        if self._filtered:
            option_list = self.query_one("#workspace-list", OptionList)
            option_list.highlighted = 0
        return len(self._filtered)

    def clear_filter(self) -> None:
        """Restore all workspaces."""
        self._filtered = list(self._workspaces)
        self._rebuild_options()

    @on(OptionList.OptionHighlighted, "#workspace-list")
    def _on_highlighted(self, event: OptionList.OptionHighlighted) -> None:
        """Post WorkspaceSelected when a workspace is highlighted."""
        if event.option is None or event.option.id is None:
            return
        ws = self._workspace_by_id(event.option.id)
        if ws:
            self.post_message(self.WorkspaceSelected(ws))

    def _workspace_by_id(self, ws_id: str) -> Workspace | None:
        """Look up a workspace by its ID."""
        for ws in self._workspaces:
            if ws.id == ws_id:
                return ws
        return None

    def refresh_workspaces(self) -> None:
        """Reload workspaces from scratch (bypasses cache)."""
        self._workspaces.clear()
        self._filtered.clear()
        self._cache_fetched_at = 0.0
        self.load_workspaces(force=True)
