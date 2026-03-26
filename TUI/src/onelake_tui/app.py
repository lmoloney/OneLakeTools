"""OneLake TUI — Terminal UI for browsing Microsoft Fabric OneLake."""

from __future__ import annotations

import argparse
import logging
import subprocess
from pathlib import Path

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.widgets import Header, Input, Tree

from onelake_client import OneLakeClient, get_environment
from onelake_client.environment import DEFAULT_ENVIRONMENT, ENVIRONMENTS, FabricEnvironment
from onelake_tui.detail import DetailPanel
from onelake_tui.item_list import ItemList
from onelake_tui.nodes import (
    FileNode,
    FolderNode,
    TableNode,
)
from onelake_tui.status_bar import StatusBar
from onelake_tui.tree import OneLakeTree
from onelake_tui.workspace_picker import WorkspacePicker

_LOG_DIR = Path.home() / ".onelake-tui"
_LOG_FILE = _LOG_DIR / "debug.log"


def _setup_logging() -> None:
    """Configure file-based logging for diagnostics."""
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    root_logger = logging.getLogger()
    # Avoid duplicate handlers on re-init
    if any(
        isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "").endswith("debug.log")
        for h in root_logger.handlers
    ):
        return

    handler = logging.FileHandler(_LOG_FILE, mode="a", encoding="utf-8")
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s  %(name)s  %(message)s"))
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.DEBUG)

    logging.getLogger("onelake_client").info("--- OneLake TUI session started ---")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


class OneLakeApp(App):
    """A TUI for browsing Microsoft Fabric OneLake."""

    TITLE = "OneLake Explorer"
    SUB_TITLE = "Browse workspaces, lakehouses, and tables"
    CSS_PATH = "app.tcss"

    BINDINGS = [
        Binding("q", "quit", "Quit", priority=True),
        Binding("r", "refresh", "Refresh"),
        Binding("y", "copy_path", "Copy Path"),
        Binding("slash", "search", "Search", show=True, priority=True),
        Binding("question_mark", "help", "Help"),
        Binding("tab", "focus_next", "Next Panel", show=False),
        Binding("shift+tab", "focus_previous", "Prev Panel", show=False),
    ]

    def __init__(self, env: FabricEnvironment | None = None):
        super().__init__()
        _setup_logging()
        self._env = env or DEFAULT_ENVIRONMENT
        try:
            self.client = OneLakeClient(env=self._env)
        except Exception as e:
            self.client = None  # type: ignore[assignment]
            self._auth_error = str(e)
        else:
            self._auth_error = None

    def on_mount(self) -> None:
        """Welcome notification with auth hint."""
        self.query_one("#search-input", Input).display = False
        # Show environment ring in subtitle and status bar
        ring = f"  [{self._env.name}]" if self._env.name != "PROD" else ""
        self.sub_title = f"Browse workspaces, lakehouses, and tables{ring}"
        self.query_one(StatusBar).env_name = self._env.name
        if self._auth_error:
            self.notify(
                f"Auth failed: {self._auth_error}\nRun 'az login' and restart.",
                severity="error",
                timeout=20,
            )
        else:
            self.notify("Connecting to OneLake... (ensure 'az login' is done)", timeout=5)

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal(id="main"):
            with Vertical(id="picker-panel"):
                yield Input(
                    placeholder="Search workspaces...",
                    id="search-input",
                )
                yield WorkspacePicker(self.client, id="picker")
                yield ItemList(self.client, id="items")
            with Vertical(id="content-panel"):
                yield OneLakeTree(self.client, id="tree")
                yield DetailPanel(self.client, id="detail")
        yield StatusBar()

    def on_workspace_picker_workspace_selected(
        self, event: WorkspacePicker.WorkspaceSelected
    ) -> None:
        """Load items for the selected workspace into the item list."""
        ws = event.workspace
        item_list = self.query_one("#items", ItemList)
        item_list.load_items(ws.id, ws.display_name)
        # Update header
        ring = f"  [{self._env.name}]" if self._env.name != "PROD" else ""
        self.sub_title = f"{ws.display_name}{ring}"
        # Update status bar
        status = self.query_one(StatusBar)
        status.update_path(f"onelake://{ws.display_name}")

    def on_item_list_item_selected(self, event: ItemList.ItemSelected) -> None:
        """Load the selected item's DFS tree."""
        tree = self.query_one("#tree", OneLakeTree)
        tree.load_item(event.workspace_id, event.workspace_name, event.item)
        # Set context for human-readable paths in detail panel
        detail = self.query_one("#detail", DetailPanel)
        detail.set_context(event.workspace_name, event.item.display_name)
        # Update status bar
        status = self.query_one(StatusBar)
        status.update_path(f"onelake://{event.workspace_name}/{event.item.display_name}")

    def on_tree_node_highlighted(self, event: Tree.NodeHighlighted) -> None:
        """Update detail panel when a tree node is highlighted."""
        data = event.node.data
        detail = self.query_one("#detail", DetailPanel)
        status = self.query_one(StatusBar)

        path = self._node_to_path(data)
        if path:
            status.update_path(path)

        if isinstance(data, (FolderNode, FileNode, TableNode)):
            detail.update_for_node(data)
        else:
            detail.update_for_node(None)

    def action_search(self) -> None:
        """Show search bar and focus it."""
        search = self.query_one("#search-input", Input)
        search.display = True
        search.value = ""
        search.focus()

    def on_input_changed(self, event: Input.Changed) -> None:
        """Filter workspace picker as user types."""
        if event.input.id == "search-input":
            picker = self.query_one("#picker", WorkspacePicker)
            query = event.value.strip()
            if query:
                count = picker.filter(query)
                event.input.placeholder = f"{count} match(es)"
            else:
                picker.clear_filter()
                event.input.placeholder = "Search workspaces..."

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Accept search — hide bar, keep filter, refocus item list."""
        if event.input.id == "search-input":
            event.input.display = False
            self.query_one("#items", ItemList).focus()

    def on_key(self, event) -> None:
        """Handle Escape from search input."""
        search = self.query_one("#search-input", Input)
        if event.key == "escape" and search.display and search.has_focus:
            search.display = False
            picker = self.query_one("#picker", WorkspacePicker)
            picker.clear_filter()
            picker.focus()
            event.prevent_default()

    def action_help(self) -> None:
        """Show keyboard shortcuts."""
        self.notify(
            "↑↓ Navigate  │  Enter Preview  │  / Search  │  r Refresh  │  q Quit",
            timeout=10,
        )

    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        """Handle Enter on tree nodes — preview files."""
        data = event.node.data
        if isinstance(data, FileNode):
            detail = self.query_one("#detail", DetailPanel)
            detail.preview_file(data)

    def action_copy_path(self) -> None:
        """Copy the current OneLake path to clipboard."""
        tree = self.query_one("#tree", OneLakeTree)
        node = tree.cursor_node
        if node is None or node.data is None:
            self.notify("No item selected", severity="warning")
            return

        data = node.data
        path = self._node_to_path(data)
        if path:
            try:
                subprocess.run(["pbcopy"], input=path.encode(), check=True)
                self.notify(f"Copied: {path}", timeout=3)
            except (FileNotFoundError, subprocess.CalledProcessError):
                self.notify(f"Path: {path}", timeout=5)

    def _node_to_path(self, data: object) -> str | None:
        """Convert node data to a human-readable OneLake path string."""
        tree = self.query_one("#tree", OneLakeTree)
        ws_name = tree._current_workspace_name or "?"
        item_name = tree._current_item.display_name if tree._current_item else "?"

        if isinstance(data, FolderNode):
            # Strip item GUID prefix to get relative path
            rel = data.directory.split("/", 1)[-1] if "/" in data.directory else data.directory
            return f"onelake://{ws_name}/{item_name}/{rel}"
        elif isinstance(data, FileNode):
            rel = data.path.split("/", 1)[-1] if "/" in data.path else data.path
            return f"onelake://{ws_name}/{item_name}/{rel}"
        elif isinstance(data, TableNode):
            return f"onelake://{ws_name}/{item_name}/Tables/{data.table_name}"
        return None

    def action_refresh(self) -> None:
        """Refresh the workspace picker, item list, and tree."""
        self.query_one("#picker", WorkspacePicker).refresh_workspaces()
        self.query_one("#items", ItemList).clear_items()
        self.query_one("#tree", OneLakeTree).refresh_tree()
        self.notify("Refreshing...")

    async def on_unmount(self) -> None:
        """Clean up the client on exit."""
        await self.client.close()


def run() -> None:
    """Entry point for the onelake-tui command."""
    parser = argparse.ArgumentParser(
        prog="onelake-tui",
        description="Terminal Explorer for Microsoft Fabric OneLake",
    )
    parser.add_argument(
        "--env",
        choices=[k.lower() for k in ENVIRONMENTS],
        default="prod",
        help="Fabric environment ring (default: prod)",
    )
    args = parser.parse_args()

    env = get_environment(args.env)
    app = OneLakeApp(env=env)
    app.run()


if __name__ == "__main__":
    run()
