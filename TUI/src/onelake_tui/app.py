"""Unofficial OneLake TUI for browsing Microsoft Fabric OneLake."""

from __future__ import annotations

import argparse
import logging
import subprocess
from pathlib import Path

from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.widgets import Header, Input, Tree

from onelake_client import OneLakeClient, create_credential, get_environment
from onelake_client.environment import DEFAULT_ENVIRONMENT, ENVIRONMENTS, FabricEnvironment
from onelake_client.exceptions import AuthenticationError
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

logger = logging.getLogger(__name__)

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

    logging.getLogger("onelake_client").info("--- OneLake TUI (Unofficial) session started ---")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


class OneLakeApp(App):
    """An unofficial TUI for browsing Microsoft Fabric OneLake."""

    TITLE = "OneLake TUI (Unofficial)"
    SUB_TITLE = "Community-built terminal UI for Microsoft Fabric OneLake"
    CSS_PATH = "app.tcss"

    BINDINGS = [
        Binding("q", "quit", "Quit", priority=True),
        Binding("r", "refresh", "Refresh"),
        Binding("y", "copy_path", "Copy Path"),
        Binding("Y", "copy_abfss", "Copy ABFSS", show=False),
        Binding("ctrl+y", "copy_https", "Copy URL", show=False),
        Binding("slash", "search", "Search", show=True, priority=True),
        Binding("question_mark", "help", "Help"),
        Binding("tab", "focus_next", "Next Panel", show=False),
        Binding("shift+tab", "focus_previous", "Prev Panel", show=False),
    ]

    def __init__(self, env: FabricEnvironment | None = None, credential=None):
        super().__init__()
        _setup_logging()
        self._env = env or DEFAULT_ENVIRONMENT
        try:
            self.client = OneLakeClient(credential=credential, env=self._env)
        except Exception as e:
            self.client = None  # type: ignore[assignment]
            self._auth_error = str(e)
        else:
            self._auth_error = None

    def on_mount(self) -> None:
        """Welcome notification with eager auth check."""
        self.query_one("#search-input", Input).display = False
        # Show environment ring in subtitle and status bar
        ring = f"  [{self._env.name}]" if self._env.name != "PROD" else ""
        self.sub_title = f"{self.SUB_TITLE}{ring}"
        self.query_one(StatusBar).env_name = self._env.name
        if self._auth_error:
            self._show_auth_error(self._auth_error)
        else:
            self._probe_auth()

    @work(exclusive=True, group="auth_probe")
    async def _probe_auth(self) -> None:
        """Eagerly validate credentials before workspace loading begins."""
        try:
            identity = self.client.auth.get_identity()
            status = self.query_one(StatusBar)
            status.identity = identity
            logger.info("Authenticated as %s", identity)
        except AuthenticationError as exc:
            self._show_auth_error(str(exc))
            return
        self.notify("Connecting to OneLake...", timeout=5)

    def _show_auth_error(self, error: str) -> None:
        """Display a prominent auth error panel."""
        # Truncate long Azure SDK errors to the useful part
        msg = str(error)
        if len(msg) > 200:
            msg = msg[:200] + "…"
        self.notify(
            f"Auth failed: {msg}\nRun 'az login' and restart.",
            severity="error",
            timeout=30,
        )
        logger.error("Auth probe failed: %s", error)

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
            "↑↓ Navigate  │  Enter Preview  │  / Search  │  r Refresh  │  q Quit\n"
            "y Copy path  │  Y Copy ABFSS  │  Ctrl+Y Copy URL",
            timeout=10,
        )

    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        """Handle Enter on tree nodes — preview files."""
        data = event.node.data
        if isinstance(data, FileNode):
            detail = self.query_one("#detail", DetailPanel)
            detail.preview_file(data)

    # ── Clipboard helpers ───────────────────────────────────────────────

    def _copy_to_clipboard(self, text: str, label: str) -> None:
        """Copy text to clipboard and show notification."""
        try:
            subprocess.run(["pbcopy"], input=text.encode(), check=True)
            self.notify(f"Copied {label}: {text}", timeout=3)
        except (FileNotFoundError, subprocess.CalledProcessError):
            self.notify(f"{label}: {text}", timeout=5)

    def action_copy_path(self) -> None:
        """Copy the current OneLake path (named) to clipboard."""
        tree = self.query_one("#tree", OneLakeTree)
        node = tree.cursor_node
        if node is None or node.data is None:
            self.notify("No item selected", severity="warning")
            return
        path = self._node_to_path(node.data)
        if path:
            self._copy_to_clipboard(path, "path")

    def action_copy_abfss(self) -> None:
        """Copy the ABFSS GUID-based path to clipboard."""
        tree = self.query_one("#tree", OneLakeTree)
        node = tree.cursor_node
        if node is None or node.data is None:
            self.notify("No item selected", severity="warning")
            return
        path = self._node_to_abfss(node.data)
        if path:
            self._copy_to_clipboard(path, "ABFSS")

    def action_copy_https(self) -> None:
        """Copy the HTTPS DFS URL to clipboard."""
        tree = self.query_one("#tree", OneLakeTree)
        node = tree.cursor_node
        if node is None or node.data is None:
            self.notify("No item selected", severity="warning")
            return
        path = self._node_to_https(node.data)
        if path:
            self._copy_to_clipboard(path, "URL")

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

    def _node_to_abfss(self, data: object) -> str | None:
        """Convert node data to an abfss:// GUID-based path."""
        tree = self.query_one("#tree", OneLakeTree)
        ws_id = tree._current_workspace_id
        host = self.client.env.dfs_host

        if isinstance(data, FolderNode):
            return f"abfss://{ws_id}@{host}/{data.directory}"
        elif isinstance(data, FileNode):
            return f"abfss://{ws_id}@{host}/{data.path}"
        elif isinstance(data, TableNode):
            return f"abfss://{ws_id}@{host}/{data.item_path}/Tables/{data.table_name}"
        return None

    def _node_to_https(self, data: object) -> str | None:
        """Convert node data to an https:// DFS URL."""
        tree = self.query_one("#tree", OneLakeTree)
        ws_id = tree._current_workspace_id
        host = self.client.env.dfs_host

        if isinstance(data, FolderNode):
            return f"https://{host}/{ws_id}/{data.directory}"
        elif isinstance(data, FileNode):
            return f"https://{host}/{ws_id}/{data.path}"
        elif isinstance(data, TableNode):
            return f"https://{host}/{ws_id}/{data.item_path}/Tables/{data.table_name}"
        return None

    def action_refresh(self) -> None:
        """Refresh the workspace picker, item list, and tree."""
        self.query_one("#picker", WorkspacePicker).refresh_workspaces()
        self.query_one("#items", ItemList).clear_items()
        self.query_one("#tree", OneLakeTree).refresh_tree()
        self.notify("Refreshing...")

    async def on_unmount(self) -> None:
        """Clean up the client on exit."""
        if self.client is not None:
            try:
                await self.client.close()
            except Exception:
                logger.exception("Error closing client during unmount")


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
    parser.add_argument(
        "--credential",
        choices=["default", "managed", "cli", "env"],
        default="default",
        help="Credential type: default (DefaultAzureCredential), "
        "managed (ManagedIdentityCredential), "
        "cli (AzureCliCredential), "
        "env (EnvironmentCredential)",
    )
    args = parser.parse_args()

    env = get_environment(args.env)
    credential = create_credential(args.credential)
    app = OneLakeApp(env=env, credential=credential)
    app.run()


if __name__ == "__main__":
    run()
