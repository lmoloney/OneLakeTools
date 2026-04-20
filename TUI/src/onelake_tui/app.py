"""Unofficial OneLake TUI for browsing Microsoft Fabric OneLake."""

from __future__ import annotations

import argparse
import asyncio
import logging
import platform
import subprocess
from pathlib import Path
from urllib.parse import quote

from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.widgets import Header, Input, OptionList, Tree

from onelake_client import OneLakeClient, create_credential, get_environment
from onelake_client.environment import DEFAULT_ENVIRONMENT, ENVIRONMENTS, FabricEnvironment
from onelake_tui.copy_menu import CopyFormatMenu
from onelake_tui.detail import DetailPanel
from onelake_tui.help_screen import HelpScreen
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
        Binding("S", "screenshot", "Screenshot", show=False),
        Binding("y", "copy", "Copy"),
        Binding("slash", "search", "Search", show=True, priority=True),
        Binding("question_mark", "help", "Help"),
        Binding("ctrl+f", "toggle_footer", "Footer", show=False),
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
        logger.debug("Auth probe starting")
        try:
            # Run blocking credential check in a thread to avoid freezing
            # the event loop (DefaultAzureCredential probes IMDS, CLI, etc.)
            identity = await asyncio.to_thread(self.client.auth.get_identity)
            status = self.query_one(StatusBar)
            status.identity = identity
            logger.info("Authenticated as %s", identity)
        except Exception as exc:
            logger.exception("Auth probe failed")
            self._show_auth_error(str(exc))
            return
        self.notify("Connecting to OneLake...", timeout=5)

    @work(exclusive=True, group="identity_fallback")
    async def _ensure_identity(self) -> None:
        """Fallback: resolve identity if _probe_auth didn't set it."""
        status = self.query_one(StatusBar)
        if status.identity:
            return
        logger.debug("Identity fallback: resolving from cached token")
        try:
            identity = await asyncio.to_thread(self.client.auth.get_identity)
            status.identity = identity
            logger.info("Identity resolved via fallback: %s", identity)
        except Exception:
            logger.debug("Identity fallback failed", exc_info=True)

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
        status.update_path(ws.display_name)
        # Fallback: ensure identity is displayed (covers race with _probe_auth)
        if not status.identity:
            self._ensure_identity()

    def on_item_list_item_selected(self, event: ItemList.ItemSelected) -> None:
        """Load the selected item's DFS tree."""
        tree = self.query_one("#tree", OneLakeTree)
        tree.load_item(event.workspace_id, event.workspace_name, event.item)
        # Set context for human-readable paths in detail panel
        detail = self.query_one("#detail", DetailPanel)
        detail.set_context(event.workspace_name, event.item.display_name)
        # Update status bar
        status = self.query_one(StatusBar)
        status.update_path(f"{event.workspace_name} / {event.item.display_name}")

    def on_tree_node_highlighted(self, event: Tree.NodeHighlighted) -> None:
        """Update detail panel when a tree node is highlighted."""
        data = event.node.data
        detail = self.query_one("#detail", DetailPanel)
        status = self.query_one(StatusBar)

        path = self._node_display_path(data)
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
        """Handle Escape, vim-style navigation (j/k/g/G), and panel switching (h/l)."""
        search = self.query_one("#search-input", Input)

        # Escape dismisses search
        if event.key == "escape" and search.display and search.has_focus:
            search.display = False
            picker = self.query_one("#picker", WorkspacePicker)
            picker.clear_filter()
            picker.focus()
            event.prevent_default()
            return

        # Skip vim keys when typing in the search input
        if search.has_focus:
            return

        focused = self.focused
        if focused is None:
            return

        # h/l panel switching
        if event.key == "h":
            self.action_focus_previous()
            event.prevent_default()
            return
        if event.key == "l":
            self.action_focus_next()
            event.prevent_default()
            return

        # j/k/g/G vim navigation on list and tree widgets
        is_nav_widget = isinstance(focused, (OptionList, Tree))
        if not is_nav_widget:
            return

        if event.key == "j":
            self.simulate_key("down")
            event.prevent_default()
        elif event.key == "k":
            self.simulate_key("up")
            event.prevent_default()
        elif event.key == "g":
            self.simulate_key("home")
            event.prevent_default()
        elif event.key == "G":
            self.simulate_key("end")
            event.prevent_default()

    def action_help(self) -> None:
        """Show full-screen help overlay."""
        self.push_screen(HelpScreen())

    def action_toggle_footer(self) -> None:
        """Toggle the status bar visibility."""
        status = self.query_one(StatusBar)
        status.display = not status.display

    def action_screenshot(self) -> None:
        """Save an SVG screenshot to the current directory."""
        path = self.save_screenshot()
        self.notify(f"Screenshot saved: {path}", timeout=5)

    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        """Handle Enter on tree nodes — preview files."""
        data = event.node.data
        if isinstance(data, FileNode):
            detail = self.query_one("#detail", DetailPanel)
            detail.preview_file(data)

    # ── Clipboard helpers ───────────────────────────────────────────────

    def _copy_to_clipboard(self, text: str, label: str) -> None:
        """Copy text to clipboard and show notification."""
        system = platform.system()
        commands: list[list[str]]
        if system == "Darwin":
            commands = [["pbcopy"]]
        elif system == "Windows":
            commands = [["clip.exe"]]
        elif system == "Linux":
            commands = [
                ["wl-copy"],
                ["xclip", "-selection", "clipboard"],
                ["xsel", "--clipboard", "--input"],
            ]
        else:
            commands = []

        for command in commands:
            try:
                subprocess.run(command, input=text.encode(), check=True)
                self.notify(f"Copied {label}: {text}", timeout=3)
                return
            except FileNotFoundError:
                continue
            except subprocess.CalledProcessError as exc:
                logger.debug("Clipboard command failed (%s): %s", " ".join(command), exc)

        self.notify(f"{label}: {text}", timeout=5)

    @staticmethod
    def _relative_item_path(path: str) -> str:
        """Strip item prefix from a DFS path for display or named URIs."""
        normalized = path.rstrip("/")
        return normalized.split("/", 1)[-1] if "/" in normalized else normalized

    @staticmethod
    def _encode_segment(value: str) -> str:
        """Percent-encode a single URI segment."""
        return quote(value, safe="")

    @staticmethod
    def _encode_path(value: str) -> str:
        """Percent-encode a URI path while preserving path separators."""
        return quote(value, safe="/")

    def action_copy(self) -> None:
        """Open copy-format menu and copy the selected URI to clipboard."""
        tree = self.query_one("#tree", OneLakeTree)
        node = tree.cursor_node
        if node is None or node.data is None:
            self.notify("No item selected", severity="warning")
            return

        def _on_format_chosen(format_key: str | None) -> None:
            if format_key is None:
                return
            builders = {
                "https_named": self._node_to_https_named,
                "https_guid": self._node_to_https_guid,
                "abfss_named": self._node_to_abfss_named,
                "abfss_guid": self._node_to_abfss_guid,
            }
            builder = builders.get(format_key)
            if builder is None:
                return
            uri = builder(node.data)
            if not uri:
                self.notify(
                    "Couldn't generate a URI for the selected item",
                    severity="warning",
                    markup=False,
                )
                return
            self._copy_to_clipboard(uri, format_key.replace("_", " ").upper())

        self.push_screen(CopyFormatMenu(), callback=_on_format_chosen)

    def _node_display_path(self, data: object) -> str | None:
        """Build a human-readable breadcrumb path for display (not clipboard)."""
        tree = self.query_one("#tree", OneLakeTree)
        ws_name = tree._current_workspace_name or "?"
        item_name = tree._current_item.display_name if tree._current_item else "?"

        if isinstance(data, FolderNode):
            rel = self._relative_item_path(data.directory)
            return f"{ws_name} / {item_name} / {rel}"
        elif isinstance(data, FileNode):
            rel = self._relative_item_path(data.path)
            return f"{ws_name} / {item_name} / {rel}"
        elif isinstance(data, TableNode):
            return f"{ws_name} / {item_name} / Tables / {data.table_name}"
        return None

    def _node_to_https_named(self, data: object) -> str | None:
        """Build an HTTPS DFS URL using workspace/item display names."""
        if self.client is None:
            return None
        tree = self.query_one("#tree", OneLakeTree)
        ws_name = tree._current_workspace_name or "?"
        item_name = tree._current_item.display_name if tree._current_item else "?"
        host = self.client.env.dfs_host
        ws_name_enc = self._encode_segment(ws_name)
        item_name_enc = self._encode_segment(item_name)

        if isinstance(data, FolderNode):
            rel = self._relative_item_path(data.directory)
            return f"https://{host}/{ws_name_enc}/{item_name_enc}/{self._encode_path(rel)}"
        elif isinstance(data, FileNode):
            rel = self._relative_item_path(data.path)
            return f"https://{host}/{ws_name_enc}/{item_name_enc}/{self._encode_path(rel)}"
        elif isinstance(data, TableNode):
            return (
                f"https://{host}/{ws_name_enc}/{item_name_enc}/Tables/"
                f"{self._encode_path(data.table_name)}"
            )
        return None

    def _node_to_https_guid(self, data: object) -> str | None:
        """Build an HTTPS DFS URL using GUIDs."""
        if self.client is None:
            return None
        host = self.client.env.dfs_host

        if isinstance(data, FolderNode):
            return f"https://{host}/{data.workspace}/{self._encode_path(data.directory)}"
        elif isinstance(data, FileNode):
            return f"https://{host}/{data.workspace}/{self._encode_path(data.path)}"
        elif isinstance(data, TableNode):
            return (
                f"https://{host}/{data.workspace}/{data.item_path}/Tables/"
                f"{self._encode_path(data.table_name)}"
            )
        return None

    def _node_to_abfss_named(self, data: object) -> str | None:
        """Build an abfss:// URI using workspace/item display names."""
        if self.client is None:
            return None
        tree = self.query_one("#tree", OneLakeTree)
        ws_name = tree._current_workspace_name or "?"
        item_name = tree._current_item.display_name if tree._current_item else "?"
        host = self.client.env.dfs_host
        ws_name_enc = self._encode_segment(ws_name)
        item_name_enc = self._encode_segment(item_name)

        if isinstance(data, FolderNode):
            rel = self._relative_item_path(data.directory)
            return f"abfss://{ws_name_enc}@{host}/{item_name_enc}/{self._encode_path(rel)}"
        elif isinstance(data, FileNode):
            rel = self._relative_item_path(data.path)
            return f"abfss://{ws_name_enc}@{host}/{item_name_enc}/{self._encode_path(rel)}"
        elif isinstance(data, TableNode):
            return (
                f"abfss://{ws_name_enc}@{host}/{item_name_enc}/Tables/"
                f"{self._encode_path(data.table_name)}"
            )
        return None

    def _node_to_abfss_guid(self, data: object) -> str | None:
        """Build an abfss:// URI using GUIDs."""
        if self.client is None:
            return None
        host = self.client.env.dfs_host

        if isinstance(data, FolderNode):
            return f"abfss://{data.workspace}@{host}/{self._encode_path(data.directory)}"
        elif isinstance(data, FileNode):
            return f"abfss://{data.workspace}@{host}/{self._encode_path(data.path)}"
        elif isinstance(data, TableNode):
            return (
                f"abfss://{data.workspace}@{host}/{data.item_path}/Tables/"
                f"{self._encode_path(data.table_name)}"
            )
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


def _get_version() -> str:
    """Get the package version from installed metadata."""
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version("onelake-tui")
    except PackageNotFoundError:
        return "unknown"


def run() -> None:
    """Entry point for the onelake-tui command."""
    parser = argparse.ArgumentParser(
        prog="onelake-tui",
        description="Terminal Explorer for Microsoft Fabric OneLake",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {_get_version()}",
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
