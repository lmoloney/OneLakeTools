"""OneLake tree widget — browses DFS paths for a single Fabric item."""

from __future__ import annotations

import logging

from textual import work
from textual.widgets import Tree
from textual.widgets._tree import TreeNode

from onelake_client import OneLakeClient
from onelake_client.exceptions import NotFoundError
from onelake_client.models import Item
from onelake_tui.nodes import FileNode, FolderNode, TableNode

logger = logging.getLogger("onelake_tui.tree")

NodeData = FolderNode | FileNode | TableNode | None

_ITEM_ICONS = {
    "Lakehouse": "🏠",
    "Warehouse": "🏭",
    "Notebook": "📓",
    "Report": "📊",
    "SQLEndpoint": "🔌",
    "SemanticModel": "📐",
    "DataPipeline": "🔄",
    "Eventstream": "⚡",
    "KQLDatabase": "📡",
    "KQLQueryset": "🔍",
}

# Item types that have DFS-browsable OneLake storage
_DFS_BROWSABLE_TYPES = {
    "Lakehouse",
    "Warehouse",
    "MirroredDatabase",
    "MirroredWarehouse",
    "Eventhouse",
    "KQLDatabase",
    "SQLEndpoint",
    "DataPipeline",
    "SparkJobDefinition",
    "MLModel",
    "MLExperiment",
    "Environment",
}


class OneLakeTree(Tree[NodeData]):
    """Tree widget that lazily browses DFS paths for a single Fabric item."""

    def __init__(self, client: OneLakeClient, **kwargs) -> None:
        super().__init__("Select an item…", **kwargs)
        self.client = client
        self.root.data = None
        self.root.expand()
        self._current_item: Item | None = None
        self._current_workspace_id: str = ""
        self._current_workspace_name: str = ""

    @work(exclusive=True, group="load_item")
    async def load_item(self, workspace_id: str, workspace_name: str, item: Item) -> None:
        """Load DFS paths for a single item as top-level tree nodes."""
        self._current_item = item
        self._current_workspace_id = workspace_id
        self._current_workspace_name = workspace_name
        self.root.set_label(f"{_ITEM_ICONS.get(item.type, '📄')} {item.display_name}")
        self.root.remove_children()

        if item.type not in _DFS_BROWSABLE_TYPES:
            self.root.add_leaf("(no file storage for this item type)", data=None)
            return

        try:
            paths = await self.client.dfs.list_paths(workspace_id, item.id)
            for p in sorted(paths, key=lambda x: (not x.is_directory, x.name.casefold())):
                name = p.name.split("/")[-1] if "/" in p.name else p.name
                if p.is_directory:
                    child = self.root.add(
                        f"📂 {name}",
                        data=FolderNode(
                            workspace=workspace_id,
                            item_path=item.id,
                            directory=p.name,
                        ),
                        allow_expand=True,
                    )
                    child.add_leaf("⏳ Loading...", data=None)
                else:
                    size = p.content_length or 0
                    self.root.add_leaf(
                        f"📄 {name} ({_format_size(size)})",
                        data=FileNode(
                            workspace=workspace_id,
                            path=p.name,
                            size=size,
                        ),
                    )
            if not paths:
                self.root.add_leaf("(no files)", data=None)
        except NotFoundError:
            self.root.add_leaf("(no DFS storage for this item)", data=None)
        except Exception as e:
            self.root.add_leaf(f"❌ {e}", data=None)
            logger.exception("Failed to load DFS paths for %s", item.display_name)
            self.app.notify(f"Error loading paths: {e}", severity="error", timeout=20, markup=False)

    def on_tree_node_expanded(self, event: Tree.NodeExpanded) -> None:
        """Handle node expansion — lazy-load folder/table children."""
        node = event.node
        data = node.data

        if data is None:
            return

        if isinstance(data, FolderNode):
            self._load_folder(node, data)
        elif isinstance(data, TableNode):
            self._load_table_files(node, data)

    @work(group="load_children")
    async def _load_folder(self, node: TreeNode, data: FolderNode) -> None:
        """Load children of a DFS folder."""
        node.remove_children()
        is_tables_dir = data.directory.rstrip("/").endswith("Tables")
        try:
            paths = await self.client.dfs.list_paths(data.workspace, data.directory)
            child_names = {
                (p.name.split("/")[-1] if "/" in p.name else p.name)
                for p in paths
                if p.is_directory
            }
            # A table directory contains _delta_log or metadata — detect this
            has_delta = "_delta_log" in child_names
            has_iceberg = "metadata" in child_names
            is_table = has_delta or has_iceberg

            for p in sorted(paths, key=lambda x: (not x.is_directory, x.name.casefold())):
                name = p.name.split("/")[-1] if "/" in p.name else p.name
                if p.is_directory:
                    if is_tables_dir and not is_table:
                        # Direct child of Tables/ that is NOT itself a table
                        # → could be a schema folder or a table
                        # Treat as a potential table (expandable)
                        child = node.add(
                            f"🗃️ {name}",
                            data=TableNode(
                                workspace=data.workspace,
                                item_path=data.item_path,
                                table_name=name,
                            ),
                            allow_expand=True,
                        )
                        child.add_leaf("⏳ Loading...", data=None)
                    else:
                        child = node.add(
                            f"📂 {name}",
                            data=FolderNode(
                                workspace=data.workspace,
                                item_path=data.item_path,
                                directory=p.name,
                            ),
                            allow_expand=True,
                        )
                        child.add_leaf("⏳ Loading...", data=None)
                else:
                    size = p.content_length or 0
                    node.add_leaf(
                        f"📄 {name} ({_format_size(size)})",
                        data=FileNode(
                            workspace=data.workspace,
                            path=p.name,
                            size=size,
                        ),
                    )
            if not paths:
                node.add_leaf("(empty)", data=None)
        except Exception as e:
            node.add_leaf(f"❌ {e}", data=None)
            logger.exception("Failed to load folder %s", data.directory)
            self.app.notify(
                f"Error loading folder: {e}",
                severity="error",
                timeout=20,
                markup=False,
            )

    @work(group="load_children")
    async def _load_table_files(self, node: TreeNode, data: TableNode) -> None:
        """Load contents of a table dir — detect schema folders vs actual tables."""
        node.remove_children()
        table_dir = f"{data.item_path}/Tables/{data.table_name}"
        try:
            paths = await self.client.dfs.list_paths(data.workspace, table_dir)
            child_names = {
                (p.name.split("/")[-1] if "/" in p.name else p.name)
                for p in paths
                if p.is_directory
            }
            # If this contains _delta_log or metadata, it's a real table → show raw files
            # Otherwise it's a schema folder → children are tables
            is_schema_folder = "_delta_log" not in child_names and "metadata" not in child_names

            for p in sorted(paths, key=lambda x: (not x.is_directory, x.name.casefold())):
                name = p.name.split("/")[-1] if "/" in p.name else p.name
                if p.is_directory:
                    if is_schema_folder:
                        # Schema folder: children are actual tables
                        child = node.add(
                            f"🗃️ {name}",
                            data=TableNode(
                                workspace=data.workspace,
                                item_path=data.item_path,
                                table_name=f"{data.table_name}/{name}",
                            ),
                            allow_expand=True,
                        )
                        child.add_leaf("⏳ Loading...", data=None)
                    else:
                        child = node.add(
                            f"📂 {name}",
                            data=FolderNode(
                                workspace=data.workspace,
                                item_path=data.item_path,
                                directory=p.name,
                            ),
                            allow_expand=True,
                        )
                        child.add_leaf("⏳ Loading...", data=None)
                else:
                    size = p.content_length or 0
                    node.add_leaf(
                        f"📄 {name} ({_format_size(size)})",
                        data=FileNode(
                            workspace=data.workspace,
                            path=p.name,
                            size=size,
                        ),
                    )
            if not paths:
                node.add_leaf("(empty table)", data=None)
        except Exception as e:
            node.add_leaf(f"❌ {e}", data=None)
            logger.exception("Failed to load table files for %s", data.table_name)

    def refresh_tree(self) -> None:
        """Reload the current item's DFS tree, or clear if none selected."""
        if self._current_item:
            self.load_item(
                self._current_workspace_id,
                self._current_workspace_name,
                self._current_item,
            )
        else:
            self.root.remove_children()
            self.root.set_label("Select an item…")


def _format_size(size_bytes: int) -> str:
    """Format byte size to human readable."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"
