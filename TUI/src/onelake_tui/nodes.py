"""Node data types for the OneLake tree."""

from __future__ import annotations

from dataclasses import dataclass

from onelake_client.models import Item, Workspace


@dataclass
class WorkspaceNode:
    """A workspace in the tree."""

    workspace: Workspace


@dataclass
class ItemNode:
    """A Fabric item (lakehouse, warehouse, etc.) in the tree."""

    workspace_id: str
    workspace_name: str
    item: Item


@dataclass
class FolderNode:
    """A folder in OneLake DFS."""

    workspace: str
    item_path: str
    directory: str


@dataclass
class FileNode:
    """A file in OneLake DFS."""

    workspace: str
    path: str
    size: int


@dataclass
class TableNode:
    """A Delta/Iceberg table in a lakehouse."""

    workspace: str
    item_path: str
    table_name: str
