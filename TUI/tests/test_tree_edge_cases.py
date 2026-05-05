"""Tests for OneLakeTree edge cases: table detection, schema folders, errors, sort order."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from textual.app import App, ComposeResult

from onelake_client.environment import DEFAULT_ENVIRONMENT
from onelake_client.exceptions import NotFoundError
from onelake_client.models import Item, PathInfo
from onelake_tui.nodes import FileNode, FolderNode, TableNode
from onelake_tui.tree import OneLakeTree

# ── Helpers ──────────────────────────────────────────────────────────


def _make_mock_client() -> MagicMock:
    """Build a mock OneLakeClient with the minimum surface area."""
    client = MagicMock()
    client.env = DEFAULT_ENVIRONMENT
    client.fabric.list_workspaces = AsyncMock(return_value=[])
    client.fabric.list_items = AsyncMock(return_value=[])
    client.dfs.list_paths = AsyncMock(return_value=[])
    client.dfs.read_file = AsyncMock()
    client.auth.get_identity = MagicMock(return_value="test@contoso.com")
    client.close = AsyncMock()
    return client


def _make_item(type_: str = "Lakehouse", name: str = "TestLH", id_: str = "item-guid") -> Item:
    return Item(id=id_, displayName=name, type=type_)


def _make_path(name: str, *, is_dir: bool = False, size: int = 0) -> PathInfo:
    return PathInfo(name=name, isDirectory=is_dir, contentLength=size)


class _TreeHarness(App):
    def __init__(self, client: MagicMock):
        super().__init__()
        self._client = client

    def compose(self) -> ComposeResult:
        yield OneLakeTree(self._client, id="tree")


async def _load_and_wait(
    pilot, tree: OneLakeTree, item: Item, ws_id: str = "ws-guid", ws_name: str = "TestWS"
):
    """Trigger load_item and wait for the worker to complete."""
    tree.load_item(ws_id, ws_name, item)
    await pilot.pause()
    await asyncio.sleep(0.3)
    await pilot.pause()


# ── TestLoadItem ─────────────────────────────────────────────────────


class TestLoadItem:
    """Tests for OneLakeTree.load_item behaviour."""

    @pytest.mark.asyncio
    async def test_non_browsable_item_type(self):
        """A 'Report' item (not in _DFS_BROWSABLE_TYPES) shows a placeholder leaf."""
        client = _make_mock_client()
        app = _TreeHarness(client)

        async with app.run_test() as pilot:
            tree = app.query_one("#tree", OneLakeTree)
            await _load_and_wait(pilot, tree, _make_item(type_="Report", name="SalesReport"))

            children = list(tree.root.children)
            assert len(children) == 1
            assert str(children[0].label) == "(no file storage for this item type)"
            assert children[0].data is None
            assert not children[0].allow_expand

    @pytest.mark.asyncio
    async def test_not_found_error(self):
        """NotFoundError from DFS shows '(no DFS storage for this item)'."""
        client = _make_mock_client()
        client.dfs.list_paths = AsyncMock(side_effect=NotFoundError("Not found"))
        app = _TreeHarness(client)

        async with app.run_test() as pilot:
            tree = app.query_one("#tree", OneLakeTree)
            await _load_and_wait(pilot, tree, _make_item())

            children = list(tree.root.children)
            assert len(children) == 1
            assert str(children[0].label) == "(no DFS storage for this item)"
            assert children[0].data is None

    @pytest.mark.asyncio
    async def test_empty_dfs_response(self):
        """Empty list_paths response shows '(no files)'."""
        client = _make_mock_client()
        client.dfs.list_paths = AsyncMock(return_value=[])
        app = _TreeHarness(client)

        async with app.run_test() as pilot:
            tree = app.query_one("#tree", OneLakeTree)
            await _load_and_wait(pilot, tree, _make_item())

            children = list(tree.root.children)
            assert len(children) == 1
            assert str(children[0].label) == "(no files)"

    @pytest.mark.asyncio
    async def test_sort_order_folders_first(self):
        """Tree children are sorted: folders first, then files, both case-insensitive."""
        client = _make_mock_client()
        client.dfs.list_paths = AsyncMock(
            return_value=[
                _make_path("zebra.txt", size=100),
                _make_path("Alpha", is_dir=True),
                _make_path("data.csv", size=200),
                _make_path("beta", is_dir=True),
            ]
        )
        app = _TreeHarness(client)

        async with app.run_test() as pilot:
            tree = app.query_one("#tree", OneLakeTree)
            await _load_and_wait(pilot, tree, _make_item())

            children = list(tree.root.children)
            assert len(children) == 4

            # Folders first, case-insensitive: Alpha, beta
            assert isinstance(children[0].data, FolderNode)
            assert "Alpha" in str(children[0].label)
            assert isinstance(children[1].data, FolderNode)
            assert "beta" in str(children[1].label)

            # Files next, case-insensitive: data.csv, zebra.txt
            assert isinstance(children[2].data, FileNode)
            assert "data.csv" in str(children[2].label)
            assert isinstance(children[3].data, FileNode)
            assert "zebra.txt" in str(children[3].label)

    @pytest.mark.asyncio
    async def test_item_icon_displayed(self):
        """Lakehouse root label starts with the 🏠 icon."""
        client = _make_mock_client()
        app = _TreeHarness(client)

        async with app.run_test() as pilot:
            tree = app.query_one("#tree", OneLakeTree)
            await _load_and_wait(pilot, tree, _make_item(type_="Lakehouse", name="MyLH"))

            label = str(tree.root.label)
            assert label.startswith("🏠"), f"Root label should start with 🏠, got: {label!r}"
            assert "MyLH" in label


# ── TestFolderExpansion ──────────────────────────────────────────────


class TestFolderExpansion:
    """Tests for folder expansion and table detection in _load_folder."""

    @pytest.mark.asyncio
    async def test_table_detection_with_delta_log(self):
        """Folder under Tables/ with _delta_log → FolderNode/FileNode (real table)."""
        client = _make_mock_client()
        # First call: load_item returns a Tables/ folder
        # Second call: expanding Tables/ returns children including _delta_log
        client.dfs.list_paths = AsyncMock(
            side_effect=[
                # load_item: top-level paths
                [_make_path("Tables", is_dir=True)],
                # Expand Tables/: contains _delta_log → it's a real table dir
                [
                    _make_path("Tables/_delta_log", is_dir=True),
                    _make_path("Tables/part-00000.parquet", size=1024),
                ],
            ]
        )
        app = _TreeHarness(client)

        async with app.run_test() as pilot:
            tree = app.query_one("#tree", OneLakeTree)
            await _load_and_wait(pilot, tree, _make_item())

            # Root should have one child: Tables/ folder
            root_children = list(tree.root.children)
            assert len(root_children) == 1
            tables_node = root_children[0]
            assert isinstance(tables_node.data, FolderNode)

            # Expand Tables/ folder
            tables_node.expand()
            await pilot.pause()
            await asyncio.sleep(0.3)
            await pilot.pause()

            # Children should be FolderNode/FileNode (real table detected)
            table_children = list(tables_node.children)
            assert len(table_children) == 2
            # _delta_log is a directory → FolderNode
            assert isinstance(table_children[0].data, FolderNode)
            assert "_delta_log" in str(table_children[0].label)
            # parquet file → FileNode
            assert isinstance(table_children[1].data, FileNode)
            assert "part-00000.parquet" in str(table_children[1].label)

    @pytest.mark.asyncio
    async def test_schema_folder_detection(self):
        """A Tables/ folder without _delta_log or metadata → children are TableNodes."""
        client = _make_mock_client()
        client.dfs.list_paths = AsyncMock(
            side_effect=[
                # load_item: top-level paths
                [_make_path("Tables", is_dir=True)],
                # Expand Tables/: children are plain dirs (no _delta_log, no metadata)
                [
                    _make_path("Tables/customers", is_dir=True),
                    _make_path("Tables/orders", is_dir=True),
                ],
            ]
        )
        app = _TreeHarness(client)

        async with app.run_test() as pilot:
            tree = app.query_one("#tree", OneLakeTree)
            await _load_and_wait(pilot, tree, _make_item())

            tables_node = list(tree.root.children)[0]
            tables_node.expand()
            await pilot.pause()
            await asyncio.sleep(0.3)
            await pilot.pause()

            table_children = list(tables_node.children)
            assert len(table_children) == 2
            # Both should be TableNode (schema folder detection)
            assert isinstance(table_children[0].data, TableNode)
            assert table_children[0].data.table_name == "customers"
            assert isinstance(table_children[1].data, TableNode)
            assert table_children[1].data.table_name == "orders"

    @pytest.mark.asyncio
    async def test_empty_folder(self):
        """Expanding an empty folder shows '(empty)' leaf."""
        client = _make_mock_client()
        client.dfs.list_paths = AsyncMock(
            side_effect=[
                # load_item: top-level paths
                [_make_path("Files", is_dir=True)],
                # Expand Files/: empty
                [],
            ]
        )
        app = _TreeHarness(client)

        async with app.run_test() as pilot:
            tree = app.query_one("#tree", OneLakeTree)
            await _load_and_wait(pilot, tree, _make_item())

            files_node = list(tree.root.children)[0]
            files_node.expand()
            await pilot.pause()
            await asyncio.sleep(0.3)
            await pilot.pause()

            folder_children = list(files_node.children)
            assert len(folder_children) == 1
            assert str(folder_children[0].label) == "(empty)"
            assert folder_children[0].data is None


# ── TestTableFiles ───────────────────────────────────────────────────


class TestTableFiles:
    """Tests for _load_table_files — schema folder vs real table detection."""

    @pytest.mark.asyncio
    async def test_schema_folder_children_are_table_nodes(self):
        """TableNode without _delta_log → schema folder → children are TableNodes."""
        client = _make_mock_client()
        client.dfs.list_paths = AsyncMock(
            side_effect=[
                # load_item: top-level
                [_make_path("Tables", is_dir=True)],
                # Expand Tables/: no _delta_log → schema folder → TableNodes
                [_make_path("Tables/APPUSER", is_dir=True)],
                # Expand APPUSER (TableNode): children lack _delta_log → nested schema
                [
                    _make_path("Tables/APPUSER/SENSOR_READINGS", is_dir=True),
                    _make_path("Tables/APPUSER/EVENTS", is_dir=True),
                ],
            ]
        )
        app = _TreeHarness(client)

        async with app.run_test() as pilot:
            tree = app.query_one("#tree", OneLakeTree)
            await _load_and_wait(pilot, tree, _make_item())

            # Expand Tables/
            tables_node = list(tree.root.children)[0]
            tables_node.expand()
            await pilot.pause()
            await asyncio.sleep(0.3)
            await pilot.pause()

            # APPUSER should be a TableNode
            appuser_node = list(tables_node.children)[0]
            assert isinstance(appuser_node.data, TableNode)
            assert appuser_node.data.table_name == "APPUSER"

            # Expand APPUSER
            appuser_node.expand()
            await pilot.pause()
            await asyncio.sleep(0.3)
            await pilot.pause()

            children = list(appuser_node.children)
            assert len(children) == 2
            # Sorted case-insensitive: EVENTS, SENSOR_READINGS
            assert isinstance(children[0].data, TableNode)
            assert children[0].data.table_name == "APPUSER/EVENTS"
            assert isinstance(children[1].data, TableNode)
            assert children[1].data.table_name == "APPUSER/SENSOR_READINGS"

    @pytest.mark.asyncio
    async def test_real_table_shows_files(self):
        """A TableNode whose children include _delta_log → real table → FolderNode/FileNode."""
        client = _make_mock_client()
        client.dfs.list_paths = AsyncMock(
            side_effect=[
                # load_item: top-level
                [_make_path("Tables", is_dir=True)],
                # Expand Tables/: no _delta_log → children are TableNodes
                [_make_path("Tables/sales", is_dir=True)],
                # Expand sales (TableNode): contains _delta_log → real table
                [
                    _make_path("Tables/sales/_delta_log", is_dir=True),
                    _make_path("Tables/sales/part-00000.parquet", size=2048),
                    _make_path("Tables/sales/part-00001.parquet", size=4096),
                ],
            ]
        )
        app = _TreeHarness(client)

        async with app.run_test() as pilot:
            tree = app.query_one("#tree", OneLakeTree)
            await _load_and_wait(pilot, tree, _make_item())

            # Expand Tables/
            tables_node = list(tree.root.children)[0]
            tables_node.expand()
            await pilot.pause()
            await asyncio.sleep(0.3)
            await pilot.pause()

            # Expand sales TableNode
            sales_node = list(tables_node.children)[0]
            assert isinstance(sales_node.data, TableNode)
            sales_node.expand()
            await pilot.pause()
            await asyncio.sleep(0.3)
            await pilot.pause()

            children = list(sales_node.children)
            assert len(children) == 3
            # _delta_log folder first (sorted: dirs first)
            assert isinstance(children[0].data, FolderNode)
            assert "_delta_log" in str(children[0].label)
            # Parquet files
            assert isinstance(children[1].data, FileNode)
            assert "part-00000.parquet" in str(children[1].label)
            assert isinstance(children[2].data, FileNode)
            assert "part-00001.parquet" in str(children[2].label)

    @pytest.mark.asyncio
    async def test_empty_table(self):
        """An empty table directory shows '(empty table)' leaf."""
        client = _make_mock_client()
        client.dfs.list_paths = AsyncMock(
            side_effect=[
                # load_item: top-level
                [_make_path("Tables", is_dir=True)],
                # Expand Tables/: no _delta_log → TableNodes
                [_make_path("Tables/empty_tbl", is_dir=True)],
                # Expand empty_tbl (TableNode): empty
                [],
            ]
        )
        app = _TreeHarness(client)

        async with app.run_test() as pilot:
            tree = app.query_one("#tree", OneLakeTree)
            await _load_and_wait(pilot, tree, _make_item())

            tables_node = list(tree.root.children)[0]
            tables_node.expand()
            await pilot.pause()
            await asyncio.sleep(0.3)
            await pilot.pause()

            table_node = list(tables_node.children)[0]
            assert isinstance(table_node.data, TableNode)
            table_node.expand()
            await pilot.pause()
            await asyncio.sleep(0.3)
            await pilot.pause()

            children = list(table_node.children)
            assert len(children) == 1
            assert str(children[0].label) == "(empty table)"
            assert children[0].data is None
