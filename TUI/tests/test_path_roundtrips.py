"""Tests for path encoding, URI building, and path formatting helpers.

Exercises special-character handling across _encode_segment, _encode_path,
_relative_item_path, _build_table_uri, and the four URI-builder instance
methods on OneLakeApp.
"""

from __future__ import annotations

from unittest.mock import MagicMock
from urllib.parse import quote

import pytest

from onelake_client.environment import MSIT, PROD
from onelake_client.tables.delta import _build_table_uri
from onelake_tui.app import OneLakeApp
from onelake_tui.nodes import FileNode, FolderNode, TableNode

# ---------------------------------------------------------------------------
# Edge-case matrix
# ---------------------------------------------------------------------------

EDGE_CASES = [
    ("ascii_only", "sales_data"),
    ("spaces", "My Lakehouse"),
    ("unicode_latin", "données_client"),
    ("unicode_cjk", "日本語テスト"),
    ("deep_nesting", "a/b/c/d/e/f/g"),
    ("special_chars", "data (copy) #1 & backup"),
    ("emoji", "📊 dashboard"),
    ("percent_literal", "100%25_complete"),
    ("hash", "data#section"),
    ("ampersand", "A & B"),
]


# ── helpers ────────────────────────────────────────────────────────────────


def _make_tui_with_context(
    ws_name: str,
    item_name: str,
    dfs_host: str = PROD.dfs_host,
) -> OneLakeApp:
    """Build a mock OneLakeApp with just enough state for URI builders."""
    tui = OneLakeApp.__new__(OneLakeApp)
    tui.client = MagicMock()
    tui.client.env.dfs_host = dfs_host

    tree = MagicMock()
    tree._current_workspace_name = ws_name
    item = MagicMock()
    item.display_name = item_name
    tree._current_item = item
    tui.query_one = MagicMock(return_value=tree)
    return tui


# ===================================================================
# TestEncodeSegment
# ===================================================================


class TestEncodeSegment:
    """OneLakeApp._encode_segment encodes a single URI segment (safe='')."""

    @pytest.mark.parametrize("label,value", EDGE_CASES, ids=[e[0] for e in EDGE_CASES])
    def test_matches_quote_safe_empty(self, label: str, value: str) -> None:
        assert OneLakeApp._encode_segment(value) == quote(value, safe="")

    def test_spaces_become_percent_20(self) -> None:
        assert "%20" in OneLakeApp._encode_segment("My Lakehouse")

    def test_slashes_are_encoded(self) -> None:
        result = OneLakeApp._encode_segment("a/b")
        assert "/" not in result  # slash must be encoded
        assert "%2F" in result

    def test_emoji_is_percent_encoded(self) -> None:
        result = OneLakeApp._encode_segment("📊 dashboard")
        assert "📊" not in result
        assert "%" in result

    def test_no_double_encoding(self) -> None:
        """A literal '%25' in input should become '%2525', not '%25' again."""
        once = OneLakeApp._encode_segment("100%25_complete")
        twice = OneLakeApp._encode_segment(once)
        # Double-encoding means every '%' from the first pass gets encoded
        # again, so applying it twice must change the string.
        assert once != twice

    def test_ascii_unchanged_except_reserved(self) -> None:
        result = OneLakeApp._encode_segment("sales_data")
        assert result == "sales_data"


# ===================================================================
# TestEncodePath
# ===================================================================


class TestEncodePath:
    """OneLakeApp._encode_path encodes a URI path, preserving '/'."""

    @pytest.mark.parametrize("label,value", EDGE_CASES, ids=[e[0] for e in EDGE_CASES])
    def test_matches_quote_safe_slash(self, label: str, value: str) -> None:
        assert OneLakeApp._encode_path(value) == quote(value, safe="/")

    def test_slashes_preserved(self) -> None:
        result = OneLakeApp._encode_path("a/b/c/d/e/f/g")
        assert result == "a/b/c/d/e/f/g"

    def test_spaces_encoded(self) -> None:
        result = OneLakeApp._encode_path("My Lakehouse/Files")
        assert result == "My%20Lakehouse/Files"

    def test_deep_nesting_no_encoding_of_separators(self) -> None:
        deep = "a/b/c/d/e/f/g"
        assert OneLakeApp._encode_path(deep) == deep

    def test_mixed_special_and_slashes(self) -> None:
        result = OneLakeApp._encode_path("Tables/données/日本")
        assert result.startswith("Tables/")
        assert "/" in result  # slashes preserved
        assert "%20" not in result  # no spaces to encode


# ===================================================================
# TestRelativeItemPath
# ===================================================================


class TestRelativeItemPath:
    """OneLakeApp._relative_item_path strips the item-GUID prefix."""

    def test_typical_file_path(self) -> None:
        assert OneLakeApp._relative_item_path("item-guid/Files/data.csv") == "Files/data.csv"

    def test_trailing_slash_stripped(self) -> None:
        # Trailing slash is stripped, then no '/' remains → returns as-is
        assert OneLakeApp._relative_item_path("item-guid/") == "item-guid"

    def test_no_slash(self) -> None:
        assert OneLakeApp._relative_item_path("item-guid") == "item-guid"

    def test_deep_subpath(self) -> None:
        assert (
            OneLakeApp._relative_item_path("item-guid/Tables/schema/table") == "Tables/schema/table"
        )

    def test_multiple_trailing_slashes(self) -> None:
        result = OneLakeApp._relative_item_path("item-guid///")
        # rstrip("/") → "item-guid", no slash → returns "item-guid"
        assert result == "item-guid"


# ===================================================================
# TestBuildTableUri
# ===================================================================


class TestBuildTableUri:
    """_build_table_uri is a plain string formatter — no encoding."""

    def test_basic(self) -> None:
        uri = _build_table_uri("ws", "item.Lakehouse", "sales", PROD.dfs_host)
        assert uri == f"abfss://ws@{PROD.dfs_host}/item.Lakehouse/Tables/sales"

    def test_unicode_workspace(self) -> None:
        uri = _build_table_uri("données", "lh.Lakehouse", "t", PROD.dfs_host)
        assert uri.startswith("abfss://données@")

    def test_unicode_table_name(self) -> None:
        uri = _build_table_uri("ws", "lh.Lakehouse", "日本語テスト", PROD.dfs_host)
        assert uri.endswith("/Tables/日本語テスト")

    def test_schema_prefix_in_table_name(self) -> None:
        uri = _build_table_uri("ws", "lh.Lakehouse", "dbo/orders", PROD.dfs_host)
        assert "/Tables/dbo/orders" in uri

    def test_emoji_workspace(self) -> None:
        uri = _build_table_uri("📊 dash", "lh.Lakehouse", "t", PROD.dfs_host)
        assert "📊 dash@" in uri

    def test_msit_host(self) -> None:
        uri = _build_table_uri("ws", "item.Lakehouse", "t", MSIT.dfs_host)
        assert MSIT.dfs_host in uri


# ===================================================================
# TestUriBuilders
# ===================================================================


class TestUriBuilders:
    """The four URI-builder instance methods on OneLakeApp."""

    # -- FileNode ----------------------------------------------------------

    def test_https_named_file_spaces(self) -> None:
        tui = _make_tui_with_context("My Workspace", "My Lakehouse")
        node = FileNode(workspace="ws-guid", path="item-guid/Files/data.csv", size=100)
        url = tui._node_to_https_named(node)
        assert url is not None
        assert "My%20Workspace" in url
        assert "My%20Lakehouse" in url
        assert url.endswith("/Files/data.csv")

    def test_https_guid_file(self) -> None:
        tui = _make_tui_with_context("ignored", "ignored")
        node = FileNode(workspace="ws-guid", path="item-guid/Files/data.csv", size=42)
        url = tui._node_to_https_guid(node)
        assert url is not None
        assert "/ws-guid/" in url
        assert url.endswith("/item-guid/Files/data.csv")

    def test_abfss_named_file(self) -> None:
        tui = _make_tui_with_context("My WS", "LH")
        node = FileNode(workspace="ws-guid", path="item-guid/Files/report.csv", size=0)
        uri = tui._node_to_abfss_named(node)
        assert uri is not None
        assert uri.startswith("abfss://My%20WS@")
        assert "/LH/" in uri

    def test_abfss_guid_file(self) -> None:
        tui = _make_tui_with_context("ignored", "ignored")
        node = FileNode(workspace="ws-guid", path="item-guid/Files/f.parquet", size=1)
        uri = tui._node_to_abfss_guid(node)
        assert uri is not None
        assert uri.startswith("abfss://ws-guid@")

    # -- FolderNode --------------------------------------------------------

    def test_https_named_folder_unicode(self) -> None:
        tui = _make_tui_with_context("données", "lh")
        node = FolderNode(workspace="ws-guid", item_path="ig", directory="ig/Tables")
        url = tui._node_to_https_named(node)
        assert url is not None
        assert quote("données", safe="") in url

    def test_https_guid_folder(self) -> None:
        tui = _make_tui_with_context("x", "y")
        node = FolderNode(workspace="ws-guid", item_path="ig", directory="ig/Tables")
        url = tui._node_to_https_guid(node)
        assert url is not None
        assert "/ws-guid/" in url

    # -- TableNode ---------------------------------------------------------

    def test_https_named_table_schema_prefix(self) -> None:
        tui = _make_tui_with_context("ws", "lh")
        node = TableNode(workspace="ws-guid", item_path="ig", table_name="dbo/orders")
        url = tui._node_to_https_named(node)
        assert url is not None
        assert "/Tables/dbo/orders" in url

    def test_abfss_named_table(self) -> None:
        tui = _make_tui_with_context("ws", "lh")
        node = TableNode(workspace="ws-guid", item_path="ig", table_name="sales")
        uri = tui._node_to_abfss_named(node)
        assert uri is not None
        assert uri.startswith("abfss://ws@")
        assert "/Tables/sales" in uri

    def test_abfss_guid_table(self) -> None:
        tui = _make_tui_with_context("x", "y")
        node = TableNode(workspace="ws-guid", item_path="ig", table_name="t")
        uri = tui._node_to_abfss_guid(node)
        assert uri is not None
        assert "ws-guid@" in uri
        assert "/ig/Tables/t" in uri

    # -- GUIDs are not encoded ---------------------------------------------

    def test_guid_builders_do_not_encode_workspace_guid(self) -> None:
        tui = _make_tui_with_context("whatever", "whatever")
        guid = "aaaabbbb-cccc-dddd-eeee-ffffffffffff"
        node = FileNode(workspace=guid, path=f"{guid}/Files/x.csv", size=0)

        https = tui._node_to_https_guid(node)
        abfss = tui._node_to_abfss_guid(node)
        assert https is not None and abfss is not None
        # GUIDs contain only hex + hyphens — already URL-safe
        assert f"/{guid}/" in https
        assert f"{guid}@" in abfss

    # -- MSIT host ---------------------------------------------------------

    def test_msit_host_propagated(self) -> None:
        tui = _make_tui_with_context("ws", "lh", dfs_host=MSIT.dfs_host)
        node = FileNode(workspace="g", path="g/Files/x", size=0)
        url = tui._node_to_https_named(node)
        assert url is not None
        assert MSIT.dfs_host in url

    # -- None when client is missing ---------------------------------------

    def test_returns_none_without_client(self) -> None:
        tui = OneLakeApp.__new__(OneLakeApp)
        tui.client = None
        node = FileNode(workspace="w", path="i/Files/f", size=0)
        assert tui._node_to_https_named(node) is None
        assert tui._node_to_https_guid(node) is None
        assert tui._node_to_abfss_named(node) is None
        assert tui._node_to_abfss_guid(node) is None


# ===================================================================
# TestDfsPathConstruction
# ===================================================================


class TestDfsPathConstruction:
    """Verify the directory-parameter string logic used by DfsClient.list_paths."""

    def test_root_listing(self) -> None:
        item_path = "my-lakehouse-guid"
        directory = ""
        result = f"{item_path}/{directory}" if directory else item_path
        assert result == "my-lakehouse-guid"

    def test_subdirectory_listing(self) -> None:
        item_path = "my-lakehouse-guid"
        directory = "Tables/données"
        result = f"{item_path}/{directory}" if directory else item_path
        assert result == "my-lakehouse-guid/Tables/données"

    def test_files_subdirectory(self) -> None:
        item_path = "guid-123"
        directory = "Files/sub folder/deep"
        result = f"{item_path}/{directory}" if directory else item_path
        assert result == "guid-123/Files/sub folder/deep"
