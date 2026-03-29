"""Edge-case tests for onelake-client models, DFS/Fabric clients, and pagination."""

from __future__ import annotations

import pytest

from onelake_client._http import create_client, paginate_dfs, paginate_fabric
from onelake_client.dfs.client import DfsClient
from onelake_client.environment import PROD
from onelake_client.exceptions import ApiError, FileTooLargeError
from onelake_client.fabric.client import FabricClient
from onelake_client.models import Item, PathInfo, Workspace

FABRIC_URL = PROD.fabric_api_url
DFS_URL = f"https://{PROD.dfs_host}"


# ---------------------------------------------------------------------------
# 1. Unicode handling in Pydantic models
# ---------------------------------------------------------------------------


class TestUnicodeModels:
    def test_workspace_japanese_name(self):
        ws = Workspace(id="ws-1", displayName="日本語ワークスペース", type="Workspace")
        assert ws.display_name == "日本語ワークスペース"

    def test_workspace_accented_name(self):
        ws = Workspace(id="ws-2", displayName="Données Financières", type="Workspace")
        assert ws.display_name == "Données Financières"

    def test_workspace_emoji_name(self):
        ws = Workspace(id="ws-3", displayName="📊 Analytics", type="Workspace")
        assert ws.display_name == "📊 Analytics"

    def test_pathinfo_unicode_filename(self):
        pi = PathInfo(name="データ/レポート.csv", isDirectory=False, contentLength=42)
        assert pi.name == "データ/レポート.csv"
        assert pi.is_directory is False
        assert pi.content_length == 42

    def test_item_emoji_display_name(self):
        item = Item(id="i-1", displayName="🚀 Rocket Lakehouse", type="Lakehouse")
        assert item.display_name == "🚀 Rocket Lakehouse"

    def test_workspace_roundtrip_json(self):
        ws = Workspace(id="ws-4", displayName="日本語ワークスペース", type="Workspace")
        data = ws.model_dump(by_alias=True)
        ws2 = Workspace.model_validate(data)
        assert ws2.display_name == "日本語ワークスペース"


# ---------------------------------------------------------------------------
# 2. Empty API responses
# ---------------------------------------------------------------------------


class TestEmptyResponses:
    async def test_list_workspaces_empty(self, httpx_mock, auth):
        httpx_mock.add_response(
            url=f"{FABRIC_URL}/workspaces",
            json={"value": []},
        )
        client = FabricClient(auth)
        workspaces = await client.list_workspaces()
        assert workspaces == []
        await client.close()

    async def test_list_items_empty(self, httpx_mock, auth):
        httpx_mock.add_response(
            json={"value": []},
        )
        client = FabricClient(auth)
        items = await client.list_items("ws-001")
        assert items == []
        await client.close()

    async def test_list_paths_empty(self, httpx_mock, auth):
        httpx_mock.add_response(
            json={"paths": []},
        )
        client = DfsClient(auth)
        paths = await client.list_paths("my-workspace", "LH.Lakehouse")
        assert paths == []
        await client.close()


# ---------------------------------------------------------------------------
# 3. Malformed JSON handling
# ---------------------------------------------------------------------------


class TestMalformedJson:
    async def test_paginate_fabric_malformed_json(self, httpx_mock):
        httpx_mock.add_response(
            url="https://example.com/items",
            content=b"THIS IS NOT JSON",
            headers={"content-type": "application/json"},
        )
        client = create_client(base_url="https://example.com")
        with pytest.raises(ApiError, match="Malformed JSON"):
            items = []
            async for item in paginate_fabric(
                client,
                "https://example.com/items",
                headers={"Authorization": "Bearer fake"},
            ):
                items.append(item)
        await client.aclose()

    async def test_paginate_dfs_malformed_json(self, httpx_mock):
        httpx_mock.add_response(
            url="https://example.com/fs",
            content=b"{truncated",
            headers={"content-type": "application/json"},
        )
        client = create_client(base_url="https://example.com")
        with pytest.raises(ApiError, match="Malformed JSON"):
            async for _ in paginate_dfs(
                client,
                "https://example.com/fs",
                headers={"Authorization": "Bearer fake"},
            ):
                pass
        await client.aclose()


# ---------------------------------------------------------------------------
# 4. Special characters in names
# ---------------------------------------------------------------------------


class TestSpecialCharacters:
    def test_workspace_with_rich_markup_brackets(self):
        ws = Workspace(id="ws-5", displayName="[bold]Production[/bold]", type="Workspace")
        assert ws.display_name == "[bold]Production[/bold]"

    def test_pathinfo_with_spaces_and_symbols(self):
        pi = PathInfo(
            name="Files/my report #1 (final $copy) & backup%20.csv",
            isDirectory=False,
            contentLength=100,
        )
        assert pi.name == "Files/my report #1 (final $copy) & backup%20.csv"

    def test_item_with_quotes_and_backslashes(self):
        item = Item(
            id="i-2",
            displayName='Sales "Q1" \\2024\\',
            type="Lakehouse",
        )
        assert item.display_name == 'Sales "Q1" \\2024\\'

    def test_workspace_model_validate_special_chars(self):
        raw = {
            "id": "ws-6",
            "displayName": "[red]DANGER[/red] & <script>alert('xss')</script>",
            "type": "Workspace",
        }
        ws = Workspace.model_validate(raw)
        assert "[red]DANGER[/red]" in ws.display_name
        assert "<script>" in ws.display_name


# ---------------------------------------------------------------------------
# 5. File size limit enforcement (read_file max_bytes)
# ---------------------------------------------------------------------------


class TestFileSizeLimit:
    async def test_read_file_exceeds_max_bytes(self, httpx_mock, auth):
        httpx_mock.add_response(
            url=f"{DFS_URL}/ws/LH.Lakehouse/Files/big.bin",
            content=b"x" * 100,
            headers={"Content-Length": "2048"},
        )
        client = DfsClient(auth)
        with pytest.raises(FileTooLargeError) as exc_info:
            await client.read_file("ws", "LH.Lakehouse/Files/big.bin", max_bytes=1024)
        assert exc_info.value.size == 2048
        assert exc_info.value.max_bytes == 1024
        await client.close()

    async def test_read_file_within_max_bytes(self, httpx_mock, auth):
        httpx_mock.add_response(
            url=f"{DFS_URL}/ws/LH.Lakehouse/Files/small.bin",
            content=b"hello",
            headers={"Content-Length": "5"},
        )
        client = DfsClient(auth)
        data = await client.read_file("ws", "LH.Lakehouse/Files/small.bin", max_bytes=1024)
        assert data == b"hello"
        await client.close()

    async def test_read_file_no_max_bytes_backwards_compat(self, httpx_mock, auth):
        httpx_mock.add_response(
            url=f"{DFS_URL}/ws/LH.Lakehouse/Files/any.bin",
            content=b"data",
            headers={"Content-Length": "999999"},
        )
        client = DfsClient(auth)
        data = await client.read_file("ws", "LH.Lakehouse/Files/any.bin")
        assert data == b"data"
        await client.close()

    async def test_read_file_max_bytes_no_content_length(self, httpx_mock, auth):
        httpx_mock.add_response(
            url=f"{DFS_URL}/ws/LH.Lakehouse/Files/mystery.bin",
            content=b"mystery data",
        )
        client = DfsClient(auth)
        data = await client.read_file("ws", "LH.Lakehouse/Files/mystery.bin", max_bytes=1024)
        assert data == b"mystery data"
        await client.close()


# ---------------------------------------------------------------------------
# 6. Pagination limit enforcement (max_items)
# ---------------------------------------------------------------------------


class TestPaginationLimits:
    @pytest.mark.httpx_mock(
        can_send_already_matched_responses=True,
        assert_all_responses_were_requested=False,
    )
    async def test_paginate_fabric_max_items(self, httpx_mock):
        httpx_mock.add_response(
            url="https://example.com/items",
            json={
                "value": [{"n": i} for i in range(5)],
                "continuationToken": "page2",
            },
        )
        httpx_mock.add_response(
            url="https://example.com/items",
            json={
                "value": [{"n": i} for i in range(5, 10)],
            },
        )
        client = create_client(base_url="https://example.com")
        results = []
        async for item in paginate_fabric(
            client,
            "https://example.com/items",
            headers={"Authorization": "Bearer fake"},
            max_items=5,
        ):
            results.append(item)
        assert len(results) == 5
        assert results[-1]["n"] == 4
        await client.aclose()

    async def test_paginate_dfs_max_items(self, httpx_mock):
        httpx_mock.add_response(
            url="https://example.com/fs",
            json={
                "paths": [{"name": f"file{i}"} for i in range(10)],
            },
        )
        client = create_client(base_url="https://example.com")
        results = []
        async for item in paginate_dfs(
            client,
            "https://example.com/fs",
            headers={"Authorization": "Bearer fake"},
            max_items=3,
        ):
            results.append(item)
        assert len(results) == 3
        assert results[-1]["name"] == "file2"
        await client.aclose()

    async def test_paginate_fabric_max_items_spans_pages(self, httpx_mock):
        """max_items=3 with 2 items on page 1 should fetch page 2 and stop at 3 total."""
        httpx_mock.add_response(
            url="https://example.com/items",
            json={
                "value": [{"n": 0}, {"n": 1}],
                "continuationToken": "page2",
            },
        )
        httpx_mock.add_response(
            url="https://example.com/items?continuationToken=page2",
            json={
                "value": [{"n": 2}, {"n": 3}, {"n": 4}],
            },
        )
        client = create_client(base_url="https://example.com")
        results = []
        async for item in paginate_fabric(
            client,
            "https://example.com/items",
            headers={"Authorization": "Bearer fake"},
            max_items=3,
        ):
            results.append(item)
        assert len(results) == 3
        assert [r["n"] for r in results] == [0, 1, 2]
        await client.aclose()
