"""Tests for DfsClient."""

from __future__ import annotations

from onelake_client.dfs.client import DfsClient
from onelake_client.environment import PROD

BASE_URL = f"https://{PROD.dfs_host}"


async def test_list_paths(httpx_mock, auth):
    httpx_mock.add_response(
        url=(
            f"{BASE_URL}/my-workspace"
            "?resource=filesystem&recursive=false&directory=MyLakehouse.Lakehouse"
        ),
        json={
            "paths": [
                {
                    "name": "Tables",
                    "isDirectory": "true",
                    "contentLength": "0",
                    "lastModified": "2025-01-15T10:30:00Z",
                },
                {
                    "name": "Files",
                    "isDirectory": "true",
                    "contentLength": "0",
                },
                {
                    "name": "Files/data.csv",
                    "isDirectory": "false",
                    "contentLength": "1024",
                },
            ]
        },
    )

    client = DfsClient(auth)
    paths = await client.list_paths("my-workspace", "MyLakehouse.Lakehouse")

    assert len(paths) == 3
    assert paths[0].name == "Tables"
    assert paths[0].is_directory is True
    assert paths[2].name == "Files/data.csv"
    assert paths[2].is_directory is False
    assert paths[2].content_length == 1024

    await client.close()


async def test_read_file(httpx_mock, auth):
    httpx_mock.add_response(
        url=f"{BASE_URL}/my-workspace/MyLakehouse.Lakehouse/Files/test.txt",
        content=b"hello world",
    )

    client = DfsClient(auth)
    content = await client.read_file("my-workspace", "MyLakehouse.Lakehouse/Files/test.txt")

    assert content == b"hello world"
    await client.close()


async def test_get_properties(httpx_mock, auth):
    httpx_mock.add_response(
        url=f"{BASE_URL}/my-workspace/MyLakehouse.Lakehouse/Files/test.txt",
        headers={
            "Content-Length": "1024",
            "Content-Type": "text/plain",
            "ETag": '"abc123"',
            "x-ms-resource-type": "file",
        },
    )

    client = DfsClient(auth)
    props = await client.get_properties("my-workspace", "MyLakehouse.Lakehouse/Files/test.txt")

    assert props.content_length == 1024
    assert props.content_type == "text/plain"
    assert props.resource_type == "file"

    await client.close()


async def test_exists_returns_true(httpx_mock, auth):
    httpx_mock.add_response(
        url=f"{BASE_URL}/my-workspace/MyLakehouse.Lakehouse/Files/exists.txt",
        headers={"Content-Length": "100", "x-ms-resource-type": "file"},
    )

    client = DfsClient(auth)
    assert await client.exists("my-workspace", "MyLakehouse.Lakehouse/Files/exists.txt") is True
    await client.close()


async def test_exists_returns_false_on_404(httpx_mock, auth):
    httpx_mock.add_response(
        url=f"{BASE_URL}/my-workspace/MyLakehouse.Lakehouse/Files/missing.txt",
        status_code=404,
        json={"error": {"code": "PathNotFound"}},
    )

    client = DfsClient(auth)
    assert await client.exists("my-workspace", "MyLakehouse.Lakehouse/Files/missing.txt") is False
    await client.close()
