"""Tests for DfsClient."""

from __future__ import annotations

import pytest

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


async def test_read_file_stream_yields_chunks(httpx_mock, auth):
    """Test that read_file_stream yields chunks of data correctly."""

    chunk1 = b"hello "
    chunk2 = b"world "
    chunk3 = b"streaming"

    httpx_mock.add_response(
        url=f"{BASE_URL}/my-workspace/MyLakehouse.Lakehouse/Files/large.txt",
        content=chunk1 + chunk2 + chunk3,
    )

    client = DfsClient(auth)
    chunks = []
    async for chunk in client.read_file_stream(
        "my-workspace", "MyLakehouse.Lakehouse/Files/large.txt", chunk_size=6
    ):
        chunks.append(chunk)

    assert len(chunks) > 0
    combined = b"".join(chunks)
    assert combined == chunk1 + chunk2 + chunk3

    await client.close()


async def test_read_file_stream_404_raises_not_found(httpx_mock, auth):
    """Test that read_file_stream raises NotFoundError on 404."""
    from onelake_client.exceptions import NotFoundError

    httpx_mock.add_response(
        url=f"{BASE_URL}/my-workspace/MyLakehouse.Lakehouse/Files/missing.txt",
        status_code=404,
        text="Not found",
    )

    client = DfsClient(auth)
    with pytest.raises(NotFoundError):
        async for _ in client.read_file_stream(
            "my-workspace", "MyLakehouse.Lakehouse/Files/missing.txt"
        ):
            pass

    await client.close()


async def test_read_file_stream_403_raises_auth_error(httpx_mock, auth):
    """Test that read_file_stream raises AuthenticationError on 403."""
    from onelake_client.exceptions import AuthenticationError

    httpx_mock.add_response(
        url=f"{BASE_URL}/my-workspace/MyLakehouse.Lakehouse/Files/forbidden.txt",
        status_code=403,
        text="Forbidden",
    )

    client = DfsClient(auth)
    with pytest.raises(AuthenticationError):
        async for _ in client.read_file_stream(
            "my-workspace", "MyLakehouse.Lakehouse/Files/forbidden.txt"
        ):
            pass

    await client.close()


async def test_read_file_stream_500_raises_api_error(httpx_mock, auth):
    """Test that read_file_stream raises ApiError on 5xx status."""
    from onelake_client.exceptions import ApiError

    httpx_mock.add_response(
        url=f"{BASE_URL}/my-workspace/MyLakehouse.Lakehouse/Files/error.txt",
        status_code=500,
        text="Internal Server Error",
    )

    client = DfsClient(auth)
    with pytest.raises(ApiError) as exc_info:
        async for _ in client.read_file_stream(
            "my-workspace", "MyLakehouse.Lakehouse/Files/error.txt"
        ):
            pass
    assert exc_info.value.status_code == 500

    await client.close()


async def test_list_paths_403_raises(httpx_mock, auth):
    """Test that list_paths raises PermissionDeniedError on 403."""
    from onelake_client.exceptions import PermissionDeniedError

    httpx_mock.add_response(
        url=(
            f"{BASE_URL}/my-workspace"
            "?resource=filesystem&recursive=false&directory=MyLakehouse.Lakehouse"
        ),
        status_code=403,
        text="Forbidden",
    )

    client = DfsClient(auth)
    with pytest.raises(PermissionDeniedError):
        await client.list_paths("my-workspace", "MyLakehouse.Lakehouse")

    await client.close()


async def test_read_file_network_timeout(httpx_mock, auth):
    """Test that network timeout is handled during streaming."""
    import httpx

    httpx_mock.add_exception(
        httpx.ReadTimeout("Read timed out"),
        url=f"{BASE_URL}/my-workspace/MyLakehouse.Lakehouse/Files/timeout.txt",
    )

    client = DfsClient(auth)
    with pytest.raises(httpx.ReadTimeout):
        async for _ in client.read_file_stream(
            "my-workspace", "MyLakehouse.Lakehouse/Files/timeout.txt"
        ):
            pass

    await client.close()
