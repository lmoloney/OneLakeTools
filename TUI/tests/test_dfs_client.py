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


async def test_read_file_max_bytes_head_check_rejects(httpx_mock, auth):
    """HEAD-first check should raise FileTooLargeError before downloading body."""
    from onelake_client.exceptions import FileTooLargeError

    url = f"{BASE_URL}/my-workspace/MyLakehouse.Lakehouse/Files/big.bin"
    # HEAD response reports file is 10MB
    httpx_mock.add_response(url=url, method="HEAD", headers={"Content-Length": "10485760"})
    # GET should NOT be called — if it is, the test will fail
    # because pytest-httpx raises on unexpected requests

    client = DfsClient(auth)
    with pytest.raises(FileTooLargeError) as exc_info:
        await client.read_file(
            "my-workspace", "MyLakehouse.Lakehouse/Files/big.bin", max_bytes=1024
        )
    assert exc_info.value.size == 10485760
    assert exc_info.value.max_bytes == 1024

    # Verify only 1 request was made (HEAD), not a GET
    requests = httpx_mock.get_requests()
    assert len(requests) == 1
    assert requests[0].method == "HEAD"

    await client.close()


async def test_read_file_max_bytes_within_limit(httpx_mock, auth):
    """HEAD reports size within limit, so file is downloaded normally."""
    url = f"{BASE_URL}/my-workspace/MyLakehouse.Lakehouse/Files/small.txt"
    httpx_mock.add_response(url=url, method="HEAD", headers={"Content-Length": "11"})
    httpx_mock.add_response(url=url, method="GET", content=b"hello world")

    client = DfsClient(auth)
    content = await client.read_file(
        "my-workspace", "MyLakehouse.Lakehouse/Files/small.txt", max_bytes=1024
    )
    assert content == b"hello world"
    await client.close()


async def test_read_file_no_max_bytes_skips_head(httpx_mock, auth):
    """Without max_bytes, no HEAD request should be issued."""
    url = f"{BASE_URL}/my-workspace/MyLakehouse.Lakehouse/Files/test.txt"
    httpx_mock.add_response(url=url, method="GET", content=b"data")

    client = DfsClient(auth)
    content = await client.read_file("my-workspace", "MyLakehouse.Lakehouse/Files/test.txt")
    assert content == b"data"

    requests = httpx_mock.get_requests()
    assert all(r.method == "GET" for r in requests)
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


async def test_read_file_stream_403_raises_permission_denied(httpx_mock, auth):
    """Test that read_file_stream raises PermissionDeniedError on 403."""
    from onelake_client.exceptions import PermissionDeniedError

    httpx_mock.add_response(
        url=f"{BASE_URL}/my-workspace/MyLakehouse.Lakehouse/Files/forbidden.txt",
        status_code=403,
        text="Forbidden",
    )

    client = DfsClient(auth)
    with pytest.raises(PermissionDeniedError):
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


async def test_read_file_range_suffix(httpx_mock, auth):
    """Suffix range sends Range: bytes=-N and returns partial content."""
    url = f"{BASE_URL}/my-workspace/MyLakehouse.Lakehouse/Files/data.parquet"
    httpx_mock.add_response(url=url, status_code=206, content=b"partial data")

    client = DfsClient(auth)
    result = await client.read_file_range(
        "my-workspace", "MyLakehouse.Lakehouse/Files/data.parquet", suffix_length=1024
    )

    assert result == b"partial data"
    req = httpx_mock.get_requests()[0]
    assert req.headers["Range"] == "bytes=-1024"
    await client.close()


async def test_read_file_range_offset_length(httpx_mock, auth):
    """Offset+length range sends Range: bytes=X-Y."""
    url = f"{BASE_URL}/my-workspace/MyLakehouse.Lakehouse/Files/data.parquet"
    httpx_mock.add_response(url=url, status_code=206, content=b"partial data")

    client = DfsClient(auth)
    result = await client.read_file_range(
        "my-workspace", "MyLakehouse.Lakehouse/Files/data.parquet", offset=100, length=100
    )

    assert result == b"partial data"
    req = httpx_mock.get_requests()[0]
    assert req.headers["Range"] == "bytes=100-199"
    await client.close()


async def test_read_file_range_offset_only(httpx_mock, auth):
    """Offset-only range sends Range: bytes=X-."""
    url = f"{BASE_URL}/my-workspace/MyLakehouse.Lakehouse/Files/data.parquet"
    httpx_mock.add_response(url=url, status_code=206, content=b"partial data")

    client = DfsClient(auth)
    result = await client.read_file_range(
        "my-workspace", "MyLakehouse.Lakehouse/Files/data.parquet", offset=100
    )

    assert result == b"partial data"
    req = httpx_mock.get_requests()[0]
    assert req.headers["Range"] == "bytes=100-"
    await client.close()


async def test_read_file_range_accepts_200(httpx_mock, auth):
    """Server returning 200 (full file) instead of 206 should still succeed.

    Per RFC 7233 §4.4 a server MAY ignore the Range header and return 200.
    OneLake DFS does this for small files.
    """
    url = f"{BASE_URL}/my-workspace/MyLakehouse.Lakehouse/Files/data.parquet"
    httpx_mock.add_response(url=url, status_code=200, content=b"full file contents")

    client = DfsClient(auth)
    result = await client.read_file_range(
        "my-workspace", "MyLakehouse.Lakehouse/Files/data.parquet", suffix_length=1024
    )
    assert result == b"full file contents"
    await client.close()


async def test_read_file_range_no_params(auth):
    """Calling read_file_range with no range params raises ValueError."""
    client = DfsClient(auth)
    with pytest.raises(ValueError, match="At least one of"):
        await client.read_file_range("my-workspace", "MyLakehouse.Lakehouse/Files/data.parquet")
    await client.close()


async def test_read_file_range_suffix_with_offset(auth):
    """Combining suffix_length with offset raises ValueError."""
    client = DfsClient(auth)
    with pytest.raises(ValueError, match="suffix_length cannot be combined"):
        await client.read_file_range(
            "my-workspace",
            "MyLakehouse.Lakehouse/Files/data.parquet",
            suffix_length=1024,
            offset=100,
        )
    await client.close()


async def test_read_file_range_length_without_offset(auth):
    """Providing length without offset raises ValueError."""
    client = DfsClient(auth)
    with pytest.raises(ValueError, match="length requires offset"):
        await client.read_file_range(
            "my-workspace",
            "MyLakehouse.Lakehouse/Files/data.parquet",
            length=100,
        )
    await client.close()


async def test_read_file_range_max_bytes_rejects_large_file(httpx_mock, auth):
    """HEAD pre-check should raise FileTooLargeError before GET for large files."""
    from onelake_client.exceptions import FileTooLargeError

    url = f"{BASE_URL}/my-workspace/MyLakehouse.Lakehouse/Files/huge.parquet"
    # HEAD returns large Content-Length
    httpx_mock.add_response(url=url, method="HEAD", headers={"Content-Length": "500000000"})
    # GET should NOT be called
    client = DfsClient(auth)
    with pytest.raises(FileTooLargeError) as exc_info:
        await client.read_file_range(
            "my-workspace",
            "MyLakehouse.Lakehouse/Files/huge.parquet",
            suffix_length=1024,
            max_bytes=1024 * 1024,
        )
    assert exc_info.value.size == 500_000_000
    await client.close()


async def test_read_file_range_max_bytes_allows_small_file(httpx_mock, auth):
    """HEAD pre-check should allow files within max_bytes, then GET succeeds."""
    url = f"{BASE_URL}/my-workspace/MyLakehouse.Lakehouse/Files/small.parquet"
    httpx_mock.add_response(url=url, method="HEAD", headers={"Content-Length": "5000"})
    httpx_mock.add_response(url=url, status_code=200, content=b"file data")
    client = DfsClient(auth)
    result = await client.read_file_range(
        "my-workspace",
        "MyLakehouse.Lakehouse/Files/small.parquet",
        suffix_length=1024,
        max_bytes=1024 * 1024,
    )
    assert result == b"file data"
    await client.close()


async def test_read_file_range_no_max_bytes_skips_head(httpx_mock, auth):
    """Without max_bytes, no HEAD request should be issued."""
    url = f"{BASE_URL}/my-workspace/MyLakehouse.Lakehouse/Files/data.parquet"
    httpx_mock.add_response(url=url, status_code=206, content=b"partial")
    client = DfsClient(auth)
    result = await client.read_file_range(
        "my-workspace",
        "MyLakehouse.Lakehouse/Files/data.parquet",
        suffix_length=1024,
    )
    assert result == b"partial"
    # Only 1 request (GET), no HEAD
    assert len(httpx_mock.get_requests()) == 1
    assert httpx_mock.get_requests()[0].method == "GET"
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
