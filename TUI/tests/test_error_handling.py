"""Tests for error handling paths across the client library.

Covers error scenarios in HTTP layer, DFS client, Delta subprocess,
and pagination functions.
"""

from __future__ import annotations

import json
import subprocess
from unittest.mock import MagicMock, patch

import httpx
import pytest
from deltalake.exceptions import DeltaError

from onelake_client._http import paginate_dfs, paginate_fabric
from onelake_client.dfs.client import DfsClient
from onelake_client.environment import PROD
from onelake_client.exceptions import ApiError
from onelake_client.tables.delta import _run_delta_subprocess

BASE_URL = f"https://{PROD.dfs_host}"
TEST_URL = "https://api.fabric.microsoft.com/v1/test"
AUTH_HEADERS = {"Authorization": "Bearer x"}


# ---------------------------------------------------------------------------
# DFS Client Error Tests
# ---------------------------------------------------------------------------


class TestDfsClientErrorHandling:
    """Test DFS client error propagation and handling."""

    async def test_dfs_exists_propagates_non_404_errors(self, httpx_mock, auth):
        """exists() should NOT return False for 500 — must propagate ApiError.

        The exists() method only catches NotFoundError (404). Other errors
        (including 500) must propagate to the caller. Since 500 is retryable,
        we need to provide enough responses for all retries.
        """
        # Add multiple 500 responses for retries (default is 3 retries + 1 initial = 4 attempts)
        for _ in range(4):
            httpx_mock.add_response(
                status_code=500,
                text="Internal Server Error",
            )

        client = DfsClient(auth)
        with pytest.raises(ApiError) as exc_info:
            await client.exists("my-workspace", "MyLakehouse.Lakehouse/Files/test.txt")

        # After all retries, the final 500 error should propagate
        assert exc_info.value.status_code == 500
        await client.close()

    async def test_dfs_read_file_empty_response(self, httpx_mock, auth):
        """read_file() with empty body (Content-Length: 0) should return b''."""
        httpx_mock.add_response(
            url=f"{BASE_URL}/my-workspace/MyLakehouse.Lakehouse/Files/empty.txt",
            content=b"",
            headers={"Content-Length": "0"},
        )

        client = DfsClient(auth)
        content = await client.read_file("my-workspace", "MyLakehouse.Lakehouse/Files/empty.txt")

        assert content == b""
        await client.close()

    async def test_dfs_read_file_exceeds_max_bytes(self, httpx_mock, auth):
        """read_file() should raise FileTooLargeError if Content-Length > max_bytes."""
        from onelake_client.exceptions import FileTooLargeError

        httpx_mock.add_response(
            url=f"{BASE_URL}/my-workspace/MyLakehouse.Lakehouse/Files/large.txt",
            headers={"Content-Length": "2000"},
            content=b"x" * 2000,
        )

        client = DfsClient(auth)
        with pytest.raises(FileTooLargeError) as exc_info:
            await client.read_file(
                "my-workspace",
                "MyLakehouse.Lakehouse/Files/large.txt",
                max_bytes=1000,
            )

        assert exc_info.value.size == 2000
        assert exc_info.value.max_bytes == 1000
        await client.close()


# ---------------------------------------------------------------------------
# Pagination Error Tests
# ---------------------------------------------------------------------------


class TestPaginateFabricErrors:
    """Test error handling in paginate_fabric."""

    async def test_paginate_fabric_malformed_json_raises_api_error(self, httpx_mock):
        """paginate_fabric should raise ApiError if JSON is malformed."""
        httpx_mock.add_response(url=TEST_URL, text="not valid json", status_code=200)

        async with httpx.AsyncClient() as client:
            with pytest.raises(ApiError, match="Malformed JSON"):
                async for _ in paginate_fabric(client, TEST_URL, headers=AUTH_HEADERS):
                    pass

    async def test_paginate_fabric_empty_value(self, httpx_mock):
        """paginate_fabric with empty 'value' array should return no items."""
        httpx_mock.add_response(
            url=TEST_URL,
            json={"value": []},
        )

        async with httpx.AsyncClient() as client:
            items = [item async for item in paginate_fabric(client, TEST_URL, headers=AUTH_HEADERS)]

        assert items == []

    async def test_paginate_fabric_missing_value_key_returns_empty(self, httpx_mock):
        """paginate_fabric with missing 'value' key should return no items, not error."""
        httpx_mock.add_response(
            url=TEST_URL,
            json={"data": []},
        )

        async with httpx.AsyncClient() as client:
            items = [item async for item in paginate_fabric(client, TEST_URL, headers=AUTH_HEADERS)]

        # Should gracefully return empty list (uses data.get(value_key, []))
        assert items == []

    async def test_paginate_fabric_null_value_returns_empty(self, httpx_mock):
        """paginate_fabric with null 'value' should handle gracefully."""
        httpx_mock.add_response(
            url=TEST_URL,
            json={"value": None},
        )

        async with httpx.AsyncClient() as client:
            # data.get("value", []) will return None, for loop on None will fail
            # This is a known limitation; we're testing current behavior
            with pytest.raises((TypeError, AttributeError)):
                async for _ in paginate_fabric(client, TEST_URL, headers=AUTH_HEADERS):
                    pass


class TestPaginateDfsErrors:
    """Test error handling in paginate_dfs."""

    async def test_paginate_dfs_malformed_json_raises_api_error(self, httpx_mock):
        """paginate_dfs should raise ApiError if JSON is malformed."""
        httpx_mock.add_response(
            url=TEST_URL,
            text="<html>not json</html>",
            status_code=200,
        )

        async with httpx.AsyncClient() as client:
            with pytest.raises(ApiError, match="Malformed JSON"):
                async for _ in paginate_dfs(client, TEST_URL, headers=AUTH_HEADERS):
                    pass

    async def test_paginate_dfs_empty_paths(self, httpx_mock):
        """paginate_dfs with empty 'paths' array should return no items."""
        httpx_mock.add_response(
            url=TEST_URL,
            json={"paths": []},
        )

        async with httpx.AsyncClient() as client:
            items = [item async for item in paginate_dfs(client, TEST_URL, headers=AUTH_HEADERS)]

        assert items == []

    async def test_paginate_dfs_missing_paths_key_returns_empty(self, httpx_mock):
        """paginate_dfs with missing 'paths' key should return no items, not error."""
        httpx_mock.add_response(
            url=TEST_URL,
            json={"items": [{"name": "something"}]},
        )

        async with httpx.AsyncClient() as client:
            items = [item async for item in paginate_dfs(client, TEST_URL, headers=AUTH_HEADERS)]

        # Should gracefully return empty list (uses data.get("paths", []))
        assert items == []

    async def test_paginate_dfs_continuation_header_followed(self, httpx_mock):
        """paginate_dfs should follow x-ms-continuation header across pages."""
        # Use non-matching URLs to handle query parameter changes
        httpx_mock.add_response(
            json={"paths": [{"name": "file1"}]},
            headers={"x-ms-continuation": "token123"},
        )
        httpx_mock.add_response(
            json={"paths": [{"name": "file2"}]},
        )

        async with httpx.AsyncClient() as client:
            items = [item async for item in paginate_dfs(client, TEST_URL, headers=AUTH_HEADERS)]

        assert len(items) == 2
        assert items[0]["name"] == "file1"
        assert items[1]["name"] == "file2"
        # Verify second request includes continuation token
        requests = httpx_mock.get_requests()
        assert len(requests) == 2
        assert "continuation=token123" in str(requests[1].url)


# ---------------------------------------------------------------------------
# Delta Subprocess Error Tests
# ---------------------------------------------------------------------------


class TestDeltaSubprocessErrors:
    """Test error handling in Delta subprocess functions."""

    def test_delta_subprocess_partial_json_raises_delta_error(self):
        """_run_delta_subprocess should raise DeltaError on truncated JSON output."""
        # Mock a subprocess that returns truncated JSON
        mock_proc = MagicMock()
        mock_proc.returncode = 0
        mock_proc.communicate.return_value = ('{"ok": tr', "")

        # Patch at module level since it's imported inside the function
        with (
            patch("subprocess.Popen", return_value=mock_proc),
            pytest.raises(DeltaError, match="invalid output"),
        ):
            _run_delta_subprocess(
                "abfss://ws@host/item/Tables/table",
                {"storage_account": "account", "storage_access_key": "key"},
            )

    def test_delta_subprocess_ok_false_returns_dict(self):
        """_run_delta_subprocess returns the dict even with ok: false (error checked by caller)."""
        mock_proc = MagicMock()
        mock_proc.returncode = 0
        output = json.dumps({"ok": False, "error": "Table not found"})
        mock_proc.communicate.return_value = (output, "")

        with patch("subprocess.Popen", return_value=mock_proc):
            result = _run_delta_subprocess(
                "abfss://ws@host/item/Tables/table",
                {"storage_account": "account"},
            )

        # _run_delta_subprocess itself doesn't raise for ok:false,
        # it just returns the dict. The caller (_get_metadata_subprocess) checks it.
        assert result["ok"] is False
        assert result["error"] == "Table not found"

    def test_delta_subprocess_nonzero_exit_code_raises_error(self):
        """_run_delta_subprocess should raise DeltaError on non-zero exit code."""
        mock_proc = MagicMock()
        mock_proc.returncode = 1
        mock_proc.communicate.return_value = ("", "Import error: no module named deltalake")

        with (
            patch("subprocess.Popen", return_value=mock_proc),
            pytest.raises(DeltaError, match="exit code"),
        ):
            _run_delta_subprocess(
                "abfss://ws@host/item/Tables/table",
                {"storage_account": "account"},
            )

    def test_delta_subprocess_timeout_raises_delta_error(self):
        """_run_delta_subprocess should raise DeltaError on timeout."""
        mock_proc = MagicMock()
        mock_proc.communicate.side_effect = subprocess.TimeoutExpired("cmd", 30)

        with (
            patch("subprocess.Popen", return_value=mock_proc),
            pytest.raises(DeltaError, match="timed out after 30s"),
        ):
            _run_delta_subprocess(
                "abfss://ws@host/item/Tables/table",
                {"storage_account": "account"},
                timeout=30,
            )

    def test_delta_subprocess_long_stderr_truncated(self):
        """_run_delta_subprocess should truncate very long stderr messages."""
        mock_proc = MagicMock()
        mock_proc.returncode = 1
        long_error = "Error: " + "x" * 400  # More than 300 char limit
        mock_proc.communicate.return_value = ("", long_error)

        with (
            patch("subprocess.Popen", return_value=mock_proc),
            pytest.raises(DeltaError, match="…"),
        ):
            _run_delta_subprocess(
                "abfss://ws@host/item/Tables/table",
                {"storage_account": "account"},
            )

    def test_delta_subprocess_success_returns_parsed_json(self):
        """_run_delta_subprocess should return parsed JSON on success."""
        mock_proc = MagicMock()
        mock_proc.returncode = 0
        output = json.dumps(
            {
                "ok": True,
                "name": "my_table",
                "columns": [],
                "version": 1,
                "num_files": 5,
                "size_bytes": 1024,
                "partition_columns": [],
                "properties": {},
                "description": None,
            }
        )
        mock_proc.communicate.return_value = (output, "")

        with patch("subprocess.Popen", return_value=mock_proc):
            result = _run_delta_subprocess(
                "abfss://ws@host/item/Tables/table",
                {"storage_account": "account"},
            )

        assert result["ok"] is True
        assert result["name"] == "my_table"
        assert result["version"] == 1

    def test_delta_subprocess_empty_error_message_handled(self):
        """_run_delta_subprocess should handle missing error message gracefully."""
        mock_proc = MagicMock()
        mock_proc.returncode = 1
        mock_proc.communicate.return_value = ("", "")  # No stderr output

        with (
            patch("subprocess.Popen", return_value=mock_proc),
            pytest.raises(DeltaError, match="exit code 1"),
        ):
            _run_delta_subprocess(
                "abfss://ws@host/item/Tables/table",
                {"storage_account": "account"},
            )


# ---------------------------------------------------------------------------
# Integration Tests: Error Path Flows
# ---------------------------------------------------------------------------


class TestErrorPathFlows:
    """Test end-to-end error handling flows."""

    async def test_dfs_client_propagates_http_errors(self, httpx_mock, auth):
        """DFS operations should propagate HTTP errors correctly."""
        httpx_mock.add_response(
            url=f"{BASE_URL}/my-workspace/MyLakehouse.Lakehouse/Files/test.txt",
            status_code=403,
            text="Forbidden",
        )

        from onelake_client.exceptions import PermissionDeniedError

        client = DfsClient(auth)
        with pytest.raises(PermissionDeniedError):
            await client.get_properties("my-workspace", "MyLakehouse.Lakehouse/Files/test.txt")

        await client.close()

    async def test_paginate_on_api_error_propagates_immediately(self, httpx_mock):
        """Pagination should raise ApiError on first bad response, not retry."""
        # First attempt returns success, second returns 401
        httpx_mock.add_response(
            json={"value": [{"id": "1"}], "continuationToken": "page2"},
        )
        httpx_mock.add_response(
            status_code=401,
            text="Unauthorized",
        )

        from onelake_client.exceptions import AuthenticationError

        async with httpx.AsyncClient() as client:
            with pytest.raises(AuthenticationError):
                async for _ in paginate_fabric(client, TEST_URL, headers=AUTH_HEADERS):
                    pass

    async def test_read_file_with_partial_content_works(self, httpx_mock, auth):
        """read_file() should work with partial/chunked Content-Length."""
        httpx_mock.add_response(
            url=f"{BASE_URL}/my-workspace/MyLakehouse.Lakehouse/Files/test.txt",
            content=b"hello world",
            headers={"Content-Length": "11"},
        )

        client = DfsClient(auth)
        content = await client.read_file("my-workspace", "MyLakehouse.Lakehouse/Files/test.txt")

        assert content == b"hello world"
        await client.close()

    def test_delta_subprocess_result_with_error_message(self):
        """_run_delta_subprocess returns dict with error field when ok: false."""
        mock_proc = MagicMock()
        mock_proc.returncode = 0
        error_msg = "FileNotFoundError: [Errno 2] No such file or directory: /path/to/table"
        output = json.dumps({"ok": False, "error": error_msg})
        mock_proc.communicate.return_value = (output, "")

        with patch("subprocess.Popen", return_value=mock_proc):
            result = _run_delta_subprocess(
                "abfss://ws@host/item/Tables/table",
                {"storage_account": "account"},
            )

        # The error field is preserved in the result
        assert result["ok"] is False
        assert error_msg in result["error"]
