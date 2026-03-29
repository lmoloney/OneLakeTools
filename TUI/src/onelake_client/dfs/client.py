from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

import httpx

from onelake_client._http import create_client, paginate_dfs, request_with_retry
from onelake_client.exceptions import (
    ApiError,
    AuthenticationError,
    FileTooLargeError,
    NotFoundError,
)
from onelake_client.models import FileProperties, PathInfo

if TYPE_CHECKING:
    from onelake_client.auth import OneLakeAuth
    from onelake_client.environment import FabricEnvironment

logger = logging.getLogger(__name__)

_API_VERSION = "2021-06-08"


def _dfs_headers(auth_headers: dict[str, str]) -> dict[str, str]:
    """Merge auth headers with required DFS API version header."""
    return {**auth_headers, "x-ms-version": _API_VERSION}


def _parse_path_info(raw: dict[str, Any]) -> PathInfo:
    """Parse DFS list paths response into PathInfo.

    DFS API returns isDirectory and contentLength as strings, and
    lastModified as an RFC 1123 date (e.g. "Tue, 17 Mar 2026 22:55:30 GMT").
    """
    last_modified = None
    lm_raw = raw.get("lastModified")
    if lm_raw:
        try:
            from email.utils import parsedate_to_datetime

            last_modified = parsedate_to_datetime(lm_raw)
        except (ValueError, TypeError):
            logger.debug("Failed to parse datetime value: %r", lm_raw)

    return PathInfo(
        name=raw.get("name", ""),
        isDirectory=raw.get("isDirectory", "false").lower() == "true",
        contentLength=int(raw.get("contentLength", 0) or 0),
        lastModified=last_modified,
        etag=raw.get("etag"),
        owner=raw.get("owner"),
        group=raw.get("group"),
        permissions=raw.get("permissions"),
    )


def _parse_file_properties(response: httpx.Response) -> FileProperties:
    """Parse HEAD response headers into FileProperties."""
    headers = response.headers
    last_modified_str = headers.get("Last-Modified")
    last_modified = None
    if last_modified_str:
        try:
            from email.utils import parsedate_to_datetime

            last_modified = parsedate_to_datetime(last_modified_str)
        except (ValueError, TypeError):
            pass

    return FileProperties(
        contentLength=int(headers.get("Content-Length", 0)),
        contentType=headers.get("Content-Type"),
        lastModified=last_modified,
        etag=headers.get("ETag"),
        resourceType=headers.get("x-ms-resource-type"),
    )


class DfsClient:
    """Client for the OneLake DFS API (data plane).

    ADLS Gen2-compatible file and folder operations.

    Usage::

        auth = OneLakeAuth()
        dfs = DfsClient(auth)
        paths = await dfs.list_paths("MyWorkspace", "MyLakehouse.Lakehouse")
    """

    def __init__(
        self,
        auth: OneLakeAuth,
        *,
        env: FabricEnvironment | None = None,
        client: httpx.AsyncClient | None = None,
    ):
        self._auth = auth
        if env is None:
            from onelake_client.environment import DEFAULT_ENVIRONMENT

            env = DEFAULT_ENVIRONMENT
        self._dfs_host = env.dfs_host
        self._base_url = f"https://{self._dfs_host}"
        self._client = client
        self._owns_client = client is None
        self._client_lock = asyncio.Lock()

    @property
    def dfs_host(self) -> str:
        """The DFS hostname in use."""
        return self._dfs_host

    async def _get_client(self) -> httpx.AsyncClient:
        async with self._client_lock:
            if self._client is None:
                self._client = create_client(base_url=self._base_url)
                self._owns_client = True
            return self._client

    async def close(self) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def list_paths(
        self,
        workspace: str,
        item_path: str,
        *,
        directory: str = "",
        recursive: bool = False,
    ) -> list[PathInfo]:
        """List files and folders under a path in OneLake.

        Uses the workspace as the DFS filesystem and passes the item
        identifier (GUID or ``DisplayName.Type``) via the ``directory``
        query parameter.

        Args:
            workspace: Workspace name or GUID.
            item_path: Item identifier — ``DisplayName.Type`` or a bare GUID.
            directory: Subdirectory to list (relative to item root). Empty = root.
            recursive: If True, list all files recursively.

        Returns:
            List of PathInfo objects.
        """
        client = await self._get_client()
        headers = _dfs_headers(self._auth.dfs_headers())

        full_directory = f"{item_path}/{directory}" if directory else item_path

        params: dict[str, str] = {
            "resource": "filesystem",
            "recursive": str(recursive).lower(),
            "directory": full_directory,
        }

        url = f"{self._base_url}/{workspace}"
        paths: list[PathInfo] = []

        async for raw in paginate_dfs(client, url, headers=headers, params=params):
            paths.append(_parse_path_info(raw))

        return paths

    async def read_file(
        self, workspace: str, path: str, *, max_bytes: int | None = None
    ) -> bytes:
        """Read an entire file from OneLake.

        Args:
            workspace: Workspace name or GUID.
            path: Full path within the workspace
                  (e.g., "MyLakehouse.Lakehouse/Files/data.csv").
            max_bytes: Optional size limit. If the server reports a
                ``Content-Length`` exceeding this value, a
                :class:`~onelake_client.exceptions.FileTooLargeError` is
                raised *before* the body is read.

        Returns:
            File content as bytes.

        Raises:
            FileTooLargeError: If the file exceeds *max_bytes*.
        """
        client = await self._get_client()
        headers = _dfs_headers(self._auth.dfs_headers())

        response = await request_with_retry(
            client,
            "GET",
            f"{self._base_url}/{workspace}/{path}",
            headers=headers,
        )

        if max_bytes is not None:
            content_length = response.headers.get("Content-Length")
            if content_length is not None:
                size = int(content_length)
                if size > max_bytes:
                    raise FileTooLargeError(size=size, max_bytes=max_bytes)

        return response.content

    async def read_file_stream(
        self, workspace: str, path: str, *, chunk_size: int = 65536
    ) -> AsyncIterator[bytes]:
        """Stream a file from OneLake in chunks.

        Useful for large files — avoids loading everything into memory.

        Args:
            workspace: Workspace name or GUID.
            path: Full path within the workspace.
            chunk_size: Bytes per chunk (default 64 KB).

        Yields:
            Chunks of file content.
        """
        client = await self._get_client()
        headers = _dfs_headers(self._auth.dfs_headers())

        stream_timeout = httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0)
        async with client.stream(
            "GET",
            f"{self._base_url}/{workspace}/{path}",
            headers=headers,
            timeout=stream_timeout,
        ) as response:
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                body = exc.response.text
                if status == 404:
                    raise NotFoundError(
                        resource=f"{workspace}/{path}",
                        message=f"Not found: {workspace}/{path}",
                    ) from exc
                if status in (401, 403):
                    raise AuthenticationError(
                        f"Authentication/authorization failed ({status}): {body}"
                    ) from exc
                raise ApiError(status_code=status, body=body) from exc
            async for chunk in response.aiter_bytes(chunk_size):
                yield chunk

    async def get_properties(self, workspace: str, path: str) -> FileProperties:
        """Get properties of a file or directory.

        Args:
            workspace: Workspace name or GUID.
            path: Full path within the workspace.

        Returns:
            FileProperties with size, content type, last modified, etc.
        """
        client = await self._get_client()
        headers = _dfs_headers(self._auth.dfs_headers())

        response = await request_with_retry(
            client,
            "HEAD",
            f"{self._base_url}/{workspace}/{path}",
            headers=headers,
        )
        return _parse_file_properties(response)

    async def exists(self, workspace: str, path: str) -> bool:
        """Check if a file or directory exists.

        Args:
            workspace: Workspace name or GUID.
            path: Full path within the workspace.

        Returns:
            True if the path exists, False otherwise.
        """
        try:
            await self.get_properties(workspace, path)
        except NotFoundError:
            return False
        return True
