from __future__ import annotations

import asyncio
import json
import logging
import random
from collections.abc import AsyncIterator
from typing import Any

import httpx

from onelake_client.exceptions import (
    ApiError,
    AuthenticationError,
    NotFoundError,
    PermissionDeniedError,
    RateLimitError,
)

logger = logging.getLogger("onelake_client")

_USER_AGENT = "onelake-client/0.1.0"
_DEFAULT_TIMEOUT = httpx.Timeout(30.0, connect=10.0)
_MAX_RETRIES = 3
_RETRY_STATUSES = {429, 500, 502, 503, 504}
_INITIAL_BACKOFF = 1.0  # seconds


def create_client(**kwargs: Any) -> httpx.AsyncClient:
    """Create a configured httpx.AsyncClient."""
    defaults = {
        "timeout": _DEFAULT_TIMEOUT,
        "headers": {"User-Agent": _USER_AGENT},
        "follow_redirects": True,
    }
    defaults.update(kwargs)
    return httpx.AsyncClient(**defaults)


async def request_with_retry(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    max_retries: int = _MAX_RETRIES,
    **kwargs: Any,
) -> httpx.Response:
    """Make an HTTP request with automatic retry on transient errors.

    Retries on 429 (rate limit) and 5xx errors with exponential backoff.
    Respects Retry-After header for 429 responses.
    """
    last_exc: Exception | None = None
    backoff = _INITIAL_BACKOFF

    for attempt in range(max_retries + 1):
        try:
            response = await client.request(method, url, headers=headers, params=params, **kwargs)

            if response.status_code not in _RETRY_STATUSES:
                raise_for_status(response)
                return response

            # Retryable status — back off and try again
            if attempt < max_retries:
                wait = _get_retry_wait(response, backoff)
                logger.warning(
                    "Retryable %d from %s %s (attempt %d/%d, waiting %.1fs)",
                    response.status_code,
                    method,
                    url,
                    attempt + 1,
                    max_retries + 1,
                    wait,
                )
                await asyncio.sleep(wait)
                backoff *= 2
                backoff *= 0.5 + random.random()
                continue

            # Last attempt — raise the error
            raise_for_status(response)
            return response  # unreachable but keeps type checker happy

        except httpx.TransportError as exc:
            last_exc = exc
            if attempt < max_retries:
                logger.warning(
                    "Transport error on %s %s (attempt %d/%d): %s",
                    method,
                    url,
                    attempt + 1,
                    max_retries + 1,
                    exc,
                )
                await asyncio.sleep(backoff)
                backoff *= 2
                backoff *= 0.5 + random.random()
                continue
            msg = f"Transport error after {max_retries + 1} attempts: {exc}"
            raise ApiError(0, message=msg) from exc

    # Should never reach here, but just in case
    raise ApiError(0, message=f"Request failed after {max_retries + 1} attempts") from last_exc


def raise_for_status(response: httpx.Response) -> None:
    """Map HTTP error responses to typed exceptions."""
    status = response.status_code

    if 200 <= status < 300:
        return

    body = response.text

    if status == 401:
        raise AuthenticationError(f"Authentication failed: {body}")
    if status == 403:
        raise PermissionDeniedError(f"Permission denied: {body}")
    if status == 404:
        raise NotFoundError(resource=str(response.url), message=f"Not found: {response.url}")
    if status == 429:
        retry_after = _get_retry_wait(response, default=60.0)
        raise RateLimitError(retry_after=retry_after)

    raise ApiError(status_code=status, body=body)


def _get_retry_wait(response: httpx.Response, default: float = 1.0) -> float:
    """Extract Retry-After wait time from response headers."""
    retry_after = response.headers.get("Retry-After")
    if retry_after is not None:
        try:
            return float(retry_after)
        except ValueError:
            pass
    return default


async def paginate_fabric(
    client: httpx.AsyncClient,
    url: str,
    *,
    headers: dict[str, str],
    params: dict[str, Any] | None = None,
    value_key: str = "value",
    max_items: int | None = None,
) -> AsyncIterator[dict[str, Any]]:
    """Paginate through Fabric REST API responses.

    Fabric REST API uses `continuationToken` in the response body
    and accepts it as a query parameter for the next page.

    Args:
        max_items: Stop after yielding this many items. None means unlimited.
    """
    params = dict(params or {})
    count = 0

    while True:
        response = await request_with_retry(client, "GET", url, headers=headers, params=params)
        try:
            data = response.json()
        except json.JSONDecodeError as e:
            raise ApiError(
                status_code=response.status_code,
                message=f"Malformed JSON in API response: {e}",
            ) from e

        for item in data.get(value_key, []):
            yield item
            count += 1
            if max_items is not None and count >= max_items:
                return

        continuation = data.get("continuationToken")
        if not continuation:
            break
        params["continuationToken"] = continuation


async def paginate_dfs(
    client: httpx.AsyncClient,
    url: str,
    *,
    headers: dict[str, str],
    params: dict[str, Any] | None = None,
    max_items: int | None = None,
) -> AsyncIterator[dict[str, Any]]:
    """Paginate through DFS (ADLS Gen2) list responses.

    DFS API returns a `continuation` header for the next page
    and uses `continuation` as a query parameter.
    The response body contains a `paths` array.

    Args:
        max_items: Stop after yielding this many items. None means unlimited.
    """
    params = dict(params or {})
    count = 0

    while True:
        response = await request_with_retry(client, "GET", url, headers=headers, params=params)
        try:
            data = response.json()
        except json.JSONDecodeError as e:
            raise ApiError(
                status_code=response.status_code,
                message=f"Malformed JSON in API response: {e}",
            ) from e

        for item in data.get("paths", []):
            yield item
            count += 1
            if max_items is not None and count >= max_items:
                return

        continuation = response.headers.get("x-ms-continuation")
        if not continuation:
            break
        params["continuation"] = continuation
