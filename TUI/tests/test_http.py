"""Tests for onelake_client._http module.

Covers retry logic, exception mapping, pagination, and client factory.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from onelake_client._http import (
    _INITIAL_BACKOFF,
    _MAX_RETRIES,
    _USER_AGENT,
    create_client,
    paginate_dfs,
    paginate_fabric,
    raise_for_status,
    request_with_retry,
)
from onelake_client.exceptions import (
    ApiError,
    AuthenticationError,
    NotFoundError,
    OneLakeError,
    PermissionDeniedError,
    RateLimitError,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TEST_URL = "https://api.fabric.microsoft.com/v1/test"
AUTH_HEADERS = {"Authorization": "Bearer x"}


# ---------------------------------------------------------------------------
# create_client
# ---------------------------------------------------------------------------


class TestCreateClient:
    async def test_default_headers(self):
        client = create_client()
        assert client.headers["user-agent"] == _USER_AGENT
        await client.aclose()

    async def test_follow_redirects(self):
        client = create_client()
        assert client.follow_redirects is True
        await client.aclose()

    async def test_custom_base_url(self):
        client = create_client(base_url="https://example.com")
        assert str(client.base_url) == "https://example.com"
        await client.aclose()

    async def test_kwargs_override_defaults(self):
        client = create_client(follow_redirects=False)
        assert client.follow_redirects is False
        await client.aclose()


# ---------------------------------------------------------------------------
# raise_for_status
# ---------------------------------------------------------------------------


class TestRaiseForStatus:
    def _response(self, status_code: int, body: str = "", url: str = TEST_URL, headers=None):
        return httpx.Response(
            status_code,
            text=body,
            request=httpx.Request("GET", url),
            headers=headers or {},
        )

    def test_200_no_error(self):
        raise_for_status(self._response(200))

    def test_204_no_error(self):
        raise_for_status(self._response(204))

    def test_401_raises_authentication_error(self):
        with pytest.raises(AuthenticationError, match="Authentication failed"):
            raise_for_status(self._response(401, body="Unauthorized"))

    def test_403_raises_permission_denied_error(self):
        with pytest.raises(PermissionDeniedError, match="Permission denied"):
            raise_for_status(self._response(403, body="Forbidden"))

    def test_404_raises_not_found_error(self):
        with pytest.raises(NotFoundError, match="Not found"):
            raise_for_status(self._response(404))

    def test_404_includes_url_as_resource(self):
        with pytest.raises(NotFoundError) as exc_info:
            raise_for_status(self._response(404, url="https://api.example.com/thing"))
        assert exc_info.value.resource == "https://api.example.com/thing"

    def test_429_raises_rate_limit_error(self):
        with pytest.raises(RateLimitError):
            raise_for_status(self._response(429))

    def test_429_respects_retry_after_header(self):
        resp = self._response(429, headers={"Retry-After": "42"})
        with pytest.raises(RateLimitError) as exc_info:
            raise_for_status(resp)
        assert exc_info.value.retry_after == 42.0

    def test_429_default_retry_after_without_header(self):
        with pytest.raises(RateLimitError) as exc_info:
            raise_for_status(self._response(429))
        assert exc_info.value.retry_after == 60.0

    def test_500_raises_api_error(self):
        with pytest.raises(ApiError) as exc_info:
            raise_for_status(self._response(500, body="Internal Server Error"))
        assert exc_info.value.status_code == 500
        assert exc_info.value.body == "Internal Server Error"

    def test_502_raises_api_error(self):
        with pytest.raises(ApiError) as exc_info:
            raise_for_status(self._response(502, body="Bad Gateway"))
        assert exc_info.value.status_code == 502

    def test_400_raises_api_error(self):
        with pytest.raises(ApiError) as exc_info:
            raise_for_status(self._response(400, body="Bad Request"))
        assert exc_info.value.status_code == 400

    def test_error_message_includes_status_and_body(self):
        with pytest.raises(ApiError, match="418.*teapot"):
            raise_for_status(self._response(418, body="teapot"))


# ---------------------------------------------------------------------------
# request_with_retry
# ---------------------------------------------------------------------------


class TestRequestWithRetry:
    async def test_success_on_first_attempt(self, httpx_mock):
        httpx_mock.add_response(url=TEST_URL, json={"ok": True})
        async with httpx.AsyncClient() as client:
            resp = await request_with_retry(client, "GET", TEST_URL)
        assert resp.status_code == 200

    async def test_429_retries_with_retry_after(self, httpx_mock):
        httpx_mock.add_response(
            url=TEST_URL,
            status_code=429,
            headers={"Retry-After": "0.01"},
        )
        httpx_mock.add_response(url=TEST_URL, json={"ok": True})

        with patch("onelake_client._http.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            async with httpx.AsyncClient() as client:
                resp = await request_with_retry(client, "GET", TEST_URL)

        assert resp.status_code == 200
        mock_sleep.assert_awaited_once()
        # Should use the Retry-After value (0.01), not the default backoff
        assert mock_sleep.await_args[0][0] == pytest.approx(0.01)

    @pytest.mark.parametrize("status", [500, 502, 503, 504])
    async def test_5xx_triggers_retry(self, httpx_mock, status):
        httpx_mock.add_response(url=TEST_URL, status_code=status, text="error")
        httpx_mock.add_response(url=TEST_URL, json={"ok": True})

        with patch("onelake_client._http.asyncio.sleep", new_callable=AsyncMock):
            async with httpx.AsyncClient() as client:
                resp = await request_with_retry(client, "GET", TEST_URL)
        assert resp.status_code == 200

    @pytest.mark.parametrize("status", [400, 401, 403, 404])
    async def test_4xx_non_retryable_fails_immediately(self, httpx_mock, status):
        httpx_mock.add_response(url=TEST_URL, status_code=status, text="nope")
        async with httpx.AsyncClient() as client:
            with pytest.raises(OneLakeError):
                await request_with_retry(client, "GET", TEST_URL)
        # Only one request should have been made (no retry)
        assert len(httpx_mock.get_requests()) == 1

    async def test_401_raises_authentication_error_no_retry(self, httpx_mock):
        httpx_mock.add_response(url=TEST_URL, status_code=401, text="bad token")
        async with httpx.AsyncClient() as client:
            with pytest.raises(AuthenticationError):
                await request_with_retry(client, "GET", TEST_URL)

    async def test_404_raises_not_found_error_no_retry(self, httpx_mock):
        httpx_mock.add_response(url=TEST_URL, status_code=404, text="gone")
        async with httpx.AsyncClient() as client:
            with pytest.raises(NotFoundError):
                await request_with_retry(client, "GET", TEST_URL)

    async def test_exponential_backoff_timing(self, httpx_mock):
        """Verify sleep durations increase across retries."""
        for _ in range(3):
            httpx_mock.add_response(url=TEST_URL, status_code=500, text="fail")
        httpx_mock.add_response(url=TEST_URL, json={"ok": True})

        sleep_times: list[float] = []

        async def fake_sleep(t):
            sleep_times.append(t)

        # Pin random to 0.5 so jitter multiplier is exactly 1.0 → pure doubling
        with (
            patch("onelake_client._http.asyncio.sleep", side_effect=fake_sleep),
            patch("onelake_client._http.random.random", return_value=0.5),
        ):
            async with httpx.AsyncClient() as client:
                resp = await request_with_retry(client, "GET", TEST_URL, max_retries=3)

        assert resp.status_code == 200
        assert len(sleep_times) == 3
        # With random()=0.5 the jitter factor is (0.5 + 0.5) = 1.0,
        # so backoff stays: 1.0 → 2.0 → 4.0 (default uses _INITIAL_BACKOFF=1.0)
        assert sleep_times[0] == pytest.approx(_INITIAL_BACKOFF)
        for i in range(1, len(sleep_times)):
            assert sleep_times[i] > sleep_times[i - 1]

    async def test_jitter_varies_backoff(self, httpx_mock):
        """Ensure jitter causes non-deterministic backoff values."""
        for _ in range(2):
            httpx_mock.add_response(url=TEST_URL, status_code=500, text="fail")
        httpx_mock.add_response(url=TEST_URL, json={"ok": True})

        sleep_times: list[float] = []

        async def fake_sleep(t):
            sleep_times.append(t)

        # Use real random — backoff * (0.5 + random()) won't be exact powers of 2
        with patch("onelake_client._http.asyncio.sleep", side_effect=fake_sleep):
            async with httpx.AsyncClient() as client:
                await request_with_retry(client, "GET", TEST_URL, max_retries=3)

        # The first retry uses the default backoff (response has no Retry-After),
        # but subsequent retries have jittered backoff.
        # The second sleep should NOT be exactly 2× the first, due to jitter.
        assert len(sleep_times) == 2
        ratio = sleep_times[1] / sleep_times[0]
        # With jitter, ratio should be between ~1.0 and ~4.0 (not exactly 2.0)
        assert ratio != pytest.approx(2.0, abs=0.001)

    async def test_max_retries_exhaustion(self, httpx_mock):
        """After max retries, the final error should propagate."""
        for _ in range(_MAX_RETRIES + 1):
            httpx_mock.add_response(url=TEST_URL, status_code=500, text="persistent failure")

        with patch("onelake_client._http.asyncio.sleep", new_callable=AsyncMock):
            async with httpx.AsyncClient() as client:
                with pytest.raises(ApiError) as exc_info:
                    await request_with_retry(client, "GET", TEST_URL)

        assert exc_info.value.status_code == 500
        # All attempts used: initial + max_retries
        assert len(httpx_mock.get_requests()) == _MAX_RETRIES + 1

    async def test_transport_error_triggers_retry(self, httpx_mock):
        httpx_mock.add_exception(httpx.ConnectError("connection refused"), url=TEST_URL)
        httpx_mock.add_response(url=TEST_URL, json={"ok": True})

        with patch("onelake_client._http.asyncio.sleep", new_callable=AsyncMock):
            async with httpx.AsyncClient() as client:
                resp = await request_with_retry(client, "GET", TEST_URL)
        assert resp.status_code == 200

    async def test_transport_error_exhaustion_raises_api_error(self, httpx_mock):
        for _ in range(_MAX_RETRIES + 1):
            httpx_mock.add_exception(httpx.ReadTimeout("timeout"), url=TEST_URL)

        with patch("onelake_client._http.asyncio.sleep", new_callable=AsyncMock):
            async with httpx.AsyncClient() as client:
                with pytest.raises(ApiError, match="Transport error after"):
                    await request_with_retry(client, "GET", TEST_URL)

    async def test_custom_max_retries(self, httpx_mock):
        httpx_mock.add_response(url=TEST_URL, status_code=503, text="unavailable")
        httpx_mock.add_response(url=TEST_URL, json={"ok": True})

        with patch("onelake_client._http.asyncio.sleep", new_callable=AsyncMock):
            async with httpx.AsyncClient() as client:
                resp = await request_with_retry(client, "GET", TEST_URL, max_retries=1)
        assert resp.status_code == 200

    async def test_zero_retries_fails_on_first_error(self, httpx_mock):
        httpx_mock.add_response(url=TEST_URL, status_code=500, text="fail")

        async with httpx.AsyncClient() as client:
            with pytest.raises(ApiError):
                await request_with_retry(client, "GET", TEST_URL, max_retries=0)
        assert len(httpx_mock.get_requests()) == 1


# ---------------------------------------------------------------------------
# paginate_fabric
# ---------------------------------------------------------------------------


class TestPaginateFabric:
    """Pagination tests omit url= from mocks so responses match in order,
    regardless of query-string changes across pages."""

    async def test_single_page(self, httpx_mock):
        httpx_mock.add_response(json={"value": [{"id": "1"}, {"id": "2"}]})
        async with httpx.AsyncClient() as client:
            items = [item async for item in paginate_fabric(client, TEST_URL, headers=AUTH_HEADERS)]
        assert items == [{"id": "1"}, {"id": "2"}]

    async def test_follows_continuation_token(self, httpx_mock):
        httpx_mock.add_response(json={"value": [{"id": "1"}], "continuationToken": "page2"})
        httpx_mock.add_response(json={"value": [{"id": "2"}]})
        async with httpx.AsyncClient() as client:
            items = [item async for item in paginate_fabric(client, TEST_URL, headers=AUTH_HEADERS)]
        assert items == [{"id": "1"}, {"id": "2"}]
        requests = httpx_mock.get_requests()
        assert len(requests) == 2
        assert "continuationToken=page2" in str(requests[1].url)

    async def test_three_pages(self, httpx_mock):
        httpx_mock.add_response(json={"value": [{"id": "a"}], "continuationToken": "tok1"})
        httpx_mock.add_response(json={"value": [{"id": "b"}], "continuationToken": "tok2"})
        httpx_mock.add_response(json={"value": [{"id": "c"}]})
        async with httpx.AsyncClient() as client:
            items = [item async for item in paginate_fabric(client, TEST_URL, headers=AUTH_HEADERS)]
        assert [i["id"] for i in items] == ["a", "b", "c"]
        assert len(httpx_mock.get_requests()) == 3

    async def test_max_items_stops_early(self, httpx_mock):
        httpx_mock.add_response(
            json={"value": [{"id": "1"}, {"id": "2"}, {"id": "3"}], "continuationToken": "more"},
        )
        async with httpx.AsyncClient() as client:
            items = [
                item
                async for item in paginate_fabric(
                    client, TEST_URL, headers=AUTH_HEADERS, max_items=2
                )
            ]
        assert len(items) == 2
        assert items == [{"id": "1"}, {"id": "2"}]
        # Should not have fetched the second page
        assert len(httpx_mock.get_requests()) == 1

    async def test_max_items_across_pages(self, httpx_mock):
        httpx_mock.add_response(json={"value": [{"id": "1"}], "continuationToken": "tok"})
        httpx_mock.add_response(
            json={"value": [{"id": "2"}, {"id": "3"}], "continuationToken": "tok2"},
        )
        async with httpx.AsyncClient() as client:
            items = [
                item
                async for item in paginate_fabric(
                    client, TEST_URL, headers=AUTH_HEADERS, max_items=2
                )
            ]
        assert len(items) == 2
        assert items == [{"id": "1"}, {"id": "2"}]

    async def test_empty_response(self, httpx_mock):
        httpx_mock.add_response(json={"value": []})
        async with httpx.AsyncClient() as client:
            items = [item async for item in paginate_fabric(client, TEST_URL, headers=AUTH_HEADERS)]
        assert items == []

    async def test_missing_value_key(self, httpx_mock):
        httpx_mock.add_response(json={"data": "something"})
        async with httpx.AsyncClient() as client:
            items = [item async for item in paginate_fabric(client, TEST_URL, headers=AUTH_HEADERS)]
        assert items == []

    async def test_custom_value_key(self, httpx_mock):
        httpx_mock.add_response(json={"items": [{"id": "1"}]})
        async with httpx.AsyncClient() as client:
            items = [
                item
                async for item in paginate_fabric(
                    client, TEST_URL, headers=AUTH_HEADERS, value_key="items"
                )
            ]
        assert items == [{"id": "1"}]

    async def test_json_decode_error_raises_api_error(self, httpx_mock):
        httpx_mock.add_response(text="not json at all", status_code=200)
        async with httpx.AsyncClient() as client:
            with pytest.raises(ApiError, match="Malformed JSON"):
                async for _ in paginate_fabric(client, TEST_URL, headers=AUTH_HEADERS):
                    pass


# ---------------------------------------------------------------------------
# paginate_dfs
# ---------------------------------------------------------------------------


class TestPaginateDfs:
    async def test_single_page(self, httpx_mock):
        httpx_mock.add_response(json={"paths": [{"name": "a"}, {"name": "b"}]})
        async with httpx.AsyncClient() as client:
            items = [item async for item in paginate_dfs(client, TEST_URL, headers=AUTH_HEADERS)]
        assert items == [{"name": "a"}, {"name": "b"}]

    async def test_follows_continuation_header(self, httpx_mock):
        httpx_mock.add_response(
            json={"paths": [{"name": "a"}]},
            headers={"x-ms-continuation": "page2token"},
        )
        httpx_mock.add_response(json={"paths": [{"name": "b"}]})
        async with httpx.AsyncClient() as client:
            items = [item async for item in paginate_dfs(client, TEST_URL, headers=AUTH_HEADERS)]
        assert items == [{"name": "a"}, {"name": "b"}]
        requests = httpx_mock.get_requests()
        assert len(requests) == 2
        assert "continuation=page2token" in str(requests[1].url)

    async def test_three_pages(self, httpx_mock):
        httpx_mock.add_response(
            json={"paths": [{"name": "x"}]},
            headers={"x-ms-continuation": "t1"},
        )
        httpx_mock.add_response(
            json={"paths": [{"name": "y"}]},
            headers={"x-ms-continuation": "t2"},
        )
        httpx_mock.add_response(json={"paths": [{"name": "z"}]})
        async with httpx.AsyncClient() as client:
            items = [item async for item in paginate_dfs(client, TEST_URL, headers=AUTH_HEADERS)]
        assert [i["name"] for i in items] == ["x", "y", "z"]

    async def test_max_items_stops_early(self, httpx_mock):
        httpx_mock.add_response(
            json={"paths": [{"name": "a"}, {"name": "b"}, {"name": "c"}]},
            headers={"x-ms-continuation": "more"},
        )
        async with httpx.AsyncClient() as client:
            items = [
                item
                async for item in paginate_dfs(client, TEST_URL, headers=AUTH_HEADERS, max_items=2)
            ]
        assert len(items) == 2
        assert items == [{"name": "a"}, {"name": "b"}]
        assert len(httpx_mock.get_requests()) == 1

    async def test_max_items_across_pages(self, httpx_mock):
        httpx_mock.add_response(
            json={"paths": [{"name": "a"}]},
            headers={"x-ms-continuation": "t"},
        )
        httpx_mock.add_response(
            json={"paths": [{"name": "b"}, {"name": "c"}]},
            headers={"x-ms-continuation": "t2"},
        )
        async with httpx.AsyncClient() as client:
            items = [
                item
                async for item in paginate_dfs(client, TEST_URL, headers=AUTH_HEADERS, max_items=2)
            ]
        assert len(items) == 2

    async def test_empty_response(self, httpx_mock):
        httpx_mock.add_response(json={"paths": []})
        async with httpx.AsyncClient() as client:
            items = [item async for item in paginate_dfs(client, TEST_URL, headers=AUTH_HEADERS)]
        assert items == []

    async def test_json_decode_error_raises_api_error(self, httpx_mock):
        httpx_mock.add_response(text="<html>bad</html>", status_code=200)
        async with httpx.AsyncClient() as client:
            with pytest.raises(ApiError, match="Malformed JSON"):
                async for _ in paginate_dfs(client, TEST_URL, headers=AUTH_HEADERS):
                    pass
