"""Tests for OneLakeAuth."""

import base64
import json
import time
from unittest.mock import MagicMock, patch

import pytest

from onelake_client.auth import OneLakeAuth, _CachedToken, _decode_jwt_claims, create_credential
from onelake_client.environment import PROD
from onelake_client.exceptions import AuthenticationError
from azure.core.exceptions import ClientAuthenticationError

from .conftest import FakeCredential


def _make_jwt(claims: dict) -> str:
    """Build a fake JWT with the given payload claims (no signature)."""
    header = base64.urlsafe_b64encode(b'{"alg":"none"}').rstrip(b"=").decode()
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).rstrip(b"=").decode()
    return f"{header}.{payload}.sig"


def test_get_token_returns_string():
    auth = OneLakeAuth(credential=FakeCredential())
    token = auth.get_token(PROD.fabric_scope)
    assert token == "fake-token-12345"


def test_fabric_headers():
    auth = OneLakeAuth(credential=FakeCredential())
    headers = auth.fabric_headers()
    assert headers["Authorization"] == "Bearer fake-token-12345"


def test_dfs_headers():
    auth = OneLakeAuth(credential=FakeCredential())
    headers = auth.dfs_headers()
    assert headers["Authorization"] == "Bearer fake-token-12345"


def test_storage_options():
    auth = OneLakeAuth(credential=FakeCredential())
    opts = auth.storage_options()
    assert opts["account_name"] == "onelake"
    assert opts["account_host"] == "onelake.dfs.fabric.microsoft.com"
    assert opts["azure_storage_token"] == "fake-token-12345"


def test_token_caching():
    """Token should be cached and not re-fetched on second call."""
    cred = FakeCredential()
    auth = OneLakeAuth(credential=cred)

    t1 = auth.get_token(PROD.fabric_scope)
    t2 = auth.get_token(PROD.fabric_scope)
    assert t1 == t2  # Same cached token


def test_different_scopes_get_separate_tokens():
    auth = OneLakeAuth(credential=FakeCredential())
    auth.get_token(PROD.fabric_scope)
    auth.get_token(PROD.storage_scope)
    assert PROD.fabric_scope in auth._token_cache
    assert PROD.storage_scope in auth._token_cache


# ── invalidate_token tests ──────────────────────────────────────────────


def test_invalidate_token_clears_specific_scope():
    auth = OneLakeAuth(credential=FakeCredential())
    auth.get_token(PROD.fabric_scope)
    auth.get_token(PROD.storage_scope)
    assert len(auth._token_cache) == 2

    auth.invalidate_token(PROD.fabric_scope)
    assert PROD.fabric_scope not in auth._token_cache
    assert PROD.storage_scope in auth._token_cache


def test_invalidate_token_clears_all():
    auth = OneLakeAuth(credential=FakeCredential())
    auth.get_token(PROD.fabric_scope)
    auth.get_token(PROD.storage_scope)
    auth.invalidate_token()
    assert len(auth._token_cache) == 0


def test_invalidate_token_nonexistent_scope_is_noop():
    auth = OneLakeAuth(credential=FakeCredential())
    auth.invalidate_token("nonexistent-scope")  # should not raise


# ── get_identity tests ──────────────────────────────────────────────────


class JwtCredential:
    """Credential that returns a JWT with configurable claims."""

    def __init__(self, claims: dict):
        self._token = _make_jwt(claims)

    def get_token(self, *scopes, **kwargs):
        return MagicMock(token=self._token, expires_on=9999999999.0)


def test_get_identity_preferred_username():
    cred = JwtCredential({"preferred_username": "luke@contoso.com", "oid": "abc"})
    auth = OneLakeAuth(credential=cred)
    assert auth.get_identity() == "luke@contoso.com"


def test_get_identity_upn_fallback():
    cred = JwtCredential({"upn": "luke@fabric.com"})
    auth = OneLakeAuth(credential=cred)
    assert auth.get_identity() == "luke@fabric.com"


def test_get_identity_name_fallback():
    cred = JwtCredential({"name": "Luke Moloney"})
    auth = OneLakeAuth(credential=cred)
    assert auth.get_identity() == "Luke Moloney"


def test_get_identity_oid_fallback():
    cred = JwtCredential({"oid": "guid-1234"})
    auth = OneLakeAuth(credential=cred)
    assert auth.get_identity() == "guid-1234"


def test_get_identity_unknown_when_no_claims():
    auth = OneLakeAuth(credential=FakeCredential())
    # FakeCredential returns "fake-token-12345" which is not a valid JWT
    assert auth.get_identity() == "unknown"


def test_get_identity_caches_result():
    cred = JwtCredential({"preferred_username": "cached@test.com"})
    auth = OneLakeAuth(credential=cred)
    result1 = auth.get_identity()
    result2 = auth.get_identity()
    assert result1 == result2 == "cached@test.com"


# ── _decode_jwt_claims tests ────────────────────────────────────────────


def test_decode_jwt_claims_valid():
    claims = {"sub": "user1", "oid": "abc"}
    token = _make_jwt(claims)
    decoded = _decode_jwt_claims(token)
    assert decoded["sub"] == "user1"
    assert decoded["oid"] == "abc"


def test_decode_jwt_claims_invalid_token():
    assert _decode_jwt_claims("not-a-jwt") == {}
    assert _decode_jwt_claims("") == {}


# ── create_credential tests ─────────────────────────────────────────────


def test_create_credential_unknown_kind():
    with pytest.raises(ValueError, match="Unknown credential kind"):
        create_credential("bogus")


# ── _CachedToken.is_expired tests ───────────────────────────────────────


def test_cached_token_not_expired():
    """Token expiring 10 minutes from now should not be expired."""
    expires_on = time.time() + 600
    token = _CachedToken(token="test-token", expires_on=expires_on)
    assert token.is_expired is False


def test_cached_token_expired():
    """Token that expired 1 minute ago should be expired."""
    expires_on = time.time() - 60
    token = _CachedToken(token="test-token", expires_on=expires_on)
    assert token.is_expired is True


def test_cached_token_within_refresh_buffer():
    """Token expiring in 200 seconds (within 300s buffer) should trigger early refresh."""
    expires_on = time.time() + 200
    token = _CachedToken(token="test-token", expires_on=expires_on)
    # Should be expired because 200 seconds < 300 second buffer
    assert token.is_expired is True


def test_cached_token_exactly_at_boundary():
    """Token expiring at exactly 300 seconds should be expired (>= comparison)."""
    # Use patch to make time.time() deterministic
    current_time = 1000.0
    expires_on = current_time + 300  # Exactly at buffer boundary

    with patch("onelake_client.auth.time.time", return_value=current_time):
        token = _CachedToken(token="test-token", expires_on=expires_on)
        # At boundary: time.time() >= (expires_on - 300) => 1000 >= 1000 => True
        assert token.is_expired is True


def test_get_token_refreshes_expired():
    """Expired token should be refreshed on next get_token call."""

    class MultiTokenCredential:
        """Credential that returns different tokens on successive calls."""

        def __init__(self):
            self.call_count = 0

        def get_token(self, *scopes, **kwargs):
            self.call_count += 1
            token = f"token-{self.call_count}"
            # Return far-future expiry on first call
            expires_on = time.time() + 10000 if self.call_count == 1 else time.time() + 10000
            return MagicMock(token=token, expires_on=expires_on)

    cred = MultiTokenCredential()
    auth = OneLakeAuth(credential=cred)

    # First call: credential gets token-1
    token1 = auth.get_token(PROD.fabric_scope)
    assert token1 == "token-1"
    assert cred.call_count == 1

    # Store the original expires_on
    cached_token = auth._token_cache[PROD.fabric_scope]
    original_expires_on = cached_token.expires_on

    # Patch time.time to simulate token expiry
    # Mock returns a time that is way past the token expiry
    with patch("onelake_client.auth.time.time") as mock_time:
        mock_time.return_value = original_expires_on + 1000  # Well past expiry

        token2 = auth.get_token(PROD.fabric_scope)
        # Second call should get a new token (credential called again)
        assert token2 == "token-2"
        assert cred.call_count == 2


def test_get_token_auth_failure_raises():
    """Credential auth failure should raise AuthenticationError."""

    class FailingCredential:
        def get_token(self, *scopes, **kwargs):
            raise ClientAuthenticationError("Authentication failed")

    cred = FailingCredential()
    auth = OneLakeAuth(credential=cred)

    with pytest.raises(AuthenticationError, match="Authentication failed"):
        auth.get_token(PROD.fabric_scope)
