"""Tests for OneLakeAuth."""

from onelake_client.auth import OneLakeAuth
from onelake_client.environment import PROD

from .conftest import FakeCredential


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
