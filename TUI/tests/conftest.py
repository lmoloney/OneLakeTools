"""Shared test fixtures for onelake-client tests."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from onelake_client.auth import OneLakeAuth


class FakeCredential:
    """A fake TokenCredential that returns a static token."""

    def get_token(self, *scopes, **kwargs):
        return MagicMock(token="fake-token-12345", expires_on=9999999999.0)


@pytest.fixture
def fake_credential():
    return FakeCredential()


@pytest.fixture
def auth(fake_credential):
    return OneLakeAuth(credential=fake_credential)
