from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from azure.core.exceptions import ClientAuthenticationError

from onelake_client.exceptions import AuthenticationError

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential

    from onelake_client.environment import FabricEnvironment

# Refresh tokens 5 minutes before expiry
_REFRESH_BUFFER_SECONDS = 300


@dataclass
class _CachedToken:
    """A cached OAuth2 access token with expiry tracking."""

    token: str
    expires_on: float  # epoch seconds

    @property
    def is_expired(self) -> bool:
        return time.time() >= (self.expires_on - _REFRESH_BUFFER_SECONDS)


class OneLakeAuth:
    """Manages authentication for OneLake API surfaces.

    Handles two token scopes:
    - Fabric REST API (api.fabric.microsoft.com)
    - OneLake DFS / Table APIs (storage.azure.com)

    Usage:
        from azure.identity import DefaultAzureCredential

        auth = OneLakeAuth()  # uses DefaultAzureCredential
        headers = auth.fabric_headers()
        headers = auth.dfs_headers()
        opts = auth.storage_options()  # for deltalake / pyiceberg
    """

    def __init__(
        self,
        credential: TokenCredential | None = None,
        *,
        env: FabricEnvironment | None = None,
    ):
        if credential is None:
            from azure.identity import DefaultAzureCredential

            credential = DefaultAzureCredential()
        self._credential = credential

        if env is None:
            from onelake_client.environment import DEFAULT_ENVIRONMENT

            env = DEFAULT_ENVIRONMENT
        self._env = env
        self._token_cache: dict[str, _CachedToken] = {}
        self._locks: dict[str, threading.Lock] = {}

    def get_token(self, scope: str) -> str:
        """Get a valid access token for the given scope, using cache."""
        lock = self._locks.setdefault(scope, threading.Lock())
        with lock:
            cached = self._token_cache.get(scope)
            if cached is not None and not cached.is_expired:
                return cached.token

            try:
                result = self._credential.get_token(scope)
            except ClientAuthenticationError as exc:
                raise AuthenticationError(str(exc)) from exc
            self._token_cache[scope] = _CachedToken(
                token=result.token,
                expires_on=result.expires_on,
            )
            return result.token

    def fabric_headers(self) -> dict[str, str]:
        """Authorization headers for Fabric REST API requests."""
        token = self.get_token(self._env.fabric_scope)
        return {"Authorization": f"Bearer {token}"}

    def dfs_headers(self) -> dict[str, str]:
        """Authorization headers for OneLake DFS and Table API requests."""
        token = self.get_token(self._env.storage_scope)
        return {"Authorization": f"Bearer {token}"}

    def storage_options(self) -> dict[str, Any]:
        """Storage options dict for deltalake / pyiceberg libraries."""
        token = self.get_token(self._env.storage_scope)
        return {
            "account_name": "onelake",
            "account_host": self._env.dfs_host,
            "azure_storage_token": token,
        }

    @property
    def env(self) -> FabricEnvironment:
        """The active Fabric environment configuration."""
        return self._env

    @property
    def credential(self) -> TokenCredential:
        """The underlying credential object."""
        return self._credential
