from __future__ import annotations

import base64
import json
import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from azure.core.exceptions import ClientAuthenticationError

from onelake_client.exceptions import AuthenticationError

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential

    from onelake_client.environment import FabricEnvironment

logger = logging.getLogger(__name__)

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


def create_credential(kind: str = "default") -> TokenCredential:
    """Create a credential of the requested type.

    Args:
        kind: One of ``"default"``, ``"managed"``, ``"cli"``, ``"env"``.

    Returns:
        An Azure SDK ``TokenCredential`` instance.
    """
    from azure.identity import (
        AzureCliCredential,
        DefaultAzureCredential,
        EnvironmentCredential,
        ManagedIdentityCredential,
    )

    factories: dict[str, Callable[[], TokenCredential]] = {
        "default": DefaultAzureCredential,
        "managed": ManagedIdentityCredential,
        "cli": AzureCliCredential,
        "env": EnvironmentCredential,
    }
    factory = factories.get(kind)
    if factory is None:
        valid = ", ".join(sorted(factories))
        raise ValueError(f"Unknown credential kind {kind!r}. Valid: {valid}")
    return factory()


def _decode_jwt_claims(token: str) -> dict[str, Any]:
    """Decode the payload segment of a JWT without signature verification.

    Used only for extracting display-friendly identity claims (username, oid).
    """
    try:
        payload = token.split(".")[1]
        # Pad base64url to a multiple of 4
        padded = payload + "=" * (-len(payload) % 4)
        decoded = base64.urlsafe_b64decode(padded)
        return json.loads(decoded)
    except Exception:
        return {}


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
        self._identity: str | None = None

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

    def invalidate_token(self, scope: str | None = None) -> None:
        """Clear cached token(s) so the next request re-acquires.

        Args:
            scope: Specific scope to invalidate, or ``None`` to clear all.
        """
        if scope:
            self._token_cache.pop(scope, None)
            logger.debug("Invalidated cached token for scope %s", scope)
        else:
            self._token_cache.clear()
            logger.debug("Invalidated all cached tokens")

    def get_identity(self) -> str:
        """Return a human-readable identity string from the current token.

        Decodes JWT claims to extract ``preferred_username``, ``name``,
        or ``oid``. Falls back to ``"unknown"`` if no claims are available
        (e.g. managed identity tokens without standard claims).
        """
        if self._identity is not None:
            return self._identity

        try:
            token = self.get_token(self._env.fabric_scope)
        except AuthenticationError:
            return "not authenticated"

        claims = _decode_jwt_claims(token)
        identity = (
            claims.get("preferred_username")
            or claims.get("upn")
            or claims.get("name")
            or claims.get("oid", "unknown")
        )
        self._identity = identity
        logger.debug("Resolved identity: %s", identity)
        return identity

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
