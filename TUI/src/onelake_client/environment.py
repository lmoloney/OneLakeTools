"""Fabric environment definitions — PROD, MSIT, DXT, DAILY."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class FabricEnvironment:
    """URL configuration for a Fabric deployment ring.

    Note: Token scopes (fabric_scope, storage_scope) are the same across all
    rings — only the API base URLs and DFS hosts differ.
    """

    name: str
    fabric_api_base: str
    dfs_host: str
    fabric_scope: str = "https://api.fabric.microsoft.com/.default"
    storage_scope: str = "https://storage.azure.com/.default"

    @property
    def fabric_api_url(self) -> str:
        return f"{self.fabric_api_base}/v1"


PROD = FabricEnvironment(
    name="PROD",
    fabric_api_base="https://api.fabric.microsoft.com",
    dfs_host="onelake.dfs.fabric.microsoft.com",
)

MSIT = FabricEnvironment(
    name="MSIT",
    fabric_api_base="https://msitapi.fabric.microsoft.com",
    dfs_host="msit-onelake.dfs.fabric.microsoft.com",
)

DXT = FabricEnvironment(
    name="DXT",
    fabric_api_base="https://api.dxt.fabric.microsoft.com",
    dfs_host="dxt-onelake.dfs.fabric.microsoft.com",
)

DAILY = FabricEnvironment(
    name="DAILY",
    fabric_api_base="https://api.daily.fabric.microsoft.com",
    dfs_host="daily-onelake.dfs.fabric.microsoft.com",
)

ENVIRONMENTS: dict[str, FabricEnvironment] = {
    "PROD": PROD,
    "MSIT": MSIT,
    "DXT": DXT,
    "DAILY": DAILY,
}

DEFAULT_ENVIRONMENT = PROD


def get_environment(name: str) -> FabricEnvironment:
    """Look up an environment by name (case-insensitive).

    Raises ValueError if the name is not recognised.
    """
    key = name.upper()
    if key not in ENVIRONMENTS:
        valid = ", ".join(ENVIRONMENTS)
        raise ValueError(f"Unknown environment {name!r}. Valid: {valid}")
    return ENVIRONMENTS[key]
