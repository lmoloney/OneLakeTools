"""onelake-client — Async Python client for Microsoft Fabric OneLake."""

from onelake_client.auth import OneLakeAuth, create_credential
from onelake_client.dfs import DfsClient
from onelake_client.environment import (
    DEFAULT_ENVIRONMENT,
    ENVIRONMENTS,
    FabricEnvironment,
    get_environment,
)
from onelake_client.exceptions import (
    ApiError,
    AuthenticationError,
    FileTooLargeError,
    NotFoundError,
    OneLakeError,
    PermissionDeniedError,
    RateLimitError,
)
from onelake_client.fabric import FabricClient
from onelake_client.models import (
    Column,
    DeltaTableInfo,
    FileProperties,
    IcebergTableInfo,
    Item,
    Lakehouse,
    LakehouseProperties,
    PathInfo,
    SqlEndpointProperties,
    Workspace,
)
from onelake_client.tables import DeltaTableReader, IcebergTableReader

__all__ = [
    # Facade
    "OneLakeClient",
    # Environment
    "FabricEnvironment",
    "ENVIRONMENTS",
    "DEFAULT_ENVIRONMENT",
    "get_environment",
    # Sub-clients
    "FabricClient",
    "DfsClient",
    "DeltaTableReader",
    "IcebergTableReader",
    # Auth
    "OneLakeAuth",
    "create_credential",
    # Models
    "Workspace",
    "Item",
    "Lakehouse",
    "LakehouseProperties",
    "SqlEndpointProperties",
    "PathInfo",
    "FileProperties",
    "Column",
    "DeltaTableInfo",
    "IcebergTableInfo",
    # Exceptions
    "OneLakeError",
    "AuthenticationError",
    "FileTooLargeError",
    "NotFoundError",
    "PermissionDeniedError",
    "RateLimitError",
    "ApiError",
    # Version
    "__version__",
]

__version__ = "0.2.0b1"


class OneLakeClient:
    """Unified client for Microsoft Fabric OneLake.

    Composes all sub-clients under a single entry point:
    - ``fabric`` — Fabric REST API (workspaces, items, lakehouses)
    - ``dfs`` — OneLake DFS (file/folder operations)
    - ``delta`` — Delta table metadata reader
    - ``iceberg`` — Iceberg table metadata reader

    Args:
        credential: Any ``azure.core.credentials.TokenCredential``.
            Defaults to ``DefaultAzureCredential``.
        env: A :class:`FabricEnvironment` selecting the deployment ring
            (PROD, MSIT, DXT, DAILY). Defaults to PROD.
    """

    def __init__(self, credential=None, *, env: FabricEnvironment | None = None):
        if env is None:
            env = DEFAULT_ENVIRONMENT
        self.env = env
        self.auth = OneLakeAuth(credential, env=env)
        self.fabric = FabricClient(self.auth, env=env)
        self.dfs = DfsClient(self.auth, env=env)
        self.delta = DeltaTableReader(self.auth, dfs_host=env.dfs_host)
        self.iceberg = IcebergTableReader(self.auth)

    async def close(self) -> None:
        """Close underlying HTTP clients."""
        await self.fabric.close()
        await self.dfs.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return False
