from pydantic import BaseModel, Field


class Item(BaseModel):
    """A Fabric item (lakehouse, warehouse, notebook, report, etc.)."""

    id: str
    display_name: str = Field(alias="displayName")
    description: str | None = None
    type: str  # "Lakehouse", "Warehouse", "Notebook", "Report", "SQLEndpoint", etc.
    workspace_id: str | None = Field(default=None, alias="workspaceId")

    model_config = {"populate_by_name": True}


class SqlEndpointProperties(BaseModel):
    """SQL endpoint connection details."""

    id: str | None = None
    connection_string: str | None = Field(default=None, alias="connectionString")
    provisioning_status: str | None = Field(default=None, alias="provisioningStatus")

    model_config = {"populate_by_name": True}


class LakehouseProperties(BaseModel):
    """Lakehouse-specific properties returned by GET /lakehouses/{id}."""

    onelake_tables_path: str | None = Field(default=None, alias="oneLakeTablesPath")
    onelake_files_path: str | None = Field(default=None, alias="oneLakeFilesPath")
    sql_endpoint_properties: SqlEndpointProperties | None = Field(
        default=None, alias="sqlEndpointProperties"
    )
    default_schema: str | None = Field(default=None, alias="defaultSchema")

    model_config = {"populate_by_name": True}


class Lakehouse(Item):
    """A Fabric lakehouse with extra properties."""

    properties: LakehouseProperties | None = None
