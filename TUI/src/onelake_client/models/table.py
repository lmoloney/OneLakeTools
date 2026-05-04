from typing import Any

from pydantic import BaseModel


class Column(BaseModel):
    """A table column."""

    name: str
    type: str
    nullable: bool = True
    metadata: dict[str, Any] | None = None
    comment: str | None = None


class DeltaTableInfo(BaseModel):
    """Metadata for a Delta table."""

    name: str | None = None
    schema_: list[Column] = []  # 'schema' is reserved in Pydantic
    version: int = 0
    num_files: int = 0
    size_bytes: int = 0
    partition_columns: list[str] = []
    properties: dict[str, str] = {}
    description: str | None = None


class IcebergTableInfo(BaseModel):
    """Metadata for an Iceberg table."""

    name: str | None = None
    schema_: list[Column] = []
    current_snapshot_id: int | None = None
    format_version: int = 2
    location: str | None = None
    partition_spec: list[dict] = []
    properties: dict[str, str] = {}
