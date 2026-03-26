from datetime import datetime

from pydantic import BaseModel, Field


class PathInfo(BaseModel):
    """A file or directory in OneLake (from DFS list paths)."""

    name: str
    is_directory: bool = Field(alias="isDirectory")
    content_length: int | None = Field(default=None, alias="contentLength")
    last_modified: datetime | None = Field(default=None, alias="lastModified")
    etag: str | None = None
    owner: str | None = None
    group: str | None = None
    permissions: str | None = None

    model_config = {"populate_by_name": True}


class FileProperties(BaseModel):
    """Detailed properties for a single file/folder (from HEAD request)."""

    content_length: int = Field(alias="contentLength", default=0)
    content_type: str | None = Field(default=None, alias="contentType")
    last_modified: datetime | None = Field(default=None, alias="lastModified")
    etag: str | None = None
    resource_type: str | None = Field(default=None, alias="resourceType")  # "file" or "directory"

    model_config = {"populate_by_name": True}
