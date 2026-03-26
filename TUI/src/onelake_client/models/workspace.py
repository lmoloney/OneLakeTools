from pydantic import BaseModel, Field


class Workspace(BaseModel):
    """A Fabric workspace."""

    id: str
    display_name: str = Field(alias="displayName")
    description: str | None = None
    type: str  # "Workspace", "PersonalWorkspace", "AdminWorkspace"
    capacity_id: str | None = Field(default=None, alias="capacityId")
    state: str | None = None  # "Active", "Deleted", etc.

    model_config = {"populate_by_name": True}
