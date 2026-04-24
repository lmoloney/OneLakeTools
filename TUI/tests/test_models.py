"""Tests for Pydantic models."""

from onelake_client.models import (
    Column,
    DeltaTableInfo,
    Lakehouse,
    PathInfo,
    Workspace,
)


def test_workspace_from_api_json():
    """Test that Workspace can be constructed from Fabric API JSON (camelCase)."""
    data = {
        "id": "ws-001",
        "displayName": "Test Workspace",
        "description": "desc",
        "type": "Workspace",
        "capacityId": "cap-001",
    }
    ws = Workspace.model_validate(data)
    assert ws.id == "ws-001"
    assert ws.display_name == "Test Workspace"
    assert ws.capacity_id == "cap-001"


def test_workspace_from_snake_case():
    """Test that Workspace can also be constructed with snake_case."""
    ws = Workspace(
        id="ws-001",
        display_name="Test",
        type="Workspace",
    )
    assert ws.display_name == "Test"


def test_lakehouse_with_properties():
    data = {
        "id": "lh-001",
        "displayName": "Sales",
        "type": "Lakehouse",
        "properties": {
            "oneLakeTablesPath": "/tables",
            "oneLakeFilesPath": "/files",
            "sqlEndpointProperties": {
                "id": "sql-001",
                "provisioningStatus": "Success",
            },
        },
    }
    lh = Lakehouse.model_validate(data)
    assert lh.properties.onelake_tables_path == "/tables"
    assert lh.properties.sql_endpoint_properties.provisioning_status == "Success"


def test_path_info_string_coercion():
    """DFS API returns isDirectory as string — model should handle it."""
    pi = PathInfo(name="test", isDirectory=True, contentLength=100)
    assert pi.is_directory is True
    assert pi.content_length == 100


def test_column_metadata_non_string_values():
    """Non-string metadata values (int, bool, nested dict) round-trip correctly (#25)."""
    meta = {
        "delta.columnMapping.id": 42,
        "delta.columnMapping.physicalName": "col-abc-123",
        "delta.generationExpression.enabled": True,
        "custom.nested": {"key": "value"},
    }
    col = Column(name="sensor_id", type="long", metadata=meta)
    assert col.metadata["delta.columnMapping.id"] == 42
    assert col.metadata["delta.generationExpression.enabled"] is True
    assert col.metadata["custom.nested"] == {"key": "value"}
    assert col.metadata["delta.columnMapping.physicalName"] == "col-abc-123"


def test_delta_table_info():
    info = DeltaTableInfo(
        name="customers",
        schema_=[Column(name="id", type="long"), Column(name="name", type="string")],
        version=5,
        num_files=10,
        size_bytes=1048576,
        partition_columns=["region"],
    )
    assert len(info.schema_) == 2
    assert info.partition_columns == ["region"]
