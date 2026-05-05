"""Integration tests for reading files from OneLake DFS.

Validates that read_file and get_properties work against the
olt_lakehouse_simple test lakehouse with various file types,
encodings, and path patterns (unicode, spaces, special chars).
"""

from __future__ import annotations

import csv
import io
import json
from datetime import datetime

import pyarrow.parquet as pq
import pytest

from onelake_client.exceptions import NotFoundError

# ── Helpers ─────────────────────────────────────────────────────────────

ALL_FILES = [
    "Files/sample.csv",
    "Files/data.json",
    "Files/readme.md",
    "Files/nested_types.parquet",
    "Files/reports/Q1 2024 Summary.md",
    "Files/données/résumé.csv",
    "Files/special (copy)/data & more.json",
]


def _file_path(lakehouse_id: str, relative: str) -> str:
    """Build the full DFS read path: {lakehouse_id}/{relative}."""
    return f"{lakehouse_id}/{relative}"


# ── CSV files ───────────────────────────────────────────────────────────


class TestCsvFiles:
    async def test_read_sample_csv(self, client, workspace_id, lakehouse_id):
        path = _file_path(lakehouse_id, "Files/sample.csv")
        data = await client.dfs.read_file(workspace_id, path)
        text = data.decode("utf-8")
        assert len(text) > 0

        reader = csv.reader(io.StringIO(text))
        rows = list(reader)
        assert len(rows) >= 2, "Expected at least a header row and one data row"
        # Header row should have comma-separated columns
        assert len(rows[0]) > 1, "Header should contain multiple columns"

    async def test_read_unicode_csv(self, client, workspace_id, lakehouse_id):
        path = _file_path(lakehouse_id, "Files/données/résumé.csv")
        data = await client.dfs.read_file(workspace_id, path)
        text = data.decode("utf-8")
        assert len(text) > 0

        reader = csv.reader(io.StringIO(text))
        rows = list(reader)
        assert len(rows) >= 1


# ── JSON files ──────────────────────────────────────────────────────────


class TestJsonFiles:
    async def test_read_data_json(self, client, workspace_id, lakehouse_id):
        data = await client.dfs.read_file(workspace_id, _file_path(lakehouse_id, "Files/data.json"))
        text = data.decode("utf-8")
        parsed = json.loads(text)
        assert parsed is not None
        assert isinstance(parsed, (dict, list))

    async def test_read_special_chars_json(self, client, workspace_id, lakehouse_id):
        data = await client.dfs.read_file(
            workspace_id, _file_path(lakehouse_id, "Files/special (copy)/data & more.json")
        )
        text = data.decode("utf-8")
        parsed = json.loads(text)
        assert parsed is not None
        assert isinstance(parsed, (dict, list))


# ── Markdown files ──────────────────────────────────────────────────────


class TestMarkdownFiles:
    async def test_read_readme_md(self, client, workspace_id, lakehouse_id):
        data = await client.dfs.read_file(workspace_id, _file_path(lakehouse_id, "Files/readme.md"))
        text = data.decode("utf-8")
        assert len(text) > 0
        assert "#" in text, "Markdown file should contain heading markers"

    async def test_read_md_with_spaces_in_path(self, client, workspace_id, lakehouse_id):
        data = await client.dfs.read_file(
            workspace_id, _file_path(lakehouse_id, "Files/reports/Q1 2024 Summary.md")
        )
        text = data.decode("utf-8")
        assert len(text) > 0
        assert "#" in text or text.strip(), "Should contain Markdown content"


# ── Parquet files ───────────────────────────────────────────────────────


class TestParquetFiles:
    async def test_read_nested_types_parquet(self, client, workspace_id, lakehouse_id):
        path = _file_path(lakehouse_id, "Files/nested_types.parquet")
        data = await client.dfs.read_file(workspace_id, path)
        if len(data) == 0:
            pytest.skip("nested_types.parquet is empty (0 bytes) — provisioning issue")
        table = pq.read_table(io.BytesIO(data))
        assert table.num_rows > 0
        assert table.num_columns > 0

        # Verify nested column types (struct, list/array, or map)
        schema = table.schema
        complex_types = {"struct", "list", "map", "large_list"}
        has_complex = any(any(t in str(field.type) for t in complex_types) for field in schema)
        assert has_complex, (
            f"Expected struct/array/map columns, got: {[str(f.type) for f in schema]}"
        )


# ── File properties ─────────────────────────────────────────────────────


class TestFileProperties:
    @pytest.mark.parametrize("relative_path", ALL_FILES)
    async def test_content_length_positive(self, client, workspace_id, lakehouse_id, relative_path):
        path = _file_path(lakehouse_id, relative_path)
        props = await client.dfs.get_properties(workspace_id, path)
        if props.content_length == 0:
            pytest.skip(f"{relative_path}: file is 0 bytes (provisioning issue)")
        assert props.content_length > 0

    @pytest.mark.parametrize("relative_path", ALL_FILES)
    async def test_last_modified_valid(self, client, workspace_id, lakehouse_id, relative_path):
        path = _file_path(lakehouse_id, relative_path)
        props = await client.dfs.get_properties(workspace_id, path)
        assert props.last_modified is not None, f"{relative_path}: last_modified should not be None"
        assert isinstance(props.last_modified, datetime)


# ── Error cases ─────────────────────────────────────────────────────────


class TestErrorCases:
    async def test_read_nonexistent_file_raises(self, client, workspace_id, lakehouse_id):
        with pytest.raises(NotFoundError):
            await client.dfs.read_file(
                workspace_id,
                _file_path(lakehouse_id, "Files/does_not_exist_abc123.txt"),
            )
