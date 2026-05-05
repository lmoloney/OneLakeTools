"""DFS browsing integration tests — validate structure against test environment manifest.

These tests verify that the DFS file system of the test lakehouse matches the
expected directory structure, tables, and files declared in fabric-test-env.json.

Run locally (manifest auto-detected from ~/.config/onelaketools/fabric-test-env.json):
    uv run pytest tests/integration/test_dfs_browsing.py -v

Requires ``az login`` (or AZURE_* env vars in CI) for live Fabric credentials.
"""

from __future__ import annotations

import pytest

# ── Helpers ─────────────────────────────────────────────────────────────


def _leaf_names(paths, *, dirs_only: bool = False) -> set[str]:
    """Extract leaf directory/file names from a list of PathInfo objects."""
    items = paths if not dirs_only else [p for p in paths if p.is_directory]
    return {p.name.split("/")[-1] for p in items}


# ── Root directory structure ────────────────────────────────────────────


class TestRootStructure:
    """Verify the lakehouse root directories match the manifest."""

    async def test_root_dirs_match_manifest(
        self, client, workspace_id, lakehouse_id, lakehouse_simple_item
    ):
        """All expected_root_dirs from the manifest are present at the root."""
        paths = await client.dfs.list_paths(workspace_id, lakehouse_id)
        dir_names = _leaf_names(paths, dirs_only=True)
        expected = set(lakehouse_simple_item["expected_root_dirs"])
        assert expected <= dir_names, f"Missing root dirs: {expected - dir_names}"

    async def test_root_contains_no_unexpected_files(self, client, workspace_id, lakehouse_id):
        """Root level should only contain directories, not loose files."""
        paths = await client.dfs.list_paths(workspace_id, lakehouse_id)
        files = [p for p in paths if not p.is_directory]
        assert files == [], f"Unexpected files at lakehouse root: {[p.name for p in files]}"


# ── Table discovery ─────────────────────────────────────────────────────


class TestTableDiscovery:
    """Verify all expected Delta tables exist under Tables/."""

    async def test_all_expected_tables_exist(
        self, client, workspace_id, lakehouse_id, lakehouse_simple_item
    ):
        """Each expected_table from the manifest appears as a directory under Tables/."""
        root = await client.dfs.list_paths(workspace_id, lakehouse_id)
        tables_dir = next(
            (p for p in root if p.is_directory and p.name.endswith("/Tables")),
            None,
        )
        assert tables_dir is not None, "Tables/ directory not found at lakehouse root"

        tables = await client.dfs.list_paths(workspace_id, tables_dir.name)
        table_names = _leaf_names(tables, dirs_only=True)
        expected = set(lakehouse_simple_item["expected_tables"])
        assert expected <= table_names, f"Missing tables: {expected - table_names}"

    async def test_tables_entries_are_directories(self, client, workspace_id, lakehouse_id):
        """Every entry directly under Tables/ should be a directory."""
        root = await client.dfs.list_paths(workspace_id, lakehouse_id)
        tables_dir = next(
            (p for p in root if p.is_directory and p.name.endswith("/Tables")),
            None,
        )
        if tables_dir is None:
            pytest.skip("Tables/ directory not found")

        children = await client.dfs.list_paths(workspace_id, tables_dir.name)
        non_dirs = [p for p in children if not p.is_directory]
        assert non_dirs == [], f"Non-directory entries under Tables/: {[p.name for p in non_dirs]}"


# ── File existence ──────────────────────────────────────────────────────


class TestFileExistence:
    """Verify all expected files from the manifest are reachable via DFS."""

    @pytest.fixture
    def _expected_files(self, lakehouse_simple_item) -> list[str]:
        return lakehouse_simple_item["expected_files"]

    async def test_all_expected_files_exist(
        self, client, workspace_id, lakehouse_id, _expected_files
    ):
        """dfs.exists() returns True for every file in expected_files."""
        for rel_path in _expected_files:
            full_path = f"{lakehouse_id}/{rel_path}"
            exists = await client.dfs.exists(workspace_id, full_path)
            assert exists, f"Expected file not found: {rel_path}"


# ── Unicode and special-character paths ─────────────────────────────────


class TestSpecialPaths:
    """DFS should handle unicode, spaces, parentheses, and ampersands."""

    async def test_unicode_path_exists(self, client, workspace_id, lakehouse_id):
        """A file with accented characters in its path is accessible."""
        path = f"{lakehouse_id}/Files/données/résumé.csv"
        assert await client.dfs.exists(workspace_id, path), (
            "Unicode path Files/données/résumé.csv not reachable"
        )

    async def test_unicode_path_properties(self, client, workspace_id, lakehouse_id):
        """get_properties succeeds for a unicode-named file."""
        path = f"{lakehouse_id}/Files/données/résumé.csv"
        props = await client.dfs.get_properties(workspace_id, path)
        assert props.content_length > 0

    async def test_space_in_path(self, client, workspace_id, lakehouse_id):
        """File path with spaces is accessible."""
        path = f"{lakehouse_id}/Files/reports/Q1 2024 Summary.md"
        assert await client.dfs.exists(workspace_id, path), "Path with spaces not reachable"

    async def test_parentheses_and_ampersand_in_path(self, client, workspace_id, lakehouse_id):
        """File path containing parentheses and ampersand is accessible."""
        path = f"{lakehouse_id}/Files/special (copy)/data & more.json"
        assert await client.dfs.exists(workspace_id, path), (
            "Path with parens/ampersand not reachable"
        )


# ── File properties / metadata ──────────────────────────────────────────


class TestFileProperties:
    """get_properties should return valid metadata for known files."""

    async def test_known_file_has_positive_content_length(self, client, workspace_id, lakehouse_id):
        """sample.csv should have content_length > 0."""
        path = f"{lakehouse_id}/Files/sample.csv"
        props = await client.dfs.get_properties(workspace_id, path)
        assert props.content_length > 0, "content_length should be > 0"

    async def test_known_file_has_last_modified(self, client, workspace_id, lakehouse_id):
        """sample.csv should have a non-None last_modified timestamp."""
        path = f"{lakehouse_id}/Files/sample.csv"
        props = await client.dfs.get_properties(workspace_id, path)
        assert props.last_modified is not None


# ── GUID vs friendly-name addressing ───────────────────────────────────


class TestAddressingModes:
    """Both GUID and friendly-name addressing should return identical results."""

    async def test_root_dirs_identical_across_modes(
        self,
        client,
        workspace_id,
        lakehouse_id,
        workspace_name,
        lakehouse_display_path,
    ):
        """GUID and friendly-name paths list the same root directories."""
        guid_paths = await client.dfs.list_paths(workspace_id, lakehouse_id)
        named_paths = await client.dfs.list_paths(workspace_name, lakehouse_display_path)

        guid_dirs = _leaf_names(guid_paths, dirs_only=True)
        named_dirs = _leaf_names(named_paths, dirs_only=True)
        assert guid_dirs == named_dirs, f"Mismatch: GUID={guid_dirs}, friendly={named_dirs}"

    async def test_tables_identical_across_modes(
        self,
        client,
        workspace_id,
        lakehouse_id,
        workspace_name,
        lakehouse_display_path,
    ):
        """GUID and friendly-name paths list the same tables."""
        guid_root = await client.dfs.list_paths(workspace_id, lakehouse_id)
        guid_tables_dir = next(
            (p for p in guid_root if p.is_directory and p.name.endswith("/Tables")),
            None,
        )
        if guid_tables_dir is None:
            pytest.skip("Tables/ not found via GUID path")

        named_root = await client.dfs.list_paths(workspace_name, lakehouse_display_path)
        named_tables_dir = next(
            (p for p in named_root if p.is_directory and p.name.endswith("/Tables")),
            None,
        )
        assert named_tables_dir is not None, "Tables/ not found via friendly path"

        guid_tables = await client.dfs.list_paths(workspace_id, guid_tables_dir.name)
        named_tables = await client.dfs.list_paths(workspace_name, named_tables_dir.name)

        assert _leaf_names(guid_tables) == _leaf_names(named_tables)


# ── Subdirectory and recursive listing ──────────────────────────────────


class TestDirectoryTraversal:
    """Verify subdirectory expansion and recursive listing."""

    async def test_tables_children_are_directories(self, client, workspace_id, lakehouse_id):
        """Expanding Tables/ yields only directories (one per table)."""
        root = await client.dfs.list_paths(workspace_id, lakehouse_id)
        tables_dir = next(
            (p for p in root if p.is_directory and p.name.endswith("/Tables")),
            None,
        )
        if tables_dir is None:
            pytest.skip("Tables/ not found")

        children = await client.dfs.list_paths(workspace_id, tables_dir.name)
        assert len(children) > 0, "Tables/ should not be empty"
        for child in children:
            assert child.is_directory, f"Expected directory under Tables/, got file: {child.name}"

    async def test_recursive_listing_finds_nested_files(self, client, workspace_id, lakehouse_id):
        """recursive=True on Files/ returns items in subdirectories."""
        root = await client.dfs.list_paths(workspace_id, lakehouse_id)
        files_dir = next(
            (p for p in root if p.is_directory and p.name.endswith("/Files")),
            None,
        )
        if files_dir is None:
            pytest.skip("Files/ not found")

        all_items = await client.dfs.list_paths(workspace_id, files_dir.name, recursive=True)
        nested = [p for p in all_items if p.name.count("/") > files_dir.name.count("/") + 1]
        assert len(nested) > 0, (
            "Recursive listing should include items nested beyond the first level"
        )

    async def test_recursive_listing_includes_known_deep_file(
        self, client, workspace_id, lakehouse_id
    ):
        """Recursive listing of Files/ should include a known nested file."""
        root = await client.dfs.list_paths(workspace_id, lakehouse_id)
        files_dir = next(
            (p for p in root if p.is_directory and p.name.endswith("/Files")),
            None,
        )
        if files_dir is None:
            pytest.skip("Files/ not found")

        all_items = await client.dfs.list_paths(workspace_id, files_dir.name, recursive=True)
        item_names = {p.name for p in all_items}
        # The manifest declares "Files/reports/Q1 2024 Summary.md"
        target = f"{files_dir.name}/reports/Q1 2024 Summary.md"
        assert target in item_names, (
            f"Expected '{target}' in recursive listing, got {len(all_items)} items"
        )
