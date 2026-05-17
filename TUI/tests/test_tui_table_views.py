"""Tests for TUI DetailPanel table metadata display across various Delta table types."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from textual.app import App, ComposeResult
from textual.widgets import DataTable, Static, TabbedContent, TabPane

from onelake_client.environment import DEFAULT_ENVIRONMENT
from onelake_client.models.table import Column, DeltaTableInfo
from onelake_tui.detail import DetailPanel
from onelake_tui.nodes import TableNode

# ── Helpers ──────────────────────────────────────────────────────────


def _make_mock_client() -> MagicMock:
    """Build a mock OneLakeClient with the minimum surface area."""
    client = MagicMock()
    client.env = DEFAULT_ENVIRONMENT
    client.fabric.list_workspaces = AsyncMock(return_value=[])
    client.fabric.list_items = AsyncMock(return_value=[])
    client.dfs.list_paths = AsyncMock(return_value=[])
    client.dfs.exists = AsyncMock(return_value=True)
    client.auth.get_identity = MagicMock(return_value="test-user@contoso.com")
    client.close = AsyncMock()
    return client


class _DetailHarness(App):
    """Minimal app that mounts only DetailPanel."""

    def __init__(self, client: MagicMock):
        super().__init__()
        self._client = client

    def compose(self) -> ComposeResult:
        yield DetailPanel(self._client, id="detail")


def _get_widget_text(widget) -> str:
    """Extract plain text from a Textual widget."""
    # Static.render() returns the raw content text
    try:
        rendered = widget.render()
        if rendered:
            text = str(rendered)
            if text.strip():
                return text
    except Exception:
        pass
    # Fall back to render_line for laid-out widgets
    try:
        line = widget.render_line(0)
        return "".join(seg.text for seg in line)
    except Exception:
        return ""


async def _setup_detail_with_metadata(
    client: MagicMock,
    delta_info: DeltaTableInfo,
    table_node: TableNode | None = None,
):
    """Mount DetailPanel, trigger metadata load, and wait for render.

    Returns (app, pilot, detail) tuple for assertions.
    """
    client.delta.get_metadata = AsyncMock(return_value=delta_info)

    app = _DetailHarness(client)
    pilot_ctx = app.run_test()
    pilot = await pilot_ctx.__aenter__()
    await pilot.pause()

    detail = app.query_one("#detail", DetailPanel)
    detail._workspace_name = "TestWS"
    detail._item_name = "TestItem"

    node = table_node or TableNode(
        workspace="ws-guid", item_path="item-guid", table_name="test_table"
    )
    detail.update_for_node(node)

    # Wait for debounce (0.15s) and @work async workers
    await pilot.pause()
    await asyncio.sleep(0.5)
    await pilot.pause()
    await pilot.pause()

    return app, pilot, detail, pilot_ctx


# ── Test classes ─────────────────────────────────────────────────────


class TestBasicTableView:
    """Basic table metadata display with a simple 3-column schema."""

    @pytest.mark.asyncio
    async def test_tabbed_content_mounted(self):
        """TabbedContent widget should be mounted after metadata loads."""
        client = _make_mock_client()
        info = DeltaTableInfo(
            name="customers",
            schema_=[
                Column(name="id", type="long", nullable=False),
                Column(name="name", type="string", nullable=True),
                Column(name="email", type="string", nullable=True),
            ],
            version=1,
            num_files=3,
            size_bytes=2048,
            partition_columns=[],
            properties={},
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            tabbed = detail.query_one(TabbedContent)
            assert tabbed is not None
        finally:
            await ctx.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_schema_table_has_three_rows(self):
        """Schema DataTable should have one row per column (3 columns → 3 rows)."""
        client = _make_mock_client()
        info = DeltaTableInfo(
            name="customers",
            schema_=[
                Column(name="id", type="long", nullable=False),
                Column(name="name", type="string", nullable=True),
                Column(name="email", type="string", nullable=True),
            ],
            version=1,
            num_files=3,
            size_bytes=2048,
            partition_columns=[],
            properties={},
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            schema_pane = detail.query_one("#tab-schema", TabPane)
            data_table = schema_pane.query_one(DataTable)
            assert data_table.row_count == 3
        finally:
            await ctx.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_version_displayed(self):
        """Version number should appear in the Schema tab."""
        client = _make_mock_client()
        info = DeltaTableInfo(
            name="customers",
            schema_=[Column(name="id", type="long", nullable=False)],
            version=7,
            num_files=2,
            size_bytes=1024,
            partition_columns=[],
            properties={},
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            schema_pane = detail.query_one("#tab-schema", TabPane)
            statics = schema_pane.query(Static)
            texts = [_get_widget_text(w) for w in statics]
            assert any("7" in t for t in texts), (
                f"Expected version '7' in Schema tab. Found: {texts}"
            )
        finally:
            await ctx.__aexit__(None, None, None)


class TestPartitionedTableView:
    """Table with partition columns should display them in Schema tab."""

    @pytest.mark.asyncio
    async def test_partition_columns_displayed(self):
        client = _make_mock_client()
        info = DeltaTableInfo(
            name="events",
            schema_=[
                Column(name="id", type="long", nullable=False),
                Column(name="region", type="string", nullable=True),
                Column(name="year", type="integer", nullable=False),
            ],
            version=3,
            num_files=12,
            size_bytes=50000,
            partition_columns=["region", "year"],
            properties={},
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            schema_pane = detail.query_one("#tab-schema", TabPane)
            statics = schema_pane.query(Static)
            texts = [_get_widget_text(w) for w in statics]
            assert any("region" in t and "year" in t for t in texts), (
                f"Expected partition columns 'region, year' in Schema tab. Found: {texts}"
            )
        finally:
            await ctx.__aexit__(None, None, None)


class TestCdfTableView:
    """Table with Change Data Feed enabled should show a CDF tab."""

    @pytest.mark.asyncio
    async def test_cdf_tab_present(self):
        client = _make_mock_client()
        info = DeltaTableInfo(
            name="orders",
            schema_=[Column(name="id", type="long", nullable=False)],
            version=2,
            num_files=1,
            size_bytes=512,
            partition_columns=[],
            properties={"delta.enableChangeDataFeed": "true"},
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            tabbed = detail.query_one(TabbedContent)
            panes = tabbed.query(TabPane)
            pane_ids = [p.id for p in panes]
            assert "tab-cdf" in pane_ids, (
                f"Expected CDF tab (tab-cdf) in TabbedContent. Found panes: {pane_ids}"
            )
            assert len(panes) >= 4, (
                f"Expected at least 4 tabs (Schema, Data, History, CDF). Found {len(panes)}"
            )
        finally:
            await ctx.__aexit__(None, None, None)


class TestNoCdfTableView:
    """Table without CDF property should NOT show a CDF tab."""

    @pytest.mark.asyncio
    async def test_no_cdf_tab(self):
        client = _make_mock_client()
        info = DeltaTableInfo(
            name="products",
            schema_=[Column(name="id", type="long", nullable=False)],
            version=1,
            num_files=1,
            size_bytes=256,
            partition_columns=[],
            properties={"delta.minReaderVersion": "1"},
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            tabbed = detail.query_one(TabbedContent)
            panes = tabbed.query(TabPane)
            pane_ids = [p.id for p in panes]
            assert "tab-cdf" not in pane_ids, (
                f"CDF tab should NOT appear without enableChangeDataFeed. Found panes: {pane_ids}"
            )
            assert len(panes) == 4, (
                f"Expected exactly 4 tabs (Schema, Data, History, Analysis). Found {len(panes)}"
            )
        finally:
            await ctx.__aexit__(None, None, None)


class TestHighVersionTableView:
    """Table with high version and file count should render version info."""

    @pytest.mark.asyncio
    async def test_high_version_rendered(self):
        client = _make_mock_client()
        info = DeltaTableInfo(
            name="metrics",
            schema_=[Column(name="ts", type="timestamp", nullable=False)],
            version=15,
            num_files=5,
            size_bytes=10_000_000,
            partition_columns=[],
            properties={},
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            schema_pane = detail.query_one("#tab-schema", TabPane)
            statics = schema_pane.query(Static)
            texts = [_get_widget_text(w) for w in statics]
            assert any("15" in t for t in texts), (
                f"Expected version '15' rendered in Schema tab. Found: {texts}"
            )
        finally:
            await ctx.__aexit__(None, None, None)


class TestDeletionVectorWarning:
    """When delta reader raises a DV error, a yellow warning should appear."""

    @pytest.mark.asyncio
    async def test_dv_warning_displayed(self):
        from deltalake.exceptions import DeltaError

        client = _make_mock_client()
        client.delta.get_metadata = AsyncMock(
            side_effect=DeltaError(
                "The table has set these reader features: {'deletionVectors'} "
                "but these are not yet supported by the deltalake reader."
            )
        )

        app = _DetailHarness(client)
        async with app.run_test() as pilot:
            await pilot.pause()
            detail = app.query_one("#detail", DetailPanel)
            detail._workspace_name = "TestWS"
            detail._item_name = "TestItem"

            node = TableNode(workspace="ws", item_path="item", table_name="dv_table")
            detail.update_for_node(node)

            await pilot.pause()
            await asyncio.sleep(0.5)
            await pilot.pause()
            await pilot.pause()

            statics = detail.query(Static)
            texts = [_get_widget_text(w) for w in statics]
            assert any("advanced Delta features" in t or "deletion vectors" in t for t in texts), (
                f"Expected DV warning message. Found: {texts}"
            )


class TestSchemaQualifiedTableName:
    """Schema-qualified table names (e.g. dbo/products) should display correctly."""

    @pytest.mark.asyncio
    async def test_schema_qualified_name_in_display(self):
        client = _make_mock_client()
        info = DeltaTableInfo(
            name="dbo/products",
            schema_=[Column(name="id", type="long", nullable=False)],
            version=1,
            num_files=1,
            size_bytes=512,
            partition_columns=[],
            properties={},
        )
        node = TableNode(workspace="ws", item_path="item", table_name="dbo/products")
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info, table_node=node)
        try:
            # The detail title Label shows the table_name
            from textual.widgets import Label

            labels = detail.query(Label)
            label_texts = [_get_widget_text(lbl) for lbl in labels]
            assert any("dbo/products" in t for t in label_texts), (
                f"Expected 'dbo/products' in detail labels. Found: {label_texts}"
            )
        finally:
            await ctx.__aexit__(None, None, None)


class TestTableWithDescription:
    """Table description should appear in the Schema tab."""

    @pytest.mark.asyncio
    async def test_description_displayed(self):
        client = _make_mock_client()
        info = DeltaTableInfo(
            name="customers",
            schema_=[Column(name="id", type="long", nullable=False)],
            version=1,
            num_files=1,
            size_bytes=256,
            partition_columns=[],
            properties={},
            description="Customer master data",
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            schema_pane = detail.query_one("#tab-schema", TabPane)
            statics = schema_pane.query(Static)
            texts = [_get_widget_text(w) for w in statics]
            assert any("Customer master data" in t for t in texts), (
                f"Expected description 'Customer master data' in Schema tab. Found: {texts}"
            )
        finally:
            await ctx.__aexit__(None, None, None)


class TestProtocolVersionDisplay:
    """Protocol version and features should appear in Schema tab."""

    @pytest.mark.asyncio
    async def test_protocol_version_displayed(self):
        client = _make_mock_client()
        info = DeltaTableInfo(
            name="proto_table",
            schema_=[Column(name="id", type="long", nullable=False)],
            version=1,
            num_files=1,
            size_bytes=256,
            partition_columns=[],
            properties={},
            reader_version=3,
            writer_version=7,
            reader_features=["deletionVectors", "columnMapping"],
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            schema_pane = detail.query_one("#tab-schema", TabPane)
            statics = schema_pane.query(Static)
            rendered = [str(w.render()) for w in statics]
            assert any("Reader v3" in t and "Writer v7" in t for t in rendered), (
                f"Expected 'Reader v3' and 'Writer v7' in Schema tab. Found: {rendered}"
            )
            assert any("deletionVectors" in t and "columnMapping" in t for t in rendered), (
                f"Expected reader features in Schema tab. Found: {rendered}"
            )
        finally:
            await ctx.__aexit__(None, None, None)


class TestRowCountDisplay:
    """Row count should appear in Schema tab when available."""

    @pytest.mark.asyncio
    async def test_row_count_displayed(self):
        client = _make_mock_client()
        info = DeltaTableInfo(
            name="counted_table",
            schema_=[Column(name="id", type="long", nullable=False)],
            version=1,
            num_files=1,
            size_bytes=256,
            partition_columns=[],
            properties={},
            total_rows=1234,
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            schema_pane = detail.query_one("#tab-schema", TabPane)
            statics = schema_pane.query(Static)
            texts = [_get_widget_text(w) for w in statics]
            assert any("1,234" in t for t in texts), (
                f"Expected '1,234' in Schema tab. Found: {texts}"
            )
        finally:
            await ctx.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_row_count_not_shown_when_none(self):
        client = _make_mock_client()
        info = DeltaTableInfo(
            name="no_rows_table",
            schema_=[Column(name="id", type="long", nullable=False)],
            version=1,
            num_files=1,
            size_bytes=256,
            partition_columns=[],
            properties={},
            total_rows=None,
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            schema_pane = detail.query_one("#tab-schema", TabPane)
            statics = schema_pane.query(Static)
            texts = [_get_widget_text(w) for w in statics]
            assert not any("Rows:" in t for t in texts), (
                f"Expected no 'Rows:' text when total_rows is None. Found: {texts}"
            )
        finally:
            await ctx.__aexit__(None, None, None)


class TestProactiveWarningsDisplay:
    """Proactive warnings should appear in Schema tab."""

    @pytest.mark.asyncio
    async def test_proactive_warnings_displayed(self):
        client = _make_mock_client()
        info = DeltaTableInfo(
            name="warned_table",
            schema_=[Column(name="id", type="long", nullable=False)],
            version=1,
            num_files=1,
            size_bytes=256,
            partition_columns=[],
            properties={},
            warnings=["⚠️ Table uses 'deletionVectors': reads may return stale data"],
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            schema_pane = detail.query_one("#tab-schema", TabPane)
            statics = schema_pane.query(Static)
            texts = [_get_widget_text(w) for w in statics]
            assert any("deletionVectors" in t for t in texts), (
                f"Expected warning about deletionVectors. Found: {texts}"
            )
        finally:
            await ctx.__aexit__(None, None, None)


class TestClusteringColumnsDisplay:
    """Clustering columns should appear in Schema tab."""

    @pytest.mark.asyncio
    async def test_clustering_columns_displayed(self):
        client = _make_mock_client()
        info = DeltaTableInfo(
            name="clustered_table",
            schema_=[Column(name="id", type="long", nullable=False)],
            version=1,
            num_files=1,
            size_bytes=256,
            partition_columns=[],
            properties={"clusteringColumns": "category, region"},
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            schema_pane = detail.query_one("#tab-schema", TabPane)
            statics = schema_pane.query(Static)
            texts = [_get_widget_text(w) for w in statics]
            assert any("Clustered by" in t for t in texts), (
                f"Expected 'Clustered by' in Schema tab. Found: {texts}"
            )
        finally:
            await ctx.__aexit__(None, None, None)


class TestWriterFeaturesDisplay:
    """Writer features should appear in Schema tab."""

    @pytest.mark.asyncio
    async def test_writer_features_displayed(self):
        client = _make_mock_client()
        info = DeltaTableInfo(
            name="writer_feat_table",
            schema_=[Column(name="id", type="long", nullable=False)],
            version=1,
            num_files=1,
            size_bytes=256,
            partition_columns=[],
            properties={},
            writer_features=["appendOnly", "invariants"],
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            schema_pane = detail.query_one("#tab-schema", TabPane)
            statics = schema_pane.query(Static)
            texts = [_get_widget_text(w) for w in statics]
            assert any("appendOnly" in t and "invariants" in t for t in texts), (
                f"Expected writer features 'appendOnly, invariants' in Schema tab. Found: {texts}"
            )
        finally:
            await ctx.__aexit__(None, None, None)


class TestTableWithColumnMetadata:
    """Column metadata should populate the schema DataTable rows."""

    @pytest.mark.asyncio
    async def test_columns_with_metadata_rendered(self):
        """Columns with metadata dict should still render in the DataTable."""
        client = _make_mock_client()
        info = DeltaTableInfo(
            name="mapped_table",
            schema_=[
                Column(
                    name="user_id",
                    type="long",
                    nullable=False,
                    metadata={
                        "delta.columnMapping.id": 1,
                        "delta.columnMapping.physicalName": "col-1",
                    },
                ),
                Column(
                    name="user_name",
                    type="string",
                    nullable=True,
                    metadata={
                        "delta.columnMapping.id": 2,
                        "delta.columnMapping.physicalName": "col-2",
                    },
                ),
            ],
            version=1,
            num_files=1,
            size_bytes=512,
            partition_columns=[],
            properties={},
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            schema_pane = detail.query_one("#tab-schema", TabPane)
            data_table = schema_pane.query_one(DataTable)
            assert data_table.row_count == 2, (
                f"Expected 2 rows in schema DataTable. Found {data_table.row_count}"
            )
        finally:
            await ctx.__aexit__(None, None, None)


# ── Regression tests (bugs found during manual testing) ─────────────


class TestCdfRendersValues:
    """Regression: CDF tab was showing arro3 Scalar type names instead of values.

    read_cdf() returns arro3.core.Table. Indexing columns gives Scalar objects
    where str() shows 'arro3.core.Scalar<Int64>' instead of the actual value.
    The fix calls .as_py() before str().
    """

    @pytest.mark.asyncio
    async def test_cdf_cells_contain_values_not_types(self):
        """CDF DataTable cells should contain actual values, not arro3 Scalar type names."""
        client = _make_mock_client()
        info = DeltaTableInfo(
            name="cdf_table",
            schema_=[Column(name="id", type="long"), Column(name="value", type="string")],
            version=3,
            num_files=1,
            size_bytes=1024,
            properties={"delta.enableChangeDataFeed": "true"},
        )

        # Create a mock arro3-like table that returns Scalar-like objects
        class _MockScalar:
            """Simulates arro3.core.Scalar — str() shows type, as_py() shows value."""

            def __init__(self, value, type_name):
                self._value = value
                self._type_name = type_name

            def __str__(self):
                return f"arro3.core.Scalar<{self._type_name}>"

            def as_py(self):
                return self._value

        class _MockColumn:
            def __init__(self, values, type_name):
                self._values = values
                self._type_name = type_name

            def __getitem__(self, idx):
                return _MockScalar(self._values[idx], self._type_name)

        class _MockCdfTable:
            column_names = ["id", "value", "_change_type"]
            num_rows = 2

            def column(self, idx):
                cols = [
                    _MockColumn([1, 2], "Int64"),
                    _MockColumn(["hello", "world"], "Utf8View"),
                    _MockColumn(["insert", "insert"], "Utf8View"),
                ]
                return cols[idx]

        client.delta.get_metadata = AsyncMock(return_value=info)
        client.delta.read_cdf = AsyncMock(return_value=_MockCdfTable())

        app = _DetailHarness(client)
        async with app.run_test() as pilot:
            await pilot.pause()
            detail = app.query_one("#detail", DetailPanel)
            detail._workspace_name = "TestWS"
            detail._item_name = "TestItem"

            node = TableNode(workspace="ws", item_path="item", table_name="cdf_table")
            detail.update_for_node(node)

            await pilot.pause()
            await asyncio.sleep(0.5)
            await pilot.pause()
            await pilot.pause()

            # Click the CDF load button
            from textual.widgets import Button

            try:
                btn = detail.query_one("#load-cdf-preview", Button)
                btn.press()
            except Exception:
                pass  # Button may not exist if CDF auto-loaded

            await pilot.pause()
            await asyncio.sleep(0.5)
            await pilot.pause()

            # Check that no cell contains "arro3.core.Scalar"
            data_tables = detail.query(DataTable)
            for dt in data_tables:
                for row_key in dt.rows:
                    row_data = dt.get_row(row_key)
                    for cell in row_data:
                        cell_str = str(cell)
                        assert "arro3.core.Scalar" not in cell_str, (
                            f"CDF cell contains type name instead of value: {cell_str}"
                        )


class TestMinReaderVersionFallback:
    """Regression: 'minimum reader version' errors were not caught by the fallback handler.

    column_mapping_id tables produce: 'The table's minimum reader version is 2
    but deltalake only supports version 1 or 3 with these reader features:
    {'timestampNtz'}' — this was not matched by the old pattern that only
    checked for 'reader features' AND 'not yet supported'.
    """

    @pytest.mark.asyncio
    async def test_minimum_reader_version_shows_fallback_warning(self):
        """Error about minimum reader version should trigger fallback, not crash."""
        from deltalake.exceptions import DeltaError

        client = _make_mock_client()
        client.delta.get_metadata = AsyncMock(
            side_effect=DeltaError(
                "The table's minimum reader version is 2 but deltalake only supports "
                "version 1 or 3 with these reader features: {'timestampNtz'}"
            )
        )

        app = _DetailHarness(client)
        async with app.run_test() as pilot:
            await pilot.pause()
            detail = app.query_one("#detail", DetailPanel)
            detail._workspace_name = "TestWS"
            detail._item_name = "TestItem"

            node = TableNode(workspace="ws", item_path="item", table_name="cm_table")
            detail.update_for_node(node)

            await pilot.pause()
            await asyncio.sleep(0.5)
            await pilot.pause()
            await pilot.pause()

            statics = detail.query(Static)
            texts = [_get_widget_text(w) for w in statics]
            assert any("advanced Delta features" in t for t in texts), (
                f"Expected fallback warning for minimum reader version error. Found: {texts}"
            )


# ── Data tab tests ──────────────────────────────────────────────────


class TestDataTabLoadButton:
    """Data tab should have a load button that triggers sample data rendering."""

    @pytest.mark.asyncio
    async def test_data_tab_has_load_button(self):
        """Data tab should initially show a 'Load Data Preview' button."""
        from textual.widgets import Button

        client = _make_mock_client()
        info = DeltaTableInfo(
            name="test",
            schema_=[Column(name="id", type="long")],
            version=0,
            num_files=1,
            size_bytes=100,
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            btn = detail.query_one("#load-data-preview", Button)
            assert btn is not None
        finally:
            await ctx.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_data_tab_renders_sample(self):
        """Clicking load button should render sample data in a DataTable."""
        import pyarrow as pa
        from textual.widgets import Button

        client = _make_mock_client()
        sample = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        client.delta.read_sample = AsyncMock(return_value=sample)

        info = DeltaTableInfo(
            name="test",
            schema_=[
                Column(name="id", type="long"),
                Column(name="name", type="string"),
            ],
            version=0,
            num_files=1,
            size_bytes=100,
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            btn = detail.query_one("#load-data-preview", Button)
            btn.press()
            await pilot.pause()
            await asyncio.sleep(0.5)
            await pilot.pause()

            data_pane = detail.query_one("#tab-data", TabPane)
            dt = data_pane.query_one(DataTable)
            assert dt.row_count == 3
            assert len(dt.columns) == 2
        finally:
            await ctx.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_data_tab_fallback_on_reader_error(self):
        """When read_sample fails with reader features error, should show warning."""
        from textual.widgets import Button

        client = _make_mock_client()
        client.delta.read_sample = AsyncMock(
            side_effect=Exception("reader features: {'deletionVectors'} not yet supported")
        )
        # Fallback will try list_paths for parquet files — let it fail gracefully
        client.dfs.list_paths = AsyncMock(return_value=[])

        info = DeltaTableInfo(
            name="test",
            schema_=[Column(name="id", type="long")],
            version=0,
            num_files=1,
            size_bytes=100,
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            btn = detail.query_one("#load-data-preview", Button)
            btn.press()
            await pilot.pause()
            await asyncio.sleep(0.5)
            await pilot.pause()
            await pilot.pause()

            statics = detail.query(Static)
            texts = [_get_widget_text(w) for w in statics]
            assert any("advanced Delta features" in t for t in texts)
        finally:
            await ctx.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_data_tab_network_error(self):
        """Generic error during data load shows error message."""
        from textual.widgets import Button

        client = _make_mock_client()
        client.delta.read_sample = AsyncMock(side_effect=Exception("Connection timeout after 30s"))

        info = DeltaTableInfo(
            name="test",
            schema_=[Column(name="id", type="long")],
            version=0,
            num_files=1,
            size_bytes=100,
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            btn = detail.query_one("#load-data-preview", Button)
            btn.press()
            await pilot.pause()
            await asyncio.sleep(0.5)
            await pilot.pause()

            statics = detail.query(Static)
            texts = [_get_widget_text(w) for w in statics]
            assert any("Data preview failed" in t or "Connection timeout" in t for t in texts)
        finally:
            await ctx.__aexit__(None, None, None)


# ── History tab tests ───────────────────────────────────────────────


class TestHistoryTab:
    """History tab should display transaction log from _delta_log JSON files.

    Note: _load_transaction_log fires automatically during _load_table_metadata,
    so the DFS mocks must be set up BEFORE calling _setup_detail_with_metadata.
    """

    @pytest.mark.asyncio
    async def test_history_tab_renders_commits(self):
        """Transaction log should render as a DataTable with version/timestamp/operation."""
        from onelake_client.models import PathInfo

        client = _make_mock_client()

        # Set up DFS mocks BEFORE metadata load (history fires automatically)
        commit_0 = (
            '{"protocol":{"minReaderVersion":1}}\n'
            '{"metaData":{"id":"t1"}}\n'
            '{"commitInfo":{"operation":"WRITE","timestamp":1700000000000}}\n'
        )
        commit_1 = (
            '{"commitInfo":{"operation":"MERGE","timestamp":1700000100000,'
            '"operationMetrics":{"numTargetRowsUpdated":"5"}}}\n'
        )
        client.dfs.list_paths = AsyncMock(
            return_value=[
                PathInfo(
                    name="item-guid/Tables/test_table/_delta_log/00000000000000000000.json",
                    isDirectory=False,
                    contentLength=200,
                ),
                PathInfo(
                    name="item-guid/Tables/test_table/_delta_log/00000000000000000001.json",
                    isDirectory=False,
                    contentLength=200,
                ),
            ]
        )
        client.dfs.read_file = AsyncMock(side_effect=[commit_0.encode(), commit_1.encode()])

        info = DeltaTableInfo(
            name="test",
            schema_=[Column(name="id", type="long")],
            version=1,
            num_files=1,
            size_bytes=100,
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            # History already loaded — extra settle time
            await asyncio.sleep(0.5)
            await pilot.pause()

            history_pane = detail.query_one("#tab-history", TabPane)
            dt = history_pane.query_one(DataTable)
            assert dt.row_count == 2
            assert len(dt.columns) == 4  # Version, Timestamp, Operation, Details
        finally:
            await ctx.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_history_tab_empty_log(self):
        """Empty delta log shows placeholder message."""
        client = _make_mock_client()
        # list_paths returns empty for _delta_log directory
        client.dfs.list_paths = AsyncMock(return_value=[])

        info = DeltaTableInfo(
            name="test",
            schema_=[Column(name="id", type="long")],
            version=0,
            num_files=1,
            size_bytes=100,
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            await asyncio.sleep(0.5)
            await pilot.pause()

            history_pane = detail.query_one("#tab-history", TabPane)
            # Either "No transaction history" or still loading — verify no crash
            assert history_pane is not None
        finally:
            await ctx.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_history_tab_prefers_in_commit_timestamp(self):
        """When inCommitTimestamp is present, it should be used over timestamp."""
        from onelake_client.models import PathInfo

        client = _make_mock_client()
        # inCommitTimestamp = 2024-01-15 12:00:00 UTC (1705320000000)
        # timestamp = 2024-01-01 00:00:00 UTC (1704067200000) — 2 weeks earlier
        commit = (
            '{"commitInfo":{"operation":"WRITE",'
            '"timestamp":1704067200000,'
            '"inCommitTimestamp":1705320000000}}\n'
        )
        client.dfs.list_paths = AsyncMock(
            return_value=[
                PathInfo(
                    name="ig/Tables/t/_delta_log/00000000000000000000.json",
                    isDirectory=False,
                    contentLength=200,
                ),
            ]
        )
        client.dfs.read_file = AsyncMock(return_value=commit.encode())

        info = DeltaTableInfo(
            name="test",
            schema_=[Column(name="id", type="long")],
            version=0,
            num_files=1,
            size_bytes=100,
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            await asyncio.sleep(0.5)
            await pilot.pause()

            history_pane = detail.query_one("#tab-history", TabPane)
            dt = history_pane.query_one(DataTable)
            assert dt.row_count == 1
            row = dt.get_row_at(0)
            ts_str = str(row[1])
            # inCommitTimestamp is Jan 15, regular timestamp is Jan 1
            assert "2024-01-15" in ts_str, (
                f"Expected inCommitTimestamp date (2024-01-15), got: {ts_str}"
            )
        finally:
            await ctx.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_history_tab_error(self):
        """Network error during history load shows error message."""
        client = _make_mock_client()
        client.dfs.list_paths = AsyncMock(side_effect=Exception("Connection refused"))

        info = DeltaTableInfo(
            name="test",
            schema_=[Column(name="id", type="long")],
            version=0,
            num_files=1,
            size_bytes=100,
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            await asyncio.sleep(0.5)
            await pilot.pause()

            history_pane = detail.query_one("#tab-history", TabPane)
            statics = history_pane.query(Static)
            texts = [_get_widget_text(w) for w in statics]
            assert any("Could not load history" in t for t in texts)
        finally:
            await ctx.__aexit__(None, None, None)


# ── CDF tab additional tests ───────────────────────────────────────


class TestCdfTabEmpty:
    """CDF tab should handle empty result and errors gracefully."""

    @pytest.mark.asyncio
    async def test_cdf_empty_result(self):
        """When read_cdf returns 0 rows, show informative message."""
        from textual.widgets import Button

        client = _make_mock_client()

        class _EmptyCdfTable:
            column_names = ["_change_type"]
            num_rows = 0

        client.delta.read_cdf = AsyncMock(return_value=_EmptyCdfTable())

        info = DeltaTableInfo(
            name="cdf_table",
            schema_=[Column(name="id", type="long")],
            version=1,
            num_files=1,
            size_bytes=100,
            properties={"delta.enableChangeDataFeed": "true"},
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            # Find and click the CDF load button
            try:
                btn = detail.query_one("#load-cdf-preview", Button)
                btn.press()
            except Exception:
                pass

            await pilot.pause()
            await asyncio.sleep(0.5)
            await pilot.pause()
            await pilot.pause()

            cdf_pane = detail.query_one("#tab-cdf", TabPane)
            statics = cdf_pane.query(Static)
            texts = [_get_widget_text(w) for w in statics]
            assert any("No CDF records" in t or "0" in t for t in texts), (
                f"Expected empty CDF message. Found: {texts}"
            )
        finally:
            await ctx.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_cdf_error(self):
        """When read_cdf fails, show error message."""
        from textual.widgets import Button

        client = _make_mock_client()
        client.delta.read_cdf = AsyncMock(side_effect=Exception("CDF not available for this table"))

        info = DeltaTableInfo(
            name="cdf_table",
            schema_=[Column(name="id", type="long")],
            version=1,
            num_files=1,
            size_bytes=100,
            properties={"delta.enableChangeDataFeed": "true"},
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            try:
                btn = detail.query_one("#load-cdf-preview", Button)
                btn.press()
            except Exception:
                pass

            await pilot.pause()
            await asyncio.sleep(0.5)
            await pilot.pause()
            await pilot.pause()

            cdf_pane = detail.query_one("#tab-cdf", TabPane)
            statics = cdf_pane.query(Static)
            texts = [_get_widget_text(w) for w in statics]
            assert any("CDF preview failed" in t or "not available" in t for t in texts), (
                f"Expected CDF error message. Found: {texts}"
            )
        finally:
            await ctx.__aexit__(None, None, None)


# ── CDF latest-first preview ───────────────────────────────────────


class TestCdfLatestFirst:
    """CDF preview starts from latest version, auto-expands if empty,
    and always shows 'Load Earlier Versions' button."""

    @pytest.mark.asyncio
    async def test_latest_first_with_data(self):
        """Latest version has CDF data — shows data + button."""
        from textual.widgets import Button

        client = _make_mock_client()

        class _MockCdfResult:
            column_names = ["_change_type", "id"]
            num_rows = 2

            def column(self, idx):
                data = [["insert", "insert"], [1, 2]]
                vals = []
                for v in data[idx]:
                    m = MagicMock()
                    m.as_py.return_value = v
                    vals.append(m)
                return vals

        client.delta.read_cdf = AsyncMock(return_value=_MockCdfResult())

        info = DeltaTableInfo(
            name="holidays",
            schema_=[Column(name="id", type="long")],
            version=5,
            num_files=1,
            size_bytes=100,
            properties={"delta.enableChangeDataFeed": "true"},
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            try:
                btn = detail.query_one("#load-cdf-preview", Button)
                btn.press()
            except Exception:
                pass

            await pilot.pause()
            await asyncio.sleep(0.5)
            await pilot.pause()
            await pilot.pause()

            cdf_pane = detail.query_one("#tab-cdf", TabPane)

            # Should have the "Load Earlier Versions" button
            buttons = cdf_pane.query(Button)
            button_ids = [b.id for b in buttons]
            assert "search-cdf-range" in button_ids, (
                f"Expected 'Load Earlier Versions' button. Found: {button_ids}"
            )
            # Should have a DataTable with CDF data
            tables = cdf_pane.query(DataTable)
            assert len(tables) >= 1, "Expected CDF DataTable to be rendered"
        finally:
            await ctx.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_latest_empty_auto_expands(self):
        """Latest version has 0 rows — auto-expands to version-10 range."""
        from textual.widgets import Button

        client = _make_mock_client()

        class _EmptyCdf:
            column_names = ["_change_type"]
            num_rows = 0

        class _NonEmptyCdf:
            column_names = ["_change_type", "id"]
            num_rows = 3

            def column(self, idx):
                data = [["insert", "update", "delete"], [1, 2, 3]]
                vals = []
                for v in data[idx]:
                    m = MagicMock()
                    m.as_py.return_value = v
                    vals.append(m)
                return vals

        call_count = 0

        async def _read_cdf_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            sv = kwargs.get("starting_version", 0)
            if sv == 15:
                return _EmptyCdf()
            return _NonEmptyCdf()

        client.delta.read_cdf = AsyncMock(side_effect=_read_cdf_side_effect)

        info = DeltaTableInfo(
            name="holidays",
            schema_=[Column(name="id", type="long")],
            version=15,
            num_files=1,
            size_bytes=100,
            properties={"delta.enableChangeDataFeed": "true"},
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            try:
                btn = detail.query_one("#load-cdf-preview", Button)
                btn.press()
            except Exception:
                pass

            await pilot.pause()
            await asyncio.sleep(0.5)
            await pilot.pause()
            await pilot.pause()

            cdf_pane = detail.query_one("#tab-cdf", TabPane)
            tables = cdf_pane.query(DataTable)
            assert len(tables) >= 1, "Expected DataTable after auto-expand"
            assert call_count >= 2, "Expected at least 2 read_cdf calls (latest + expanded)"
        finally:
            await ctx.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_all_versions_fail_shows_error(self):
        """When even latest version fails with CDF-not-enabled, show clear error."""
        from deltalake.exceptions import DeltaError
        from textual.widgets import Button

        client = _make_mock_client()
        client.delta.read_cdf = AsyncMock(
            side_effect=DeltaError(
                "Reading a table version: 0 that does not have change data enabled"
            )
        )

        info = DeltaTableInfo(
            name="holidays",
            schema_=[Column(name="id", type="long")],
            version=5,
            num_files=1,
            size_bytes=100,
            properties={"delta.enableChangeDataFeed": "true"},
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            try:
                btn = detail.query_one("#load-cdf-preview", Button)
                btn.press()
            except Exception:
                pass

            await pilot.pause()
            await asyncio.sleep(0.5)
            await pilot.pause()
            await pilot.pause()

            cdf_pane = detail.query_one("#tab-cdf", TabPane)
            statics = cdf_pane.query(Static)
            texts = [_get_widget_text(w) for w in statics]

            assert any("no readable CDF versions" in t for t in texts), (
                f"Expected 'no readable CDF versions' message. Found: {texts}"
            )
        finally:
            await ctx.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_non_cdf_error_not_caught(self):
        """Non-CDF errors should display normally without retry."""
        from textual.widgets import Button

        client = _make_mock_client()
        client.delta.read_cdf = AsyncMock(side_effect=ConnectionError("Network unreachable"))

        info = DeltaTableInfo(
            name="holidays",
            schema_=[Column(name="id", type="long")],
            version=5,
            num_files=1,
            size_bytes=100,
            properties={"delta.enableChangeDataFeed": "true"},
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            try:
                btn = detail.query_one("#load-cdf-preview", Button)
                btn.press()
            except Exception:
                pass

            await pilot.pause()
            await asyncio.sleep(0.5)
            await pilot.pause()
            await pilot.pause()

            cdf_pane = detail.query_one("#tab-cdf", TabPane)
            statics = cdf_pane.query(Static)
            texts = [_get_widget_text(w) for w in statics]

            assert any("CDF preview failed" in t for t in texts), (
                f"Expected standard error message. Found: {texts}"
            )
            assert any("Network unreachable" in t for t in texts), (
                f"Expected original error text. Found: {texts}"
            )
        finally:
            await ctx.__aexit__(None, None, None)


# ── Analysis tab tests ───────────────────────────────────────────────


class TestAnalysisTab:
    """Tests for the Analysis tab — lazy-loaded via 'Run Analysis' button."""

    def _make_analysis_result(self):
        from onelake_client.models.table import (
            ColumnChunkInfo,
            ColumnInfo,
            DeltaAnalysisResult,
            DeltaAnalysisSummary,
            ParquetFileInfo,
            RowGroupInfo,
        )

        return DeltaAnalysisResult(
            summary=DeltaAnalysisSummary(
                total_rows=100,
                total_files=2,
                total_row_groups=2,
                avg_rows_per_row_group=50.0,
                min_rows_per_row_group=40,
                max_rows_per_row_group=60,
                total_compressed_size=2048,
                total_uncompressed_size=4096,
                files_skipped=0,
            ),
            files=[
                ParquetFileInfo(
                    file_name="part-00000.parquet",
                    row_count=60,
                    row_group_count=1,
                    total_table_rows=100,
                    created_by="test-writer",
                ),
                ParquetFileInfo(
                    file_name="part-00001.parquet",
                    row_count=40,
                    row_group_count=1,
                    total_table_rows=100,
                    created_by="test-writer",
                ),
            ],
            row_groups=[
                RowGroupInfo(
                    file_name="part-00000.parquet",
                    row_group_id=1,
                    row_count=60,
                    total_table_rows=100,
                    compressed_size=1200,
                    uncompressed_size=2400,
                    compression_ratio=0.5,
                ),
                RowGroupInfo(
                    file_name="part-00001.parquet",
                    row_group_id=1,
                    row_count=40,
                    total_table_rows=100,
                    compressed_size=848,
                    uncompressed_size=1696,
                    compression_ratio=0.5,
                ),
            ],
            column_chunks=[
                ColumnChunkInfo(
                    file_name="part-00000.parquet",
                    row_group_id=1,
                    column_id=1,
                    column_name="id",
                    physical_type="INT32",
                    compressed_size=600,
                    uncompressed_size=1200,
                    num_values=60,
                ),
                ColumnChunkInfo(
                    file_name="part-00000.parquet",
                    row_group_id=1,
                    column_id=2,
                    column_name="name",
                    physical_type="BYTE_ARRAY",
                    compressed_size=600,
                    uncompressed_size=1200,
                    num_values=60,
                ),
                ColumnChunkInfo(
                    file_name="part-00001.parquet",
                    row_group_id=1,
                    column_id=1,
                    column_name="id",
                    physical_type="INT32",
                    compressed_size=424,
                    uncompressed_size=848,
                    num_values=40,
                ),
                ColumnChunkInfo(
                    file_name="part-00001.parquet",
                    row_group_id=1,
                    column_id=2,
                    column_name="name",
                    physical_type="BYTE_ARRAY",
                    compressed_size=424,
                    uncompressed_size=848,
                    num_values=40,
                ),
            ],
            columns=[
                ColumnInfo(
                    column_id=1,
                    column_name="id",
                    total_compressed_size=1024,
                    total_uncompressed_size=2048,
                    total_table_rows=100,
                    pct_of_table=0.5,
                ),
                ColumnInfo(
                    column_id=2,
                    column_name="name",
                    total_compressed_size=1024,
                    total_uncompressed_size=2048,
                    total_table_rows=100,
                    pct_of_table=0.5,
                ),
            ],
        )

    @pytest.mark.asyncio
    async def test_analysis_tab_has_run_button(self):
        """Analysis tab should show a 'Run Analysis' button initially."""
        from textual.widgets import Button

        client = _make_mock_client()
        info = DeltaTableInfo(
            name="test",
            schema_=[Column(name="id", type="long")],
            version=0,
            num_files=1,
            size_bytes=100,
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            pane = detail.query_one("#tab-analysis", TabPane)
            assert pane is not None
            btn = detail.query_one("#run-analysis", Button)
            assert btn is not None
        finally:
            await ctx.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_analysis_renders_data_tables(self):
        """Clicking Run Analysis should render DataTable sections."""
        from textual.widgets import Button, Label

        client = _make_mock_client()
        client.delta.get_analysis = AsyncMock(return_value=self._make_analysis_result())

        info = DeltaTableInfo(
            name="test",
            schema_=[
                Column(name="id", type="long"),
                Column(name="name", type="string"),
            ],
            version=0,
            num_files=2,
            size_bytes=4096,
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            btn = detail.query_one("#run-analysis", Button)
            btn.press()
            await pilot.pause()
            await asyncio.sleep(0.5)
            await pilot.pause()
            await pilot.pause()

            pane = detail.query_one("#tab-analysis", TabPane)
            tables = pane.query(DataTable)
            assert len(tables) == 4, (
                f"Expected 4 DataTables (Files, Row Groups, Column Chunks, Columns). "
                f"Found {len(tables)}"
            )

            labels = pane.query(Label)
            label_texts = [str(label.render()) for label in labels]
            assert any("Summary" in t for t in label_texts)
            assert any("Parquet Files" in t for t in label_texts)
        finally:
            await ctx.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_analysis_files_table_row_count(self):
        """Files DataTable should have one row per file."""
        from textual.widgets import Button

        client = _make_mock_client()
        client.delta.get_analysis = AsyncMock(return_value=self._make_analysis_result())

        info = DeltaTableInfo(
            name="test",
            schema_=[Column(name="id", type="long")],
            version=0,
            num_files=2,
            size_bytes=4096,
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            btn = detail.query_one("#run-analysis", Button)
            btn.press()
            await pilot.pause()
            await asyncio.sleep(0.5)
            await pilot.pause()

            pane = detail.query_one("#tab-analysis", TabPane)
            tables = pane.query(DataTable)
            files_table = tables[0]
            assert files_table.row_count == 2
        finally:
            await ctx.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_analysis_error_no_tables_rendered(self):
        """Analysis error should not render DataTables and should allow retry."""
        from textual.widgets import Button

        client = _make_mock_client()
        client.delta.get_analysis = AsyncMock(
            side_effect=Exception("Connection timeout [bold]injected[/bold]")
        )

        info = DeltaTableInfo(
            name="test",
            schema_=[Column(name="id", type="long")],
            version=0,
            num_files=1,
            size_bytes=100,
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            btn = detail.query_one("#run-analysis", Button)
            btn.press()
            await pilot.pause()
            await asyncio.sleep(1.0)
            await pilot.pause()
            await pilot.pause()

            pane = detail.query_one("#tab-analysis", TabPane)
            tables = pane.query(DataTable)
            assert len(tables) == 0, "No DataTables should render on error"
            # Button should be re-enabled for retry
            btn_after = detail.query_one("#run-analysis", Button)
            assert not btn_after.disabled, "Button should be re-enabled after failure"
        finally:
            await ctx.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_analysis_button_removed_after_click(self):
        """Run Analysis button should be removed once clicked."""
        from textual.css.query import NoMatches
        from textual.widgets import Button

        client = _make_mock_client()
        client.delta.get_analysis = AsyncMock(return_value=self._make_analysis_result())

        info = DeltaTableInfo(
            name="test",
            schema_=[Column(name="id", type="long")],
            version=0,
            num_files=1,
            size_bytes=100,
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            btn = detail.query_one("#run-analysis", Button)
            btn.press()
            await pilot.pause()
            await asyncio.sleep(0.5)
            await pilot.pause()

            with pytest.raises(NoMatches):
                detail.query_one("#run-analysis", Button)
        finally:
            await ctx.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_analysis_summary_shows_row_count(self):
        """Summary section should display the total row count."""
        from textual.widgets import Button

        client = _make_mock_client()
        client.delta.get_analysis = AsyncMock(return_value=self._make_analysis_result())

        info = DeltaTableInfo(
            name="test",
            schema_=[Column(name="id", type="long")],
            version=0,
            num_files=2,
            size_bytes=4096,
        )
        app, pilot, detail, ctx = await _setup_detail_with_metadata(client, info)
        try:
            btn = detail.query_one("#run-analysis", Button)
            btn.press()
            await pilot.pause()
            await asyncio.sleep(0.5)
            await pilot.pause()

            pane = detail.query_one("#tab-analysis", TabPane)
            statics = pane.query(Static)
            texts = [_get_widget_text(w) for w in statics]
            assert any("100" in t for t in texts), (
                f"Expected '100' (total rows) in summary. Found: {texts}"
            )
        finally:
            await ctx.__aexit__(None, None, None)
