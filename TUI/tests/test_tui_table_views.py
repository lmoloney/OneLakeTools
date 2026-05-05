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
    """Extract plain text from a rendered Textual widget."""
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
            assert len(panes) == 3, (
                f"Expected exactly 3 tabs (Schema, Data, History). Found {len(panes)}"
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
