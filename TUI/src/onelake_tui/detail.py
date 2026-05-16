from __future__ import annotations

import asyncio
import contextlib
import csv
import io
import itertools
import json
import logging
from datetime import UTC, datetime

from rich.markup import escape as esc
from rich.syntax import Syntax
from textual import work
from textual.app import ComposeResult
from textual.containers import VerticalScroll
from textual.css.query import NoMatches
from textual.widgets import (
    Button,
    DataTable,
    Label,
    LoadingIndicator,
    Markdown,
    Static,
    TabbedContent,
    TabPane,
    TextArea,
)

from onelake_client import OneLakeClient
from onelake_client.exceptions import FileTooLargeError
from onelake_client.tables import coerce_timestamps, is_cdf_not_enabled_error
from onelake_tui.nodes import FileNode, FolderNode, TableNode
from onelake_tui.sprite import OneLakeSprite, get_welcome

logger = logging.getLogger("onelake_tui.detail")

NodeData = FolderNode | FileNode | TableNode | None

_MAX_PREVIEW_BYTES = 512 * 1024  # 512KB text preview limit
_MAX_BINARY_BYTES = 50 * 1024 * 1024  # 50MB binary preview limit
_MAX_DELTA_LOG_BYTES = 2 * 1024 * 1024  # 2MB per _delta_log file
_MAX_HISTORY_FILES = 20
_HISTORY_FETCH_CONCURRENCY = 4

_SYNTAX_LEXERS = {
    ".py": "python",
    ".sql": "sql",
    ".json": "json",
    ".yaml": "yaml",
    ".yml": "yaml",
    ".xml": "xml",
    ".html": "html",
    ".js": "javascript",
    ".ts": "typescript",
    ".sh": "bash",
    ".toml": "toml",
    ".ini": "ini",
    ".cfg": "ini",
    ".r": "r",
    ".scala": "scala",
    ".java": "java",
    ".cs": "csharp",
    ".cpp": "cpp",
    ".c": "c",
    ".rs": "rust",
    ".go": "go",
    ".rb": "ruby",
    ".txt": "text",
    ".log": "text",
}


class DetailPanel(VerticalScroll):
    """Panel showing details of the currently selected tree node."""

    DEFAULT_CSS = """
    DetailPanel {
        padding: 1 2;
    }
    DetailPanel .detail-title {
        text-style: bold;
        color: $accent;
        margin-bottom: 1;
    }
    DetailPanel .detail-section {
        margin-bottom: 1;
    }
    DetailPanel .detail-label {
        color: $text-muted;
    }
    DetailPanel .detail-value {
        margin-left: 2;
    }
    DetailPanel DataTable {
        height: auto;
        max-height: 20;
        margin-top: 1;
    }
    """

    def __init__(self, client: OneLakeClient, **kwargs):
        super().__init__(**kwargs)
        self.client = client
        self._workspace_name: str = ""
        self._item_name: str = ""
        self._current_table_data: TableNode | None = None
        self._current_file_data: FileNode | None = None
        self._current_delta_info = None
        self._analysis_file_paths: dict[str, tuple[str, str]] = {}
        self._analysis_result = None
        self._data_preview_loaded: bool = False
        self._debounce_timer = None
        self._pending_node: NodeData = None

    def set_context(self, workspace_name: str, item_name: str) -> None:
        """Set human-readable names for path display."""
        self._workspace_name = workspace_name
        self._item_name = item_name

    def compose(self) -> ComposeResult:
        yield OneLakeSprite(id="detail-content")

    def update_for_node(self, data: NodeData) -> None:
        """Update the panel — debounced to avoid API spam on rapid arrow keys."""
        self._pending_node = data
        if self._debounce_timer is not None:
            self._debounce_timer.stop()
        self._debounce_timer = self.set_timer(0.15, self._apply_pending_node)

    def _apply_pending_node(self) -> None:
        """Apply the debounced node update."""
        if not self.is_mounted:
            return
        data = self._pending_node
        if data is None:
            self._show_placeholder()
        elif isinstance(data, FolderNode):
            self._show_folder(data)
        elif isinstance(data, FileNode):
            self._show_file(data)
        elif isinstance(data, TableNode):
            self._show_table(data)

    def _clear(self) -> None:
        """Remove all children."""
        self._current_table_data = None
        self._current_file_data = None
        self._current_delta_info = None
        self._analysis_file_paths = {}
        self._analysis_result = None
        self._data_preview_loaded = False
        self.remove_children()

    def _show_placeholder(self) -> None:
        self._clear()
        self.mount(Static(get_welcome()))

    def _show_folder(self, data: FolderNode) -> None:
        self._clear()
        folder_name = data.directory.split("/")[-1] if "/" in data.directory else data.directory
        self.mount(Label(f"📂 {folder_name}", classes="detail-title"))
        rel = data.directory.split("/", 1)[-1] if "/" in data.directory else data.directory
        friendly = f"{self._workspace_name} / {self._item_name} / {rel}"
        self.mount(Static(f"[b]Path:[/b] {esc(friendly)}", classes="detail-section"))

    def _show_file(self, data: FileNode) -> None:
        self._clear()
        self._current_file_data = data
        file_name = data.path.split("/")[-1]
        self.mount(Label(f"📄 {file_name}", classes="detail-title"))
        rel = data.path.split("/", 1)[-1] if "/" in data.path else data.path
        friendly = f"{self._workspace_name} / {self._item_name} / {rel}"
        self.mount(Static(f"[b]Path:[/b] {esc(friendly)}", classes="detail-section"))
        self.mount(Static(f"[b]Size:[/b] {_format_size(data.size)}", classes="detail-section"))
        self._load_file_properties(data)

    @work(exclusive=True, group="detail_load")
    async def _load_file_properties(self, data: FileNode) -> None:
        """Load file properties via DFS HEAD request."""
        try:
            props = await self.client.dfs.get_properties(data.workspace, data.path)
            if props.content_type:
                self.mount(
                    Static(
                        f"[b]Content type:[/b] {esc(props.content_type)}",
                        classes="detail-section",
                    )
                )
            if props.last_modified:
                self.mount(
                    Static(
                        f"[b]Modified:[/b] {esc(props.last_modified.isoformat())}",
                        classes="detail-section",
                    )
                )
        except Exception as e:
            logger.debug("Could not load file properties: %s", e)

    # ── Delta table tabbed view ─────────────────────────────────────────

    def _show_table(self, data: TableNode) -> None:
        self._clear()
        self._current_table_data = data
        self.mount(Label(f"🗃️ {data.table_name}", classes="detail-title"))
        friendly = f"{self._workspace_name} / {self._item_name} / Tables / {data.table_name}"
        self.mount(Static(f"[b]Path:[/b] {esc(friendly)}", classes="detail-section"))
        self.mount(LoadingIndicator(classes="table-loading"))
        self._load_table_metadata(data)

    @work(exclusive=True, group="detail_load")
    async def _load_table_metadata(self, data: TableNode) -> None:
        """Load Delta table metadata and build tabbed view."""
        try:
            logger.debug(
                "Loading delta metadata: workspace=%s item=%s table=%s",
                data.workspace,
                data.item_path,
                data.table_name,
            )
            # Guard: verify _delta_log exists before calling deltalake.
            # Schema folders (e.g. Tables/dbo) don't have _delta_log and
            # the Rust FFI can panic instead of raising a Python exception.
            delta_log_path = f"{data.item_path}/Tables/{data.table_name}/_delta_log"
            try:
                has_delta = await self.client.dfs.exists(data.workspace, delta_log_path)
            except Exception as check_err:
                logger.debug("Failed to check _delta_log existence: %s", check_err)
                has_delta = True  # optimistic: try loading, let error handler catch it
            if self._current_table_data is not data:
                return
            if not has_delta:
                with contextlib.suppress(NoMatches):
                    self.query_one(".table-loading").remove()
                self.mount(
                    Static(
                        "[dim]Not a Delta table — this may be a schema folder. "
                        "Expand the node in the tree to browse its tables.[/dim]",
                        classes="detail-section",
                    )
                )
                return
            info = await self.client.delta.get_metadata(
                data.workspace, data.item_path, data.table_name
            )
            if self._current_table_data is not data:
                return
            self._current_delta_info = info

            # Remove loading spinner
            with contextlib.suppress(NoMatches):
                self.query_one(".table-loading").remove()

            # Build tabbed interface
            tc = TabbedContent(id="table-tabs")
            await self.mount(tc)

            # ── Schema tab ──────────────────────────────────────────────
            schema_pane = TabPane("Schema", id="tab-schema")
            await tc.add_pane(schema_pane)
            rows_text = f"  [b]Rows:[/b] {info.total_rows:,}" if info.total_rows is not None else ""
            await schema_pane.mount(
                Static(
                    f"[b]Version:[/b] {info.version}  "
                    f"[b]Files:[/b] {info.num_files}  "
                    f"[b]Size:[/b] {_format_size(info.size_bytes)}{rows_text}",
                    classes="detail-section",
                )
            )
            # Protocol info
            proto_text = (
                f"[b]Protocol:[/b] Reader v{info.reader_version} / Writer v{info.writer_version}"
            )
            if info.reader_features:
                proto_text += f"  [b]Reader Features:[/b] {esc(', '.join(info.reader_features))}"
            if info.writer_features:
                proto_text += f"  [b]Writer Features:[/b] {esc(', '.join(info.writer_features))}"
            await schema_pane.mount(Static(proto_text, classes="detail-section"))
            if info.partition_columns:
                await schema_pane.mount(
                    Static(
                        f"[b]Partitioned by:[/b] {esc(', '.join(info.partition_columns))}",
                        classes="detail-section",
                    )
                )
            # Clustering columns (liquid clustering)
            clustering_cols = info.properties.get("clusteringColumns")
            if not clustering_cols:
                clustering_cols = info.properties.get("delta.liquid.clustering.table.columns")
            if clustering_cols:
                await schema_pane.mount(
                    Static(
                        f"[b]Clustered by:[/b] {esc(clustering_cols)}",
                        classes="detail-section",
                    )
                )
            if info.description:
                await schema_pane.mount(
                    Static(
                        f"[b]Description:[/b] {esc(info.description)}",
                        classes="detail-section",
                    )
                )
            # Proactive warnings
            if info.warnings:
                for warning in info.warnings:
                    await schema_pane.mount(
                        Static(
                            f"[yellow]{esc(warning)}[/yellow]",
                            classes="detail-section",
                        )
                    )
            if info.schema_:
                await schema_pane.mount(Label("Columns", classes="detail-title"))
                schema_table = DataTable()
                await schema_pane.mount(schema_table)
                schema_table.add_columns("Name", "Type", "Nullable")
                for col in info.schema_:
                    schema_table.add_row(col.name, col.type, "✓" if col.nullable else "✗")

            # ── Data Preview tab (lazy) ─────────────────────────────────
            data_pane = TabPane("Data", id="tab-data")
            await tc.add_pane(data_pane)
            await data_pane.mount(
                Button(
                    "Load Data Preview",
                    id="load-data-preview",
                    variant="primary",
                )
            )
            await data_pane.mount(
                Static(
                    "[dim]Reads parquet data files from OneLake (first 100 rows)[/dim]",
                    classes="detail-section",
                )
            )

            # ── Transactions tab ────────────────────────────────────────
            txn_pane = TabPane("History", id="tab-history")
            await tc.add_pane(txn_pane)
            await txn_pane.mount(LoadingIndicator(id="txn-loading"))
            self._load_transaction_log(data)

            # ── CDF tab (conditional) ───────────────────────────────────
            cdf_enabled = info.properties.get("delta.enableChangeDataFeed") == "true"
            if cdf_enabled:
                cdf_pane = TabPane("CDF", id="tab-cdf")
                await tc.add_pane(cdf_pane)
                await cdf_pane.mount(
                    Static(
                        "[b]Change Data Feed[/b] is enabled for this table.",
                        classes="detail-section",
                    )
                )
                await cdf_pane.mount(
                    Button(
                        "Load CDF Preview",
                        id="load-cdf-preview",
                        variant="primary",
                    )
                )
                await cdf_pane.mount(
                    Static(
                        "[dim]Shows recent change records "
                        "(_change_type, _commit_version, _commit_timestamp)[/dim]",
                        classes="detail-section",
                    )
                )

            # ── Analysis tab (lazy) ─────────────────────────────────────
            analysis_pane = TabPane("Analysis", id="tab-analysis")
            await tc.add_pane(analysis_pane)
            await analysis_pane.mount(
                Button(
                    "Run Analysis",
                    id="run-analysis",
                    variant="primary",
                )
            )
            await analysis_pane.mount(
                Static(
                    "[dim]Reads parquet file metadata from OneLake "
                    "(may take a moment for tables with many files)[/dim]",
                    classes="detail-section",
                )
            )

        except Exception as e:
            with contextlib.suppress(NoMatches):
                self.query_one(".table-loading").remove()
            err_msg = str(e)
            if "No files in log" in err_msg or "log segment" in err_msg:
                self.mount(
                    Static(
                        "[dim]Not a Delta table (may be Iceberg or empty). "
                        "Expand the node in the tree to browse raw files.[/dim]",
                        classes="detail-section",
                    )
                )
            elif "reader features" in err_msg or "minimum reader version" in err_msg:
                self.mount(
                    Static(
                        "⚠️ [yellow]This table uses advanced Delta features "
                        "not fully supported by the local reader. "
                        "Metadata could not be loaded — browse the table's "
                        "files directly in the tree view.[/yellow]",
                        classes="detail-section",
                    )
                )
            else:
                self.mount(
                    Static(
                        f"❌ Could not load metadata: {esc(err_msg)}",
                        classes="detail-section",
                    )
                )
            logger.debug("Table metadata unavailable for %s: %s", data.table_name, e)

    @work(group="detail_aux", exclusive=True)
    async def _load_transaction_log(self, data: TableNode) -> None:
        """Read _delta_log/*.json commit files and display as summary table."""
        table_data = self._current_table_data
        try:
            log_dir = f"{data.item_path}/Tables/{data.table_name}/_delta_log"
            paths = await self.client.dfs.list_paths(data.workspace, log_dir)

            json_files = sorted(
                [p for p in paths if p.name.endswith(".json")],
                key=lambda p: p.name,
                reverse=True,
            )[:_MAX_HISTORY_FILES]

            commits: list[dict] = []
            semaphore = asyncio.Semaphore(_HISTORY_FETCH_CONCURRENCY)

            async def _read_commit_file(path_info) -> list[dict]:
                file_commits: list[dict] = []
                async with semaphore:
                    raw = await self.client.dfs.read_file(
                        data.workspace,
                        path_info.name,
                        max_bytes=_MAX_DELTA_LOG_BYTES,
                    )

                # Collect metaData configuration from the same log file
                config_changes: dict[str, str] = {}
                commit_lines: list[dict] = []
                for line in raw.decode("utf-8", errors="replace").strip().splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    if "metaData" in obj:
                        cfg = obj["metaData"].get("configuration", {})
                        if cfg:
                            config_changes = cfg

                    if "commitInfo" in obj:
                        commit_lines.append(obj)

                for obj in commit_lines:
                    ci = obj["commitInfo"]
                    fname = path_info.name.split("/")[-1]
                    version = fname.replace(".json", "").lstrip("0") or "0"
                    ts_raw = ci.get("inCommitTimestamp") or ci.get("timestamp")
                    if ts_raw is None and path_info.last_modified:
                        ts_raw = path_info.last_modified
                    file_commits.append(
                        {
                            "version": version,
                            "timestamp": ts_raw,
                            "operation": ci.get("operation", ""),
                            "metrics": ci.get("operationMetrics", {}),
                            "configuration": config_changes,
                        }
                    )
                return file_commits

            results = await asyncio.gather(
                *(_read_commit_file(pf) for pf in json_files),
                return_exceptions=True,
            )
            for result in results:
                if isinstance(result, Exception):
                    logger.debug("Skipping unreadable commit file: %s", result)
                    continue
                commits.extend(result)

            with contextlib.suppress(NoMatches):
                self.query_one("#txn-loading").remove()

            if self._current_table_data is not table_data:
                return

            txn_pane = self.query_one("#tab-history", TabPane)
            if commits:
                tbl = DataTable(id="txn-table")
                await txn_pane.mount(tbl)
                tbl.add_columns("Version", "Timestamp", "Operation", "Details")
                for c in commits:
                    ts = c["timestamp"]
                    if isinstance(ts, datetime):
                        ts = ts.strftime("%Y-%m-%d %H:%M:%S UTC")
                    elif isinstance(ts, (int, float)):
                        ts = datetime.fromtimestamp(ts / 1000, tz=UTC).strftime(
                            "%Y-%m-%d %H:%M:%S UTC"
                        )
                    else:
                        ts = str(ts) if ts else ""
                    metrics = c.get("metrics") or {}
                    metrics_str = (
                        ", ".join(f"{k}={v}" for k, v in metrics.items()) if metrics else ""
                    )
                    config = c.get("configuration") or {}
                    config_str = ", ".join(f"{k}={v}" for k, v in config.items()) if config else ""
                    details_parts = [p for p in [metrics_str, config_str] if p]
                    details = "\n".join(details_parts)
                    row_height = len(details_parts) if len(details_parts) > 1 else 1
                    tbl.add_row(
                        str(c["version"]),
                        str(ts),
                        c["operation"],
                        details,
                        height=row_height,
                    )
            else:
                await txn_pane.mount(Static("[dim]No transaction history found[/dim]"))
        except Exception as e:
            with contextlib.suppress(NoMatches):
                self.query_one("#txn-loading").remove()
            try:
                txn_pane = self.query_one("#tab-history", TabPane)
                await txn_pane.mount(
                    Static(f"❌ Could not load history: {esc(str(e))}", classes="detail-section")
                )
            except Exception:
                pass
            logger.debug("Transaction log load failed: %s", e)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle lazy-load buttons in delta table tabs."""
        if event.button.id == "load-data-preview":
            self._load_data_preview()
        elif event.button.id == "load-cdf-preview":
            self._load_cdf_preview()
        elif event.button.id == "search-cdf-range":
            self._search_cdf_range()
        elif event.button.id == "run-analysis":
            self._load_analysis()
        elif event.button.id == "analysis-back-overview":
            self._show_cached_analysis()
        elif event.button.id == "analyze-parquet-file":
            self._analyze_parquet_file()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """Handle row selection in analysis files table for drill-down."""
        dt = event.data_table
        if "analysis-files-table" not in dt.classes:
            return
        row = dt.get_row(event.row_key)
        # First column is the file name (may be Rich-escaped)
        from rich.text import Text

        file_name = str(row[0]) if not isinstance(row[0], Text) else row[0].plain
        if file_name in self._analysis_file_paths:
            self._drill_into_parquet_file(file_name)

    @work(group="detail_aux", exclusive=True)
    async def _load_data_preview(self) -> None:
        """Fetch first 100 rows from the Delta table's parquet files."""
        table_data = self._current_table_data
        if table_data is None:
            return

        with contextlib.suppress(NoMatches):
            self.query_one("#load-data-preview", Button).remove()

        data_pane = self.query_one("#tab-data", TabPane)
        for child in list(data_pane.children):
            child.remove()
        await data_pane.mount(
            Static("Loading data preview…", id="data-loading", classes="detail-section")
        )

        try:
            sample = await self.client.delta.read_sample(
                table_data.workspace, table_data.item_path, table_data.table_name
            )
            if self._current_table_data is not table_data:
                return
            await self._render_data_table(data_pane, sample)
        except Exception as e:
            err_msg = str(e)
            # Handle unsupported reader features (e.g. deletionVectors, timestampNtz)
            if "reader features" in err_msg or "minimum reader version" in err_msg:
                logger.debug(
                    "Delta reader unsupported features, falling back to DFS parquet: %s", e
                )
                with contextlib.suppress(NoMatches):
                    self.query_one("#data-loading").remove()
                await data_pane.mount(
                    Static(
                        "⚠️ [yellow]Table uses advanced Delta features "
                        "not supported by the local reader. "
                        "Falling back to raw parquet preview — "
                        "may include soft-deleted rows.[/yellow]",
                        classes="detail-section",
                    )
                )
                try:
                    sample = await self._read_parquet_fallback(table_data)
                    await self._render_data_table(data_pane, sample)
                except Exception as fallback_err:
                    await data_pane.mount(
                        Static(
                            f"❌ Fallback preview also failed: {esc(str(fallback_err))}",
                            classes="detail-section",
                        )
                    )
                    logger.debug("Parquet fallback failed: %s", fallback_err)
            else:
                with contextlib.suppress(NoMatches):
                    self.query_one("#data-loading").remove()
                await data_pane.mount(
                    Static(f"❌ Data preview failed: {esc(err_msg)}", classes="detail-section")
                )
                logger.debug("Data preview failed: %s", e)

    async def _render_data_table(self, pane: TabPane, sample) -> None:
        """Render a pyarrow.Table sample into a DataTable widget."""
        with contextlib.suppress(NoMatches):
            self.query_one("#data-loading").remove()

        if sample.num_rows == 0:
            await pane.mount(Static("[dim]Table is empty[/dim]"))
            return

        await pane.mount(
            Static(
                f"[dim]Showing first {sample.num_rows} rows[/dim]",
                classes="detail-section",
            )
        )
        tbl = DataTable(id="data-preview-table")
        await pane.mount(tbl)
        col_names = sample.column_names
        tbl.add_columns(*col_names)
        for row_idx in range(sample.num_rows):
            row = [str(sample.column(c)[row_idx]) for c in range(len(col_names))]
            tbl.add_row(*row)
        self._data_preview_loaded = True

    async def _read_parquet_fallback(self, data: TableNode):
        """Read parquet files directly via DFS when deltalake library fails."""
        import pyarrow.parquet as pq

        table_dir = f"{data.item_path}/Tables/{data.table_name}"
        paths = await self.client.dfs.list_paths(data.workspace, table_dir)

        parquet_files = [p for p in paths if not p.is_directory and p.name.endswith(".parquet")]
        if not parquet_files:
            raise FileNotFoundError("No parquet files found in table directory")

        # Prefer known-size parquet files first (largest to smallest), then unknown-size files.
        # Tuple sort key is `(has_known_size, size_or_zero)` with `reverse=True`, so `True`
        # (known size, even when zero) sorts before `False` (unknown size / None).
        parquet_files.sort(
            key=lambda p: (p.content_length is not None, p.content_length or 0),
            reverse=True,
        )
        size_filtered = [
            p
            for p in parquet_files
            if p.content_length is None or p.content_length <= _MAX_BINARY_BYTES
        ]
        over_limit_msg = (
            f"No parquet files are within the {_format_size(_MAX_BINARY_BYTES)} preview limit"
        )
        if not size_filtered:
            raise ValueError(over_limit_msg)

        for target in size_filtered:
            try:
                raw = await self.client.dfs.read_file(
                    data.workspace,
                    target.name,
                    max_bytes=_MAX_BINARY_BYTES,
                )
            except FileTooLargeError:
                logger.debug(
                    "Skipping parquet candidate over preview limit despite "
                    "missing/incorrect size: %s",
                    target.name,
                )
                continue

            pf = pq.ParquetFile(io.BytesIO(raw))
            sample = pf.read_row_groups([0]).slice(0, 100)
            return coerce_timestamps(sample)

        raise ValueError(over_limit_msg)

    @work(group="detail_aux", exclusive=True)
    async def _load_cdf_preview(self) -> None:
        """Load Change Data Feed records from the Delta table.

        Strategy: start from the latest version (fast, always works regardless
        of when CDF was enabled).  If the latest version returns 0 rows,
        auto-expand to the last 10 versions.  Always offer a "Load Earlier
        Versions" button for binary-search discovery of the full CDF range.
        """
        table_data = self._current_table_data
        delta_info = self._current_delta_info
        if table_data is None or delta_info is None:
            return

        with contextlib.suppress(NoMatches):
            self.query_one("#load-cdf-preview", Button).remove()

        cdf_pane = self.query_one("#tab-cdf", TabPane)
        for child in list(cdf_pane.children):
            child.remove()

        await cdf_pane.mount(
            Static(
                f"Loading CDF data (version {delta_info.version})…",
                id="cdf-loading",
                classes="detail-section",
            )
        )

        try:
            # 1. Try latest version first
            cdf_table = await self.client.delta.read_cdf(
                table_data.workspace,
                table_data.item_path,
                table_data.table_name,
                starting_version=delta_info.version,
            )

            # 2. If 0 rows, auto-expand to last 10 versions
            starting = delta_info.version
            if cdf_table.num_rows == 0 and delta_info.version > 0:
                expanded_start = max(0, delta_info.version - 10)
                with contextlib.suppress(NoMatches):
                    loading = self.query_one("#cdf-loading", Static)
                    loading.update(
                        f"No records at latest version — "
                        f"expanding to versions {expanded_start}–{delta_info.version}…"
                    )
                try:
                    cdf_table = await self.client.delta.read_cdf(
                        table_data.workspace,
                        table_data.item_path,
                        table_data.table_name,
                        starting_version=expanded_start,
                    )
                    starting = expanded_start
                except Exception as expand_err:
                    if is_cdf_not_enabled_error(expand_err):
                        # CDF was enabled after creation — discover the
                        # actual start within the expanded window
                        try:
                            discovered = await self.client.delta.find_cdf_start_version(
                                table_data.workspace,
                                table_data.item_path,
                                table_data.table_name,
                                low=expanded_start,
                                high=delta_info.version,
                            )
                            cdf_table = await self.client.delta.read_cdf(
                                table_data.workspace,
                                table_data.item_path,
                                table_data.table_name,
                                starting_version=discovered,
                            )
                            starting = discovered
                        except Exception:
                            # Discovery failed — keep the latest-only result
                            starting = delta_info.version
                    else:
                        raise

            if self._current_table_data is not table_data:
                return

            await self._render_cdf_result(cdf_pane, cdf_table, starting, delta_info)
        except Exception as e:
            with contextlib.suppress(NoMatches):
                self.query_one("#cdf-loading").remove()
            if is_cdf_not_enabled_error(e):
                await cdf_pane.mount(
                    Static(
                        "❌ CDF appears enabled in table properties "
                        "but no readable CDF versions were found.",
                        classes="detail-section",
                    )
                )
            else:
                await cdf_pane.mount(
                    Static(
                        f"❌ CDF preview failed: {esc(str(e))}",
                        classes="detail-section",
                    )
                )
            logger.debug("CDF preview failed: %s", e)

    async def _render_cdf_result(self, cdf_pane, cdf_table, starting, delta_info):
        """Render CDF data with a 'Load Earlier Versions' button."""
        if self._current_table_data is None:
            return

        with contextlib.suppress(NoMatches):
            self.query_one("#cdf-loading").remove()

        # Always show the "Load Earlier Versions" button
        await cdf_pane.mount(
            Button(
                "Load Earlier Versions",
                id="search-cdf-range",
                variant="default",
            )
        )

        if cdf_table.num_rows == 0:
            await cdf_pane.mount(
                Static(
                    "[dim]No CDF records found in recent versions.[/dim]",
                    classes="detail-section",
                )
            )
            return

        version_label = (
            f"version {starting}"
            if starting == delta_info.version
            else f"versions {starting}–{delta_info.version}"
        )
        await cdf_pane.mount(
            Static(
                f"[dim]Showing {min(cdf_table.num_rows, 100)} of {cdf_table.num_rows} "
                f"CDF records ({version_label})[/dim]",
                classes="detail-section",
            )
        )
        await self._render_cdf_table(cdf_pane, cdf_table)

    async def _render_cdf_table(self, pane, cdf_table):
        """Mount a DataTable widget populated with CDF rows."""
        tbl = DataTable(id="cdf-table")
        await pane.mount(tbl)
        col_names = cdf_table.column_names
        tbl.add_columns(*col_names)
        for row_idx in range(min(cdf_table.num_rows, 100)):
            row = []
            for c in range(len(col_names)):
                val = cdf_table.column(c)[row_idx]
                row.append(str(val.as_py() if hasattr(val, "as_py") else val))
            tbl.add_row(*row)

    @work(group="detail_aux", exclusive=True)
    async def _search_cdf_range(self) -> None:
        """Binary-search for the earliest CDF-enabled version, then re-render."""
        table_data = self._current_table_data
        delta_info = self._current_delta_info
        if table_data is None or delta_info is None:
            return

        with contextlib.suppress(NoMatches):
            self.query_one("#search-cdf-range", Button).remove()

        cdf_pane = self.query_one("#tab-cdf", TabPane)
        for child in list(cdf_pane.children):
            child.remove()
        await cdf_pane.mount(
            Static(
                "Searching for earliest CDF-enabled version — this may take a moment…",
                id="cdf-loading",
                classes="detail-section",
            )
        )

        try:
            start_ver = await self.client.delta.find_cdf_start_version(
                table_data.workspace,
                table_data.item_path,
                table_data.table_name,
                low=0,
                high=delta_info.version,
            )
            if self._current_table_data is not table_data:
                return

            cdf_table = await self.client.delta.read_cdf(
                table_data.workspace,
                table_data.item_path,
                table_data.table_name,
                starting_version=start_ver,
            )
            if self._current_table_data is not table_data:
                return

            with contextlib.suppress(NoMatches):
                self.query_one("#cdf-loading").remove()

            await cdf_pane.mount(
                Static(
                    f"[dim]CDF available from version {start_ver} "
                    f"(table has {delta_info.version + 1} versions). "
                    f"Showing {min(cdf_table.num_rows, 100)} of "
                    f"{cdf_table.num_rows} records.[/dim]",
                    classes="detail-section",
                )
            )
            if cdf_table.num_rows > 0:
                await self._render_cdf_table(cdf_pane, cdf_table)
            else:
                await cdf_pane.mount(Static("[dim]No change records found in the CDF range.[/dim]"))
        except Exception as e:
            with contextlib.suppress(NoMatches):
                self.query_one("#cdf-loading").remove()
            await cdf_pane.mount(
                Static(
                    f"❌ CDF range search failed: {esc(str(e))}",
                    classes="detail-section",
                )
            )
            logger.debug("CDF range search failed: %s", e)

    # ── Delta Analysis ──────────────────────────────────────────────────

    @work(group="detail_aux", exclusive=True)
    async def _show_cached_analysis(self) -> None:
        """Re-render analysis from cached results (no API calls)."""
        if self._analysis_result is None:
            self._load_analysis()
            return

        analysis_pane = self.query_one("#tab-analysis", TabPane)
        for child in list(analysis_pane.children):
            child.remove()
        await self._render_analysis(analysis_pane, self._analysis_result)

    @work(group="detail_aux", exclusive=True)
    async def _load_analysis(self) -> None:
        """Run Delta Analysis — read parquet metadata and display statistics."""
        table_data = self._current_table_data
        if table_data is None:
            return

        with contextlib.suppress(NoMatches):
            self.query_one("#run-analysis", Button).remove()

        analysis_pane = self.query_one("#tab-analysis", TabPane)
        for child in list(analysis_pane.children):
            child.remove()
        await analysis_pane.mount(
            Static(
                "[dim]Starting analysis…[/dim]",
                id="analysis-progress",
                classes="detail-section",
            )
        )

        try:

            async def _progress(current: int, total: int, filename: str) -> None:
                with contextlib.suppress(NoMatches):
                    self.query_one("#analysis-progress", Static).update(
                        f"[dim]Analysing file {current} of {total}: {esc(filename)}[/dim]"
                    )

            result = await self.client.delta.get_analysis(
                table_data.workspace,
                table_data.item_path,
                table_data.table_name,
                progress_callback=_progress,
            )

            if self._current_table_data is not table_data:
                return

            with contextlib.suppress(NoMatches):
                self.query_one("#analysis-progress", Static).remove()

            await self._render_analysis(analysis_pane, result)
            self._analysis_file_paths = result.file_paths
            self._analysis_result = result

        except Exception as e:
            with contextlib.suppress(NoMatches):
                self.query_one("#analysis-progress", Static).remove()
            self.notify(f"Analysis failed: {e}", severity="error", markup=False)
            logger.exception("Delta analysis failed for %s", table_data.table_name)

    async def _render_analysis(self, pane: TabPane, result) -> None:
        """Render the 5 analysis sections as DataTables."""
        s = result.summary

        # ── Summary ─────────────────────────────────────────────────
        await pane.mount(Label("Summary", classes="detail-title"))
        skipped_text = (
            f"  [yellow]({s.files_skipped} files skipped)[/yellow]"
            if s.files_skipped
            else ""
        )
        await pane.mount(
            Static(
                f"[b]Rows:[/b] {s.total_rows:,}  "
                f"[b]Files:[/b] {s.total_files}{skipped_text}  "
                f"[b]Row Groups:[/b] {s.total_row_groups}",
                classes="detail-section",
            )
        )
        await pane.mount(
            Static(
                f"[b]Rows/RG:[/b] avg {s.avg_rows_per_row_group:,.0f} · "
                f"min {s.min_rows_per_row_group:,} · max {s.max_rows_per_row_group:,}",
                classes="detail-section",
            )
        )
        await pane.mount(
            Static(
                f"[b]Compressed:[/b] {_format_size(s.total_compressed_size)}  "
                f"[b]Uncompressed:[/b] {_format_size(s.total_uncompressed_size)}",
                classes="detail-section",
            )
        )

        # ── Parquet Files (clickable for drill-down) ──────────────────
        if result.files:
            await pane.mount(
                Static(
                    "[dim]Select a file and press Enter to drill into it[/dim]",
                    classes="detail-section",
                )
            )
            await pane.mount(Label("Parquet Files", classes="detail-title"))
            files_table = DataTable(classes="analysis-files-table", cursor_type="row")
            await pane.mount(files_table)
            files_table.add_columns("File", "Rows", "Row Groups", "Created By")
            for f in result.files:
                files_table.add_row(
                    esc(f.file_name),
                    f"{f.row_count:,}",
                    str(f.row_group_count),
                    esc(f.created_by or ""),
                )

        # ── Row Groups ──────────────────────────────────────────────
        if result.row_groups:
            await pane.mount(Label("Row Groups", classes="detail-title"))
            rg_table = DataTable()
            await pane.mount(rg_table)
            rg_table.add_columns(
                "RG", "File", "Rows", "Compressed", "Uncompressed", "Ratio"
            )
            for rg in result.row_groups:
                rg_table.add_row(
                    str(rg.row_group_id),
                    esc(rg.file_name),
                    f"{rg.row_count:,}",
                    _format_size(rg.compressed_size),
                    _format_size(rg.uncompressed_size),
                    f"{rg.compression_ratio:.1%}",
                )

        # ── Column Chunks ───────────────────────────────────────────
        if result.column_chunks:
            await pane.mount(Label("Column Chunks", classes="detail-title"))
            cc_table = DataTable()
            await pane.mount(cc_table)
            cc_table.add_columns(
                "File", "RG", "Col", "Name", "Type", "Compressed", "Uncompressed", "Values"
            )
            for cc in result.column_chunks:
                cc_table.add_row(
                    esc(cc.file_name),
                    str(cc.row_group_id),
                    str(cc.column_id),
                    esc(cc.column_name),
                    cc.physical_type,
                    _format_size(cc.compressed_size),
                    _format_size(cc.uncompressed_size),
                    f"{cc.num_values:,}",
                )

        # ── Columns (aggregated) ────────────────────────────────────
        if result.columns:
            await pane.mount(Label("Columns", classes="detail-title"))
            col_table = DataTable()
            await pane.mount(col_table)
            col_table.add_columns(
                "Col", "Name", "Compressed", "Uncompressed", "% of Table"
            )
            for c in result.columns:
                col_table.add_row(
                    str(c.column_id),
                    esc(c.column_name),
                    _format_size(c.total_compressed_size),
                    _format_size(c.total_uncompressed_size),
                    f"{c.pct_of_table:.1%}",
                )

    # ── Parquet file analysis ──────────────────────────────────────────────

    @work(group="detail_aux", exclusive=True)
    async def _drill_into_parquet_file(self, file_name: str) -> None:
        """Drill into a single parquet file from the analysis files table."""
        table_data = self._current_table_data
        if table_data is None:
            return

        ws, dfs_path = self._analysis_file_paths[file_name]

        analysis_pane = self.query_one("#tab-analysis", TabPane)
        for child in list(analysis_pane.children):
            child.remove()
        await analysis_pane.mount(
            Static(
                f"[dim]Analysing {esc(file_name)}…[/dim]",
                classes="detail-section",
            )
        )

        try:
            result = await self.client.delta.analyze_parquet_file(ws, dfs_path)

            if self._current_table_data is not table_data:
                return

            for child in list(analysis_pane.children):
                child.remove()

            await analysis_pane.mount(
                Button("← Back to Overview", id="analysis-back-overview", variant="default")
            )
            await analysis_pane.mount(
                Label(f"📄 {file_name}", classes="detail-title")
            )
            await self._render_analysis_in_pane(analysis_pane, result)

        except Exception as e:
            for child in list(analysis_pane.children):
                child.remove()
            self.notify(
                f"File analysis failed: {e}", severity="error", markup=False
            )
            await analysis_pane.mount(
                Button("← Back to Overview", id="analysis-back-overview", variant="default")
            )

    async def _render_analysis_in_pane(self, pane, result) -> None:
        """Render single-file analysis inside a TabPane."""
        s = result.summary
        await pane.mount(
            Static(
                f"[b]Rows:[/b] {s.total_rows:,}  "
                f"[b]Row Groups:[/b] {s.total_row_groups}",
                classes="detail-section",
            )
        )
        await pane.mount(
            Static(
                f"[b]Compressed:[/b] {_format_size(s.total_compressed_size)}  "
                f"[b]Uncompressed:[/b] {_format_size(s.total_uncompressed_size)}",
                classes="detail-section",
            )
        )
        if s.total_row_groups > 1:
            await pane.mount(
                Static(
                    f"[b]Rows/RG:[/b] avg {s.avg_rows_per_row_group:,.0f} · "
                    f"min {s.min_rows_per_row_group:,} · "
                    f"max {s.max_rows_per_row_group:,}",
                    classes="detail-section",
                )
            )
            await pane.mount(Label("Row Groups", classes="detail-title"))
            rg_table = DataTable()
            await pane.mount(rg_table)
            rg_table.add_columns(
                "RG", "Rows", "Compressed", "Uncompressed", "Ratio"
            )
            for rg in result.row_groups:
                rg_table.add_row(
                    str(rg.row_group_id),
                    f"{rg.row_count:,}",
                    _format_size(rg.compressed_size),
                    _format_size(rg.uncompressed_size),
                    f"{rg.compression_ratio:.1%}",
                )

        if result.columns:
            await pane.mount(Label("Columns", classes="detail-title"))
            col_table = DataTable()
            await pane.mount(col_table)
            col_table.add_columns(
                "Col", "Name", "Type", "Compressed", "Uncompressed", "% of File"
            )
            col_types: dict[str, str] = {}
            for cc in result.column_chunks:
                if cc.column_name not in col_types:
                    col_types[cc.column_name] = cc.physical_type
            for c in result.columns:
                col_table.add_row(
                    str(c.column_id),
                    esc(c.column_name),
                    col_types.get(c.column_name, ""),
                    _format_size(c.total_compressed_size),
                    _format_size(c.total_uncompressed_size),
                    f"{c.pct_of_table:.1%}",
                )

    @work(group="detail_aux", exclusive=True)
    async def _analyze_parquet_file(self) -> None:
        """Analyse a standalone parquet file by reading its metadata."""
        file_data = self._current_file_data
        if file_data is None:
            return

        # Determine render target: Analysis tab pane (preview) or inline (highlight)
        try:
            pane = self.query_one("#pq-tab-analysis", TabPane)
            in_tab = True
        except NoMatches:
            pane = None
            in_tab = False

        with contextlib.suppress(NoMatches):
            self.query_one("#analyze-parquet-file", Button).remove()

        if in_tab:
            for child in list(pane.children):
                child.remove()
            await pane.mount(
                Static(
                    "[dim]Reading parquet metadata…[/dim]",
                    id="parquet-analysis-progress",
                    classes="detail-section",
                )
            )
        else:
            self.mount(
                Static(
                    "[dim]Reading parquet metadata…[/dim]",
                    id="parquet-analysis-progress",
                    classes="detail-section",
                )
            )

        try:
            result = await self.client.delta.analyze_parquet_file(
                file_data.workspace, file_data.path
            )

            if self._current_file_data is not file_data:
                return

            with contextlib.suppress(NoMatches):
                self.query_one("#parquet-analysis-progress", Static).remove()

            if in_tab:
                await self._render_analysis_in_pane(pane, result)
            else:
                await self._render_analysis_inline(result)

        except Exception as e:
            with contextlib.suppress(NoMatches):
                self.query_one("#parquet-analysis-progress", Static).remove()
            self.notify(f"Parquet analysis failed: {e}", severity="error", markup=False)
            logger.exception("Parquet analysis failed for %s", file_data.path)

    async def _render_analysis_inline(self, result) -> None:
        """Render analysis results inline (for standalone parquet files)."""
        s = result.summary

        self.mount(Label("Analysis", classes="detail-title"))
        self.mount(
            Static(
                f"[b]Rows:[/b] {s.total_rows:,}  "
                f"[b]Row Groups:[/b] {s.total_row_groups}",
                classes="detail-section",
            )
        )
        self.mount(
            Static(
                f"[b]Compressed:[/b] {_format_size(s.total_compressed_size)}  "
                f"[b]Uncompressed:[/b] {_format_size(s.total_uncompressed_size)}",
                classes="detail-section",
            )
        )
        if s.total_row_groups > 1:
            self.mount(
                Static(
                    f"[b]Rows/RG:[/b] avg {s.avg_rows_per_row_group:,.0f} · "
                    f"min {s.min_rows_per_row_group:,} · max {s.max_rows_per_row_group:,}",
                    classes="detail-section",
                )
            )

        if result.row_groups and s.total_row_groups > 1:
            self.mount(Label("Row Groups", classes="detail-title"))
            rg_table = DataTable()
            self.mount(rg_table)
            rg_table.add_columns("RG", "Rows", "Compressed", "Uncompressed", "Ratio")
            for rg in result.row_groups:
                rg_table.add_row(
                    str(rg.row_group_id),
                    f"{rg.row_count:,}",
                    _format_size(rg.compressed_size),
                    _format_size(rg.uncompressed_size),
                    f"{rg.compression_ratio:.1%}",
                )

        if result.columns:
            self.mount(Label("Columns", classes="detail-title"))
            col_table = DataTable()
            self.mount(col_table)
            col_table.add_columns(
                "Col", "Name", "Type", "Compressed", "Uncompressed", "% of File"
            )
            # Get type from column chunks (first occurrence of each column)
            col_types: dict[str, str] = {}
            for cc in result.column_chunks:
                if cc.column_name not in col_types:
                    col_types[cc.column_name] = cc.physical_type
            for c in result.columns:
                col_table.add_row(
                    str(c.column_id),
                    esc(c.column_name),
                    col_types.get(c.column_name, ""),
                    _format_size(c.total_compressed_size),
                    _format_size(c.total_uncompressed_size),
                    f"{c.pct_of_table:.1%}",
                )

    # ── File preview ────────────────────────────────────────────────────

    @work(exclusive=True, group="detail_load")
    async def preview_file(self, data: FileNode) -> None:
        """Fetch and render a rich preview of file contents."""
        self._clear()
        file_name = data.path.split("/")[-1]
        ext = ("." + file_name.rsplit(".", 1)[-1]).lower() if "." in file_name else ""
        self.mount(Label(f"👁 Preview: {file_name}", classes="detail-title"))
        rel = data.path.split("/", 1)[-1] if "/" in data.path else data.path
        friendly = f"{self._workspace_name} / {self._item_name} / {rel}"
        self.mount(
            Static(
                f"[b]Path:[/b] {esc(friendly)}  │  [b]Size:[/b] {_format_size(data.size)}",
                classes="detail-section",
            )
        )
        self.mount(Static("Loading preview…", classes="preview-loading detail-section"))

        try:
            is_binary = ext in (".parquet", ".avro")
            size_limit = _MAX_BINARY_BYTES if is_binary else _MAX_PREVIEW_BYTES
            if data.size > size_limit:
                self._remove_loading()
                self.mount(
                    Static(
                        f"⚠️ File too large to preview "
                        f"({_format_size(data.size)}; limit {_format_size(size_limit)}).",
                        classes="detail-section",
                    )
                )
                return

            if ext == ".parquet":
                await self._preview_parquet(data)
            elif ext == ".avro":
                await self._preview_avro(data)
            else:
                raw = await self.client.dfs.read_file(data.workspace, data.path)
                text = raw.decode("utf-8", errors="replace")
                self._remove_loading()
                self._render_text(file_name, ext, text)
        except Exception as e:
            self._remove_loading()
            self.mount(Static(f"❌ Preview failed: {esc(str(e))}", classes="detail-section"))
            logger.exception("Failed to preview %s", data.path)

    def _remove_loading(self) -> None:
        """Remove the loading placeholder if present."""
        with contextlib.suppress(NoMatches):
            self.query_one(".preview-loading", Static).remove()

    def _render_text(self, file_name: str, ext: str, text: str) -> None:
        """Render text content with appropriate formatting."""
        if ext == ".md":
            self.mount(Markdown(text, classes="preview-content"))
        elif ext == ".csv":
            self._render_csv(text)
        elif ext == ".json":
            # Pretty-print JSON — handle both single JSON and NDJSON
            try:
                parsed = json.loads(text)
                text = json.dumps(parsed, indent=2, ensure_ascii=False)
            except (json.JSONDecodeError, ValueError):
                # Try NDJSON (e.g. Delta log files: one JSON per line)
                lines = text.strip().splitlines()
                formatted = []
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        formatted.append(json.dumps(obj, indent=2, ensure_ascii=False))
                    except (json.JSONDecodeError, ValueError):
                        formatted.append(line)
                if formatted:
                    text = "\n".join(formatted)
            # TextArea for selectable/copyable text
            ta = TextArea(text, language="json", read_only=True, classes="preview-content")
            self.mount(ta)
        else:
            # Try to detect if it's binary
            if "\x00" in text[:512]:
                self._render_hex(text.encode("utf-8", errors="replace")[:256])
            else:
                lang = _SYNTAX_LEXERS.get(ext, "text") if ext in _SYNTAX_LEXERS else None
                ta = TextArea(text, language=lang, read_only=True, classes="preview-content")
                self.mount(ta)

    def _render_csv(self, text: str) -> None:
        """Parse CSV and render as a DataTable."""
        try:
            reader = csv.reader(io.StringIO(text))
            rows = list(itertools.islice(reader, 102))
            if not rows:
                self.mount(Static("(empty CSV)", classes="detail-section"))
                return
            table = DataTable(classes="preview-content")
            self.mount(table)
            # Use first row as headers
            headers = rows[0]
            table.add_columns(*headers)
            for row in rows[1:101]:  # max 100 data rows
                # Pad short rows
                padded = row + [""] * (len(headers) - len(row))
                table.add_row(*padded[: len(headers)])
            if len(rows) > 101:
                msg = "[dim]Showing first 100 rows[/dim]"
                self.mount(Static(msg, classes="detail-section"))
        except Exception as e:
            self.mount(Static(f"❌ CSV parse error: {esc(str(e))}", classes="detail-section"))

    async def _preview_parquet(self, data: FileNode) -> None:
        """Read parquet file with pyarrow and display tabbed Schema + Data + Analysis."""
        try:
            import pyarrow.parquet as pq

            raw = await self.client.dfs.read_file(data.workspace, data.path)
            buf = io.BytesIO(raw)
            pf = pq.ParquetFile(buf)
            schema = pf.schema_arrow
            metadata = pf.metadata

            self._remove_loading()
            self._current_file_data = data

            # Summary line
            self.mount(
                Static(
                    f"[b]Rows:[/b] {metadata.num_rows:,}  "
                    f"[b]Columns:[/b] {len(schema)}  "
                    f"[b]Row groups:[/b] {metadata.num_row_groups}",
                    classes="detail-section",
                )
            )

            tc = TabbedContent(classes="parquet-tabs")
            await self.mount(tc)

            # ── Schema tab ──────────────────────────────────────────
            schema_pane = TabPane("Schema", id="pq-tab-schema")
            await tc.add_pane(schema_pane)
            schema_table = DataTable()
            await schema_pane.mount(schema_table)
            schema_table.add_columns("Column", "Type", "Nullable")
            for i in range(len(schema)):
                field = schema.field(i)
                schema_table.add_row(
                    field.name, str(field.type), "✓" if field.nullable else "✗"
                )

            # ── Data tab ────────────────────────────────────────────
            data_pane = TabPane("Data", id="pq-tab-data")
            await tc.add_pane(data_pane)
            sample = coerce_timestamps(pf.read_row_groups([0]).slice(0, 100))
            data_table = DataTable(classes="preview-content")
            await data_pane.mount(data_table)
            col_names = [schema.field(i).name for i in range(len(schema))]
            data_table.add_columns(*col_names)
            for row_idx in range(sample.num_rows):
                row_vals = [
                    str(sample.column(c)[row_idx]) for c in range(len(col_names))
                ]
                data_table.add_row(*row_vals)
            if sample.num_rows >= 100:
                await data_pane.mount(
                    Static("[dim]Showing first 100 rows[/dim]", classes="detail-section")
                )

            # ── Analysis tab (lazy) ─────────────────────────────────
            analysis_pane = TabPane("Analysis", id="pq-tab-analysis")
            await tc.add_pane(analysis_pane)
            await analysis_pane.mount(
                Button(
                    "Run Analysis",
                    id="analyze-parquet-file",
                    variant="primary",
                )
            )
            await analysis_pane.mount(
                Static(
                    "[dim]Analyses row groups, column chunks, "
                    "and compression statistics[/dim]",
                    classes="detail-section",
                )
            )
        except ImportError:
            self._remove_loading()
            self.mount(
                Static(
                    "❌ pyarrow not installed. Run: pip install pyarrow",
                    classes="detail-section",
                )
            )
        except Exception as e:
            self._remove_loading()
            self.mount(Static(f"❌ Parquet error: {esc(str(e))}", classes="detail-section"))
            logger.exception("Failed to preview parquet %s", data.path)

    async def _preview_avro(self, data: FileNode) -> None:
        """Read Avro file with fastavro and display schema + sample rows."""
        try:
            import fastavro

            raw = await self.client.dfs.read_file(data.workspace, data.path)
            buf = io.BytesIO(raw)
            reader = fastavro.reader(buf)
            avro_schema = reader.writer_schema

            self._remove_loading()

            # Schema info from Avro schema
            fields = avro_schema.get("fields", []) if avro_schema else []
            self.mount(
                Static(
                    f"[b]Format:[/b] Apache Avro  [b]Columns:[/b] {len(fields)}",
                    classes="detail-section",
                )
            )
            if avro_schema and avro_schema.get("name"):
                self.mount(
                    Static(
                        f"[b]Record type:[/b] {esc(avro_schema['name'])}",
                        classes="detail-section",
                    )
                )

            # Schema table
            if fields:
                self.mount(Label("Schema", classes="detail-title"))
                schema_table = DataTable(id="schema-table")
                self.mount(schema_table)
                schema_table.add_columns("Column", "Type", "Nullable")
                for f in fields:
                    ftype = f.get("type", "unknown")
                    nullable = False
                    # Avro union types: ["null", "string"] means nullable string
                    if isinstance(ftype, list):
                        nullable = "null" in ftype
                        non_null = [t for t in ftype if t != "null"]
                        ftype = non_null[0] if len(non_null) == 1 else str(non_null)
                    elif isinstance(ftype, dict):
                        ftype = ftype.get("type", str(ftype))
                    schema_table.add_row(
                        f.get("name", "?"),
                        str(ftype),
                        "✓" if nullable else "✗",
                    )

            # Sample data (first 100 rows)
            rows_data: list[dict] = []
            for i, record in enumerate(reader):
                if i >= 100:
                    break
                rows_data.append(record)

            if rows_data:
                col_names = [f.get("name", f"col_{i}") for i, f in enumerate(fields)]
                if not col_names and rows_data:
                    col_names = list(rows_data[0].keys())

                self.mount(Label(f"Data (first {len(rows_data)} rows)", classes="detail-title"))
                data_table = DataTable(classes="preview-content")
                self.mount(data_table)
                data_table.add_columns(*col_names)
                for record in rows_data:
                    row = [str(record.get(c, "")) for c in col_names]
                    data_table.add_row(*row)
            else:
                self.mount(Static("[dim](empty Avro file)[/dim]", classes="detail-section"))

        except ImportError:
            self._remove_loading()
            self.mount(
                Static(
                    "❌ fastavro not installed. Run: pip install fastavro",
                    classes="detail-section",
                )
            )
        except Exception as e:
            self._remove_loading()
            self.mount(Static(f"❌ Avro error: {esc(str(e))}", classes="detail-section"))
            logger.exception("Failed to preview avro %s", data.path)

    def _render_hex(self, raw: bytes) -> None:
        """Render a hex dump of binary content."""
        lines = []
        for offset in range(0, len(raw), 16):
            chunk = raw[offset : offset + 16]
            hex_part = " ".join(f"{b:02x}" for b in chunk)
            ascii_part = "".join(chr(b) if 32 <= b < 127 else "." for b in chunk)
            lines.append(f"{offset:08x}  {hex_part:<48}  {ascii_part}")
        self.mount(
            Static(
                Syntax("\n".join(lines), "text", theme="monokai"),
                classes="preview-content",
            )
        )
        self.mount(
            Static(
                "[dim]Binary file — showing first 256 bytes[/dim]",
                classes="detail-section",
            )
        )


def _format_size(size_bytes: int) -> str:
    """Format byte size to human readable."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"
