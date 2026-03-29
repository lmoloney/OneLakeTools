from __future__ import annotations

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
from onelake_tui.nodes import FileNode, FolderNode, TableNode
from onelake_tui.sprite import OneLakeSprite, get_welcome

logger = logging.getLogger("onelake_tui.detail")

NodeData = FolderNode | FileNode | TableNode | None

_MAX_PREVIEW_BYTES = 512 * 1024  # 512KB text preview limit
_MAX_BINARY_BYTES = 50 * 1024 * 1024  # 50MB binary preview limit

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
        self._current_delta_info = None
        self._data_preview_loaded: bool = False
        self._debounce_timer = None
        self._pending_node: NodeData = None

    def set_context(self, workspace_name: str, item_name: str) -> None:
        """Set human-readable names for path display."""
        self._workspace_name = workspace_name
        self._item_name = item_name

    def compose(self) -> ComposeResult:
        yield OneLakeSprite(animate=True, id="detail-content")

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
        self._current_delta_info = None
        self._data_preview_loaded = False
        self.remove_children()

    def _show_placeholder(self) -> None:
        self._clear()
        self.mount(Static(get_welcome(), markup=True))

    def _show_folder(self, data: FolderNode) -> None:
        self._clear()
        folder_name = data.directory.split("/")[-1] if "/" in data.directory else data.directory
        self.mount(Label(f"📂 {folder_name}", classes="detail-title"))
        rel = data.directory.split("/", 1)[-1] if "/" in data.directory else data.directory
        friendly = f"onelake://{self._workspace_name}/{self._item_name}/{rel}"
        self.mount(Static(f"[b]Path:[/b] {esc(friendly)}", classes="detail-section"))

    def _show_file(self, data: FileNode) -> None:
        self._clear()
        file_name = data.path.split("/")[-1]
        self.mount(Label(f"📄 {file_name}", classes="detail-title"))
        rel = data.path.split("/", 1)[-1] if "/" in data.path else data.path
        friendly = f"onelake://{self._workspace_name}/{self._item_name}/{rel}"
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
        friendly = f"onelake://{self._workspace_name}/{self._item_name}/Tables/{data.table_name}"
        self.mount(Static(f"[b]Path:[/b] {esc(friendly)}", classes="detail-section"))
        self.mount(LoadingIndicator(id="table-spinner"))
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
            info = await self.client.delta.get_metadata(
                data.workspace, data.item_path, data.table_name
            )
            if self._current_table_data is not data:
                return
            self._current_delta_info = info

            # Remove loading spinner
            with contextlib.suppress(NoMatches):
                self.query_one("#table-spinner").remove()

            # Build tabbed interface
            tc = TabbedContent(id="table-tabs")
            await self.mount(tc)

            # ── Schema tab ──────────────────────────────────────────────
            schema_pane = TabPane("Schema", id="tab-schema")
            await tc.add_pane(schema_pane)
            await schema_pane.mount(
                Static(
                    f"[b]Version:[/b] {info.version}  "
                    f"[b]Files:[/b] {info.num_files}  "
                    f"[b]Size:[/b] {_format_size(info.size_bytes)}",
                    classes="detail-section",
                )
            )
            if info.partition_columns:
                await schema_pane.mount(
                    Static(
                        f"[b]Partitioned by:[/b] {esc(', '.join(info.partition_columns))}",
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

        except Exception as e:
            with contextlib.suppress(NoMatches):
                self.query_one("#table-spinner").remove()
            err_msg = str(e)
            if "No files in log" in err_msg or "log segment" in err_msg:
                self.mount(
                    Static(
                        "[dim]Not a Delta table (may be Iceberg or empty). "
                        "Expand the node in the tree to browse raw files.[/dim]",
                        classes="detail-section",
                    )
                )
            elif "reader features" in err_msg and "not yet supported" in err_msg:
                self.mount(
                    Static(
                        "⚠️ [yellow]This table uses advanced Delta features "
                        "(e.g. deletion vectors) not fully supported by the local reader. "
                        "Schema and history tabs may still work — "
                        "try the Data tab for a raw parquet preview.[/yellow]",
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
            )[:20]

            commits: list[dict] = []
            for pf in json_files:
                raw = await self.client.dfs.read_file(data.workspace, pf.name)
                for line in raw.decode("utf-8", errors="replace").strip().splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        if "commitInfo" in obj:
                            ci = obj["commitInfo"]
                            fname = pf.name.split("/")[-1]
                            version = fname.replace(".json", "").lstrip("0") or "0"
                            # Use commitInfo.timestamp (ms epoch), fall back to file lastModified
                            ts_raw = ci.get("timestamp")
                            if ts_raw is None and pf.last_modified:
                                ts_raw = pf.last_modified
                            commits.append(
                                {
                                    "version": version,
                                    "timestamp": ts_raw,
                                    "operation": ci.get("operation", ""),
                                    "metrics": ci.get("operationMetrics", {}),
                                }
                            )
                    except json.JSONDecodeError:
                        continue

            with contextlib.suppress(NoMatches):
                self.query_one("#txn-loading").remove()

            if self._current_table_data is not table_data:
                return

            txn_pane = self.query_one("#tab-history", TabPane)
            if commits:
                tbl = DataTable(id="txn-table")
                await txn_pane.mount(tbl)
                tbl.add_columns("Version", "Timestamp", "Operation", "Metrics")
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
                    tbl.add_row(str(c["version"]), str(ts), c["operation"], metrics_str)
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
            # Handle unsupported reader features (e.g. deletionVectors)
            if "reader features" in err_msg and "not yet supported" in err_msg:
                logger.debug(
                    "Delta reader unsupported features, falling back to DFS parquet: %s", e
                )
                with contextlib.suppress(NoMatches):
                    self.query_one("#data-loading").remove()
                await data_pane.mount(
                    Static(
                        "⚠️ [yellow]Table uses advanced Delta features "
                        "(e.g. deletion vectors) not supported by the local reader. "
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

        # Read the first (largest) parquet file for a representative sample
        parquet_files.sort(key=lambda p: p.content_length or 0, reverse=True)
        target = parquet_files[0]
        raw = await self.client.dfs.read_file(data.workspace, target.name)
        pf = pq.ParquetFile(io.BytesIO(raw))
        return pf.read_row_groups([0]).slice(0, 100)

    @work(group="detail_aux", exclusive=True)
    async def _load_cdf_preview(self) -> None:
        """Load Change Data Feed records from the Delta table."""
        table_data = self._current_table_data
        delta_info = self._current_delta_info
        if table_data is None or delta_info is None:
            return

        with contextlib.suppress(NoMatches):
            self.query_one("#load-cdf-preview", Button).remove()

        cdf_pane = self.query_one("#tab-cdf", TabPane)
        # Clear placeholder content but keep the status line
        for child in list(cdf_pane.children):
            child.remove()
        await cdf_pane.mount(
            Static("Loading CDF data…", id="cdf-loading", classes="detail-section")
        )

        try:
            starting = max(0, delta_info.version - 10)
            cdf_table = await self.client.delta.read_cdf(
                table_data.workspace,
                table_data.item_path,
                table_data.table_name,
                starting_version=starting,
            )

            if self._current_table_data is not table_data:
                return

            with contextlib.suppress(NoMatches):
                self.query_one("#cdf-loading").remove()

            if cdf_table.num_rows == 0:
                await cdf_pane.mount(Static("[dim]No CDF records in the last 10 versions[/dim]"))
                return

            await cdf_pane.mount(
                Static(
                    f"[dim]Showing {min(cdf_table.num_rows, 100)} of {cdf_table.num_rows} "
                    f"CDF records (versions {starting}–{delta_info.version})[/dim]",
                    classes="detail-section",
                )
            )
            tbl = DataTable(id="cdf-table")
            await cdf_pane.mount(tbl)
            col_names = cdf_table.column_names
            tbl.add_columns(*col_names)
            for row_idx in range(min(cdf_table.num_rows, 100)):
                row = [str(cdf_table.column(c)[row_idx]) for c in range(len(col_names))]
                tbl.add_row(*row)
        except Exception as e:
            with contextlib.suppress(NoMatches):
                self.query_one("#cdf-loading").remove()
            await cdf_pane.mount(
                Static(f"❌ CDF preview failed: {esc(str(e))}", classes="detail-section")
            )
            logger.debug("CDF preview failed: %s", e)

    # ── File preview ────────────────────────────────────────────────────

    @work(exclusive=True, group="detail_load")
    async def preview_file(self, data: FileNode) -> None:
        """Fetch and render a rich preview of file contents."""
        self._clear()
        file_name = data.path.split("/")[-1]
        ext = ("." + file_name.rsplit(".", 1)[-1]).lower() if "." in file_name else ""
        self.mount(Label(f"👁 Preview: {file_name}", classes="detail-title"))
        rel = data.path.split("/", 1)[-1] if "/" in data.path else data.path
        friendly = f"onelake://{self._workspace_name}/{self._item_name}/{rel}"
        self.mount(
            Static(
                f"[b]Path:[/b] {esc(friendly)}  │  [b]Size:[/b] {_format_size(data.size)}",
                classes="detail-section",
            )
        )
        self.mount(Static("Loading preview…", id="preview-loading", classes="detail-section"))

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
            self.query_one("#preview-loading", Static).remove()

    def _render_text(self, file_name: str, ext: str, text: str) -> None:
        """Render text content with appropriate formatting."""
        if ext == ".md":
            self.mount(Markdown(text, id="preview-content"))
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
            ta = TextArea(text, language="json", read_only=True, id="preview-content")
            self.mount(ta)
        else:
            # Try to detect if it's binary
            if "\x00" in text[:512]:
                self._render_hex(text.encode("utf-8", errors="replace")[:256])
            else:
                lang = _SYNTAX_LEXERS.get(ext, "text") if ext in _SYNTAX_LEXERS else None
                ta = TextArea(text, language=lang, read_only=True, id="preview-content")
                self.mount(ta)

    def _render_csv(self, text: str) -> None:
        """Parse CSV and render as a DataTable."""
        try:
            reader = csv.reader(io.StringIO(text))
            rows = list(itertools.islice(reader, 102))
            if not rows:
                self.mount(Static("(empty CSV)", classes="detail-section"))
                return
            table = DataTable(id="preview-content")
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
        """Read parquet file with pyarrow and display schema + sample rows."""
        try:
            import pyarrow.parquet as pq

            raw = await self.client.dfs.read_file(data.workspace, data.path)
            buf = io.BytesIO(raw)
            pf = pq.ParquetFile(buf)
            schema = pf.schema_arrow
            metadata = pf.metadata

            self._remove_loading()

            # Schema info
            self.mount(
                Static(
                    f"[b]Rows:[/b] {metadata.num_rows:,}  "
                    f"[b]Columns:[/b] {len(schema)}  "
                    f"[b]Row groups:[/b] {metadata.num_row_groups}",
                    classes="detail-section",
                )
            )

            # Schema table
            self.mount(Label("Schema", classes="detail-title"))
            schema_table = DataTable(id="schema-table")
            self.mount(schema_table)
            schema_table.add_columns("Column", "Type", "Nullable")
            for i in range(len(schema)):
                field = schema.field(i)
                schema_table.add_row(field.name, str(field.type), "✓" if field.nullable else "✗")

            # Sample data (first 100 rows)
            sample = pf.read_row_groups([0]).slice(0, 100)
            self.mount(Label("Data (first 100 rows)", classes="detail-title"))
            data_table = DataTable(id="preview-content")
            self.mount(data_table)
            col_names = [schema.field(i).name for i in range(len(schema))]
            data_table.add_columns(*col_names)
            for row_idx in range(sample.num_rows):
                row_vals = [str(sample.column(c)[row_idx]) for c in range(len(col_names))]
                data_table.add_row(*row_vals)
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
                data_table = DataTable(id="preview-content")
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
                id="preview-content",
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
