from __future__ import annotations

import contextlib
import csv
import io
import json
import logging

from rich.markup import escape as esc
from rich.syntax import Syntax
from textual import work
from textual.app import ComposeResult
from textual.containers import VerticalScroll
from textual.widgets import DataTable, Label, Markdown, Static, TextArea

from onelake_client import OneLakeClient
from onelake_tui.nodes import FileNode, FolderNode, TableNode
from onelake_tui.sprite import OneLakeSprite, get_welcome

logger = logging.getLogger("onelake_tui.detail")

NodeData = FolderNode | FileNode | TableNode | None

_MAX_PREVIEW_BYTES = 512 * 1024  # 512KB text preview limit

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

    def set_context(self, workspace_name: str, item_name: str) -> None:
        """Set human-readable names for path display."""
        self._workspace_name = workspace_name
        self._item_name = item_name

    def compose(self) -> ComposeResult:
        yield OneLakeSprite(animate=True, id="detail-content")

    def update_for_node(self, data: NodeData) -> None:
        """Update the panel to show details for the given node data."""
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

    @work(group="detail_load")
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

    def _show_table(self, data: TableNode) -> None:
        self._clear()
        self.mount(Label(f"🗃️ {data.table_name}", classes="detail-title"))
        friendly = f"onelake://{self._workspace_name}/{self._item_name}/Tables/{data.table_name}"
        self.mount(Static(f"[b]Path:[/b] {esc(friendly)}", classes="detail-section"))
        self.mount(Static("Loading table metadata...", classes="detail-section table-loading"))
        self._load_table_metadata(data)

    @work(group="detail_load")
    async def _load_table_metadata(self, data: TableNode) -> None:
        """Load Delta table metadata and show schema."""
        try:
            info = await self.client.delta.get_metadata(
                data.workspace, data.item_path, data.table_name
            )

            # Remove loading placeholder
            for w in self.query(".table-loading"):
                w.remove()

            self.mount(
                Static(
                    f"[b]Version:[/b] {info.version}  "
                    f"[b]Files:[/b] {info.num_files}  "
                    f"[b]Size:[/b] {_format_size(info.size_bytes)}",
                    classes="detail-section",
                )
            )

            if info.partition_columns:
                self.mount(
                    Static(
                        f"[b]Partitioned by:[/b] {esc(', '.join(info.partition_columns))}",
                        classes="detail-section",
                    )
                )

            if info.description:
                self.mount(
                    Static(
                        f"[b]Description:[/b] {esc(info.description)}",
                        classes="detail-section",
                    )
                )

            if info.schema_:
                self.mount(Label("Schema", classes="detail-title"))
                table = DataTable(id="schema-table")
                self.mount(table)
                table.add_columns("Name", "Type", "Nullable")
                for col in info.schema_:
                    table.add_row(col.name, col.type, "✓" if col.nullable else "✗")

        except Exception as e:
            for w in self.query(".table-loading"):
                w.remove()
            err_msg = str(e)
            if "No files in log" in err_msg or "log segment" in err_msg:
                self.mount(
                    Static(
                        "[dim]Not a Delta table (may be Iceberg or empty). "
                        "Expand the node in the tree to browse raw files.[/dim]",
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

    @work(group="detail_load")
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
        self.mount(Static("Loading preview...", id="preview-loading", classes="detail-section"))

        try:
            if ext == ".parquet":
                await self._preview_parquet(data)
            elif data.size > _MAX_PREVIEW_BYTES:
                raw = await self.client.dfs.read_file(data.workspace, data.path)
                text = raw[:_MAX_PREVIEW_BYTES].decode("utf-8", errors="replace")
                self._remove_loading()
                self.mount(
                    Static(
                        f"[dim]Showing first {_format_size(_MAX_PREVIEW_BYTES)} "
                        f"of {_format_size(data.size)}[/dim]",
                        classes="detail-section",
                    )
                )
                self._render_text(file_name, ext, text)
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
        with contextlib.suppress(Exception):
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
            rows = list(reader)
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
                msg = f"[dim]Showing 100 of {len(rows) - 1} rows[/dim]"
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
