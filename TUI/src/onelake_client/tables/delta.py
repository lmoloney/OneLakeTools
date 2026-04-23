from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from deltalake.exceptions import DeltaError

from onelake_client.models.table import (
    Column,
    ColumnChunkInfo,
    ColumnInfo,
    DeltaAnalysisResult,
    DeltaAnalysisSummary,
    DeltaFileStats,
    DeltaTableInfo,
    ParquetFileInfo,
    RowGroupInfo,
)

if TYPE_CHECKING:
    from onelake_client.auth import OneLakeAuth

logger = logging.getLogger("onelake_client.tables.delta")

_SUBPROCESS_TIMEOUT = 30  # seconds


def _build_table_uri(workspace: str, item_path: str, table_name: str, dfs_host: str) -> str:
    """Build an abfss:// URI for a Delta table on OneLake.

    Args:
        workspace: Workspace name or GUID.
        item_path: Item path like "MyLakehouse.Lakehouse".
        table_name: Table name under the Tables/ directory.
        dfs_host: DFS endpoint hostname (varies per ring).

    Returns:
        abfss:// URI pointing to the Delta table.
    """
    return f"abfss://{workspace}@{dfs_host}/{item_path}/Tables/{table_name}"


def _clean_type_str(t) -> str:
    """Return a human-readable type string from a deltalake type object.

    deltalake >= 1.0 wraps primitive types as PrimitiveType("string") when
    str()-ed. This extracts just the inner name for primitives and falls back
    to the raw string for complex types (ArrayType, MapType, StructType).
    """
    # PrimitiveType exposes the plain name via its .type attribute
    if hasattr(t, "type") and isinstance(getattr(t, "type"), str):
        return t.type
    return str(t)


def _schema_to_columns(schema) -> list[Column]:
    """Convert a deltalake Schema to our Column model."""
    columns: list[Column] = []
    # deltalake >= 0.18: Schema is not directly iterable, use .fields
    fields = schema.fields if hasattr(schema, "fields") else schema
    for field in fields:
        columns.append(
            Column(
                name=field.name,
                type=_clean_type_str(field.type),
                nullable=field.nullable,
                metadata=field.metadata if field.metadata else None,
            )
        )
    return columns


# ── Subprocess workers (top-level for pickling) ────────────────────────


_METADATA_SCRIPT = """
import sys, json
from deltalake import DeltaTable

data = json.load(sys.stdin)
uri = data["uri"]
storage_options = data["storage_options"]

try:
    dt = DeltaTable(uri, storage_options=storage_options)

    schema = dt.schema()
    fields = schema.fields if hasattr(schema, "fields") else schema
    columns = [
        {
            "name": f.name,
            "type": f.type.type if (hasattr(f.type, "type") and isinstance(getattr(f.type, "type"), str)) else str(f.type),
            "nullable": f.nullable,
            "metadata": dict(f.metadata) if f.metadata else None,
        }
        for f in fields
    ]

    version = dt.version()
    files = dt.file_uris()
    meta = dt.metadata()

    size_bytes = 0
    try:
        add_actions = dt.get_add_actions(flatten=True)
        if hasattr(add_actions, "to_pydict"):
            sd = add_actions.to_pydict()
            size_bytes = sum(sd.get("size_bytes", sd.get("size", [])))
        elif hasattr(add_actions, "column"):
            cn = add_actions.column_names
            sk = "size_bytes" if "size_bytes" in cn else "size"
            if sk in cn:
                size_bytes = sum(add_actions.column(sk).to_pylist())
    except Exception:
        pass

    json.dump({
        "ok": True,
        "name": meta.name or "",
        "columns": columns,
        "version": version,
        "num_files": len(files),
        "size_bytes": size_bytes,
        "partition_columns": list(meta.partition_columns),
        "properties": dict(meta.configuration) if meta.configuration else {},
        "description": meta.description,
    }, sys.stdout)
except Exception as e:
    json.dump({"ok": False, "error": f"{type(e).__name__}: {e}"}, sys.stdout)
"""

_FILE_STATS_SCRIPT = """
import sys, json
from deltalake import DeltaTable

data = json.load(sys.stdin)
uri = data["uri"]
storage_options = data["storage_options"]

try:
    dt = DeltaTable(uri, storage_options=storage_options)
    add_actions = dt.get_add_actions(flatten=True)

    col_names = list(add_actions.column_names) if hasattr(add_actions, "column_names") else []

    size_key = "size_bytes" if "size_bytes" in col_names else "size"
    sizes = add_actions.column(size_key).to_pylist() if size_key in col_names else []
    valid_sizes = [s for s in sizes if s is not None and isinstance(s, (int, float))]

    num_files = len(valid_sizes)
    total_bytes = int(sum(valid_sizes))
    min_bytes = int(min(valid_sizes)) if valid_sizes else 0
    max_bytes = int(max(valid_sizes)) if valid_sizes else 0
    avg_bytes = total_bytes / num_files if num_files else 0.0

    partition_counts = {}
    try:
        meta = dt.metadata()
        partition_cols = list(meta.partition_columns)
        for col in partition_cols:
            col_key = f"partition.{col}"
            if col_key in col_names:
                values = add_actions.column(col_key).to_pylist()
                for v in values:
                    label = f"{col}={v if v is not None else 'null'}"
                    partition_counts[label] = partition_counts.get(label, 0) + 1
    except Exception:
        pass

    json.dump({
        "ok": True,
        "num_files": num_files,
        "total_bytes": total_bytes,
        "min_file_bytes": min_bytes,
        "max_file_bytes": max_bytes,
        "avg_file_bytes": avg_bytes,
        "partition_counts": partition_counts,
    }, sys.stdout)
except Exception as e:
    json.dump({"ok": False, "error": f"{type(e).__name__}: {e}"}, sys.stdout)
"""


def _run_delta_subprocess(
    uri: str, storage_options: dict, timeout: int = _SUBPROCESS_TIMEOUT
) -> dict:
    """Run the Delta metadata script in a subprocess.

    Uses ``subprocess.Popen`` with explicit pipe management to avoid
    file-descriptor inheritance issues on macOS + Python 3.14.
    Passes data via stdin to keep bearer tokens out of ps output.
    """
    import json
    import os
    import subprocess
    import sys

    input_data = json.dumps({"uri": uri, "storage_options": storage_options})
    popen_kwargs: dict[str, object] = {
        "stdin": subprocess.PIPE,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
        "text": True,
        "close_fds": True,
    }
    if os.name == "posix":
        popen_kwargs["start_new_session"] = True

    proc = subprocess.Popen(
        [sys.executable, "-c", _METADATA_SCRIPT],
        **popen_kwargs,  # type: ignore[arg-type]
    )
    try:
        stdout, stderr = proc.communicate(input=input_data, timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        raise DeltaError(f"Delta table load timed out after {timeout}s") from None

    if proc.returncode != 0:
        err = (stderr or "").strip()
        if len(err) > 300:
            err = err[:300] + "…"
        raise DeltaError(
            f"Delta reader process crashed (exit code {proc.returncode}). "
            f"{err or 'This table may use features not supported by the local reader.'}"
        )

    try:
        return json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise DeltaError(f"Delta reader returned invalid output: {stdout[:200]}") from exc


def _run_file_stats_subprocess(
    uri: str, storage_options: dict, timeout: int = _SUBPROCESS_TIMEOUT
) -> dict:
    """Run the file-stats script in a subprocess (same isolation as metadata)."""
    import json
    import os
    import subprocess
    import sys

    input_data = json.dumps({"uri": uri, "storage_options": storage_options})
    popen_kwargs: dict[str, object] = {
        "stdin": subprocess.PIPE,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
        "text": True,
        "close_fds": True,
    }
    if os.name == "posix":
        popen_kwargs["start_new_session"] = True

    proc = subprocess.Popen(
        [sys.executable, "-c", _FILE_STATS_SCRIPT],
        **popen_kwargs,  # type: ignore[arg-type]
    )
    try:
        stdout, stderr = proc.communicate(input=input_data, timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        raise DeltaError(f"Delta file-stats timed out after {timeout}s") from None

    if proc.returncode != 0:
        err = (stderr or "").strip()
        if len(err) > 300:
            err = err[:300] + "…"
        raise DeltaError(
            f"Delta file-stats process crashed (exit code {proc.returncode}). "
            f"{err or 'Check table compatibility.'}"
        )

    try:
        return json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise DeltaError(f"Delta file-stats returned invalid output: {stdout[:200]}") from exc


class DeltaTableReader:
    """Reads Delta table metadata from OneLake.

    Uses the `deltalake` library (delta-rs Python bindings) with OneLake
    storage options for authentication.

    The ``get_metadata`` method runs deltalake in a **subprocess** to
    isolate the main process from Rust panics that can occur with certain
    table features (e.g. deletion vectors, v2 checkpoints).

    Usage:
        auth = OneLakeAuth()
        reader = DeltaTableReader(auth, dfs_host="onelake.dfs.fabric.microsoft.com")
        info = await reader.get_metadata("MyWorkspace", "MyLakehouse.Lakehouse", "customers")
    """

    def __init__(self, auth: OneLakeAuth, dfs_host: str = "onelake.dfs.fabric.microsoft.com"):
        self._auth = auth
        self._dfs_host = dfs_host
        self._isolate = True  # subprocess isolation for Rust panic safety

    def _get_storage_options(self) -> dict:
        """Get fresh storage options with a current token."""
        return self._auth.storage_options()

    def _load_table_sync(self, uri: str):
        """Synchronously load a DeltaTable (called via to_thread)."""
        from deltalake import DeltaTable

        return DeltaTable(uri, storage_options=self._get_storage_options())

    async def get_metadata(self, workspace: str, item_path: str, table_name: str) -> DeltaTableInfo:
        """Get metadata for a Delta table.

        Runs deltalake in a subprocess by default to isolate Rust panics.

        Args:
            workspace: Workspace name or GUID.
            item_path: Item path like "MyLakehouse.Lakehouse".
            table_name: Table name under Tables/.

        Returns:
            DeltaTableInfo with schema, version, file count, size, etc.
        """
        uri = _build_table_uri(workspace, item_path, table_name, self._dfs_host)
        logger.debug("Loading Delta table: %s", uri)

        if self._isolate:
            return await self._get_metadata_subprocess(uri, table_name)
        return await self._get_metadata_inprocess(uri, table_name)

    async def _get_metadata_subprocess(self, uri: str, table_name: str) -> DeltaTableInfo:
        """Load metadata in an isolated subprocess (Rust-panic safe)."""
        storage_options = self._get_storage_options()
        result = await asyncio.to_thread(_run_delta_subprocess, uri, storage_options)

        if not result.get("ok"):
            raise DeltaError(result.get("error", "Unknown error in Delta reader"))

        return DeltaTableInfo(
            name=result["name"] or table_name,
            schema_=[Column(**c) for c in result["columns"]],
            version=result["version"],
            num_files=result["num_files"],
            size_bytes=result["size_bytes"],
            partition_columns=result["partition_columns"],
            properties=result["properties"],
            description=result["description"],
        )

    async def _get_metadata_inprocess(self, uri: str, table_name: str) -> DeltaTableInfo:
        """Load metadata in-process (used when _isolate=False, e.g. tests)."""
        dt = await asyncio.to_thread(self._load_table_sync, uri)

        schema = _schema_to_columns(dt.schema())
        version = dt.version()
        files = dt.file_uris()
        metadata = dt.metadata()

        size_bytes = 0
        try:
            add_actions = await asyncio.to_thread(dt.get_add_actions, flatten=True)
            if hasattr(add_actions, "to_pydict"):
                size_dict = add_actions.to_pydict()
                size_bytes = sum(size_dict.get("size_bytes", size_dict.get("size", [])))
            elif hasattr(add_actions, "column"):
                col_names = add_actions.column_names
                size_key = "size_bytes" if "size_bytes" in col_names else "size"
                if size_key in col_names:
                    size_bytes = sum(add_actions.column(size_key).to_pylist())
        except (DeltaError, KeyError, IndexError, ValueError) as e:
            logger.warning("Failed to compute Delta table size: %s", e)

        return DeltaTableInfo(
            name=metadata.name or table_name,
            schema_=schema,
            version=version,
            num_files=len(files),
            size_bytes=size_bytes,
            partition_columns=list(metadata.partition_columns),
            properties=dict(metadata.configuration) if metadata.configuration else {},
            description=metadata.description,
        )

    async def read_sample(
        self, workspace: str, item_path: str, table_name: str, *, limit: int = 100
    ):
        """Read a sample of rows from a Delta table.

        Args:
            workspace: Workspace name or GUID.
            item_path: Item path like "MyLakehouse.Lakehouse".
            table_name: Table name under Tables/.
            limit: Maximum number of rows to return.

        Returns:
            pyarrow.Table with up to ``limit`` rows.
        """
        uri = _build_table_uri(workspace, item_path, table_name, self._dfs_host)
        logger.debug("Reading sample (%d rows) from: %s", limit, uri)
        dt = await asyncio.to_thread(self._load_table_sync, uri)

        def _head():
            ds = dt.to_pyarrow_dataset()
            return ds.head(limit)

        return await asyncio.to_thread(_head)

    async def read_cdf(
        self,
        workspace: str,
        item_path: str,
        table_name: str,
        *,
        starting_version: int = 0,
        ending_version: int | None = None,
    ):
        """Read Change Data Feed records from a Delta table.

        Args:
            workspace: Workspace name or GUID.
            item_path: Item path.
            table_name: Table name under Tables/.
            starting_version: First version to include.
            ending_version: Last version to include (None = latest).

        Returns:
            pyarrow.Table with CDF records including _change_type,
            _commit_version, and _commit_timestamp columns.
        """
        uri = _build_table_uri(workspace, item_path, table_name, self._dfs_host)
        logger.debug("Reading CDF from: %s (v%d→%s)", uri, starting_version, ending_version)
        dt = await asyncio.to_thread(self._load_table_sync, uri)

        def _load_cdf():
            kwargs: dict = {"starting_version": starting_version}
            if ending_version is not None:
                kwargs["ending_version"] = ending_version
            cdf = dt.load_cdf(**kwargs)
            return cdf.read_all() if hasattr(cdf, "read_all") else cdf

        return await asyncio.to_thread(_load_cdf)

    async def list_files(self, workspace: str, item_path: str, table_name: str) -> list[str]:
        """List data files in a Delta table.

        Returns:
            List of relative file paths (parquet files).
        """
        uri = _build_table_uri(workspace, item_path, table_name, self._dfs_host)
        dt = await asyncio.to_thread(self._load_table_sync, uri)
        return dt.file_uris()

    async def get_file_stats(
        self, workspace: str, item_path: str, table_name: str
    ) -> DeltaFileStats:
        """Get per-file size statistics and partition distribution for a Delta table.

        Runs in a subprocess to match the same Rust-panic isolation used by
        ``get_metadata``.

        Args:
            workspace: Workspace name or GUID.
            item_path: Item path like "MyLakehouse.Lakehouse".
            table_name: Table name under Tables/.

        Returns:
            :class:`DeltaFileStats` with file count, size metrics, and
            partition value counts.
        """
        uri = _build_table_uri(workspace, item_path, table_name, self._dfs_host)
        logger.debug("Loading Delta file stats: %s", uri)
        storage_options = self._get_storage_options()
        result = await asyncio.to_thread(_run_file_stats_subprocess, uri, storage_options)

        if not result.get("ok"):
            raise DeltaError(result.get("error", "Unknown error in Delta file-stats reader"))

        return DeltaFileStats(
            num_files=result["num_files"],
            total_bytes=result["total_bytes"],
            min_file_bytes=result["min_file_bytes"],
            max_file_bytes=result["max_file_bytes"],
            avg_file_bytes=result["avg_file_bytes"],
            partition_counts=result["partition_counts"],
        )

    async def get_analysis(
        self,
        workspace: str,
        item_path: str,
        table_name: str,
        dfs_client,
        *,
        max_files: int = 20,
        progress_callback=None,
    ) -> DeltaAnalysisResult:
        """Analyze a Delta table's parquet files: row groups, column chunks, columns.

        Mirrors the 5-dataframe structure of semantic-link-labs delta_analyzer.
        Reads only the parquet footer (HTTP Range tail read) per file — no full
        file downloads required.

        Args:
            workspace: Workspace name or GUID.
            item_path: Item path like "MyLakehouse.Lakehouse".
            table_name: Table name under Tables/.
            dfs_client: An active :class:`onelake_client.dfs.DfsClient` instance.
            max_files: Cap on number of parquet files to inspect.
            progress_callback: Optional ``async callable(current, total, filename)``
                called after each file is analysed.

        Returns:
            :class:`DeltaAnalysisResult` with summary, per-file, row-group,
            column-chunk, and aggregated column data.
        """
        import io
        import struct

        import pyarrow.parquet as pq

        _PARQUET_MAGIC = b"PAR1"
        _FOOTER_TAIL = 1 * 1024 * 1024  # 1 MB — enough for any realistic footer

        # ── 1. Get the list of current active parquet file paths ────────
        uri = _build_table_uri(workspace, item_path, table_name, self._dfs_host)
        storage_options = self._get_storage_options()
        result = await asyncio.to_thread(_run_file_stats_subprocess, uri, storage_options)
        if not result.get("ok"):
            raise DeltaError(result.get("error", "Unknown error resolving file list"))

        # ── 1a. Build physical→logical column name mapping ──────────────
        # Tables with delta.columnMapping.mode=id store columns in parquet
        # using GUIDs (delta.columnMapping.physicalName) rather than the
        # logical names visible in the Delta schema.
        phys_to_logical: dict[str, str] = {}
        try:
            meta_info = await self.get_metadata(workspace, item_path, table_name)
            for col in meta_info.schema_:
                if col.metadata:
                    phys = col.metadata.get("delta.columnMapping.physicalName")
                    if phys is not None:
                        phys_to_logical[str(phys)] = col.name
        except Exception as exc:
            logger.debug("Could not load schema for column name mapping: %s", exc)

        # List parquet files in the table directory via DFS (current active files)
        table_dir = f"{item_path}/Tables/{table_name}"
        dfs_paths = await dfs_client.list_paths(workspace, table_dir)
        parquet_paths = [
            p for p in dfs_paths
            if not p.is_directory and p.name.endswith(".parquet")
        ]

        # Cap to max_files
        files_skipped = max(0, len(parquet_paths) - max_files)
        parquet_paths = parquet_paths[:max_files]

        # ── 2. Read parquet footer per file via Range request ───────────
        parquet_file_infos: list[ParquetFileInfo] = []
        row_group_infos: list[RowGroupInfo] = []
        column_chunk_infos: list[ColumnChunkInfo] = []

        total_row_count = 0
        total_row_groups = 0
        max_rows_rg = 0
        min_rows_rg: int | None = None
        total_compressed = 0

        for idx, pinfo in enumerate(parquet_paths):
            file_path = pinfo.name
            file_name = file_path.split("/")[-1]

            if progress_callback is not None:
                await progress_callback(idx + 1, len(parquet_paths), file_name)

            try:
                tail = await dfs_client.read_file_tail(workspace, file_path, _FOOTER_TAIL)
            except Exception as exc:
                logger.debug("Failed to read tail of %s: %s", file_path, exc)
                continue

            # Validate parquet magic at end of tail
            if len(tail) < 8 or tail[-4:] != _PARQUET_MAGIC:
                logger.debug("Skipping %s — not a valid parquet file", file_path)
                continue

            footer_len = struct.unpack("<I", tail[-8:-4])[0]
            footer_start = len(tail) - 8 - footer_len
            if footer_start < 0:
                # Footer overflows our tail; fall back to full file read
                try:
                    raw = await dfs_client.read_file(
                        workspace, file_path, max_bytes=50 * 1024 * 1024
                    )
                    pf = pq.ParquetFile(io.BytesIO(raw))
                except Exception as exc2:
                    logger.debug("Fallback full read failed for %s: %s", file_path, exc2)
                    continue
            else:
                footer_bytes = tail[footer_start : len(tail) - 8]
                # Reconstruct a minimal valid parquet file buffer that pyarrow can parse:
                # PAR1 + footer + len(footer) [4 bytes LE] + PAR1
                fake_buf = (
                    _PARQUET_MAGIC
                    + footer_bytes
                    + struct.pack("<I", footer_len)
                    + _PARQUET_MAGIC
                )
                try:
                    pf = pq.ParquetFile(io.BytesIO(fake_buf))
                except Exception as exc3:
                    logger.debug(
                        "Footer-only parse failed for %s (%s), trying full read", file_path, exc3
                    )
                    try:
                        raw = await dfs_client.read_file(
                            workspace, file_path, max_bytes=50 * 1024 * 1024
                        )
                        pf = pq.ParquetFile(io.BytesIO(raw))
                    except Exception as exc4:
                        logger.debug("Full read fallback also failed: %s", exc4)
                        continue

            meta = pf.metadata
            file_row_count = meta.num_rows
            file_row_groups = meta.num_row_groups
            created_by = meta.created_by or ""

            parquet_file_infos.append(
                ParquetFileInfo(
                    parquet_file=file_name,
                    row_count=file_row_count,
                    row_groups=file_row_groups,
                    created_by=created_by,
                )
            )

            total_row_count += file_row_count
            total_row_groups += file_row_groups

            for rg_idx in range(file_row_groups):
                rg = meta.row_group(rg_idx)
                rg_rows = rg.num_rows
                rg_compressed = 0
                rg_uncompressed = 0

                max_rows_rg = max(max_rows_rg, rg_rows)
                if min_rows_rg is None:
                    min_rows_rg = rg_rows
                else:
                    min_rows_rg = min(min_rows_rg, rg_rows)

                for col_idx in range(rg.num_columns):
                    col = rg.column(col_idx)
                    rg_compressed += col.total_compressed_size
                    rg_uncompressed += col.total_uncompressed_size

                    logical_name = phys_to_logical.get(col.path_in_schema, col.path_in_schema)
                    encodings = ", ".join(str(e) for e in col.encodings) if col.encodings else ""
                    column_chunk_infos.append(
                        ColumnChunkInfo(
                            parquet_file=file_name,
                            row_group_id=rg_idx + 1,
                            column_id=col_idx,
                            column_name=logical_name,
                            column_type=col.physical_type,
                            compressed_size=col.total_compressed_size,
                            uncompressed_size=col.total_uncompressed_size,
                            has_dict=bool(col.has_dictionary_page),
                            value_count=col.num_values,
                            encodings=encodings,
                        )
                    )

                total_compressed += rg_compressed
                ratio = (
                    rg_compressed / rg_uncompressed if rg_uncompressed else 0.0
                )
                row_group_infos.append(
                    RowGroupInfo(
                        parquet_file=file_name,
                        row_group_id=rg_idx + 1,
                        row_count=rg_rows,
                        compressed_size=rg_compressed,
                        uncompressed_size=rg_uncompressed,
                        compression_ratio=ratio,
                    )
                )

        # ── 3. Back-fill table-level totals ─────────────────────────────
        for pfi in parquet_file_infos:
            pfi.total_table_rows = total_row_count
            pfi.total_table_row_groups = total_row_groups

        for rgi in row_group_infos:
            rgi.total_table_rows = total_row_count
            rgi.total_table_row_groups = total_row_groups
            rgi.ratio_of_total_rows = (
                rgi.row_count / total_row_count * 100.0 if total_row_count else 0.0
            )

        # ── 4. Aggregate column-level stats ─────────────────────────────
        col_agg: dict[tuple[str, str], dict] = {}
        for cc in column_chunk_infos:
            key = (cc.column_name, cc.column_type)
            if key not in col_agg:
                col_agg[key] = {"compressed": 0, "uncompressed": 0}
            col_agg[key]["compressed"] += cc.compressed_size
            col_agg[key]["uncompressed"] += cc.uncompressed_size

        columns: list[ColumnInfo] = []
        for (col_name, col_type), agg in col_agg.items():
            pct = (
                agg["compressed"] / total_compressed * 100.0 if total_compressed else 0.0
            )
            columns.append(
                ColumnInfo(
                    column_name=col_name,
                    column_type=col_type,
                    compressed_size=agg["compressed"],
                    uncompressed_size=agg["uncompressed"],
                    total_table_rows=total_row_count,
                    size_percent_of_table=pct,
                )
            )
        columns.sort(key=lambda c: c.compressed_size, reverse=True)

        # ── 5. Build summary ─────────────────────────────────────────────
        avg_rg = total_row_count / total_row_groups if total_row_groups else 0.0
        skip_reason = (
            f"{files_skipped} file(s) not analysed (capped at {max_files})"
            if files_skipped
            else ""
        )
        summary = DeltaAnalysisSummary(
            row_count=total_row_count,
            parquet_files=len(parquet_file_infos),
            row_groups=total_row_groups,
            max_rows_per_row_group=max_rows_rg,
            min_rows_per_row_group=min_rows_rg or 0,
            avg_rows_per_row_group=avg_rg,
            total_compressed_size=total_compressed,
            files_skipped=files_skipped,
            files_skipped_reason=skip_reason,
        )

        return DeltaAnalysisResult(
            summary=summary,
            parquet_files=parquet_file_infos,
            row_groups=row_group_infos,
            column_chunks=column_chunk_infos,
            columns=columns,
        )
