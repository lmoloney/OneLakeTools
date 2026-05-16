from __future__ import annotations

import asyncio
import io
import logging
import struct
import uuid
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from deltalake.exceptions import DeltaError

from onelake_client.models.table import (
    Column,
    ColumnChunkInfo,
    ColumnInfo,
    DeltaAnalysisResult,
    DeltaAnalysisSummary,
    DeltaTableInfo,
    ParquetFileInfo,
    RowGroupInfo,
)

if TYPE_CHECKING:
    import pyarrow as pa

    from onelake_client.auth import OneLakeAuth
    from onelake_client.dfs.client import DfsClient
    from onelake_client.fabric import FabricClient

logger = logging.getLogger("onelake_client.tables.delta")

_SUBPROCESS_TIMEOUT = 30  # seconds


def _is_guid(value: str) -> bool:
    """Check whether *value* looks like a UUID/GUID."""
    try:
        uuid.UUID(value)
        return True
    except ValueError:
        return False


def _build_table_uri(workspace: str, item_path: str, table_name: str, dfs_host: str) -> str:
    """Build an abfss:// URI for a Delta table on OneLake.

    Args:
        workspace: Workspace name or GUID.
        item_path: Item path like "MyLakehouse.Lakehouse" or a GUID.
        table_name: Table name under the Tables/ directory.
        dfs_host: DFS endpoint hostname (varies per ring).

    Returns:
        abfss:// URI pointing to the Delta table.
    """
    return f"abfss://{workspace}@{dfs_host}/{item_path}/Tables/{table_name}"


def _clean_type_str(type_str: str) -> str:
    """Clean deltalake type string representations.

    deltalake >= 1.0 returns PrimitiveType("string") instead of "string".
    """
    s = str(type_str)
    if s.startswith("PrimitiveType("):
        s = s.removeprefix("PrimitiveType(").removesuffix(")")
        s = s.strip("\"'")
    return s


def _parse_dfs_path(uri: str) -> tuple[str, str]:
    """Parse an abfss URI into (workspace_guid, dfs_path)."""
    after_scheme = uri.split("://", 1)[1]  # ws-guid@host/path
    account, rest = after_scheme.split("@", 1)  # ws-guid, host/path
    _, path = rest.split("/", 1)  # host, path
    return account, path


_PARQUET_MAGIC = b"PAR1"


def _parse_parquet_footer(raw_bytes: bytes):
    """Parse parquet footer from file bytes and return pyarrow metadata.

    Accepts either a full file or a tail buffer. Reconstructs a minimal
    in-memory parquet file that pyarrow can parse for metadata-only access.

    Returns:
        A ``pyarrow.parquet.ParquetFile.metadata`` object, or None if
        the bytes are not a valid parquet file.
    """
    import pyarrow.parquet as pq

    if len(raw_bytes) < 8 or raw_bytes[-4:] != _PARQUET_MAGIC:
        return None

    footer_len = struct.unpack("<I", raw_bytes[-8:-4])[0]
    if footer_len + 8 > len(raw_bytes):
        return None

    footer_content = raw_bytes[-(footer_len + 8) : -8]
    fake_buf = _PARQUET_MAGIC + footer_content + struct.pack("<I", footer_len) + _PARQUET_MAGIC
    try:
        pf = pq.ParquetFile(io.BytesIO(fake_buf))
        return pf.metadata
    except Exception:
        logger.debug("Failed to parse parquet footer (%d bytes)", len(raw_bytes))
        return None


def _extract_file_stats(
    metadata,
    file_name: str,
    col_name_map: dict[str, str] | None = None,
) -> tuple[ParquetFileInfo, list[RowGroupInfo], list[ColumnChunkInfo], dict[str, dict]]:
    """Extract analysis stats from parquet file metadata.

    Args:
        metadata: pyarrow parquet FileMetaData object.
        file_name: Display name for the file.
        col_name_map: Optional physical→logical column name mapping.

    Returns:
        (file_info, row_groups, column_chunks, column_agg_dict)
    """
    if col_name_map is None:
        col_name_map = {}

    file_rows = metadata.num_rows
    file_info = ParquetFileInfo(
        file_name=file_name,
        row_count=file_rows,
        row_group_count=metadata.num_row_groups,
        created_by=metadata.created_by,
    )

    row_groups: list[RowGroupInfo] = []
    column_chunks: list[ColumnChunkInfo] = []
    column_agg: dict[str, dict] = {}

    for rg_idx in range(metadata.num_row_groups):
        rg = metadata.row_group(rg_idx)
        compressed = sum(rg.column(c).total_compressed_size for c in range(rg.num_columns))
        uncompressed = rg.total_byte_size
        ratio = compressed / uncompressed if uncompressed > 0 else 0.0

        row_groups.append(
            RowGroupInfo(
                file_name=file_name,
                row_group_id=rg_idx + 1,
                row_count=rg.num_rows,
                compressed_size=compressed,
                uncompressed_size=uncompressed,
                compression_ratio=ratio,
            )
        )

        for col_idx in range(rg.num_columns):
            cc = rg.column(col_idx)
            col_path = cc.path_in_schema
            col_name = col_name_map.get(col_path, col_path)

            column_chunks.append(
                ColumnChunkInfo(
                    file_name=file_name,
                    row_group_id=rg_idx + 1,
                    column_id=col_idx + 1,
                    column_name=col_name,
                    physical_type=str(cc.physical_type),
                    compressed_size=cc.total_compressed_size,
                    uncompressed_size=cc.total_uncompressed_size,
                    num_values=cc.num_values,
                    has_dictionary=(
                        cc.dictionary_page_offset is not None and cc.dictionary_page_offset >= 0
                    ),
                    encodings=list(cc.encodings) if hasattr(cc, "encodings") else [],
                )
            )

            if col_name not in column_agg:
                column_agg[col_name] = {
                    "compressed": 0,
                    "uncompressed": 0,
                    "col_idx": col_idx + 1,
                }
            column_agg[col_name]["compressed"] += cc.total_compressed_size
            column_agg[col_name]["uncompressed"] += cc.total_uncompressed_size

    return file_info, row_groups, column_chunks, column_agg


def _build_analysis_result(
    all_files: list[ParquetFileInfo],
    all_row_groups: list[RowGroupInfo],
    all_column_chunks: list[ColumnChunkInfo],
    column_agg: dict[str, dict],
    files_skipped: int = 0,
    file_paths: dict[str, tuple[str, str]] | None = None,
) -> DeltaAnalysisResult:
    """Build a DeltaAnalysisResult from collected stats."""
    total_rows = sum(f.row_count for f in all_files)
    total_compressed = sum(v["compressed"] for v in column_agg.values())

    columns: list[ColumnInfo] = []
    for name, agg in column_agg.items():
        columns.append(
            ColumnInfo(
                column_id=agg["col_idx"],
                column_name=name,
                total_compressed_size=agg["compressed"],
                total_uncompressed_size=agg["uncompressed"],
                total_table_rows=total_rows,
                pct_of_table=(
                    agg["compressed"] / total_compressed if total_compressed > 0 else 0.0
                ),
            )
        )

    for f in all_files:
        f.total_table_rows = total_rows
    for rg in all_row_groups:
        rg.total_table_rows = total_rows

    rg_row_counts = [rg.row_count for rg in all_row_groups]
    summary = DeltaAnalysisSummary(
        total_rows=total_rows,
        total_files=len(all_files),
        total_row_groups=len(all_row_groups),
        avg_rows_per_row_group=(total_rows / len(all_row_groups) if all_row_groups else 0),
        min_rows_per_row_group=min(rg_row_counts) if rg_row_counts else 0,
        max_rows_per_row_group=max(rg_row_counts) if rg_row_counts else 0,
        total_compressed_size=total_compressed,
        total_uncompressed_size=sum(v["uncompressed"] for v in column_agg.values()),
        files_skipped=files_skipped,
    )

    return DeltaAnalysisResult(
        summary=summary,
        files=all_files,
        row_groups=all_row_groups,
        column_chunks=all_column_chunks,
        columns=columns,
        file_paths=file_paths or {},
    )


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


_CDF_NOT_ENABLED_FRAGMENTS = [
    "does not have change data enabled",
    "change data feed is not enabled",
]


def is_cdf_not_enabled_error(exc: BaseException) -> bool:
    """Return True if *exc* indicates CDF is not enabled for a requested version.

    delta-rs uses different wording depending on the code path, so we match
    against multiple known message fragments.  Only this error should trigger
    CDF retry/discovery logic — all other exceptions must propagate.
    """
    msg = str(exc).lower()
    return any(frag in msg for frag in _CDF_NOT_ENABLED_FRAGMENTS)


def _nullify_out_of_range(col, target_type):
    """Replace timestamps outside year 0001–9999 with null.

    After a ``safe=False`` cast, corrupt sentinel values may map to
    dates far outside any reasonable range.  This helper nullifies
    them so the preview shows ``null`` instead of garbage.

    Bounds are expressed as microseconds-from-epoch and scaled to
    the target unit so the same logic works for ``us``, ``ms``, and ``s``.
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    # Year 0001-01-01T00:00:00 and year 9999-12-31T23:59:59.999999 in µs from epoch.
    _MIN_US = -62_135_596_800_000_000
    _MAX_US = 253_402_300_799_999_999

    _SCALE = {"us": 1, "ms": 1_000, "s": 1_000_000}
    scale = _SCALE.get(target_type.unit)
    if scale is None:
        return col

    lo = _MIN_US // scale
    hi = _MAX_US // scale

    int_view = col.cast(pa.int64())
    valid = pc.and_(pc.greater_equal(int_view, lo), pc.less_equal(int_view, hi))
    return pc.if_else(valid, col, pa.scalar(None, type=target_type))


def coerce_timestamps(table: pa.Table) -> pa.Table:
    """Downcast timestamp[ns] columns to timestamp[us] to avoid Arrow cast errors.

    Parquet files written with nanosecond-precision timestamps can trigger
    ``ArrowInvalid: Casting from timestamp[ns] to timestamp[us, tz=UTC]
    would lose data`` when pyarrow reads them.

    We cast with ``safe=False`` so the lossy ns→us downcast is allowed
    instead of raising.  For representable values this truncates
    sub-microsecond precision; timestamps outside year 0001–9999 are
    defensively nullified via :func:`_nullify_out_of_range`.

    If *table* is not a :class:`pyarrow.Table` (e.g. a test mock),
    it is returned unchanged.
    """
    import pyarrow as pa

    if not isinstance(table, pa.Table):
        return table

    new_columns = []
    needs_cast = False
    for i in range(table.num_columns):
        field = table.schema.field(i)
        if pa.types.is_timestamp(field.type) and field.type.unit == "ns":
            target_type = pa.timestamp("us", tz=field.type.tz)
            cast_col = table.column(i).cast(target_type, safe=False)
            new_columns.append((i, _nullify_out_of_range(cast_col, target_type)))
            needs_cast = True

    if not needs_cast:
        return table

    for col_idx, new_col in new_columns:
        field = table.schema.field(col_idx)
        target_type = pa.timestamp("us", tz=field.type.tz)
        table = table.set_column(
            col_idx,
            field.with_type(target_type),
            new_col,
        )
    return table


# ── Subprocess workers (top-level for pickling) ────────────────────────


_METADATA_SCRIPT = """
import sys, json
from deltalake import DeltaTable

def _clean_type(t):
    s = str(t)
    if s.startswith("PrimitiveType("):
        s = s.removeprefix("PrimitiveType(").removesuffix(")")
        s = s.strip("\\\"'")
    return s

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
            "type": _clean_type(f.type),
            "nullable": f.nullable,
            "metadata": dict(f.metadata) if f.metadata else None,
        }
        for f in fields
    ]

    version = dt.version()
    files = dt.file_uris()
    meta = dt.metadata()

    # Protocol extraction
    reader_version = 1
    writer_version = 2
    reader_features = []
    writer_features = []
    try:
        protocol = dt.protocol()
        reader_version = protocol.min_reader_version
        writer_version = protocol.min_writer_version
        rf = getattr(protocol, 'reader_features', None)
        reader_features = list(rf) if rf else []
        wf = getattr(protocol, 'writer_features', None)
        writer_features = list(wf) if wf else []
    except Exception:
        pass

    size_bytes = 0
    total_rows = None
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

    try:
        col_names = []
        if hasattr(add_actions, "column_names"):
            col_names = add_actions.column_names
        has_col = hasattr(add_actions, "column")
        if has_col and "num_records" in col_names:
            nr = add_actions.column("num_records").to_pylist()
            total_rows = sum(v for v in nr if v is not None)
        elif hasattr(add_actions, "to_pydict"):
            sd2 = add_actions.to_pydict()
            nr = sd2.get("num_records", [])
            if nr:
                total_rows = sum(v for v in nr if v is not None)
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
        "reader_version": reader_version,
        "writer_version": writer_version,
        "reader_features": reader_features,
        "writer_features": writer_features,
        "total_rows": total_rows,
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


# Known features that delta-rs may not fully support
_POTENTIALLY_UNSUPPORTED_FEATURES = {
    "deletionVectors": "Deletion vectors — data preview may include soft-deleted rows",
    "v2Checkpoint": "V2 checkpoints — log replay may be incomplete",
    "typeWidening": "Type widening — column types may have changed between versions",
}


def _check_reader_warnings(info: DeltaTableInfo) -> None:
    """Populate warnings for reader features that may not be fully supported."""
    for feature in info.reader_features:
        if feature in _POTENTIALLY_UNSUPPORTED_FEATURES:
            info.warnings.append(
                f"⚠️ Table uses '{feature}': {_POTENTIALLY_UNSUPPORTED_FEATURES[feature]}"
            )


class DeltaTableReader:
    """Reads Delta table metadata from OneLake.

    Uses the `deltalake` library (delta-rs Python bindings) with OneLake
    storage options for authentication.

    The ``get_metadata`` method runs deltalake in a **subprocess** to
    isolate the main process from Rust panics that can occur with certain
    table features (e.g. deletion vectors, v2 checkpoints).

    When a ``fabric_client`` is provided, friendly-name workspace/item
    paths are automatically resolved to GUIDs before calling delta-rs.
    This is required because delta-rs uses the Azure Blob API protocol,
    which doesn't resolve OneLake friendly names for service principals.

    Usage:
        auth = OneLakeAuth()
        reader = DeltaTableReader(auth, dfs_host="onelake.dfs.fabric.microsoft.com")
        info = await reader.get_metadata("MyWorkspace", "MyLakehouse.Lakehouse", "customers")
    """

    def __init__(
        self,
        auth: OneLakeAuth,
        dfs_host: str = "onelake.dfs.fabric.microsoft.com",
        fabric_client: FabricClient | None = None,
        dfs_client: DfsClient | None = None,
    ):
        self._auth = auth
        self._dfs_host = dfs_host
        self._fabric = fabric_client
        self._dfs = dfs_client
        self._isolate = True  # subprocess isolation for Rust panic safety
        self._guid_cache: dict[tuple[str, str], tuple[str, str]] = {}

    def _get_storage_options(self) -> dict:
        """Get fresh storage options with a current token."""
        return self._auth.storage_options()

    async def _resolve_uri(self, workspace: str, item_path: str, table_name: str) -> str:
        """Build an abfss URI, resolving friendly names to GUIDs when needed.

        Delta-rs uses the Azure Blob API protocol, which doesn't resolve
        OneLake friendly-name paths for service principal tokens.  When a
        ``fabric_client`` is available and either the workspace or item is
        a friendly name, this method resolves both to GUIDs via the Fabric
        REST API before building the URI.
        """
        ws, item = workspace, item_path
        needs_resolution = not _is_guid(workspace) or not _is_guid(item_path)

        if needs_resolution and self._fabric is not None:
            ws, item = await self._resolve_to_guids(workspace, item_path)
        elif needs_resolution:
            logger.debug(
                "Friendly-name path without fabric_client — "
                "delta-rs may fail for service-principal tokens"
            )

        return _build_table_uri(ws, item, table_name, self._dfs_host)

    async def _resolve_to_guids(self, workspace: str, item_path: str) -> tuple[str, str]:
        """Resolve workspace name + item display path to GUIDs.

        Results are cached for the lifetime of this reader instance.
        """
        cache_key = (workspace, item_path)
        if cache_key in self._guid_cache:
            return self._guid_cache[cache_key]

        ws_id = workspace
        if not _is_guid(workspace):
            workspaces = await self._fabric.list_workspaces()
            matches = [w for w in workspaces if w.display_name == workspace]
            if len(matches) == 0:
                raise DeltaError(
                    f"Workspace '{workspace}' not found. "
                    "Check the name or use the workspace GUID instead."
                )
            if len(matches) > 1:
                ids = ", ".join(m.id for m in matches)
                raise DeltaError(
                    f"Multiple workspaces named '{workspace}' found ({ids}). "
                    "Use the workspace GUID to disambiguate."
                )
            ws_id = matches[0].id
            logger.debug("Resolved workspace '%s' → %s", workspace, ws_id)

        item_id = item_path
        if not _is_guid(item_path):
            dot_idx = item_path.rfind(".")
            if dot_idx <= 0:
                raise DeltaError(
                    f"Item path '{item_path}' must be in 'DisplayName.Type' "
                    "format (e.g. 'MyLakehouse.Lakehouse') or a GUID."
                )
            item_name = item_path[:dot_idx]
            item_type = item_path[dot_idx + 1 :]
            items = await self._fabric.list_items(ws_id, item_type=item_type)
            matches = [i for i in items if i.display_name == item_name]
            if len(matches) == 0:
                raise DeltaError(
                    f"Item '{item_name}' (type={item_type}) not found in "
                    f"workspace '{workspace}'. Check the name or use the "
                    "item GUID instead."
                )
            if len(matches) > 1:
                ids = ", ".join(m.id for m in matches)
                raise DeltaError(
                    f"Multiple items named '{item_name}' (type={item_type}) "
                    f"found ({ids}). Use the item GUID to disambiguate."
                )
            item_id = matches[0].id
            logger.debug("Resolved item '%s' → %s", item_path, item_id)

        self._guid_cache[cache_key] = (ws_id, item_id)
        return ws_id, item_id

    def _load_table_sync(self, uri: str):
        """Synchronously load a DeltaTable (called via to_thread)."""
        from deltalake import DeltaTable

        return DeltaTable(uri, storage_options=self._get_storage_options())

    async def get_metadata(self, workspace: str, item_path: str, table_name: str) -> DeltaTableInfo:
        """Get metadata for a Delta table.

        Runs deltalake in a subprocess by default to isolate Rust panics.

        Args:
            workspace: Workspace name or GUID.
            item_path: Item path like "MyLakehouse.Lakehouse" or GUID.
            table_name: Table name under Tables/.

        Returns:
            DeltaTableInfo with schema, version, file count, size, etc.
        """
        uri = await self._resolve_uri(workspace, item_path, table_name)
        logger.debug("Loading Delta table: %s", uri)

        if self._isolate:
            info = await self._get_metadata_subprocess(uri, table_name)
        else:
            info = await self._get_metadata_inprocess(uri, table_name)
        _check_reader_warnings(info)
        return info

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
            reader_version=result.get("reader_version", 1),
            writer_version=result.get("writer_version", 2),
            reader_features=result.get("reader_features", []),
            writer_features=result.get("writer_features", []),
            total_rows=result.get("total_rows"),
        )

    async def _get_metadata_inprocess(self, uri: str, table_name: str) -> DeltaTableInfo:
        """Load metadata in-process (used when _isolate=False, e.g. tests)."""
        dt = await asyncio.to_thread(self._load_table_sync, uri)

        schema = _schema_to_columns(dt.schema())
        version = dt.version()
        files = dt.file_uris()
        metadata = dt.metadata()

        # Protocol extraction
        reader_version = 1
        writer_version = 2
        reader_features: list[str] = []
        writer_features: list[str] = []
        try:
            protocol = dt.protocol()
            reader_version = protocol.min_reader_version
            writer_version = protocol.min_writer_version
            reader_features = (
                list(protocol.reader_features)
                if hasattr(protocol, "reader_features") and protocol.reader_features
                else []
            )
            writer_features = (
                list(protocol.writer_features)
                if hasattr(protocol, "writer_features") and protocol.writer_features
                else []
            )
        except Exception:
            pass

        size_bytes = 0
        add_actions = None
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

        # Row count from add action statistics
        total_rows = None
        if add_actions is not None:
            try:
                if hasattr(add_actions, "column") and "num_records" in (
                    add_actions.column_names if hasattr(add_actions, "column_names") else []
                ):
                    total_rows = sum(
                        v for v in add_actions.column("num_records").to_pylist() if v is not None
                    )
                elif hasattr(add_actions, "to_pydict"):
                    sd = add_actions.to_pydict()
                    nr = sd.get("num_records", [])
                    total_rows = sum(v for v in nr if v is not None) if nr else None
            except Exception:
                pass

        return DeltaTableInfo(
            name=metadata.name or table_name,
            schema_=schema,
            version=version,
            num_files=len(files),
            size_bytes=size_bytes,
            partition_columns=list(metadata.partition_columns),
            properties=dict(metadata.configuration) if metadata.configuration else {},
            description=metadata.description,
            reader_version=reader_version,
            writer_version=writer_version,
            reader_features=reader_features,
            writer_features=writer_features,
            total_rows=total_rows,
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
        uri = await self._resolve_uri(workspace, item_path, table_name)
        logger.debug("Reading sample (%d rows) from: %s", limit, uri)
        dt = await asyncio.to_thread(self._load_table_sync, uri)

        def _head():
            import pyarrow as pa

            ds = dt.to_pyarrow_dataset()
            try:
                table = ds.head(limit)
            except pa.lib.ArrowInvalid as exc:
                exc_msg = str(exc).lower()
                if "timestamp[ns]" not in exc_msg or "would lose data" not in exc_msg:
                    raise
                # Delta schema says timestamp[us] but parquet has timestamp[ns].
                # Re-read fragments with their physical (parquet) schema to
                # bypass the safe cast, then let coerce_timestamps handle it.
                _MAX_FALLBACK_FRAGMENTS = 10
                batches: list[pa.RecordBatch] = []
                remaining = limit
                for frag_idx, frag in enumerate(ds.get_fragments()):
                    if remaining <= 0 or frag_idx >= _MAX_FALLBACK_FRAGMENTS:
                        break
                    for batch in frag.to_batches(schema=frag.physical_schema):
                        if remaining <= 0:
                            break
                        sliced = batch.slice(0, remaining)
                        batches.append(sliced)
                        remaining -= sliced.num_rows
                table = (
                    pa.Table.from_batches(batches)
                    if batches
                    else pa.table({f.name: pa.array([], type=f.type) for f in ds.schema})
                )
            return coerce_timestamps(table)

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
        uri = await self._resolve_uri(workspace, item_path, table_name)
        logger.debug("Reading CDF from: %s (v%d→%s)", uri, starting_version, ending_version)
        dt = await asyncio.to_thread(self._load_table_sync, uri)

        def _load_cdf():
            kwargs: dict = {"starting_version": starting_version}
            if ending_version is not None:
                kwargs["ending_version"] = ending_version
            cdf = dt.load_cdf(**kwargs)
            return cdf.read_all() if hasattr(cdf, "read_all") else cdf

        return await asyncio.to_thread(_load_cdf)

    async def find_cdf_start_version(
        self,
        workspace: str,
        item_path: str,
        table_name: str,
        *,
        low: int = 0,
        high: int | None = None,
    ) -> int:
        """Binary-search for the earliest version with CDF enabled.

        Finds the first version in [low, high] where ``load_cdf`` succeeds.
        This locates the start of the *current* contiguous CDF-enabled range —
        if CDF was enabled, disabled, then re-enabled, this returns the start
        of the latest enabled interval.

        Raises the original ``DeltaError`` if no CDF-enabled version is found,
        or if a non-CDF error occurs during the search.
        """
        uri = await self._resolve_uri(workspace, item_path, table_name)
        dt = await asyncio.to_thread(self._load_table_sync, uri)

        if high is None:
            high = dt.version()

        if low > high:
            raise ValueError(f"Invalid version range: low ({low}) > high ({high})")

        def _search():
            lo, hi = low, high
            result = -1
            last_exc: BaseException | None = None

            while lo <= hi:
                mid = (lo + hi) // 2
                try:
                    # Probe (mid, high) — not (mid, mid) — so the predicate is
                    # monotonic even if CDF was toggled on/off/on.  Once mid is
                    # inside the current contiguous range, all probes succeed.
                    cdf = dt.load_cdf(starting_version=mid, ending_version=high)
                    if hasattr(cdf, "read_all"):
                        cdf.read_all()
                    result = mid
                    hi = mid - 1
                except Exception as exc:
                    if not is_cdf_not_enabled_error(exc):
                        raise
                    last_exc = exc
                    lo = mid + 1

            if result < 0:
                raise last_exc  # type: ignore[misc]
            return result

        return await asyncio.to_thread(_search)

    async def list_files(self, workspace: str, item_path: str, table_name: str) -> list[str]:
        """List data files in a Delta table.

        Returns:
            List of relative file paths (parquet files).
        """
        uri = await self._resolve_uri(workspace, item_path, table_name)
        dt = await asyncio.to_thread(self._load_table_sync, uri)
        return dt.file_uris()

    async def get_analysis(
        self,
        workspace: str,
        item_path: str,
        table_name: str,
        *,
        max_files: int = 20,
        progress_callback: Callable[[int, int, str], Awaitable[None]] | None = None,
    ) -> DeltaAnalysisResult:
        """Analyse a Delta table by reading parquet footers via Range requests.

        Reads the Parquet footer from each data file using suffix-range HTTP
        requests, reconstructs the metadata in-memory, and aggregates row
        group, column chunk, and column-level statistics.

        Args:
            workspace: Workspace name or GUID.
            item_path: Item path like ``"MyLakehouse.Lakehouse"`` or GUID.
            table_name: Table name under ``Tables/``.
            max_files: Cap on how many data files to inspect.
            progress_callback: Optional ``async (current, total, filename)``
                callback for progress reporting.

        Returns:
            :class:`DeltaAnalysisResult` with summary, file, row-group,
            column-chunk, and column-level views.
        """
        if self._dfs is None:
            raise RuntimeError(
                "DfsClient required for get_analysis() — pass dfs_client to constructor"
            )

        _INITIAL_TAIL = 64 * 1024  # 64 KB
        _MAX_FOOTER_BYTES = 16 * 1024 * 1024  # 16 MB safety cap per file

        file_uris = await self.list_files(workspace, item_path, table_name)
        files_skipped = max(0, len(file_uris) - max_files)
        file_uris = file_uris[:max_files]

        # Build physical→logical column name mapping from Delta schema metadata
        phys_to_logical: dict[str, str] = {}
        meta_info = await self.get_metadata(workspace, item_path, table_name)
        for col in meta_info.schema_:
            if col.metadata:
                phys = col.metadata.get("delta.columnMapping.physicalName")
                if phys is not None:
                    phys_to_logical[str(phys)] = col.name

        all_files: list[ParquetFileInfo] = []
        all_row_groups: list[RowGroupInfo] = []
        all_column_chunks: list[ColumnChunkInfo] = []
        column_agg: dict[str, dict] = {}
        file_paths: dict[str, tuple[str, str]] = {}

        for idx, uri in enumerate(file_uris):
            ws_guid, file_path = _parse_dfs_path(uri)
            file_name = file_path.split("/")[-1]
            file_paths[file_name] = (ws_guid, file_path)

            if progress_callback:
                await progress_callback(idx + 1, len(file_uris), file_name)

            tail = await self._dfs.read_file_range(
                ws_guid, file_path, suffix_length=_INITIAL_TAIL, max_bytes=_MAX_FOOTER_BYTES
            )

            metadata = _parse_parquet_footer(tail)
            if metadata is None and len(tail) >= 8 and tail[-4:] == _PARQUET_MAGIC:
                # Footer didn't fit in initial tail — try larger read
                footer_len = struct.unpack("<I", tail[-8:-4])[0]
                tail = await self._dfs.read_file_range(
                    ws_guid, file_path, suffix_length=footer_len + 8, max_bytes=_MAX_FOOTER_BYTES
                )
                metadata = _parse_parquet_footer(tail)

            if metadata is None:
                logger.warning("Skipping %s — not a valid parquet file", file_name)
                continue

            fi, rgs, ccs, col_agg = _extract_file_stats(metadata, file_name, phys_to_logical)
            all_files.append(fi)
            all_row_groups.extend(rgs)
            all_column_chunks.extend(ccs)
            for name, agg in col_agg.items():
                if name not in column_agg:
                    column_agg[name] = {
                        "compressed": 0,
                        "uncompressed": 0,
                        "col_idx": agg["col_idx"],
                    }
                column_agg[name]["compressed"] += agg["compressed"]
                column_agg[name]["uncompressed"] += agg["uncompressed"]

        return _build_analysis_result(
            all_files,
            all_row_groups,
            all_column_chunks,
            column_agg,
            files_skipped,
            file_paths,
        )

    async def analyze_parquet_file(
        self,
        workspace: str,
        path: str,
    ) -> DeltaAnalysisResult:
        """Analyse a single parquet file by reading its footer.

        Unlike :meth:`get_analysis` which operates on Delta tables, this
        method works on any standalone ``.parquet`` file (e.g. under
        ``Files/``).

        Args:
            workspace: Workspace name or GUID.
            path: Full DFS path to the parquet file.

        Returns:
            :class:`DeltaAnalysisResult` with a single file entry.
        """
        if self._dfs is None:
            raise RuntimeError(
                "DfsClient required for analyze_parquet_file() — pass dfs_client to constructor"
            )

        _INITIAL_TAIL = 64 * 1024
        _MAX_FOOTER_BYTES = 16 * 1024 * 1024

        raw = await self._dfs.read_file_range(
            workspace, path, suffix_length=_INITIAL_TAIL, max_bytes=_MAX_FOOTER_BYTES
        )

        metadata = _parse_parquet_footer(raw)
        if metadata is None and len(raw) >= 8 and raw[-4:] == _PARQUET_MAGIC:
            footer_len = struct.unpack("<I", raw[-8:-4])[0]
            raw = await self._dfs.read_file_range(
                workspace, path, suffix_length=footer_len + 8, max_bytes=_MAX_FOOTER_BYTES
            )
            metadata = _parse_parquet_footer(raw)

        if metadata is None:
            raise DeltaError(f"Not a valid parquet file: {path}")

        file_name = path.split("/")[-1]
        fi, rgs, ccs, col_agg = _extract_file_stats(metadata, file_name)
        return _build_analysis_result([fi], rgs, ccs, col_agg)
