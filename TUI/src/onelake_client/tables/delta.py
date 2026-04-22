from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from deltalake.exceptions import DeltaError

from onelake_client.models.table import Column, DeltaTableInfo

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


def _schema_to_columns(schema) -> list[Column]:
    """Convert a deltalake Schema to our Column model."""
    columns: list[Column] = []
    # deltalake >= 0.18: Schema is not directly iterable, use .fields
    fields = schema.fields if hasattr(schema, "fields") else schema
    for field in fields:
        columns.append(
            Column(
                name=field.name,
                type=str(field.type),
                nullable=field.nullable,
                metadata=field.metadata if field.metadata else None,
            )
        )
    return columns


def _coerce_timestamps(table):
    """Downcast timestamp[ns] columns to timestamp[us] to avoid Arrow cast errors.

    Parquet files written with nanosecond-precision timestamps can trigger
    ``ArrowInvalid: Casting from timestamp[ns] to timestamp[us, tz=UTC]
    would lose data`` when pyarrow reads them.

    We cast with ``safe=False`` so the lossy ns→us downcast is allowed
    instead of raising. This truncates sub-microsecond precision; it does
    not implicitly convert invalid or out-of-range values to null.
    """
    import pyarrow as pa

    new_columns = []
    needs_cast = False
    for i in range(table.num_columns):
        field = table.schema.field(i)
        if pa.types.is_timestamp(field.type) and field.type.unit == "ns":
            target_type = pa.timestamp("us", tz=field.type.tz)
            new_columns.append((i, table.column(i).cast(target_type, safe=False)))
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
            "type": str(f.type),
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
            table = ds.head(limit)
            if not hasattr(table, "num_columns") or not hasattr(table, "schema"):
                return table
            return _coerce_timestamps(table)

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
