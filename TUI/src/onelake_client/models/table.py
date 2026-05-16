from typing import Any

from pydantic import BaseModel


class Column(BaseModel):
    """A table column."""

    name: str
    type: str
    nullable: bool = True
    metadata: dict[str, Any] | None = None
    comment: str | None = None


class DeltaTableInfo(BaseModel):
    """Metadata for a Delta table."""

    name: str | None = None
    schema_: list[Column] = []  # 'schema' is reserved in Pydantic
    version: int = 0
    num_files: int = 0
    size_bytes: int = 0
    partition_columns: list[str] = []
    properties: dict[str, str] = {}
    description: str | None = None
    reader_version: int = 1
    writer_version: int = 2
    reader_features: list[str] = []
    writer_features: list[str] = []
    total_rows: int | None = None
    warnings: list[str] = []


class IcebergTableInfo(BaseModel):
    """Metadata for an Iceberg table."""

    name: str | None = None
    schema_: list[Column] = []
    current_snapshot_id: int | None = None
    format_version: int = 2
    location: str | None = None
    partition_spec: list[dict] = []
    properties: dict[str, str] = {}


# ---------------------------------------------------------------------------
# Delta Analysis models (#27)
# ---------------------------------------------------------------------------


class ParquetFileInfo(BaseModel):
    """Per-file statistics from parquet footer."""

    file_name: str
    row_count: int
    row_group_count: int
    total_table_rows: int = 0  # denormalized for display convenience
    created_by: str | None = None


class RowGroupInfo(BaseModel):
    """Per-row-group statistics."""

    file_name: str
    row_group_id: int  # 1-based
    row_count: int
    total_table_rows: int = 0  # denormalized for display convenience
    compressed_size: int
    uncompressed_size: int
    compression_ratio: float  # raw ratio, e.g. 0.35 = 35%


class ColumnChunkInfo(BaseModel):
    """Per-column-chunk statistics within a row group."""

    file_name: str
    row_group_id: int  # 1-based
    column_id: int  # 1-based
    column_name: str
    physical_type: str
    compressed_size: int
    uncompressed_size: int
    num_values: int
    dictionary_page_size: int = 0
    encodings: list[str] = []


class ColumnInfo(BaseModel):
    """Aggregated per-column statistics across all row groups."""

    column_id: int  # 1-based
    column_name: str
    total_compressed_size: int
    total_uncompressed_size: int
    total_table_rows: int = 0  # denormalized for display convenience
    pct_of_table: float = 0.0


class DeltaAnalysisSummary(BaseModel):
    """Overall table analysis summary."""

    total_rows: int
    total_files: int
    total_row_groups: int
    avg_rows_per_row_group: float
    min_rows_per_row_group: int
    max_rows_per_row_group: int
    total_compressed_size: int
    total_uncompressed_size: int
    files_skipped: int = 0


class DeltaAnalysisResult(BaseModel):
    """Container for all analysis views."""

    summary: DeltaAnalysisSummary
    files: list[ParquetFileInfo]
    row_groups: list[RowGroupInfo]
    column_chunks: list[ColumnChunkInfo]
    columns: list[ColumnInfo]
