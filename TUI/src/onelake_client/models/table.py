from pydantic import BaseModel


class Column(BaseModel):
    """A table column."""

    name: str
    type: str
    nullable: bool = True
    metadata: dict[str, str] | None = None
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


class DeltaFileStats(BaseModel):
    """Per-file statistics for a Delta table derived from add actions."""

    num_files: int = 0
    total_bytes: int = 0
    min_file_bytes: int = 0
    max_file_bytes: int = 0
    avg_file_bytes: float = 0.0
    partition_counts: dict[str, int] = {}


class ParquetFileInfo(BaseModel):
    """Row-level metadata for a single parquet file in a Delta table."""

    parquet_file: str
    row_count: int
    row_groups: int
    created_by: str = ""
    total_table_rows: int = 0
    total_table_row_groups: int = 0


class RowGroupInfo(BaseModel):
    """Metadata for a single row group within a parquet file."""

    parquet_file: str
    row_group_id: int
    row_count: int
    compressed_size: int
    uncompressed_size: int
    compression_ratio: float = 0.0
    total_table_rows: int = 0
    ratio_of_total_rows: float = 0.0
    total_table_row_groups: int = 0


class ColumnChunkInfo(BaseModel):
    """Metadata for a single column chunk within a row group."""

    parquet_file: str
    row_group_id: int
    column_id: int
    column_name: str
    column_type: str
    compressed_size: int
    uncompressed_size: int
    has_dict: bool = False
    value_count: int = 0
    encodings: str = ""


class ColumnInfo(BaseModel):
    """Aggregated column-level metadata across all parquet files."""

    column_name: str
    column_type: str
    compressed_size: int
    uncompressed_size: int
    total_table_rows: int = 0
    size_percent_of_table: float = 0.0


class DeltaAnalysisSummary(BaseModel):
    """Summary statistics for a Delta table analysis."""

    row_count: int = 0
    parquet_files: int = 0
    row_groups: int = 0
    max_rows_per_row_group: int = 0
    min_rows_per_row_group: int = 0
    avg_rows_per_row_group: float = 0.0
    total_compressed_size: int = 0
    files_skipped: int = 0
    files_skipped_reason: str = ""


class DeltaAnalysisResult(BaseModel):
    """Full result from delta_analyzer — mirrors the 5-dataframe structure."""

    summary: DeltaAnalysisSummary = DeltaAnalysisSummary()
    parquet_files: list[ParquetFileInfo] = []
    row_groups: list[RowGroupInfo] = []
    column_chunks: list[ColumnChunkInfo] = []
    columns: list[ColumnInfo] = []


class IcebergTableInfo(BaseModel):
    """Metadata for an Iceberg table."""

    name: str | None = None
    schema_: list[Column] = []
    current_snapshot_id: int | None = None
    format_version: int = 2
    location: str | None = None
    partition_spec: list[dict] = []
    properties: dict[str, str] = {}
