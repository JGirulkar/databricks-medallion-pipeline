from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from bronze.config import DEFAULT_CATALOG, manifest_table

PIPELINE_MANIFEST_COLUMN_NAMES: tuple[str, ...] = (
    "run_id",
    "layer",
    "entity_name",
    "parent_run_id",
    "delivery_pattern",
    "source_path",
    "files_processed",
    "rows_read",
    "rows_written",
    "rows_quarantined",
    "rows_rescued",
    "delta_version_before",
    "delta_version_after",
    "started_at",
    "completed_at",
    "status",
    "error_message",
)

PIPELINE_MANIFEST_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("entity_name", StringType(), False),
        StructField("parent_run_id", StringType(), True),
        StructField("delivery_pattern", StringType(), True),
        StructField("source_path", StringType(), True),
        StructField("files_processed", IntegerType(), False),
        StructField("rows_read", LongType(), False),
        StructField("rows_written", LongType(), False),
        StructField("rows_quarantined", LongType(), False),
        StructField("rows_rescued", LongType(), False),
        StructField("delta_version_before", LongType(), True),
        StructField("delta_version_after", LongType(), True),
        StructField("started_at", TimestampType(), False),
        StructField("completed_at", TimestampType(), True),
        StructField("status", StringType(), False),
        StructField("error_message", StringType(), True),
    ]
)


@dataclass(frozen=True)
class ManifestRecord:
    batch_id: str
    source_name: str
    delivery_pattern: str
    source_path: str
    files_processed: int
    rows_read: int
    rows_written: int
    rows_rescued: int
    delta_version_before: int | None
    delta_version_after: int | None
    started_at: datetime
    completed_at: datetime | None
    status: Literal["success", "failed"]
    error_message: str | None

    def __post_init__(self) -> None:
        if not self.batch_id:
            raise ValueError("batch_id must not be empty")
        if self.status not in {"success", "failed"}:
            raise ValueError(f"Invalid status: {self.status}")
        if self.status == "success" and self.completed_at is None:
            raise ValueError("completed_at is required for successful runs")

    def as_row(self) -> dict[str, object]:
        return {
            "run_id": self.batch_id,
            "layer": "bronze",
            "entity_name": self.source_name,
            "parent_run_id": None,
            "delivery_pattern": self.delivery_pattern,
            "source_path": self.source_path,
            "files_processed": self.files_processed,
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "rows_quarantined": 0,
            "rows_rescued": self.rows_rescued,
            "delta_version_before": self.delta_version_before,
            "delta_version_after": self.delta_version_after,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "status": self.status,
            "error_message": self.error_message,
        }


def current_delta_version(spark: SparkSession, table_name: str) -> int | None:
    rows = (
        spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1")
        .select("version")
        .collect()
    )
    return int(rows[0].version) if rows else None


def append_manifest(
    spark: SparkSession,
    record: ManifestRecord,
    catalog: str = DEFAULT_CATALOG,
) -> None:
    row_df = spark.createDataFrame([record.as_row()], schema=PIPELINE_MANIFEST_SCHEMA)
    row_df.write.format("delta").mode("append").saveAsTable(manifest_table(catalog))
