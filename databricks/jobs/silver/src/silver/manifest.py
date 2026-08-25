"""Silver pipeline manifest rows in ops.pipeline_manifest."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from pyspark.sql import SparkSession

from silver.config import DEFAULT_CATALOG, pipeline_manifest_table
from silver.schemas import PIPELINE_MANIFEST_SCHEMA


@dataclass(frozen=True)
class PipelineManifestRecord:
    run_id: str
    entity_name: str
    parent_run_id: str | None
    delivery_pattern: str | None
    source_path: str | None
    files_processed: int
    rows_read: int
    rows_written: int
    rows_quarantined: int
    rows_rescued: int
    delta_version_before: int | None
    delta_version_after: int | None
    started_at: datetime
    completed_at: datetime | None
    status: Literal["success", "failed"]
    error_message: str | None

    def __post_init__(self) -> None:
        if not self.run_id:
            raise ValueError("run_id must not be empty")
        if self.status not in {"success", "failed"}:
            raise ValueError(f"Invalid status: {self.status}")
        if self.status == "success" and self.completed_at is None:
            raise ValueError("completed_at is required for successful runs")

    def as_row(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "layer": "silver",
            "entity_name": self.entity_name,
            "parent_run_id": self.parent_run_id,
            "delivery_pattern": self.delivery_pattern,
            "source_path": self.source_path,
            "files_processed": self.files_processed,
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "rows_quarantined": self.rows_quarantined,
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


def append_silver_manifest(
    spark: SparkSession,
    record: PipelineManifestRecord,
    catalog: str = DEFAULT_CATALOG,
) -> None:
    row_df = spark.createDataFrame([record.as_row()], schema=PIPELINE_MANIFEST_SCHEMA)
    row_df.write.format("delta").mode("append").saveAsTable(
        pipeline_manifest_table(catalog)
    )
