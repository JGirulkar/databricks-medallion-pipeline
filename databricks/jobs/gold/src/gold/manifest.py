"""Gold pipeline manifest rows in ops.pipeline_manifest.

The schema is a copy of the silver package's PIPELINE_MANIFEST_SCHEMA: the
two jobs are uploaded to separate workspace directories and cannot import
each other at runtime. database/schema.sql documents the shared table; the
two copies are kept aligned by hand — no automated guard covers
ops.pipeline_manifest today.
"""

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
class GoldManifestRecord:
    run_id: str
    files_processed: int
    rows_read: int
    rows_written: int
    started_at: datetime
    completed_at: datetime | None
    status: Literal["success", "failed"]
    error_message: str | None = None

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
            "layer": "gold",
            "entity_name": "gold_aggregations",
            "parent_run_id": None,
            "delivery_pattern": None,
            "source_path": None,
            "files_processed": self.files_processed,
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "rows_quarantined": 0,
            "rows_rescued": 0,
            "delta_version_before": None,
            "delta_version_after": None,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "status": self.status,
            "error_message": self.error_message,
        }


def append_gold_manifest(
    spark: SparkSession, record: GoldManifestRecord, manifest_table: str
) -> None:
    row_df = spark.createDataFrame([record.as_row()], schema=PIPELINE_MANIFEST_SCHEMA)
    row_df.write.format("delta").mode("append").saveAsTable(manifest_table)
