"""Tests for driver-side silver sink metrics."""

from __future__ import annotations

import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pytest
from conftest import create_delta_table
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from silver.schemas import QUARANTINE_SCHEMA
from silver.sink_metrics import quarantine_rows_for_run, resolve_silver_metrics
from silver.validators import VIOLATION_ARRAY_TYPE


@pytest.mark.spark
def test_quarantine_rows_for_run_counts_by_silver_run_id(
    spark: SparkSession, monkeypatch: pytest.MonkeyPatch,
) -> None:
    table_name = "test_quarantine_metrics"
    monkeypatch.setattr(
        "silver.sink_metrics.quarantine_table",
        lambda catalog="de_assessment": table_name,
    )
    create_delta_table(spark, table_name, QUARANTINE_SCHEMA)
    run_at = datetime(2026, 1, 1, tzinfo=UTC)
    (
        spark.createDataFrame(
            [
                ("orders", "1", "{}", "run-a", "batch-1"),
                ("orders", "2", "{}", "run-b", "batch-1"),
            ],
            schema=(
                "entity_name STRING, primary_key STRING, data STRING, "
                "silver_run_id STRING, bronze_batch_id STRING"
            ),
        )
        .withColumn("violations", F.array().cast(VIOLATION_ARRAY_TYPE))
        .withColumn("quarantined_at", F.lit(run_at))
        .write.format("delta")
        .mode("append")
        .saveAsTable(table_name)
    )

    assert quarantine_rows_for_run(spark, "run-a") == 1
    assert quarantine_rows_for_run(spark, "run-b") == 1


@pytest.mark.spark
def test_resolve_silver_metrics_empty_when_no_version_change(
    spark: SparkSession, monkeypatch: pytest.MonkeyPatch,
) -> None:
    table_name = "test_silver_products_metrics"
    monkeypatch.setattr(
        "silver.sink_metrics.silver_table",
        lambda name, catalog="de_assessment": table_name,
    )
    # resolve_silver_metrics also reads the quarantine sink; local spark_catalog
    # rejects the 3-part UC name, so redirect it too.
    monkeypatch.setattr(
        "silver.sink_metrics.quarantine_table",
        lambda catalog="de_assessment": "test_silver_products_quarantine",
    )
    create_delta_table(spark, "test_silver_products_quarantine", QUARANTINE_SCHEMA)
    # Register at an isolated location. saveAsTable writes into the shared
    # spark-warehouse dir, which survives DROP TABLE and poisons later runs
    # with DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION.
    uri = Path(tempfile.mkdtemp()).as_uri()
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.createDataFrame([(1, "n")], "id INT, name STRING").write.format(
        "delta"
    ).mode("overwrite").save(uri)
    spark.sql(f"CREATE TABLE {table_name} USING delta LOCATION '{uri}'")

    metrics = resolve_silver_metrics(
        spark, "products", "run-1", "de_assessment", version_before=0, version_after=0
    )
    assert metrics.rows_read == 0
    assert metrics.rows_written == 0
