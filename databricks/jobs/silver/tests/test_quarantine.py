from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from silver.quarantine import write_quarantine
from silver.schemas import QUARANTINE_SCHEMA
from conftest import create_delta_table


@pytest.mark.spark
def test_write_quarantine_preserves_violations(
    spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    table_name = "test_silver_quarantine"
    monkeypatch.setattr(
        "silver.quarantine.quarantine_table",
        lambda catalog="de_assessment": table_name,
    )
    create_delta_table(spark, table_name, QUARANTINE_SCHEMA)
    df = spark.createDataFrame(
        [(1, "bad-email", "batch-1")],
        schema=StructType(
            [
                StructField("order_id", IntegerType(), True),
                StructField("email", StringType(), True),
                StructField("_batch_id", StringType(), True),
            ]
        ),
    )
    df = df.withColumn(
        "_violations",
        F.array(
            F.struct(
                F.lit("type_logic").alias("category"),
                F.lit("format_email").alias("rule"),
                F.lit("email").alias("column"),
                F.col("email").cast("string").alias("value"),
            )
        ),
    )
    count = write_quarantine(
        spark,
        df,
        "orders",
        "run-q1",
        datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert count == 1
    row = spark.table(table_name).collect()[0]
    assert row["violations"][0]["rule"] == "format_email"
