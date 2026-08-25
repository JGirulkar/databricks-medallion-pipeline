from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from silver.conform import merge_to_silver, prepare_silver_rows
from silver.schemas import silver_entity_schema
from conftest import create_delta_table


def _bronze_orders_schema() -> StructType:
    return StructType(
        [
            StructField("order_id", IntegerType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("order_date", DateType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("_ingest_timestamp", TimestampType(), True),
            StructField("_source_file", StringType(), True),
            StructField("_batch_id", StringType(), True),
            StructField("_delivery_pattern", StringType(), True),
        ]
    )


@pytest.mark.spark
def test_conform_incremental_dedupes_order_id(spark: SparkSession) -> None:
    from silver.conform import conform_incremental_batch

    df = spark.createDataFrame(
        [
            (
                1,
                10,
                None,
                5,
                2,
                datetime(2026, 1, 1, tzinfo=UTC),
                "f.csv",
                "b1",
                "incremental",
            ),
            (
                1,
                20,
                None,
                5,
                3,
                datetime(2026, 1, 2, tzinfo=UTC),
                "f.csv",
                "b2",
                "incremental",
            ),
        ],
        schema=_bronze_orders_schema(),
    )
    result = conform_incremental_batch(df, "orders", spark)
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["customer_id"] == 20


@pytest.mark.spark
def test_prepare_silver_rows_sets_pass_and_strips_bronze_metadata(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [
            (
                1,
                10,
                None,
                5,
                2,
                datetime(2026, 1, 1, tzinfo=UTC),
                "f.csv",
                "b1",
                "incremental",
            )
        ],
        schema=_bronze_orders_schema(),
    )
    result = prepare_silver_rows(df, "orders", datetime.now(UTC))
    row = result.collect()[0]
    assert row["quality_check_result"] == "PASS"
    assert "_batch_id" not in result.columns
    assert row["_bronze_batch_id"] == "b1"


@pytest.mark.spark
def test_merge_to_silver_inserts_row(
    spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    table_name = "test_silver_orders_merge"
    schema = silver_entity_schema("orders")
    create_delta_table(spark, table_name, schema)

    monkeypatch.setattr(
        "silver.conform.silver_table",
        lambda name, catalog="de_assessment": table_name,
    )

    df = spark.createDataFrame(
        [
            (
                1,
                10,
                None,
                5,
                2,
                None,
                None,
                None,
                None,
                datetime(2026, 1, 1, tzinfo=UTC),
                "f.csv",
                "b1",
                "incremental",
            )
        ],
        schema=StructType(
            [
                StructField("order_id", IntegerType(), True),
                StructField("customer_id", IntegerType(), True),
                StructField("order_date", DateType(), True),
                StructField("product_id", IntegerType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("unit_price", DecimalType(18, 2), True),
                StructField("total_amount", DecimalType(18, 2), True),
                StructField("order_status", StringType(), True),
                StructField("payment_date", DateType(), True),
                StructField("_ingest_timestamp", TimestampType(), True),
                StructField("_source_file", StringType(), True),
                StructField("_batch_id", StringType(), True),
                StructField("_delivery_pattern", StringType(), True),
            ]
        ),
    )
    count = merge_to_silver(df, "orders", spark)
    assert count == 1
    assert spark.table(table_name).count() == 1


@pytest.mark.unit
def test_bootstrap_ddl_contains_quarantine_and_cdf() -> None:
    from silver.bootstrap import bootstrap_ddl

    ddl = "\n".join(bootstrap_ddl())
    assert "silver.quarantine" in ddl
    assert "delta.enableChangeDataFeed" in ddl
    assert "dq_schema" in ddl
