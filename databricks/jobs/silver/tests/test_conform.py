from __future__ import annotations

from datetime import UTC, datetime

import pytest
from conftest import create_delta_table
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
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
from silver.validators import VIOLATION_ARRAY_TYPE


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


def _order_row(
    order_id: int, customer_id: int, quantity: int, batch: str, day: int
) -> tuple:
    return (
        order_id,
        customer_id,
        None,
        5,
        quantity,
        datetime(2026, 1, day, tzinfo=UTC),
        "f.csv",
        batch,
        "incremental",
    )


def _tagged(spark: SparkSession, rows: list[tuple]) -> DataFrame:
    """split_validated_batch consumes an already-validated batch."""
    return spark.createDataFrame(rows, schema=_bronze_orders_schema()).withColumn(
        "_violations", F.array().cast(VIOLATION_ARRAY_TYPE)
    )


@pytest.mark.spark
def test_split_quarantines_a_duplicate_inside_one_delivery(
    spark: SparkSession,
) -> None:
    """A key repeated in ONE delivery is a defect: latest wins, loser is kept.

    Replaces test_conform_incremental_dedupes_order_id, which only asserted
    that one row survived — never that the discarded row stayed recoverable.
    """
    from silver.conform import split_validated_batch

    tagged = _tagged(
        spark,
        [
            _order_row(1, 10, 2, "b1", 1),
            _order_row(1, 20, 3, "b1", 2),  # same key, SAME delivery
        ],
    )

    survivors, passed, failed = split_validated_batch(tagged, "orders")

    survivor_rows = survivors.collect()
    assert len(survivor_rows) == 1
    assert survivor_rows[0]["customer_id"] == 20, "latest row wins"

    assert passed.count() == 1
    assert failed.count() == 1, "the duplicate must remain recoverable"
    assert failed.collect()[0]["customer_id"] == 10


@pytest.mark.spark
def test_split_treats_a_later_delivery_as_supersession(spark: SparkSession) -> None:
    """A key reappearing in a LATER delivery is normal CDC, not a defect.

    Bronze is append-only and re-delivers the same key space each ingest, so a
    CDF window spanning several deliveries holds each key repeatedly. Treating
    that as duplication quarantined entire batches on CE.
    """
    from silver.conform import split_validated_batch

    tagged = _tagged(
        spark,
        [
            _order_row(1, 10, 2, "b1", 1),
            _order_row(1, 20, 3, "b2", 2),  # re-delivered later
        ],
    )

    survivors, passed, failed = split_validated_batch(tagged, "orders")

    assert survivors.count() == 1
    assert passed.count() == 1
    assert passed.collect()[0]["customer_id"] == 20, "latest delivery wins"
    assert failed.count() == 0, "superseded is not rejected"


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
    assert "silver.dq_metrics" in ddl
