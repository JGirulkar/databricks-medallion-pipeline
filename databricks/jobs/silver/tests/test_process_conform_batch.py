"""Integration test for the silver batch wiring.

`process_conform_batch` is the function the CDF stream actually calls. Every
collaborator around it was unit-tested in isolation, but the wiring itself was
not — which is how two missing imports (`annotate_violations`, `write_quarantine`)
reached the cluster and failed every silver run with a NameError inside
foreachBatch. These tests exercise the real chain: conform -> column rules ->
entity checks -> merge -> quarantine -> metrics. No silver module is mocked;
only the config-table reads and table-name resolvers are redirected to local
Delta tables.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

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
from silver.config import DqSchema
from silver.schemas import DQ_METRICS_SCHEMA, QUARANTINE_SCHEMA, silver_entity_schema
from conftest import create_delta_table

SILVER_TBL = "test_pcb_silver_orders"
QUARANTINE_TBL = "test_pcb_quarantine"
METRICS_TBL = "test_pcb_dq_metrics"


def _bronze_orders_cdf_schema() -> StructType:
    return StructType(
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
            StructField("_change_type", StringType(), True),
        ]
    )


def _orders_dq_schema() -> DqSchema:
    """Mirrors the shape seeded into config.source_config for orders."""
    return DqSchema.from_dict(
        {
            "$schemaVersion": "1.0",
            "validationMode": "enforce",
            "columns": [
                {"name": "order_id", "type": "integer", "nullable": False},
                {"name": "customer_id", "type": "integer", "nullable": True},
                {
                    "name": "quantity",
                    "type": "integer",
                    "nullable": True,
                    "validation": {"kind": "numeric", "minimum": 1},
                },
            ],
            "checks": [
                {
                    "kind": "not_null",
                    "column": "customer_id",
                    "category": "completeness",
                }
            ],
        }
    )


def _row(order_id: int, customer_id: int | None, quantity: int, batch: str) -> tuple:
    return (
        order_id,
        customer_id,
        None,
        500,
        quantity,
        Decimal("10.00"),
        Decimal("20.00"),
        "Completed",
        None,
        datetime(2026, 1, 1, tzinfo=UTC),
        "orders_1.csv",
        batch,
        "incremental",
        "insert",
    )


@pytest.fixture
def wired(spark: SparkSession, monkeypatch: pytest.MonkeyPatch) -> None:
    create_delta_table(spark, SILVER_TBL, silver_entity_schema("orders"))
    create_delta_table(spark, QUARANTINE_TBL, QUARANTINE_SCHEMA)
    create_delta_table(spark, METRICS_TBL, DQ_METRICS_SCHEMA)
    monkeypatch.setattr(
        "silver.main.get_delivery_pattern",
        lambda _spark, _entity, _catalog="de_assessment": "incremental",
    )
    monkeypatch.setattr(
        "silver.main.load_dq_schema",
        lambda _spark, _entity, _catalog="de_assessment": _orders_dq_schema(),
    )
    monkeypatch.setattr(
        "silver.conform.silver_table",
        lambda _entity, _catalog="de_assessment": SILVER_TBL,
    )
    monkeypatch.setattr(
        "silver.quarantine.quarantine_table",
        lambda catalog="de_assessment": QUARANTINE_TBL,
    )
    monkeypatch.setattr(
        "silver.metrics.dq_metrics_table",
        lambda catalog="de_assessment": METRICS_TBL,
    )


@pytest.mark.spark
def test_clean_row_reaches_silver_and_bad_row_is_quarantined(
    spark: SparkSession, wired: None
) -> None:
    """The wiring must run end to end — this is what the NameError broke."""
    from silver.main import process_conform_batch

    batch = spark.createDataFrame(
        [
            _row(1, 10, 2, "b1"),          # clean
            _row(2, None, 2, "b1"),        # completeness: customer_id NULL
        ],
        schema=_bronze_orders_cdf_schema(),
    )

    rows_read, rows_written, rows_quarantined = process_conform_batch(
        spark, "orders", batch, "run-pcb-1", "de_assessment", None
    )

    assert rows_read == 2
    assert rows_written == 1
    assert rows_quarantined == 1

    silver_ids = [r["order_id"] for r in spark.table(SILVER_TBL).collect()]
    assert silver_ids == [1]

    quarantined = spark.table(QUARANTINE_TBL).collect()
    assert len(quarantined) == 1
    assert quarantined[0]["primary_key"] == "2"
    categories = {v["category"] for v in quarantined[0]["violations"]}
    assert "completeness" in categories


@pytest.mark.spark
def test_column_rule_violation_is_quarantined_not_merged(
    spark: SparkSession, wired: None
) -> None:
    """Exercises annotate_violations specifically (quantity minimum=1)."""
    from silver.main import process_conform_batch

    batch = spark.createDataFrame(
        [_row(3, 30, 0, "b2")],  # quantity 0 violates minimum=1
        schema=_bronze_orders_cdf_schema(),
    )

    rows_read, rows_written, rows_quarantined = process_conform_batch(
        spark, "orders", batch, "run-pcb-2", "de_assessment", None
    )

    assert (rows_read, rows_written, rows_quarantined) == (1, 0, 1)
    assert spark.table(SILVER_TBL).count() == 0
    row = spark.table(QUARANTINE_TBL).collect()[0]
    rules = {v["rule"] for v in row["violations"]}
    assert "minimum" in rules


@pytest.mark.spark
def test_empty_batch_is_a_noop(spark: SparkSession, wired: None) -> None:
    from silver.main import process_conform_batch

    empty = spark.createDataFrame([], schema=_bronze_orders_cdf_schema())
    assert process_conform_batch(
        spark, "orders", empty, "run-pcb-3", "de_assessment", None
    ) == (0, 0, 0)
