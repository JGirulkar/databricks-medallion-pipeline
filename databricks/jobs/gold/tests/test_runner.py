"""Spark tier: runner mechanics against tiny hand-built silver tables.

Number-correctness lives in test_gold_contract.py; this file proves the
machinery — view creation, execution order, manifest row, idempotent rerun.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest
from gold.config import GOLD_TABLES
from gold.manifest import PIPELINE_MANIFEST_SCHEMA
from gold.runner import run_gold
from pyspark.sql import SparkSession

pytestmark = pytest.mark.spark

SILVER = "rt_silver"
GOLD = "rt_gold"
MANIFEST = "rt_manifest"


@pytest.fixture(scope="function")
def tiny_silver(spark: SparkSession):
    spark.sql(f"DROP DATABASE IF EXISTS {SILVER} CASCADE")
    spark.sql(f"DROP DATABASE IF EXISTS {GOLD} CASCADE")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER}")
    spark.createDataFrame(
        [
            # customer_id, name, segment, deleted
            (1, "Ada", "Premium", False),
            (2, "Ben", "Basic", False),
            (3, "Cyd", "Basic", True),  # deleted dim row
        ],
        "customer_id INT, customer_name STRING, customer_segment STRING, _is_deleted BOOLEAN",
    ).write.mode("overwrite").saveAsTable(f"{SILVER}.customers")
    spark.createDataFrame(
        [(10, "Lamp", "Home", False), (11, "Mug", "Kitchen", False)],
        "product_id INT, product_name STRING, category STRING, _is_deleted BOOLEAN",
    ).write.mode("overwrite").saveAsTable(f"{SILVER}.products")
    spark.createDataFrame(
        [
            # order, cust, prod, date, amount, status, orphan, deleted
            (100, 1, 10, dt.date(2025, 6, 1), Decimal("50.00"), "Completed", False, False),
            (101, 1, 11, dt.date(2025, 6, 2), Decimal("30.00"), "Completed", False, False),
            (102, 2, 10, dt.date(2025, 6, 3), Decimal("20.00"), "Pending", False, False),   # status-excluded
            (103, 2, 11, dt.date(2025, 6, 4), Decimal("40.00"), "Completed", True, False),  # orphan-excluded
            (104, 2, 10, dt.date(2025, 6, 5), Decimal("60.00"), "Completed", False, True),  # deleted-excluded
        ],
        "order_id INT, customer_id INT, product_id INT, order_date DATE, "
        "total_amount DECIMAL(18,2), order_status STRING, _is_orphan BOOLEAN, _is_deleted BOOLEAN",
    ).write.mode("overwrite").saveAsTable(f"{SILVER}.orders")
    spark.createDataFrame([], PIPELINE_MANIFEST_SCHEMA).write.format("delta").mode(
        "overwrite"
    ).saveAsTable(MANIFEST)
    return spark


def test_run_gold_builds_all_four_tables(tiny_silver: SparkSession) -> None:
    spark = tiny_silver
    run_id = run_gold(spark, silver_schema=SILVER, gold_schema=GOLD, manifest_table=MANIFEST)
    assert run_id
    for table in GOLD_TABLES:
        assert spark.table(f"{GOLD}.{table}").count() > 0, table


def test_only_qualifying_orders_count(tiny_silver: SparkSession) -> None:
    spark = tiny_silver
    run_gold(spark, silver_schema=SILVER, gold_schema=GOLD, manifest_table=MANIFEST)
    # Orders 102 (Pending), 103 (orphan), 104 (deleted) are excluded:
    # qualifying revenue is exactly 50 + 30, both from customer 1.
    by_customer = {
        r["customer_id"]: r
        for r in spark.table(f"{GOLD}.revenue_by_customer").collect()
    }
    assert float(by_customer[1]["total_revenue"]) == 80.0
    assert by_customer[1]["total_orders"] == 2
    assert float(by_customer[2]["total_revenue"]) == 0.0
    assert by_customer[2]["total_orders"] == 0
    assert by_customer[2]["avg_order_value"] is None
    assert 3 not in by_customer  # deleted dim row excluded


def test_manifest_row_written_with_gold_layer(tiny_silver: SparkSession) -> None:
    spark = tiny_silver
    before = spark.table(MANIFEST).count()

    # First run: verify manifest grows by exactly 1
    run_id_1 = run_gold(spark, silver_schema=SILVER, gold_schema=GOLD, manifest_table=MANIFEST)
    assert spark.table(MANIFEST).count() == before + 1

    # Verify first run's row
    rows_1 = spark.table(MANIFEST).filter(f"run_id = '{run_id_1}'").collect()
    assert len(rows_1) == 1
    row_1 = rows_1[0]
    assert row_1["layer"] == "gold"
    assert row_1["status"] == "success"
    assert row_1["files_processed"] == 4
    assert row_1["rows_read"] == 5           # all silver order rows scanned
    assert row_1["rows_written"] > 0

    # Second run: verify manifest grows by 1 again, run_ids are distinct
    run_id_2 = run_gold(spark, silver_schema=SILVER, gold_schema=GOLD, manifest_table=MANIFEST)
    assert run_id_2 != run_id_1  # distinct run_ids
    assert spark.table(MANIFEST).count() == before + 2

    # Verify second run's row
    rows_2 = spark.table(MANIFEST).filter(f"run_id = '{run_id_2}'").collect()
    assert len(rows_2) == 1
    row_2 = rows_2[0]
    assert row_2["layer"] == "gold"
    assert row_2["status"] == "success"

    # Verify both rows remain in manifest
    all_rows = spark.table(MANIFEST).collect()
    assert len(all_rows) == before + 2


def test_rerun_is_idempotent(tiny_silver: SparkSession) -> None:
    spark = tiny_silver
    run_gold(spark, silver_schema=SILVER, gold_schema=GOLD, manifest_table=MANIFEST)
    first = {
        t: sorted(map(str, spark.table(f"{GOLD}.{t}").collect())) for t in GOLD_TABLES
    }
    run_gold(spark, silver_schema=SILVER, gold_schema=GOLD, manifest_table=MANIFEST)
    second = {
        t: sorted(map(str, spark.table(f"{GOLD}.{t}").collect())) for t in GOLD_TABLES
    }
    assert first == second
