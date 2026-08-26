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
from conftest import create_delta_table
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
                },
                {
                    "kind": "uniqueness",
                    "column": "order_id",
                    "category": "uniqueness",
                },
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


@pytest.mark.spark
def test_duplicate_within_one_delivery_is_flagged_and_quarantined(
    spark: SparkSession, wired: None
) -> None:
    """A repeated key inside ONE delivery is a defect and must be flagged.

    Tagging must happen before survivorship. If conform dedupes first, the
    uniqueness check sees one row per key, so
    `count(*) over (partition by pk, _batch_id) > 1` is never true and the
    duplicate disappears with no audit trail.
    """
    from silver.main import process_conform_batch

    batch = spark.createDataFrame(
        [
            _row(7, 70, 2, "b1"),   # same order_id twice in the SAME delivery
            _row(7, 71, 5, "b1"),
        ],
        schema=_bronze_orders_cdf_schema(),
    )

    rows_read, rows_written, rows_quarantined = process_conform_batch(
        spark, "orders", batch, "run-dup", "de_assessment", None
    )

    # Both bronze rows are accounted for: one admitted, one quarantined.
    assert rows_read == 2
    assert rows_written == 1
    assert rows_quarantined == 1

    assert spark.table(SILVER_TBL).count() == 1

    q = spark.table(QUARANTINE_TBL).collect()
    assert len(q) == 1
    rules = {v["rule"] for v in q[0]["violations"]}
    assert "uniqueness" in rules, f"expected a uniqueness violation, got {rules}"


@pytest.mark.spark
def test_same_key_across_deliveries_is_supersession_not_duplication(
    spark: SparkSession, wired: None
) -> None:
    """A key reappearing in a LATER delivery is normal CDC, not a defect.

    Bronze is append-only and each ingest re-delivers the same key space, so a
    CDF window that spans several deliveries legitimately contains the same key
    many times. Scoping uniqueness to the whole window instead of one delivery
    marked every row a duplicate: measured on CE, 102,613 quarantined rows
    holding only 99,996 distinct keys, with zero rows reaching silver.

    The later delivery wins; the earlier one is superseded, and superseded is
    not the same as rejected — it must NOT land in quarantine.
    """
    from silver.main import process_conform_batch

    batch = spark.createDataFrame(
        [
            _row(8, 80, 2, "b1"),   # first delivery
            _row(8, 81, 5, "b2"),   # re-delivered later, same key
        ],
        schema=_bronze_orders_cdf_schema(),
    )

    rows_read, rows_written, rows_quarantined = process_conform_batch(
        spark, "orders", batch, "run-supersede", "de_assessment", None
    )

    assert rows_read == 2
    assert rows_written == 1
    assert rows_quarantined == 0, "superseded rows are not quality failures"

    silver_rows = spark.table(SILVER_TBL).collect()
    assert len(silver_rows) == 1
    assert silver_rows[0]["customer_id"] == 81, "latest delivery wins"

    assert spark.table(QUARANTINE_TBL).count() == 0


@pytest.mark.spark
def test_dq_metrics_report_differs_per_check_category(
    spark: SparkSession, wired: None
) -> None:
    """"% passed for each check" must be per-category, not one number reused.

    One row seeded per failure kind, so no two categories can legitimately
    share a count.
    """
    from silver.main import process_conform_batch

    batch = spark.createDataFrame(
        [
            _row(10, 100, 2, "b1"),    # clean
            _row(11, None, 2, "b1"),   # completeness: customer_id NULL
            _row(12, 120, 0, "b1"),    # type_logic: quantity < minimum
            # A genuine duplicate: same key twice in the SAME delivery.
            _row(13, 130, 2, "b1"),
            _row(13, 131, 2, "b1"),
        ],
        schema=_bronze_orders_cdf_schema(),
    )

    process_conform_batch(
        spark, "orders", batch, "run-metrics", "de_assessment", None
    )

    rows = {
        r["check_category"]: r
        for r in spark.table(METRICS_TBL)
        .filter("silver_run_id = 'run-metrics'")
        .collect()
    }
    assert {"completeness", "uniqueness", "type_logic"} <= set(rows)

    quarantined = {c: rows[c]["rows_quarantined"] for c in rows}
    assert quarantined["completeness"] == 1
    assert quarantined["type_logic"] == 1
    assert quarantined["uniqueness"] == 2, "both rows of a duplicate pair are flagged"

    pass_pcts = {rows[c]["pass_pct"] for c in ("completeness", "uniqueness", "type_logic")}
    assert len(pass_pcts) > 1, f"all categories reported the same pass_pct: {pass_pcts}"
