"""Referential failures are temporal, so they are flagged in silver, not rejected.

An order whose customer has not arrived yet is not bad data — it is early. The
previous design quarantined it, and quarantine is a dead end: once the customer
landed, nothing revisited the order and it stayed rejected forever.

Orders now land in silver carrying `_is_orphan = true`, and a healing pass
clears the flag for keys whose parent has since appeared. Permanent defects —
missing required values, bad formats, duplicates within a delivery — still go
to quarantine, because no later arrival can fix them.
"""

from __future__ import annotations

import pytest
from conftest import create_delta_table
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from silver.config import DqSchema
from silver.schemas import silver_entity_schema

CUSTOMERS = "test_orph_silver_customers"
PRODUCTS = "test_orph_silver_products"
ORDERS = "test_orph_silver_orders"


def _orders_dq_schema(catalog: str = "de_assessment") -> DqSchema:
    return DqSchema.from_dict(
        {
            "$schemaVersion": "1.0",
            "validationMode": "enforce",
            "columns": [{"name": "order_id", "type": "integer", "nullable": False}],
            "checks": [
                {"kind": "not_null", "column": "customer_id", "category": "completeness"},
                {
                    "kind": "fk_exists",
                    "column": "customer_id",
                    "category": "referential",
                    "ref_table": CUSTOMERS,
                    "ref_column": "customer_id",
                },
            ],
        }
    )


def _seed_parent(spark: SparkSession, customer_ids: list[int]) -> None:
    create_delta_table(spark, CUSTOMERS, silver_entity_schema("customers"))
    if not customer_ids:
        return
    import datetime as dt

    rows = [
        (cid, f"C{cid}", f"c{cid}@x.com", "US", None, "Standard", None,
         "PASS", None, False, False, dt.datetime(2026, 1, 1, tzinfo=dt.UTC), "b1")
        for cid in customer_ids
    ]
    spark.createDataFrame(rows, schema=silver_entity_schema("customers")) \
        .write.format("delta").mode("append").saveAsTable(CUSTOMERS)


@pytest.mark.spark
def test_order_with_missing_parent_lands_in_silver_flagged(
    spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    from silver.checks import apply_entity_checks
    from silver.conform import split_validated_batch
    from silver.validators import annotate_violations

    _seed_parent(spark, [10])  # customer 20 has NOT arrived

    batch = spark.createDataFrame(
        [(1, 10, "b1"), (2, 20, "b1")],
        schema="order_id INT, customer_id INT, _batch_id STRING",
    ).withColumn("_ingest_timestamp", F.current_timestamp())

    schema = _orders_dq_schema()
    tagged = apply_entity_checks(annotate_violations(batch, schema), schema, spark)
    _survivors, passed, failed = split_validated_batch(tagged, "orders")

    # Both rows reach silver; the orphan is flagged rather than rejected.
    assert passed.count() == 2
    assert failed.count() == 0, "a referential failure is not a permanent defect"
    flags = {r["order_id"]: r["_is_orphan"] for r in passed.collect()}
    assert flags == {1: False, 2: True}


@pytest.mark.spark
def test_permanent_defect_still_goes_to_quarantine(
    spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A missing required value cannot be fixed by a later arrival."""
    from silver.checks import apply_entity_checks
    from silver.conform import split_validated_batch
    from silver.validators import annotate_violations

    _seed_parent(spark, [10])

    batch = spark.createDataFrame(
        [(1, 10, "b1"), (3, None, "b1")],
        schema="order_id INT, customer_id INT, _batch_id STRING",
    ).withColumn("_ingest_timestamp", F.current_timestamp())

    schema = _orders_dq_schema()
    tagged = apply_entity_checks(annotate_violations(batch, schema), schema, spark)
    _survivors, passed, failed = split_validated_batch(tagged, "orders")

    assert passed.count() == 1
    assert failed.count() == 1
    assert failed.collect()[0]["order_id"] == 3


@pytest.mark.spark
def test_healing_clears_the_flag_once_the_parent_arrives(
    spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    import datetime as dt

    from silver.conform import heal_orphans

    _seed_parent(spark, [10, 20])  # customer 20 has NOW arrived
    create_delta_table(spark, ORDERS, silver_entity_schema("orders"))
    rows = [
        (1, 10, None, None, None, None, None, "Completed", None,
         "PASS", None, False, False, dt.datetime(2026, 1, 1, tzinfo=dt.UTC), "b1"),
        (2, 20, None, None, None, None, None, "Completed", None,
         "PASS", None, False, True, dt.datetime(2026, 1, 1, tzinfo=dt.UTC), "b1"),
    ]
    spark.createDataFrame(rows, schema=silver_entity_schema("orders")) \
        .write.format("delta").mode("append").saveAsTable(ORDERS)

    monkeypatch.setattr(
        "silver.conform.silver_table",
        lambda entity, _catalog="de_assessment": {
            "orders": ORDERS, "customers": CUSTOMERS, "products": PRODUCTS
        }[entity],
    )

    healed = heal_orphans(spark, "customers", [20])

    assert healed == 1
    flags = {r["order_id"]: r["_is_orphan"] for r in spark.table(ORDERS).collect()}
    assert flags == {1: False, 2: False}, "order 2 is no longer an orphan"


@pytest.mark.spark
def test_healing_leaves_still_missing_parents_flagged(
    spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    import datetime as dt

    from silver.conform import heal_orphans

    _seed_parent(spark, [10])
    create_delta_table(spark, ORDERS, silver_entity_schema("orders"))
    rows = [
        (2, 20, None, None, None, None, None, "Completed", None,
         "PASS", None, False, True, dt.datetime(2026, 1, 1, tzinfo=dt.UTC), "b1"),
    ]
    spark.createDataFrame(rows, schema=silver_entity_schema("orders")) \
        .write.format("delta").mode("append").saveAsTable(ORDERS)

    monkeypatch.setattr(
        "silver.conform.silver_table",
        lambda entity, _catalog="de_assessment": {
            "orders": ORDERS, "customers": CUSTOMERS, "products": PRODUCTS
        }[entity],
    )

    healed = heal_orphans(spark, "customers", [99])  # unrelated key arrived

    assert healed == 0
    assert spark.table(ORDERS).collect()[0]["_is_orphan"] is True
