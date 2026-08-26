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
                {
                    "kind": "fk_exists",
                    "column": "product_id",
                    "category": "referential",
                    "ref_table": PRODUCTS,
                    "ref_column": "product_id",
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
    _seed_products(spark, [5])  # the product exists for both rows

    batch = spark.createDataFrame(
        [(1, 10, 5, "b1"), (2, 20, 5, "b1")],
        schema="order_id INT, customer_id INT, product_id INT, _batch_id STRING",
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
    _seed_products(spark, [5])

    batch = spark.createDataFrame(
        [(1, 10, 5, "b1"), (3, None, 5, "b1")],
        schema="order_id INT, customer_id INT, product_id INT, _batch_id STRING",
    ).withColumn("_ingest_timestamp", F.current_timestamp())

    schema = _orders_dq_schema()
    tagged = apply_entity_checks(annotate_violations(batch, schema), schema, spark)
    _survivors, passed, failed = split_validated_batch(tagged, "orders")

    assert passed.count() == 1
    assert failed.count() == 1
    assert failed.collect()[0]["order_id"] == 3


def _seed_products(spark: SparkSession, product_ids: list[int]) -> None:
    create_delta_table(spark, PRODUCTS, silver_entity_schema("products"))
    if not product_ids:
        return
    import datetime as dt

    rows = [
        (pid, f"P{pid}", "Cat", None, None, 10, 5,
         "PASS", None, False, False, dt.datetime(2026, 1, 1, tzinfo=dt.UTC), "b1")
        for pid in product_ids
    ]
    spark.createDataFrame(rows, schema=silver_entity_schema("products")) \
        .write.format("delta").mode("append").saveAsTable(PRODUCTS)


def _seed_orders(spark: SparkSession, rows: list[tuple[int, int, int, bool]]) -> None:
    """rows = (order_id, customer_id, product_id, is_orphan)"""
    import datetime as dt

    create_delta_table(spark, ORDERS, silver_entity_schema("orders"))
    built = [
        (oid, cid, None, pid, 1, None, None, "Completed", None,
         "PASS", None, False, orphan, dt.datetime(2026, 1, 1, tzinfo=dt.UTC), "b1")
        for oid, cid, pid, orphan in rows
    ]
    spark.createDataFrame(built, schema=silver_entity_schema("orders")) \
        .write.format("delta").mode("append").saveAsTable(ORDERS)


@pytest.fixture
def wired_tables(spark: SparkSession, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "silver.conform.silver_table",
        lambda entity, _catalog="de_assessment": {
            "orders": ORDERS, "customers": CUSTOMERS, "products": PRODUCTS
        }[entity],
    )
    monkeypatch.setattr(
        "silver.conform.load_dq_schema",
        lambda _spark, _entity, _catalog="de_assessment": _orders_dq_schema(),
    )


@pytest.mark.spark
def test_healing_clears_the_flag_once_every_parent_exists(
    spark: SparkSession, wired_tables: None
) -> None:
    from silver.conform import refresh_orphan_flags

    _seed_parent(spark, [10, 20])
    _seed_products(spark, [5])
    _seed_orders(spark, [(1, 10, 5, False), (2, 20, 5, True)])

    healed = refresh_orphan_flags(spark, "de_assessment")

    assert healed == 1
    flags = {r["order_id"]: r["_is_orphan"] for r in spark.table(ORDERS).collect()}
    assert flags == {1: False, 2: False}


@pytest.mark.spark
def test_healing_leaves_a_row_flagged_while_any_parent_is_missing(
    spark: SparkSession, wired_tables: None
) -> None:
    """One parent arriving must not clear a flag the other parent still earns.

    `_is_orphan` is a single boolean covering every foreign key, so healing has
    to re-evaluate the row rather than react to one parent's arrival. Getting
    this wrong cleared the flag on 38 orders whose customer was still missing,
    simply because their product existed.
    """
    from silver.conform import refresh_orphan_flags

    _seed_parent(spark, [10])          # customer 20 is still absent
    _seed_products(spark, [5])         # the product HAS arrived
    _seed_orders(spark, [(2, 20, 5, True)])

    healed = refresh_orphan_flags(spark, "de_assessment")

    assert healed == 0, "the customer is still missing, so the row is still an orphan"
    assert spark.table(ORDERS).collect()[0]["_is_orphan"] is True


@pytest.mark.spark
def test_healing_is_idempotent(spark: SparkSession, wired_tables: None) -> None:
    from silver.conform import refresh_orphan_flags

    _seed_parent(spark, [10, 20])
    _seed_products(spark, [5])
    _seed_orders(spark, [(2, 20, 5, True)])

    assert refresh_orphan_flags(spark, "de_assessment") == 1
    assert refresh_orphan_flags(spark, "de_assessment") == 0, "nothing left to heal"


@pytest.mark.spark
def test_flag_is_set_when_a_parent_is_soft_deleted(
    spark: SparkSession, wired_tables: None
) -> None:
    """A parent disappearing must orphan its children.

    The flag was only ever computed while conforming a batch, and the repair
    pass only ever CLEARED it. So a row already in silver whose parent was later
    soft-deleted stayed marked valid: soft-deleting 3 products left 624 orders
    pointing at nothing while flagged `_is_orphan = false`.

    The pass has to be symmetric — it decides the flag from the data, in both
    directions.
    """
    from silver.conform import refresh_orphan_flags

    _seed_parent(spark, [10])
    _seed_products(spark, [5, 6])
    _seed_orders(spark, [(1, 10, 5, False), (2, 10, 6, False)])

    # Product 6 is withdrawn from the source, so order 2 now points at nothing.
    spark.sql(f"UPDATE {PRODUCTS} SET _is_deleted = true WHERE product_id = 6")

    changed = refresh_orphan_flags(spark, "de_assessment")

    assert changed == 1
    flags = {r["order_id"]: r["_is_orphan"] for r in spark.table(ORDERS).collect()}
    assert flags == {1: False, 2: True}


@pytest.mark.spark
def test_refresh_is_a_no_op_when_flags_already_agree(
    spark: SparkSession, wired_tables: None
) -> None:
    """Nothing is rewritten when the flags already match the data."""
    from silver.conform import refresh_orphan_flags

    _seed_parent(spark, [10])
    _seed_products(spark, [5])
    _seed_orders(spark, [(1, 10, 5, False)])

    assert refresh_orphan_flags(spark, "de_assessment") == 0
