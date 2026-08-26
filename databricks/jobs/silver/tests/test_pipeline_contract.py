"""End-to-end contract: generated CSV rows through to the silver tables.

Runs the real silver functions over the real generator output on local Spark,
and checks the outcome against expectations derived independently from the
input. It exists because every defect found on the cluster so far was
detectable here first, at a minute a run instead of twenty-five, and because a
cluster run that reports success is not evidence that the data is right.

The expectations are recomputed from the input rather than hardcoded, so the
test cannot quietly agree with a wrong implementation.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import math
import pathlib
import re

import pytest
from conftest import create_delta_table
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from silver.checks import apply_entity_checks
from silver.config import DqSchema
from silver.conform import (
    apply_snapshot_soft_deletes,
    merge_to_silver,
    split_validated_batch,
)
from silver.quarantine import write_quarantine
from silver.schemas import QUARANTINE_SCHEMA, silver_entity_schema
from silver.validators import annotate_violations

_GEN = (
    pathlib.Path(__file__).resolve().parents[2]
    / "data_generation" / "src" / "generate_sample_data.py"
)
_spec = importlib.util.spec_from_file_location("generate_sample_data", _GEN)
assert _spec and _spec.loader
gen = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gen)

TABLES = {
    "customers": "contract_silver_customers",
    "products": "contract_silver_products",
    "orders": "contract_silver_orders",
}
QUARANTINE = "contract_silver_quarantine"

# The max_date rule is "today", so the boundary moves with the clock.
TODAY = dt.datetime.now(dt.UTC).date().isoformat()
EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
COUNTRY = re.compile(r"^[A-Za-z .'-]{2,56}$")

BRONZE_META = "_batch_id STRING"


# --------------------------------------------------------------------------
# Expectations, derived from the input by hand — deliberately not sharing code
# with the implementation under test.
# --------------------------------------------------------------------------
def _missing(value: object) -> bool:
    """None, or a pandas NaN standing in for a null."""
    return value is None or (isinstance(value, float) and math.isnan(value))


def blocking_customer(row: object) -> list[str]:
    """Permanent defects for customers, mirroring the dq_schema seed."""
    out = []
    if _missing(row.customer_id):
        out.append("not_null:customer_id")
    if _missing(row.email):
        out.append("not_null:email")
    elif not EMAIL.match(str(row.email)):
        out.append("format_email")
    if row.customer_segment not in set(gen.CUSTOMER_SEGMENTS):
        out.append("enum:customer_segment")
    if not _missing(row.lifetime_value) and row.lifetime_value < 0:
        out.append("minimum:lifetime_value")
    name = str(row.customer_name)
    if len(name) < 2:
        out.append("min_length:customer_name")
    if len(name) > 100:
        out.append("max_length:customer_name")
    if not COUNTRY.match(str(row.country)):
        out.append("pattern:country")
    if str(row.signup_date) > TODAY:
        out.append("max_date:signup_date")
    return out


def blocking_product(row: object) -> list[str]:
    out = []
    if _missing(row.product_id):
        out.append("not_null:product_id")
    if not _missing(row.price) and row.price < 0:
        out.append("minimum:price")
    if not _missing(row.cost) and row.cost < 0:
        out.append("minimum:cost")
    if len(str(row.product_name)) > 200:
        out.append("max_length:product_name")
    if not _missing(row.stock_quantity):
        if row.stock_quantity < 0:
            out.append("minimum:stock_quantity")
        if row.stock_quantity > 100_000:
            out.append("maximum:stock_quantity")
    return out


def blocking_order(row: object) -> list[str]:
    out = []
    for column in ("order_id", "customer_id", "product_id"):
        if _missing(getattr(row, column)):
            out.append(f"not_null:{column}")
    if not _missing(row.quantity):
        if row.quantity < 1:
            out.append("minimum:quantity")
        if row.quantity > 1_000:
            out.append("maximum:quantity")
    if not _missing(row.unit_price) and row.unit_price <= 0:
        out.append("exclusive_minimum:unit_price")
    if not _missing(row.total_amount) and row.total_amount < 0:
        out.append("minimum:total_amount")
    if row.order_status not in set(gen.ORDER_STATUSES):
        out.append("enum:order_status")
    order_date = str(row.order_date)
    if order_date < "2020-01-01":
        out.append("min_date:order_date")
    if order_date > TODAY:
        out.append("max_date:order_date")
    return out


BLOCKING = {
    "customers": blocking_customer,
    "products": blocking_product,
    "orders": blocking_order,
}
PK = {"customers": "customer_id", "products": "product_id", "orders": "order_id"}


def expected_split(frame, entity: str) -> dict[str, set]:
    """Which keys should reach silver, and which rows should be quarantined.

    Applies the documented rules directly: a permanent defect quarantines the
    row; within one delivery the last occurrence of a key wins and the earlier
    ones are quarantined; a referential problem does neither.
    """
    rows = list(frame.itertuples(index=False))
    fn, pk = BLOCKING[entity], PK[entity]
    last_index: dict[object, int] = {}
    for i, row in enumerate(rows):
        key = getattr(row, pk)
        last_index[None if _missing(key) else int(key)] = i
    survivor_rows = set(last_index.values())

    silver_keys, quarantined_rows = set(), set()
    for i, row in enumerate(rows):
        key = getattr(row, pk)
        key = None if _missing(key) else int(key)
        if fn(row) or i not in survivor_rows:
            quarantined_rows.add(i)
        else:
            silver_keys.add(key)
    return {"silver_keys": silver_keys, "quarantined_rows": quarantined_rows}


# --------------------------------------------------------------------------
# Wiring
# --------------------------------------------------------------------------
def _dq_schema(entity: str) -> DqSchema:
    """The seeded schema, with ref_table pointed at the local test tables."""
    from silver.bootstrap import _dq_schema_seeds

    raw = _dq_schema_seeds("de_assessment")[entity]
    for check in raw["checks"]:
        if check.get("ref_table"):
            parent = check["ref_table"].rsplit(".", 1)[-1]
            check["ref_table"] = TABLES[parent]
    return DqSchema.from_dict(raw)


def _as_bronze_batch(spark: SparkSession, frame, entity: str, batch: str) -> DataFrame:
    """Shape a generated frame like a bronze CDF micro-batch."""
    columns = list(frame.columns)
    int_columns = {
        c for c in columns
        if c.endswith("_id") or c in ("quantity", "stock_quantity", "reorder_level")
    }

    def cell(column: str, value: object) -> object:
        if _missing(value):
            return None
        # pandas widens an integer column to float as soon as it holds a NaN,
        # so 50 arrives as 50.0 and Spark rejects it for an INT field.
        return int(value) if column in int_columns else value

    records = [
        tuple(cell(c, v) for c, v in zip(columns, rec))
        for rec in frame.itertuples(index=False, name=None)
    ]
    schema = ", ".join(
        f"{c} " + ("INT" if c in int_columns else "STRING") for c in columns
    )
    df = spark.createDataFrame(records, schema=schema)
    for numeric in ("price", "cost", "unit_price", "total_amount", "lifetime_value"):
        if numeric in columns:
            df = df.withColumn(numeric, F.col(numeric).cast("decimal(18,2)"))
    return (
        df.withColumn("_batch_id", F.lit(batch))
        .withColumn("_ingest_timestamp", F.lit(dt.datetime(2026, 1, 1, tzinfo=dt.UTC)))
        .withColumn("_change_type", F.lit("insert"))
    )


def _conform(spark: SparkSession, frame, entity: str, batch: str, run: str) -> dict:
    """Run the real silver path for one entity and one delivery."""
    schema = _dq_schema(entity)
    tagged = apply_entity_checks(
        annotate_violations(_as_bronze_batch(spark, frame, entity, batch), schema),
        schema,
        spark,
    )
    survivors, passed, failed = split_validated_batch(tagged, entity)
    written = merge_to_silver(passed, entity, spark)
    deleted = apply_snapshot_soft_deletes(spark, entity, survivors)
    quarantined = write_quarantine(
        spark, failed, entity, run, dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
    )
    return {"written": written, "quarantined": quarantined, "soft_deleted": deleted}


@pytest.fixture(scope="module")
def pipeline(spark: SparkSession):
    """Small-scale seed delivery through the real silver path."""
    gen.BASE_CUSTOMERS, gen.BASE_PRODUCTS, gen.BASE_ORDERS = 400, 60, 800
    for entity, table in TABLES.items():
        create_delta_table(spark, table, silver_entity_schema(entity))
    create_delta_table(spark, QUARANTINE, QUARANTINE_SCHEMA)

    import silver.conform as conform_mod
    import silver.quarantine as quarantine_mod
    conform_mod.silver_table = lambda entity, _c="de_assessment": TABLES[entity]
    conform_mod.load_dq_schema = lambda _s, entity, _c="de_assessment": _dq_schema(entity)
    quarantine_mod.quarantine_table = lambda _c="de_assessment": QUARANTINE

    seed = dict(zip(("customers", "products", "orders"), gen.generate_dataframes()))
    stats = {e: _conform(spark, seed[e], e, "seed", f"run-{e}") for e in
             ("products", "customers", "orders")}
    return {"spark": spark, "seed": seed, "stats": stats}


# --------------------------------------------------------------------------
# EXPECTATION: every delivered key is accounted for.
# --------------------------------------------------------------------------
@pytest.mark.spark
@pytest.mark.parametrize("entity", ["products", "customers", "orders"])
def test_every_delivered_key_reaches_silver_or_quarantine(pipeline, entity: str) -> None:
    """No row may simply vanish between bronze and silver.

    This is the invariant that survives the row-hash gate: counting rows
    stamped with the current batch does not, because an unchanged row is
    deliberately not rewritten.
    """
    spark, frame = pipeline["spark"], pipeline["seed"][entity]
    expected = expected_split(frame, entity)

    silver_keys = {
        r[0] for r in spark.table(TABLES[entity]).select(PK[entity]).collect()
    }
    quarantined_keys = {
        r[0] for r in spark.table(QUARANTINE)
        .filter(F.col("entity_name") == entity)
        .select("primary_key").collect()
    }

    for key in expected["silver_keys"]:
        if key is None:
            continue
        assert key in silver_keys, f"{entity}: key {key} should be in silver"

    delivered = {
        None if _missing(getattr(r, PK[entity])) else int(getattr(r, PK[entity]))
        for r in frame.itertuples(index=False)
    }
    lost = {
        k for k in delivered
        if k is not None and k not in silver_keys and str(k) not in quarantined_keys
    }
    assert not lost, f"{entity}: keys neither in silver nor quarantined: {sorted(lost)[:10]}"


@pytest.mark.spark
@pytest.mark.parametrize("entity", ["products", "customers", "orders"])
def test_silver_has_no_duplicate_primary_keys(pipeline, entity: str) -> None:
    """EXPECTATION: survivorship leaves exactly one row per key."""
    spark = pipeline["spark"]
    dupes = (
        spark.table(TABLES[entity])
        .groupBy(PK[entity]).count().filter(F.col("count") > 1).count()
    )
    assert dupes == 0


@pytest.mark.spark
def test_permanent_defects_are_quarantined_and_absent_from_silver(pipeline) -> None:
    """EXPECTATION: a row with a blocking defect never reaches silver.

    Checked on customers, where a NULL email is the assessment's headline
    completeness case and was silently passing before the not_null check existed.
    """
    spark, frame = pipeline["spark"], pipeline["seed"]["customers"]
    bad_keys = {
        int(r.customer_id)
        for r in frame.itertuples(index=False)
        if not _missing(r.customer_id) and blocking_customer(r)
    }
    # A key can also be delivered by a clean duplicate row, which legitimately
    # survives; only keys whose every row is defective must be absent.
    all_bad = {
        key for key in bad_keys
        if all(
            blocking_customer(r)
            for r in frame.itertuples(index=False)
            if not _missing(r.customer_id) and int(r.customer_id) == key
        )
    }
    silver_keys = {r[0] for r in spark.table(TABLES["customers"]).select("customer_id").collect()}
    leaked = all_bad & silver_keys
    assert not leaked, f"defective customers reached silver: {sorted(leaked)[:10]}"

    quarantined = {
        r[0] for r in spark.table(QUARANTINE)
        .filter(F.col("entity_name") == "customers").select("primary_key").collect()
    }
    missed = {str(k) for k in all_bad} - quarantined
    assert not missed, f"defective customers not quarantined: {sorted(missed)[:10]}"


@pytest.mark.spark
def test_referential_failures_land_in_silver_flagged_not_quarantined(pipeline) -> None:
    """EXPECTATION: an order with a missing parent is in silver, flagged.

    A referential failure is temporal — the parent may arrive later — so it must
    not be treated as a permanent defect.
    """
    spark, seed = pipeline["spark"], pipeline["seed"]
    # Parents are the keys that reached SILVER, not the keys the CSV delivered.
    # A customer that failed validation was quarantined, so its orders are
    # genuinely orphans — quarantining a parent orphans its children, which is
    # the behaviour worth asserting rather than the raw key set.
    cust = {k for k in expected_split(seed["customers"], "customers")["silver_keys"] if k}
    prod = {k for k in expected_split(seed["products"], "products")["silver_keys"] if k}
    expected_orphans = {
        int(r.order_id)
        for r in seed["orders"].itertuples(index=False)
        if not blocking_order(r)
        and (int(r.customer_id) not in cust or int(r.product_id) not in prod)
    }
    assert expected_orphans, "the seed batch must contain orphans for this to mean anything"

    flagged = {
        r[0] for r in spark.table(TABLES["orders"])
        .filter(F.col("_is_orphan")).select("order_id").collect()
    }
    assert expected_orphans <= flagged, (
        f"orders with a missing parent are not flagged: "
        f"{sorted(expected_orphans - flagged)[:10]}"
    )
    # And nothing else is flagged.
    assert not (flagged - expected_orphans), (
        f"orders flagged with no missing parent: {sorted(flagged - expected_orphans)[:10]}"
    )


@pytest.mark.spark
def test_no_row_has_a_null_orphan_flag(pipeline) -> None:
    """EXPECTATION: the flag is always decided, never left unset.

    A NULL here means the merge skipped a row that needed the flag written,
    which is what the row-hash gate did before its condition was made
    null-safe in both directions.
    """
    spark = pipeline["spark"]
    for entity, table in TABLES.items():
        nulls = spark.table(table).filter(F.col("_is_orphan").isNull()).count()
        assert nulls == 0, f"{entity}: {nulls} rows have a NULL orphan flag"


# --------------------------------------------------------------------------
# EXPECTATION: the expectations above cover every rule the schema declares.
# --------------------------------------------------------------------------
# Rules this file evaluates by hand, as (entity, column, rule).
EXPECTED_RULES: set[tuple[str, str, str]] = {
    ("customers", "customer_id", "not_null"),
    ("customers", "customer_id", "uniqueness"),
    ("customers", "email", "not_null"),
    ("customers", "email", "format_email"),
    ("customers", "customer_name", "min_length"),
    ("customers", "customer_name", "max_length"),
    ("customers", "country", "pattern"),
    ("customers", "customer_segment", "enum"),
    ("customers", "signup_date", "max_date"),
    ("customers", "lifetime_value", "minimum"),
    ("products", "product_id", "not_null"),
    ("products", "product_id", "uniqueness"),
    ("products", "price", "minimum"),
    ("products", "cost", "minimum"),
    ("products", "product_name", "max_length"),
    ("products", "stock_quantity", "minimum"),
    ("products", "stock_quantity", "maximum"),
    ("orders", "order_id", "not_null"),
    ("orders", "order_id", "uniqueness"),
    ("orders", "customer_id", "not_null"),
    ("orders", "customer_id", "fk_exists"),
    ("orders", "product_id", "not_null"),
    ("orders", "product_id", "fk_exists"),
    ("orders", "quantity", "minimum"),
    ("orders", "quantity", "maximum"),
    ("orders", "unit_price", "exclusive_minimum"),
    ("orders", "total_amount", "minimum"),
    ("orders", "order_status", "enum"),
    ("orders", "order_date", "min_date"),
    ("orders", "order_date", "max_date"),
}


@pytest.mark.spark
def test_expectations_cover_every_declared_rule(spark: SparkSession) -> None:
    """A rule the expectations forget is a rule this file cannot check.

    Three assertions failed earlier for exactly that reason: the datetime rules
    were missing here, so rows the pipeline correctly quarantined looked like
    rows it had wrongly dropped. The expectation was wrong, not the code — and
    without this guard the next omission would read the same way.
    """
    del spark  # column_predicates needs an active session
    from silver.validators import column_predicates

    declared: set[tuple[str, str, str]] = set()
    for entity in ("customers", "products", "orders"):
        schema = _dq_schema(entity)
        for column in schema.columns:
            for _category, rule, _predicate in column_predicates(column):
                declared.add((entity, column.name, rule))
        for check in schema.checks:
            declared.add((entity, check.column, check.kind))

    missing = declared - EXPECTED_RULES
    assert not missing, f"declared rules the expectations do not evaluate: {sorted(missing)}"
    stale = EXPECTED_RULES - declared
    assert not stale, f"expectations reference rules nobody declares: {sorted(stale)}"
