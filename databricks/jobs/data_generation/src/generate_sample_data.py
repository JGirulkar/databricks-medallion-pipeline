"""Generate assessment e-commerce CSVs: clean base data, then intentional DQ issues."""

from __future__ import annotations

import argparse
import random
import sys
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

_SRC_DIR: Path | None
try:
    _SRC_DIR = Path(__file__).resolve().parent
except NameError:
    _SRC_DIR = None

if _SRC_DIR is not None and str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

import pandas as pd
from faker import Faker
from job_log import configure_job_logger, run_main

RANDOM_SEED = 42

Faker.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)

LOG = configure_job_logger("data_generation")

DEFAULT_CATALOG = "de_assessment"

# Assessment PDF scale
BASE_CUSTOMERS = 10_000
BASE_PRODUCTS = 500
BASE_ORDERS = 100_000

CUSTOMER_SEGMENTS = ("Premium", "Standard", "Basic")
ORDER_STATUSES = ("Pending", "Completed", "Cancelled")

# Intentional issue counts — single source of truth (docs/ASSESSMENT_FROM_PDF.md)
DQ_ISSUE_COUNTS = {
    "null_emails": 50,
    "duplicate_customer_ids": 10,
    "null_order_customer_id": 100,
    "null_order_product_id": 200,
    "orphan_customer_id": 50,
    "orphan_product_id": 30,
    "duplicate_order_ids": 20,
    "invalid_emails": 30,
    "invalid_customer_segment": 20,
    "invalid_order_status": 20,
    "non_positive_quantity": 25,
    "negative_price": 15,
    "future_signup_date": 15,
    # --- extended coverage -------------------------------------------------
    # The block above is the set the assessment names explicitly (~700 rows).
    # These exercise the remaining validators the dq_schema declares, so a
    # single E2E run can prove every rule fires. See
    # jobs/silver/tests/test_dq_coverage.py, which fails if a declared rule
    # has no scenario here.
    "null_customer_id": 5,
    "short_customer_name": 8,
    "overlong_customer_name": 8,
    "invalid_country": 12,
    "negative_lifetime_value": 10,
    "null_product_id_products": 3,
    "duplicate_product_ids": 5,
    "negative_cost": 8,
    "overlong_product_name": 6,
    "negative_stock_quantity": 6,
    "excessive_stock_quantity": 6,
    "null_order_id": 5,
    "excessive_quantity": 12,
    "zero_unit_price": 12,
    "negative_total_amount": 10,
    "pre_launch_order_date": 12,
    "future_order_date": 12,
}

# Delivery modes. `seed` is the first full delivery; `delta` is a SECOND
# delivery that exercises the patterns config.source_config declares — orders
# are incremental (new keys only), customers and products are full_snapshot, so
# the whole world is restated and an absent key means deleted.
SEED_MODE = "seed"
DELTA_MODE = "delta"
MODES = (SEED_MODE, DELTA_MODE)

# Delta batch sizes — single source of truth, asserted in tests/test_dq_spec.py.
DELTA_NEW_ORDERS = 500
DELTA_CHANGED_CUSTOMERS = 20
DELTA_DELETED_PRODUCTS = 3
# Disjoint from the seed batch's 1..BASE_ORDERS, so a delta order can never be
# read as a re-send of a seed order.
DELTA_ORDER_ID_START = 200_001
# Seed lifetime_value is drawn from 100..5000; an uplift this large cannot
# collide with the seed value, so "changed" is never a rounding coincidence.
DELTA_LIFETIME_VALUE_UPLIFT = 1_000.0

# Bounds mirrored from the dq_schema seed in jobs/silver/src/silver/bootstrap.py.
NAME_MAX_LENGTH = 100
PRODUCT_NAME_MAX_LENGTH = 200
STOCK_QUANTITY_MAX = 100_000
ORDER_QUANTITY_MAX = 1_000
ORDER_DATE_MIN = "2020-01-01"

ORPHAN_ID_START = 900_001

CUSTOMER_COLUMNS = [
    "customer_id",
    "customer_name",
    "email",
    "country",
    "signup_date",
    "customer_segment",
    "lifetime_value",
]
PRODUCT_COLUMNS = [
    "product_id",
    "product_name",
    "category",
    "price",
    "cost",
    "stock_quantity",
    "reorder_level",
]
ORDER_COLUMNS = [
    "order_id",
    "customer_id",
    "order_date",
    "product_id",
    "quantity",
    "unit_price",
    "total_amount",
    "order_status",
    "payment_date",
]


def volume_root_for_catalog(catalog: str) -> str:
    return f"/Volumes/{catalog}/landing/raw"


def landing_batch_id(when: datetime | None = None) -> str:
    """UTC batch stamp shared by all three CSVs from one generator run."""
    ts = when or datetime.now(UTC)
    return ts.strftime("%Y%m%dT%H%M%SZ")


def landing_filename(entity: str, batch_id: str) -> str:
    """Stable pattern: {entity}_{batch_id}.csv — new batch_id => new Auto Loader file."""
    return f"{entity}_{batch_id}.csv"


def landing_paths(volume_root: str, batch_id: str) -> dict[str, str]:
    root = volume_root.rstrip("/")
    return {
        "products": f"{root}/products/{landing_filename('products', batch_id)}",
        "customers": f"{root}/customers/{landing_filename('customers', batch_id)}",
        "orders": f"{root}/orders/incoming/{landing_filename('orders', batch_id)}",
    }


def _customer_rows(fake: Faker) -> list[dict]:
    rows: list[dict] = []
    for customer_id in range(1, BASE_CUSTOMERS + 1):
        signup = fake.date_between(start_date=date(2020, 1, 1), end_date=date(2025, 12, 31))
        rows.append(
            {
                "customer_id": customer_id,
                "customer_name": fake.name(),
                "email": fake.email(),
                "country": fake.country_code(),
                "signup_date": signup.isoformat(),
                "customer_segment": random.choice(CUSTOMER_SEGMENTS),
                "lifetime_value": round(random.uniform(100, 5000), 2),
            }
        )
    return rows


def _product_rows(fake: Faker) -> list[dict]:
    rows: list[dict] = []
    categories = ["Electronics", "Apparel", "Home", "Sports", "Books"]
    for product_id in range(1, BASE_PRODUCTS + 1):
        price = round(random.uniform(10, 500), 2)
        cost = round(price * random.uniform(0.4, 0.8), 2)
        rows.append(
            {
                "product_id": product_id,
                "product_name": fake.catch_phrase(),
                "category": random.choice(categories),
                "price": price,
                "cost": cost,
                "stock_quantity": random.randint(0, 500),
                "reorder_level": random.randint(10, 50),
            }
        )
    return rows


def _order_rows(
    fake: Faker,
    order_ids: Sequence[int] | None = None,
    product_ids: Sequence[int] | None = None,
) -> list[dict]:
    """Build order rows over an id range and a product pool.

    Both arguments default to the seed batch (1..BASE_ORDERS over the full
    catalogue) and the seed path keeps drawing exactly as before — the delta
    batch passes a disjoint id range and a pool that excludes the products it
    deletes, so a new order never points at a key it also removes.
    """
    rows: list[dict] = []
    start = date(2023, 1, 1)
    end = date(2025, 12, 31)
    span_days = (end - start).days
    for order_id in order_ids if order_ids is not None else range(1, BASE_ORDERS + 1):
        order_date = start + timedelta(days=random.randint(0, span_days))
        product_id = (
            random.randint(1, BASE_PRODUCTS)
            if product_ids is None
            else random.choice(product_ids)
        )
        customer_id = random.randint(1, BASE_CUSTOMERS)
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(10, 500), 2)
        total_amount = round(quantity * unit_price, 2)
        status = random.choice(ORDER_STATUSES)
        payment_date = None
        if status == "Completed" and random.random() > 0.05:
            payment_date = (order_date + timedelta(days=random.randint(0, 3))).isoformat()
        rows.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_date": order_date.isoformat(),
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "total_amount": total_amount,
                "order_status": status,
                "payment_date": payment_date,
            }
        )
    return rows


def _append_null_pk_rows(
    df: pd.DataFrame,
    issue_key: str,
    pk_column: str,
) -> pd.DataFrame:
    """Append rows whose primary key is NULL.

    Deliberately APPENDS rather than nulling an existing row. Nulling a parent
    key in place removes it from the parent table, so every child that
    referenced it silently becomes an orphan — 3 nulled products cascaded into
    562 extra orphan orders against a spec of 30, because 100k orders over 500
    products gives each product ~200 children. An appended row has no children
    by construction, so the not_null check gets its data with no side effect on
    referential integrity.
    """
    n = _issue_count(issue_key)
    if not n or df.empty:
        return df
    extra = df.iloc[:n].copy()
    extra[pk_column] = None
    return pd.concat([df, extra], ignore_index=True)


def _apply_sample(
    df: pd.DataFrame,
    issue_key: str,
    column: str,
    value: object,
    seed: int,
) -> pd.DataFrame:
    """Set `column` to `value` on a deterministic sample of rows.

    One helper for every single-column injection so each scenario is one line
    and the random_state stays explicit and distinct per scenario.
    """
    n = _issue_count(issue_key)
    if not n:
        return df
    sample_n = min(n, len(df))
    if not sample_n:
        return df
    idx = df.sample(n=sample_n, random_state=seed).index
    out = df.copy()
    out.loc[idx, column] = value
    return out


def _issue_count(key: str) -> int:
    return int(DQ_ISSUE_COUNTS.get(key, 0))


def inject_customer_issues(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    dup_source = out.iloc[: _issue_count("duplicate_customer_ids")].copy()
    dup_source["customer_id"] = out.iloc[0]["customer_id"]

    null_n = _issue_count("null_emails")
    null_idx = out.sample(n=null_n, random_state=1).index if null_n else pd.Index([])
    if null_n:
        out.loc[null_idx, "email"] = None

    invalid_email_n = _issue_count("invalid_emails")
    if invalid_email_n:
        pool = out.index.difference(null_idx)
        sample_n = min(invalid_email_n, len(pool))
        if sample_n:
            invalid_email_idx = pool.to_series().sample(n=sample_n, random_state=11).index
            out.loc[invalid_email_idx, "email"] = "not-an-email"

    invalid_segment_n = _issue_count("invalid_customer_segment")
    if invalid_segment_n:
        sample_n = min(invalid_segment_n, len(out))
        invalid_segment_idx = out.sample(n=sample_n, random_state=12).index
        out.loc[invalid_segment_idx, "customer_segment"] = "Invalid"

    future_n = _issue_count("future_signup_date")
    if future_n:
        sample_n = min(future_n, len(out))
        future_idx = out.sample(n=sample_n, random_state=13).index
        future_date = (datetime.now(UTC).date() + timedelta(days=30)).isoformat()
        out.loc[future_idx, "signup_date"] = future_date

    # --- extended coverage ---
    out = _append_null_pk_rows(out, "null_customer_id", "customer_id")
    out = _apply_sample(out, "short_customer_name", "customer_name", "A", 21)
    out = _apply_sample(
        out,
        "overlong_customer_name",
        "customer_name",
        "X" * (NAME_MAX_LENGTH + 20),
        22,
    )
    out = _apply_sample(out, "invalid_country", "country", "X1!", 23)
    out = _apply_sample(out, "negative_lifetime_value", "lifetime_value", -25.0, 24)

    return pd.concat([out, dup_source], ignore_index=True)


def inject_order_issues(
    df: pd.DataFrame,
    valid_customer_ids: set[int],
    valid_product_ids: set[int],
) -> pd.DataFrame:
    out = df.copy()

    null_customer_idx = out.sample(
        n=DQ_ISSUE_COUNTS["null_order_customer_id"], random_state=2
    ).index
    out.loc[null_customer_idx, "customer_id"] = None

    null_product_idx = out.sample(
        n=DQ_ISSUE_COUNTS["null_order_product_id"], random_state=3
    ).index
    out.loc[null_product_idx, "product_id"] = None

    orphan_customers = list(
        range(ORPHAN_ID_START, ORPHAN_ID_START + DQ_ISSUE_COUNTS["orphan_customer_id"])
    )
    orphan_products = list(
        range(
            ORPHAN_ID_START + 10_000,
            ORPHAN_ID_START + 10_000 + DQ_ISSUE_COUNTS["orphan_product_id"],
        )
    )

    orphan_customer_idx = (
        out.index.difference(null_customer_idx)
        .to_series()
        .sample(n=DQ_ISSUE_COUNTS["orphan_customer_id"], random_state=5)
        .index
    )
    for idx, customer_id in zip(orphan_customer_idx, orphan_customers):
        out.at[idx, "customer_id"] = customer_id

    orphan_product_idx = (
        out.index.difference(null_product_idx)
        .to_series()
        .sample(n=DQ_ISSUE_COUNTS["orphan_product_id"], random_state=6)
        .index
    )
    for idx, product_id in zip(orphan_product_idx, orphan_products):
        out.at[idx, "product_id"] = product_id

    dup_orders = out.iloc[: DQ_ISSUE_COUNTS["duplicate_order_ids"]].copy()
    dup_orders["order_id"] = out.iloc[0]["order_id"]
    out = pd.concat([out, dup_orders], ignore_index=True)

    invalid_status_n = _issue_count("invalid_order_status")
    if invalid_status_n:
        invalid_status_idx = out.sample(n=invalid_status_n, random_state=14).index
        out.loc[invalid_status_idx, "order_status"] = "Invalid"

    bad_qty_n = _issue_count("non_positive_quantity")
    if bad_qty_n:
        bad_qty_idx = out.sample(n=bad_qty_n, random_state=15).index
        out.loc[bad_qty_idx, "quantity"] = 0

    # --- extended coverage ---
    out = _append_null_pk_rows(out, "null_order_id", "order_id")
    out = _apply_sample(
        out, "excessive_quantity", "quantity", ORDER_QUANTITY_MAX + 500, 31
    )
    out = _apply_sample(out, "zero_unit_price", "unit_price", 0.0, 32)
    out = _apply_sample(out, "negative_total_amount", "total_amount", -10.0, 33)
    out = _apply_sample(out, "pre_launch_order_date", "order_date", "2019-06-15", 34)
    future_order = (datetime.now(UTC).date() + timedelta(days=45)).isoformat()
    out = _apply_sample(out, "future_order_date", "order_date", future_order, 35)

    assert not any(c in valid_customer_ids for c in orphan_customers)
    assert not any(p in valid_product_ids for p in orphan_products)
    return out


def inject_product_issues(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out = _apply_sample(out, "negative_price", "price", -1.0, 16)

    # --- extended coverage ---
    out = _append_null_pk_rows(out, "null_product_id_products", "product_id")
    out = _apply_sample(out, "negative_cost", "cost", -2.0, 41)
    out = _apply_sample(
        out,
        "overlong_product_name",
        "product_name",
        "P" * (PRODUCT_NAME_MAX_LENGTH + 20),
        42,
    )
    out = _apply_sample(out, "negative_stock_quantity", "stock_quantity", -5, 43)
    out = _apply_sample(
        out,
        "excessive_stock_quantity",
        "stock_quantity",
        STOCK_QUANTITY_MAX + 1_000,
        44,
    )

    dup_n = _issue_count("duplicate_product_ids")
    if dup_n:
        dup_products = out.iloc[:dup_n].copy()
        dup_products["product_id"] = out.iloc[0]["product_id"]
        out = pd.concat([out, dup_products], ignore_index=True)
    return out


# --- delta delivery ---------------------------------------------------------
# The seed batch is one full delivery. Re-sending it unchanged proves nothing:
# every key already exists with the same values, so no update, insert or delete
# path downstream is ever taken. The delta batch is the second delivery that
# makes each declared pattern observable.


def _reset_seeds() -> None:
    """Re-arm both RNGs at the start of every batch.

    The delta batch has to rebuild the seed batch's base rows byte-identically:
    an UPDATE is only detectable if every column it does not deliberately
    change still carries the value the seed delivery landed.
    """
    Faker.seed(RANDOM_SEED)
    random.seed(RANDOM_SEED)


def _next_segment(segment: str) -> str:
    """Rotate to the next valid segment, so the value genuinely moves without
    leaving the allowed set — a changed row must not read as a DQ violation."""
    if segment not in CUSTOMER_SEGMENTS:
        return CUSTOMER_SEGMENTS[0]
    return CUSTOMER_SEGMENTS[
        (CUSTOMER_SEGMENTS.index(segment) + 1) % len(CUSTOMER_SEGMENTS)
    ]


def _delta_changed_customer_index(df: pd.DataFrame) -> pd.Index:
    """Fixed row pick for the update path — the same rows on every run."""
    n = min(DELTA_CHANGED_CUSTOMERS, len(df))
    if not n:
        return pd.Index([])
    return df.sample(n=n, random_state=51).index


def apply_delta_customer_changes(df: pd.DataFrame) -> pd.DataFrame:
    """Move value columns on a fixed sample while keeping customer_id.

    Same key, different payload is the only shape that proves the snapshot
    merge updates in place instead of inserting a second row for the key.
    """
    out = df.copy()
    idx = _delta_changed_customer_index(out)
    if not len(idx):
        return out
    out.loc[idx, "lifetime_value"] = (
        pd.to_numeric(out.loc[idx, "lifetime_value"]) + DELTA_LIFETIME_VALUE_UPLIFT
    ).round(2)
    out.loc[idx, "customer_segment"] = [
        _next_segment(str(value)) for value in out.loc[idx, "customer_segment"]
    ]
    return out


def delta_deleted_product_ids(df: pd.DataFrame) -> list[int]:
    """Keys the delta snapshot omits.

    Taken from the tail of the catalogue rather than sampled, so the deleted
    set stays predictable when reading an E2E run by eye.
    """
    ids = sorted(int(product_id) for product_id in df["product_id"].dropna().unique())
    n = min(DELTA_DELETED_PRODUCTS, len(ids))
    return ids[-n:] if n else []


def remove_delta_deleted_products(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows for the deleted keys — a snapshot feed states the whole world,
    so absence IS the delete signal; there is no tombstone column to set."""
    deleted = set(delta_deleted_product_ids(df))
    if not deleted:
        return df.copy()
    return df.loc[~df["product_id"].isin(deleted)].reset_index(drop=True)


def generate_delta_dataframes() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Second delivery: new orders only, changed customers, deleted products.

    No DQ issues are injected. A change delivery has to be attributable — any
    quarantine row an E2E sees after this batch came from the seed batch.
    """
    _reset_seeds()
    fake = Faker()
    # Draw order matters: customers, then products, then orders, exactly as the
    # seed batch draws them, so the shared RNG hands back the same base rows.
    customers = apply_delta_customer_changes(pd.DataFrame(_customer_rows(fake)))
    products = remove_delta_deleted_products(pd.DataFrame(_product_rows(fake)))
    surviving = [int(product_id) for product_id in products["product_id"].dropna()]
    order_ids = range(DELTA_ORDER_ID_START, DELTA_ORDER_ID_START + DELTA_NEW_ORDERS)
    orders = pd.DataFrame(
        _order_rows(fake, order_ids=order_ids, product_ids=surviving),
        columns=ORDER_COLUMNS,
    )
    return customers, products, orders


def summarize_delta(
    customers: pd.DataFrame,
    products: pd.DataFrame,
    orders: pd.DataFrame,
) -> dict[str, int]:
    """What this delivery claims to prove, so an E2E asserts on numbers rather
    than eyeballing the CSVs."""
    return {
        "customers": len(customers),
        "products": len(products),
        "orders": len(orders),
        "new_orders": len(orders),
        "changed_customers": min(DELTA_CHANGED_CUSTOMERS, len(customers)),
        "deleted_products": max(BASE_PRODUCTS - len(products), 0),
        "first_new_order_id": int(orders["order_id"].min()) if len(orders) else 0,
    }


def summarize_issues(
    customers: pd.DataFrame,
    orders: pd.DataFrame,
    products: pd.DataFrame,
) -> dict[str, int]:
    valid_customers = set(range(1, BASE_CUSTOMERS + 1))
    valid_products = set(range(1, BASE_PRODUCTS + 1))
    null_emails = int(customers["email"].isna().sum())
    dup_customer_rows = len(customers) - BASE_CUSTOMERS
    null_order_customer = int(orders["customer_id"].isna().sum())
    null_order_product = int(orders["product_id"].isna().sum())
    orphan_c = int(
        orders.loc[orders["customer_id"].notna(), "customer_id"]
        .astype(int)
        .isin(valid_customers)
        .eq(False)
        .sum()
    )
    orphan_p = int(
        orders.loc[orders["product_id"].notna(), "product_id"]
        .astype(int)
        .isin(valid_products)
        .eq(False)
        .sum()
    )
    dup_order_rows = len(orders) - BASE_ORDERS
    return {
        "customers": len(customers),
        "products": len(products),
        "orders": len(orders),
        "null_emails": null_emails,
        "duplicate_customer_rows": dup_customer_rows,
        "null_order_customer_id": null_order_customer,
        "null_order_product_id": null_order_product,
        "orphan_customer_id": orphan_c,
        "orphan_product_id": orphan_p,
        "duplicate_order_rows": dup_order_rows,
    }


_INTEGER_CSV_COLUMNS: dict[str, tuple[str, ...]] = {
    "customers": ("customer_id",),
    "products": ("product_id", "stock_quantity", "reorder_level"),
    "orders": ("order_id", "customer_id", "product_id", "quantity"),
}


def coerce_integer_csv_columns(df: pd.DataFrame, entity: str) -> pd.DataFrame:
    """Use nullable Int64 so NULL FK values do not promote columns to float in CSV."""
    out = df.copy()
    for column in _INTEGER_CSV_COLUMNS.get(entity, ()):
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce").astype("Int64")
    return out


def frame_to_csv(frame: pd.DataFrame, entity: str) -> str:
    return coerce_integer_csv_columns(frame, entity).to_csv(index=False)


def generate_dataframes() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    # Same reseed the delta batch does. Identical to the import-time seeding for
    # a fresh process, so seed output is unchanged, but it also pins the base
    # rows the delta batch must reproduce when both run in one process.
    _reset_seeds()
    fake = Faker()
    customers = inject_customer_issues(pd.DataFrame(_customer_rows(fake)))
    products = inject_product_issues(pd.DataFrame(_product_rows(fake)))
    valid_customers = set(range(1, BASE_CUSTOMERS + 1))
    valid_products = set(range(1, BASE_PRODUCTS + 1))
    orders = inject_order_issues(
        pd.DataFrame(_order_rows(fake)),
        valid_customer_ids=valid_customers,
        valid_product_ids=valid_products,
    )
    return customers, products, orders


def write_local_csvs(
    customers: pd.DataFrame,
    products: pd.DataFrame,
    orders: pd.DataFrame,
    output_dir: Path,
    batch_id: str,
) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "customers": output_dir / landing_filename("customers", batch_id),
        "products": output_dir / landing_filename("products", batch_id),
        "orders": output_dir / landing_filename("orders", batch_id),
    }
    paths["customers"].write_text(frame_to_csv(customers, "customers"), encoding="utf-8")
    paths["products"].write_text(frame_to_csv(products, "products"), encoding="utf-8")
    paths["orders"].write_text(frame_to_csv(orders, "orders"), encoding="utf-8")
    return {name: str(path) for name, path in paths.items()}


def write_volume_csvs(
    customers: pd.DataFrame,
    products: pd.DataFrame,
    orders: pd.DataFrame,
    volume_root: str,
    batch_id: str,
) -> dict[str, str]:
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]
        from pyspark.sql import SparkSession
    except ImportError as exc:
        LOG.exception("volume_write_import_failed")
        raise RuntimeError(
            "Volume writes require pyspark and dbutils on a Databricks cluster"
        ) from exc

    spark = SparkSession.getActiveSession()
    if spark is None:
        LOG.error("volume_write_no_spark_session volume_root=%s", volume_root)
        raise RuntimeError("Volume writes require an active SparkSession on Databricks")

    dbutils = DBUtils(spark)
    targets = {
        "products": (landing_paths(volume_root, batch_id)["products"], products),
        "customers": (landing_paths(volume_root, batch_id)["customers"], customers),
        "orders": (landing_paths(volume_root, batch_id)["orders"], orders),
    }
    written: dict[str, str] = {}
    for entity, (remote_path, frame) in targets.items():
        try:
            LOG.info(
                "volume_write_start entity=%s batch_id=%s path=%s rows=%s cols=%s",
                entity,
                batch_id,
                remote_path,
                len(frame),
                len(frame.columns),
            )
            dbutils.fs.mkdirs(str(Path(remote_path).parent))
            dbutils.fs.put(remote_path, frame_to_csv(frame, entity), overwrite=True)
            written[entity] = remote_path
            LOG.info("volume_write_success path=%s", remote_path)
        except Exception:
            LOG.exception("volume_write_failed path=%s", remote_path)
            raise
    return written


def generate(
    output_dir: Path | None = None,
    volume_root: str | None = None,
    batch_id: str | None = None,
    mode: str = SEED_MODE,
) -> dict[str, int | str | dict[str, str]]:
    try:
        if mode not in MODES:
            raise ValueError(f"unknown mode {mode!r}, expected one of {MODES}")
        run_batch_id = batch_id or landing_batch_id()
        LOG.info(
            "generate_start mode=%s batch_id=%s output_dir=%s volume_root=%s bases=(customers=%s products=%s orders=%s)",
            mode,
            run_batch_id,
            output_dir,
            volume_root,
            BASE_CUSTOMERS,
            BASE_PRODUCTS,
            BASE_ORDERS,
        )
        if mode == DELTA_MODE:
            customers, products, orders = generate_delta_dataframes()
        else:
            customers, products, orders = generate_dataframes()
        LOG.info(
            "generate_dataframes_done customers=%s products=%s orders=%s",
            len(customers),
            len(products),
            len(orders),
        )
        output_files: dict[str, str] = {}
        if output_dir is not None:
            output_files.update(
                write_local_csvs(customers, products, orders, output_dir, run_batch_id)
            )
            LOG.info("local_write_success batch_id=%s files=%s", run_batch_id, output_files)
        if volume_root is not None:
            output_files.update(
                write_volume_csvs(
                    customers, products, orders, volume_root, run_batch_id
                )
            )
        stats = (
            summarize_delta(customers, products, orders)
            if mode == DELTA_MODE
            else summarize_issues(customers, orders, products)
        )
        result: dict[str, int | str | dict[str, str]] = {
            **stats,
            "batch_id": run_batch_id,
            "mode": mode,
            "files": output_files,
        }
        LOG.info(
            "generate_complete mode=%s batch_id=%s stats=%s files=%s",
            mode,
            run_batch_id,
            stats,
            output_files,
        )
        return result
    except Exception:
        LOG.exception(
            "generate_failed mode=%s output_dir=%s volume_root=%s",
            mode,
            output_dir,
            volume_root,
        )
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate assessment sample CSVs")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Local directory for CSV output (default: repo data/ when no volume write)",
    )
    parser.add_argument(
        "--volume-root",
        type=str,
        default=None,
        help="UC volume landing root, e.g. /Volumes/de_assessment/landing/raw",
    )
    parser.add_argument(
        "--catalog",
        type=str,
        default=DEFAULT_CATALOG,
        help="Catalog name; used to derive volume root when --volume-root omitted on cluster",
    )
    parser.add_argument(
        "--mode",
        choices=MODES,
        default=SEED_MODE,
        help=(
            "seed = first full delivery with the DQ issues injected; "
            "delta = second delivery (new orders only, changed customers, "
            "deleted products) that exercises the update and delete paths"
        ),
    )
    parser.add_argument(
        "--batch-id",
        type=str,
        default=None,
        help="UTC batch stamp for filenames (default: current time, e.g. 20260821T133045Z)",
    )
    args = parser.parse_args()
    LOG.info(
        "cli_args mode=%s catalog=%s output_dir=%s volume_root=%s batch_id=%s",
        args.mode,
        args.catalog,
        args.output_dir,
        args.volume_root,
        args.batch_id,
    )

    output_dir = args.output_dir
    volume_root = args.volume_root

    if volume_root is None:
        try:
            from pyspark.sql import SparkSession

            if SparkSession.getActiveSession() is not None:
                volume_root = volume_root_for_catalog(args.catalog)
                LOG.info("derived_volume_root=%s", volume_root)
        except ImportError:
            LOG.warning("pyspark_not_available_skipping_volume_root_derivation")

    if output_dir is None and volume_root is None:
        try:
            output_dir = Path(__file__).resolve().parents[4] / "data"
        except NameError:
            LOG.error("no_output_target_set_and___file___unavailable")
            raise RuntimeError(
                "Set --output-dir or run on Databricks with --catalog for volume writes"
            ) from None
        LOG.info("default_output_dir=%s", output_dir)

    result = generate(
        output_dir=output_dir,
        volume_root=volume_root,
        batch_id=args.batch_id,
        mode=args.mode,
    )
    destinations = []
    if output_dir is not None:
        destinations.append(str(output_dir))
    if volume_root is not None:
        destinations.append(volume_root)
    LOG.info(
        "wrote_csvs mode=%s batch_id=%s destinations=%s files=%s stats=%s",
        result["mode"],
        result["batch_id"],
        destinations,
        result.get("files"),
        {k: v for k, v in result.items() if k not in ("batch_id", "files", "mode")},
    )


if __name__ == "__main__":
    run_main(main, configure_job_logger("data_generation.main"))
