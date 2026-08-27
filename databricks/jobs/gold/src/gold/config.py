"""Gold configuration: pinned business constants and SQL file loading.

The business rules are defined ONCE here and in the runner's
qualifying_orders view — the SQL files receive them as substitutions.
"""

from __future__ import annotations

from pathlib import Path

DEFAULT_CATALOG = "de_assessment"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
OPS_SCHEMA = "ops"
PIPELINE_MANIFEST_TABLE_NAME = "pipeline_manifest"

# Segment ladder constants, pinned from the measured seed distribution
# (per-customer lifetime qualifying revenue p90 ~= 4,983 -> 5,000).
# See docs/superpowers/specs/2026-08-27-gold-layer-design.md §3.3.
INACTIVE_DAYS = 90
HIGH_VALUE_REVENUE = 5000

SQL_DIR = Path(__file__).resolve().parent / "sql"

# Execution order. 01–03 read only qualifying_orders and silver dims;
# 04 reads the table 02 built (cross-foots by construction).
GOLD_SQL_FILES: tuple[str, ...] = (
    "01_sales_by_product.sql",
    "02_revenue_by_customer.sql",
    "03_daily_weekly_trends.sql",
    "04_customer_segmentation.sql",
)
GOLD_TABLES: tuple[str, ...] = (
    "sales_by_product",
    "revenue_by_customer",
    "daily_weekly_trends",
    "customer_segmentation",
)


def load_sql(filename: str) -> str:
    return (SQL_DIR / filename).read_text(encoding="utf-8")


def render_sql(text: str, *, silver: str, gold: str) -> str:
    return text.format(
        silver=silver,
        gold=gold,
        inactive_days=INACTIVE_DAYS,
        high_value_revenue=HIGH_VALUE_REVENUE,
    )


def pipeline_manifest_table(catalog: str = DEFAULT_CATALOG) -> str:
    return f"{catalog}.{OPS_SCHEMA}.{PIPELINE_MANIFEST_TABLE_NAME}"
