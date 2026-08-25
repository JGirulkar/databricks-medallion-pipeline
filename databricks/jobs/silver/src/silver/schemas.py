"""Silver table StructTypes for writers and bootstrap DDL."""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

VIOLATION_STRUCT = StructType(
    [
        StructField("category", StringType(), False),
        StructField("rule", StringType(), False),
        StructField("column", StringType(), False),
        StructField("value", StringType(), True),
    ]
)

QUARANTINE_SCHEMA = StructType(
    [
        StructField("entity_name", StringType(), False),
        StructField("primary_key", StringType(), False),
        StructField("data", StringType(), False),
        StructField("violations", ArrayType(VIOLATION_STRUCT), False),
        StructField("quarantined_at", TimestampType(), False),
        StructField("silver_run_id", StringType(), False),
        StructField("bronze_batch_id", StringType(), True),
    ]
)

DQ_METRICS_SCHEMA = StructType(
    [
        StructField("silver_run_id", StringType(), False),
        StructField("entity_name", StringType(), False),
        StructField("check_category", StringType(), False),
        StructField("rows_evaluated", LongType(), False),
        StructField("rows_passed", LongType(), False),
        StructField("rows_quarantined", LongType(), False),
        StructField("pass_pct", DoubleType(), False),
        StructField("run_at", TimestampType(), False),
    ]
)

PIPELINE_MANIFEST_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("entity_name", StringType(), False),
        StructField("parent_run_id", StringType(), True),
        StructField("delivery_pattern", StringType(), True),
        StructField("source_path", StringType(), True),
        StructField("files_processed", IntegerType(), False),
        StructField("rows_read", LongType(), False),
        StructField("rows_written", LongType(), False),
        StructField("rows_quarantined", LongType(), False),
        StructField("rows_rescued", LongType(), False),
        StructField("delta_version_before", LongType(), True),
        StructField("delta_version_after", LongType(), True),
        StructField("started_at", TimestampType(), False),
        StructField("completed_at", TimestampType(), True),
        StructField("status", StringType(), False),
        StructField("error_message", StringType(), True),
    ]
)

SILVER_CONTROL_FIELDS = (
    StructField("quality_check_result", StringType(), False),
    StructField("_row_hash", StringType(), True),
    StructField("_is_deleted", BooleanType(), False),
    StructField("_silver_updated_at", TimestampType(), False),
    StructField("_bronze_batch_id", StringType(), False),
)

_CUSTOMER_BUSINESS_FIELDS = (
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("signup_date", DateType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("lifetime_value", DecimalType(18, 2), True),
)

_PRODUCT_BUSINESS_FIELDS = (
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DecimalType(18, 2), True),
    StructField("cost", DecimalType(18, 2), True),
    StructField("stock_quantity", IntegerType(), True),
    StructField("reorder_level", IntegerType(), True),
)

_ORDER_BUSINESS_FIELDS = (
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", DateType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DecimalType(18, 2), True),
    StructField("total_amount", DecimalType(18, 2), True),
    StructField("order_status", StringType(), True),
    StructField("payment_date", DateType(), True),
)

_SILVER_ENTITY_FIELDS: dict[str, tuple[StructField, ...]] = {
    "customers": _CUSTOMER_BUSINESS_FIELDS,
    "products": _PRODUCT_BUSINESS_FIELDS,
    "orders": _ORDER_BUSINESS_FIELDS,
}


def silver_entity_schema(entity_name: str) -> StructType:
    try:
        business = _SILVER_ENTITY_FIELDS[entity_name]
    except KeyError as exc:
        raise ValueError(f"Unknown silver entity: {entity_name}") from exc
    return StructType(list(business) + list(SILVER_CONTROL_FIELDS))
