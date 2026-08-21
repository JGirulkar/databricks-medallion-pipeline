from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

COMMON_METADATA_FIELDS = (
    StructField("_ingest_timestamp", TimestampType(), False),
    StructField("_source_file", StringType(), False),
    StructField("_batch_id", StringType(), False),
    StructField("_delivery_pattern", StringType(), False),
    StructField("_rescued_data", StringType(), True),
)

_CUSTOMER_ROW_HASH_FIELD = StructField("_row_hash", StringType(), False)

_SOURCE_SCHEMAS: dict[str, StructType] = {
    "customers": StructType(
        [
            StructField("customer_id", IntegerType(), True),
            StructField("customer_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("country", StringType(), True),
            StructField("signup_date", DateType(), True),
            StructField("customer_segment", StringType(), True),
            StructField("lifetime_value", DecimalType(18, 2), True),
        ]
    ),
    "orders": StructType(
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
        ]
    ),
    "products": StructType(
        [
            StructField("product_id", IntegerType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", DecimalType(18, 2), True),
            StructField("cost", DecimalType(18, 2), True),
            StructField("stock_quantity", IntegerType(), True),
            StructField("reorder_level", IntegerType(), True),
        ]
    ),
}


def source_schema(source_name: str) -> StructType:
    try:
        return _SOURCE_SCHEMAS[source_name]
    except KeyError as exc:
        raise ValueError("Unknown source") from exc


def table_schema(source_name: str) -> StructType:
    fields = list(source_schema(source_name).fields)
    fields.extend(COMMON_METADATA_FIELDS)
    if source_name == "customers":
        fields.append(_CUSTOMER_ROW_HASH_FIELD)
    return StructType(fields)
