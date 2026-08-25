from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, IntegerType, StructField, StructType
from silver.checks import apply_entity_checks
from silver.config import DqSchema


@pytest.mark.spark
def test_uniqueness_flags_duplicate_order_id(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [(1, 10), (1, 20)],
        schema=StructType(
            [
                StructField("order_id", IntegerType(), True),
                StructField("customer_id", IntegerType(), True),
            ]
        ),
    )
    dq_schema = DqSchema.from_dict(
        {
            "$schemaVersion": "1.0",
            "validationMode": "enforce",
            "columns": [],
            "checks": [
                {
                    "kind": "uniqueness",
                    "column": "order_id",
                    "category": "uniqueness",
                }
            ],
        }
    )
    result = apply_entity_checks(df, dq_schema, spark)
    violations = result.collect()
    assert all(len(row["_violations"]) > 0 for row in violations)


@pytest.mark.spark
def test_fk_exists_flags_missing_parent(spark: SparkSession) -> None:
    table_name = "test_silver_customers_fk"
    customers = spark.createDataFrame(
        [(10, False)],
        schema=StructType(
            [
                StructField("customer_id", IntegerType(), False),
                StructField("_is_deleted", BooleanType(), False),
            ]
        ),
    )
    customers.createOrReplaceTempView(table_name)

    orders = spark.createDataFrame(
        [(1, 99)],
        schema=StructType(
            [
                StructField("order_id", IntegerType(), True),
                StructField("customer_id", IntegerType(), True),
            ]
        ),
    )
    dq_schema = DqSchema.from_dict(
        {
            "$schemaVersion": "1.0",
            "validationMode": "enforce",
            "columns": [],
            "checks": [
                {
                    "kind": "fk_exists",
                    "column": "customer_id",
                    "category": "referential",
                    "ref_table": table_name,
                    "ref_column": "customer_id",
                }
            ],
        }
    )
    result = apply_entity_checks(orders, dq_schema, spark)
    assert result.collect()[0]["_violations"][0]["rule"] == "fk_exists"
