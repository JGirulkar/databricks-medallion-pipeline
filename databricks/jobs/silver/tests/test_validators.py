from __future__ import annotations

from datetime import date

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType
from silver.config import DqSchema
from silver.validators import annotate_violations


def _customers_schema() -> StructType:
    return StructType(
        [
            StructField("customer_id", IntegerType(), True),
            StructField("email", StringType(), True),
            StructField("customer_segment", StringType(), True),
            StructField("signup_date", DateType(), True),
        ]
    )


def _dq_schema() -> DqSchema:
    return DqSchema.from_dict(
        {
            "$schemaVersion": "1.0",
            "validationMode": "enforce",
            "columns": [
                {
                    "name": "email",
                    "type": "string",
                    "nullable": True,
                    "validation": {"kind": "string", "format": "email"},
                },
                {
                    "name": "customer_segment",
                    "type": "string",
                    "nullable": True,
                    "validation": {
                        "kind": "string",
                        "enum": ["Premium", "Standard", "Basic"],
                    },
                },
                {
                    "name": "signup_date",
                    "type": "datetime",
                    "nullable": True,
                    "validation": {"kind": "datetime", "max_date": "today"},
                },
            ],
            "checks": [],
        }
    )


@pytest.mark.spark
def test_annotate_violations_clean_row(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [(1, "a@b.com", "Premium", date(2020, 1, 1))],
        schema=_customers_schema(),
    )
    result = annotate_violations(df, _dq_schema())
    row = result.collect()[0]
    assert row["_violations"] == []


@pytest.mark.spark
def test_annotate_violations_email_and_enum(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [(1, "not-an-email", "Invalid", date(2020, 1, 1))],
        schema=_customers_schema(),
    )
    result = annotate_violations(df, _dq_schema())
    violations = result.collect()[0]["_violations"]
    rules = {v["rule"] for v in violations}
    assert "format_email" in rules
    assert "enum" in rules


@pytest.mark.spark
def test_annotate_violations_not_null_on_non_nullable_column(spark: SparkSession) -> None:
    schema = DqSchema.from_dict(
        {
            "$schemaVersion": "1.0",
            "validationMode": "enforce",
            "columns": [
                {
                    "name": "customer_id",
                    "type": "integer",
                    "nullable": False,
                }
            ],
            "checks": [],
        }
    )
    df = spark.createDataFrame([(None,)], schema=StructType([StructField("customer_id", IntegerType(), True)]))
    result = annotate_violations(df, schema)
    assert result.collect()[0]["_violations"][0]["rule"] == "not_null"


@pytest.mark.spark
def test_annotate_violations_numeric_minimum(spark: SparkSession) -> None:
    schema = DqSchema.from_dict(
        {
            "$schemaVersion": "1.0",
            "validationMode": "enforce",
            "columns": [
                {
                    "name": "quantity",
                    "type": "integer",
                    "nullable": True,
                    "validation": {"kind": "numeric", "minimum": 1},
                }
            ],
            "checks": [],
        }
    )
    df = spark.createDataFrame([(0,)], schema=StructType([StructField("quantity", IntegerType(), True)]))
    result = annotate_violations(df, schema)
    assert result.collect()[0]["_violations"][0]["rule"] == "minimum"
