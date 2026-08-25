from __future__ import annotations

import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql.types import StructType


@pytest.fixture(scope="module")
def spark() -> Iterator[pytest.Any]:
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()

    builder = (
        SparkSession.builder.master("local[1]")
        .appName("silver-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


def create_delta_table(spark, table_name: str, schema: StructType) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    tmpdir = tempfile.mkdtemp()
    uri = Path(tmpdir).as_uri()
    spark.createDataFrame([], schema=schema).write.format("delta").mode(
        "overwrite"
    ).save(uri)
    spark.sql(f"CREATE TABLE {table_name} USING delta LOCATION '{uri}'")
