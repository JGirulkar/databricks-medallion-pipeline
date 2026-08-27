from __future__ import annotations

import os
import sys
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql.types import StructType


def _pin_pyspark_interpreter() -> None:
    """Force Spark workers onto this interpreter.

    Without this, workers resolve a bare `python3` from PATH. If that is a
    different minor version than the venv running the driver, every task that
    touches Python (any UDF, any Delta write of a Python-built DataFrame) dies
    with PYTHON_VERSION_MISMATCH — which reads like a Delta/Spark failure but
    is purely an environment mismatch.
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="module")
def spark() -> Iterator[pytest.Any]:
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    _pin_pyspark_interpreter()

    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()

    builder = (
        SparkSession.builder.master("local[1]")
        .appName("gold-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.sources.default", "delta")
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


@pytest.fixture(scope="function", autouse=True)
def reset_manifest_table(spark) -> Iterator[None]:
    """Recreate empty manifest table before each test for test isolation."""
    from gold.manifest import PIPELINE_MANIFEST_SCHEMA

    MANIFEST = "rt_manifest"
    spark.sql(f"DROP TABLE IF EXISTS {MANIFEST}")
    spark.createDataFrame([], PIPELINE_MANIFEST_SCHEMA).write.format("delta").mode(
        "overwrite"
    ).saveAsTable(MANIFEST)
    yield
