"""Bronze layer bootstrap entrypoint — runs on a Databricks cluster."""

from __future__ import annotations

from bronze.bootstrap import DEFAULT_CATALOG, bootstrap


def main(catalog: str = DEFAULT_CATALOG) -> None:
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active SparkSession — run this on a Databricks cluster")

    from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]

    dbutils = DBUtils(spark)
    bootstrap(spark, dbutils.fs.mkdirs, catalog=catalog)


if __name__ == "__main__":
    main()
