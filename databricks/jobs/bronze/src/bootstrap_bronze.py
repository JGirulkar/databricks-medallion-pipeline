from workspace_path import setup_bronze_src_path

setup_bronze_src_path()

from bronze.job_log import configure_job_logger, run_main
from bronze.main import parse_catalog, run_source

LOG = configure_job_logger("bronze.bootstrap")


def main() -> None:
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is None:
        LOG.error("bootstrap_no_spark_session")
        raise RuntimeError("No active SparkSession — run this on a Databricks cluster")

    from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]

    catalog = parse_catalog()
    LOG.info("bootstrap_start catalog=%s", catalog)
    try:
        dbutils = DBUtils(spark)
        from bronze.bootstrap import bootstrap

        bootstrap(spark, dbutils.fs.mkdirs, catalog=catalog)
        LOG.info("bootstrap_complete catalog=%s", catalog)
    except Exception:
        LOG.exception("bootstrap_failed catalog=%s", catalog)
        raise


if __name__ == "__main__":
    run_main(main, LOG)
