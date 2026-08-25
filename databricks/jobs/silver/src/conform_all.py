from workspace_path import setup_silver_src_path

setup_silver_src_path()

from silver.job_log import configure_job_logger, run_main
from silver.main import parse_catalog, run_conform_all

LOG = configure_job_logger("silver.conform_all")


def main() -> None:
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active SparkSession — run this on a Databricks cluster")
    catalog = parse_catalog()
    LOG.info("conform_all_start catalog=%s", catalog)
    run_conform_all(spark, catalog=catalog)
    LOG.info("conform_all_complete catalog=%s", catalog)


if __name__ == "__main__":
    run_main(main, LOG)
