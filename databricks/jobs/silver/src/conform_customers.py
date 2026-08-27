from workspace_path import setup_silver_src_path

setup_silver_src_path()

from silver.job_log import configure_job_logger, run_main
from silver.main import parse_catalog, run_conform_with_healing

LOG = configure_job_logger("silver.conform_customers")


def main() -> None:
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active SparkSession — run this on a Databricks cluster")
    catalog = parse_catalog()
    run_conform_with_healing(spark, "customers", catalog=catalog)


if __name__ == "__main__":
    run_main(main, LOG)
