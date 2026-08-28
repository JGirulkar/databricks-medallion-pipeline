from workspace_path import setup_gold_src_path

setup_gold_src_path()

import argparse

from gold.job_log import configure_job_logger, run_main
from gold.runner import run_gold

LOG = configure_job_logger("gold.run_gold")


def main() -> None:
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active SparkSession — run this on a Databricks cluster")
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="de_assessment")
    args = parser.parse_args()
    run_gold(spark, catalog=args.catalog)


if __name__ == "__main__":
    run_main(main, LOG)
