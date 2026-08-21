from workspace_path import setup_bronze_src_path

setup_bronze_src_path()

from bronze.job_log import configure_job_logger, run_main
from bronze.main import parse_catalog, run_source

LOG = configure_job_logger("bronze.ingest_orders")


def main() -> None:
    run_source("orders", parse_catalog())


if __name__ == "__main__":
    run_main(main, LOG)
