from workspace_path import setup_bronze_src_path

setup_bronze_src_path()

from bronze.job_log import configure_job_logger, run_main
from bronze.main import parse_catalog, run_source

LOG = configure_job_logger("bronze.ingest_all")


def main() -> None:
    catalog = parse_catalog()
    LOG.info("ingest_all_start catalog=%s sources=products,customers,orders", catalog)
    for source_name in ("products", "customers", "orders"):
        LOG.info("ingest_all_source_start source=%s", source_name)
        run_source(source_name, catalog)
        LOG.info("ingest_all_source_complete source=%s", source_name)


if __name__ == "__main__":
    run_main(main, LOG)
