from __future__ import annotations

import argparse
from collections.abc import Sequence

from bronze.config import DEFAULT_CATALOG
from bronze.ingest import run_source as _run_source


def parse_catalog(argv: Sequence[str] | None = None) -> str:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default=DEFAULT_CATALOG)
    return parser.parse_args(list(argv) if argv is not None else None).catalog


def run_source(source_name: str, catalog: str = DEFAULT_CATALOG) -> None:
    _run_source(source_name, catalog=catalog)
