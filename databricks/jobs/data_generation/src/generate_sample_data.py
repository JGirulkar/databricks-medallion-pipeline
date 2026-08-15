"""Generate sample e-commerce CSVs with intentional DQ issues."""

from __future__ import annotations

import argparse
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate assessment sample CSVs")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parents[4] / "data",
    )
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    # TODO: implement generation with intentional DQ issues per spec
    print(f"Placeholder: generate CSVs into {args.output_dir}")


if __name__ == "__main__":
    main()
