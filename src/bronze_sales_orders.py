#!/usr/bin/env python3
"""Land only sales-order CSVs into Bronze."""
import argparse

from bronze_common import run_one


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-id", help="Optional landing batch folder, e.g. batch-1")
    run_one("sales_orders", parser.parse_args().batch_id)


if __name__ == "__main__":
    main()
