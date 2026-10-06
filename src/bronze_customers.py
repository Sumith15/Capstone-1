#!/usr/bin/env python3
"""Land only customer CSVs from the contact release into Bronze."""
import argparse

from bronze_common import run_one


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-id", help="Optional landing batch folder, e.g. batch-1")
    run_one("customers", parser.parse_args().batch_id)


if __name__ == "__main__":
    main()
