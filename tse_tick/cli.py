#!/usr/bin/env python3
# tse_tick/cli.py
import argparse
import logging
import sys
from pathlib import Path

from tse_tick.ingest import ingest_directory, ingest_year_from_root

logger = logging.getLogger(__name__)


def _parse_years(years_str: str) -> list[int]:
    result = []
    for part in years_str.split(","):
        part = part.strip()
        if "-" in part:
            try:
                start, end = part.split("-")
                result.extend(range(int(start), int(end) + 1))
            except ValueError:
                raise ValueError(f"Invalid year range: {part}")
        else:
            try:
                result.append(int(part))
            except ValueError:
                raise ValueError(f"Invalid year: {part}")
    return sorted(set(result))


def _parse_months(months_str: str) -> list[int]:
    result = []
    for part in months_str.split(","):
        part = part.strip()
        if "-" in part:
            try:
                start, end = part.split("-")
                result.extend(range(int(start), int(end) + 1))
            except ValueError:
                raise ValueError(f"Invalid month range: {part}")
        else:
            try:
                result.append(int(part))
            except ValueError:
                raise ValueError(f"Invalid month: {part}")
    return sorted(set(result))


def cmd_ingest(args: argparse.Namespace) -> None:
    valid_types = {"individual_stock", "stock_summary", "indices", "indices_summary"}
    if args.data_type not in valid_types:
        print(f"Error: --data-type must be one of {sorted(valid_types)}", file=sys.stderr)
        sys.exit(1)

    years = _parse_years(args.years) if args.years else [args.year] if args.year else None
    if years is None:
        print("Error: --years or --year is required", file=sys.stderr)
        sys.exit(1)

    for y in years:
        if not (2016 <= y <= 2023):
            print(f"Error: year {y} is outside supported range (2016-2023)", file=sys.stderr)
            sys.exit(1)

    input_root = args.input_root
    output_root = args.output_root

    if args.flat:
        print(f"Ingesting all ZIPs from flat directory: {input_root}")
        print(f"  Data type: {args.data_type}")
        print(f"  Output: {output_root}")
        results = ingest_directory(
            input_root, output_root,
            data_type=args.data_type,
            language=args.language,
            max_workers=args.parallel,
        )
        success = sum(1 for r in results if "error" not in r)
        failed = sum(1 for r in results if "error" in r)
        print(f"Done: {success} succeeded, {failed} failed")
    else:
        print(f"Ingesting from root with discovery: {input_root}")
        print(f"  Data type: {args.data_type}")
        print(f"  Years: {years}")
        print(f"  Output: {output_root}")
        for year in years:
            print(f"\n--- Year {year} ---")
            results = ingest_year_from_root(
                input_root, output_root,
                year=year,
                data_type=args.data_type,
                language=args.language,
                resume=not args.no_resume,
            )
            success = sum(1 for r in results if "error" not in r)
            failed = sum(1 for r in results if "error" in r)
            print(f"Year {year}: {success} succeeded, {failed} failed")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tse-tick",
        description="High-performance Nikkei NEEDS tick data processing pipeline",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    ingest_parser = subparsers.add_parser("ingest", help="Ingest NEEDS ZIP files into Parquet store")
    ingest_parser.add_argument(
        "--data-type",
        required=True,
        choices=["individual_stock", "stock_summary", "indices", "indices_summary"],
        help="Type of NEEDS data to ingest",
    )
    ingest_parser.add_argument(
        "--years",
        type=str,
        default=None,
        help='Year(s) to process, e.g. "2016-2023" or "2018,2019,2020"',
    )
    ingest_parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Single year to process (alternative to --years)",
    )
    ingest_parser.add_argument(
        "--input-root",
        required=True,
        help="Root directory containing NEEDS data in {year}/{yearmonth}/ layout",
    )
    ingest_parser.add_argument(
        "--output-root",
        required=True,
        help="Root directory for Parquet output store",
    )
    ingest_parser.add_argument(
        "--language",
        default="en",
        choices=["en", "jp"],
        help="Column name language (default: en)",
    )
    ingest_parser.add_argument(
        "--parallel",
        type=int,
        default=1,
        help="Number of parallel worker processes (default: 1)",
    )
    ingest_parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Disable resume (reprocess all files even if output exists)",
    )
    ingest_parser.add_argument(
        "--flat",
        action="store_true",
        help="Input directory is a flat folder of ZIPs (no year/month structure)",
    )
    ingest_parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity level",
    )

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    logging.basicConfig(
        level=getattr(logging, args.log_level, logging.INFO),
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if args.command == "ingest":
        cmd_ingest(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
