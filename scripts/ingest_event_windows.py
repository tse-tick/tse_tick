#!/usr/bin/env python3
"""
CLI: extract ±N minute tick windows around corporate disclosure events.

Usage::

    python scripts/ingest_event_windows.py \\
        --year 2017 \\
        --input-dir "/path/to/tse_tick_data" \\
        --output-dir "/path/to/event_windows" \\
        --filter-csv "TSE_EventBase/data/exports/event_filter_list.csv"

Input layout expected under --input-dir::

    {input_dir}/{year}/{yearmonth}/HTICST120.{date}.N.zip

Output layout written to --output-dir::

    {output_dir}/year=YYYY/month=MM/YYYYMMDD.parquet

A ``corrupt_zips.txt`` log is written to --output-dir alongside the data.
"""

import argparse
import logging
import sys
from pathlib import Path

# Allow running as a standalone script from any working directory
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tse_tick.ingest import ingest_event_windows


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Extract ±window-minutes tick windows around corporate disclosure "
            "events from raw TICST120 ZIPs into partitioned Parquet files."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--year",
        type=int,
        required=True,
        help="Calendar year of the disclosure events to process (e.g. 2017).",
    )
    p.add_argument(
        "--input-dir",
        required=True,
        metavar="DIR",
        help=(
            "Root directory containing the raw TICST120 ZIPs, organised as "
            "{input_dir}/{year}/{yearmonth}/HTICST120.{date}.N.zip."
        ),
    )
    p.add_argument(
        "--output-dir",
        required=True,
        metavar="DIR",
        help=(
            "Root directory for event-window Parquet output. "
            "Files are written to {output_dir}/year=YYYY/month=MM/YYYYMMDD.parquet."
        ),
    )
    p.add_argument(
        "--filter-csv",
        required=True,
        metavar="CSV",
        help=(
            "Path to the event filter list CSV. "
            "Required columns: ticker, event_date, event_time, event_type, "
            "headline, session_type, reaction_anchor_dt, zip_date."
        ),
    )
    p.add_argument(
        "--window-minutes",
        type=int,
        default=120,
        metavar="N",
        help="Half-width of the extraction window in minutes (±N minutes around the anchor).",
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity level.",
    )
    return p


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    print(
        f"Starting event-window extraction:\n"
        f"  year         : {args.year}\n"
        f"  input-dir    : {args.input_dir}\n"
        f"  output-dir   : {args.output_dir}\n"
        f"  filter-csv   : {args.filter_csv}\n"
        f"  window       : ±{args.window_minutes} minutes\n"
    )

    ingest_event_windows(
        year=args.year,
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        filter_csv=args.filter_csv,
        window_minutes=args.window_minutes,
    )

    print("Done.")


if __name__ == "__main__":
    main()
