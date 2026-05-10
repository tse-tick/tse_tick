#!/usr/bin/env python3
"""
DEPRECATED: Use 'tse-tick ingest' CLI instead.

CLI: extract ±N minute tick windows around corporate disclosure events.

Usage::

    python scripts/ingest_event_windows.py \\
        --period 2017 \\
        --input-root "/path/to/tse_tick_data" \\
        --output-root "/path/to/event_windows" \\
        --filter-csv "TSE_EventBase/data/exports/event_filter_list.csv"

Input layout expected under --input-root::

    {input_root}/{year}/{yearmonth}/HTICST120.{date}.N.zip

Output layout written to --output-root::

    {output_root}/year=YYYY/month=MM/YYYYMMDD.parquet

A ``corrupt_zips.txt`` log is written to --output-root alongside the data.
"""

import argparse
import logging
import sys
import warnings
from pathlib import Path

# Allow running as a standalone script from any working directory
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tse_tick.ingest import ingest_event_windows_period


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Extract ±window-minutes tick windows around corporate disclosure "
            "events from raw TICST120 ZIPs into partitioned Parquet files."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--period",
        type=str,
        required=True,
        help="Date range to process: YYYY, YYYYMM-YYYYMM, or YYYYMMDD-YYYYMMDD.",
    )
    p.add_argument(
        "--input-root",
        required=True,
        metavar="DIR",
        help=(
            "Root directory containing the raw TICST120 ZIPs, organised as "
            "{input_root}/{year}/{yearmonth}/HTICST120.{date}.N.zip."
        ),
    )
    p.add_argument(
        "--output-root",
        required=True,
        metavar="DIR",
        help=(
            "Root directory for event-window Parquet output. "
            "Files are written to {output_root}/year=YYYY/month=MM/YYYYMMDD.parquet."
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
    warnings.warn(
        "scripts/ingest_event_windows.py is deprecated. "
        "Use the CLI: `tse-tick ingest --filter-csv ... --window ...` instead. "
        "This script will be removed in a future release.",
        DeprecationWarning,
        stacklevel=2,
    )

    parser = _build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    print(
        f"Starting event-window extraction:\n"
        f"  period       : {args.period}\n"
        f"  input-root   : {args.input_root}\n"
        f"  output-root  : {args.output_root}\n"
        f"  filter-csv   : {args.filter_csv}\n"
        f"  window       : ±{args.window_minutes} minutes\n"
    )

    ingest_event_windows_period(
        input_root=args.input_root,
        output_dir=args.output_root,
        period=args.period,
        filter_csv=args.filter_csv,
        window_minutes=args.window_minutes,
    )

    print("Done.")


if __name__ == "__main__":
    main()
