#!/usr/bin/env python3
# tse_tick/cli.py
import argparse
import logging
import sys

from tse_tick.ingest import ingest_directory, ingest_year_from_root, ingest_period, ingest_event_windows_period
from tse_tick.constants import DATA_TYPES, VALID_DATA_TYPES

logger = logging.getLogger(__name__)


def _parse_years(years_str: str) -> list[int]:
    result: list[int] = []
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
    result: list[int] = []
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


def _parse_tickers(tickers_str: str) -> set[str]:
    ticker_str = tickers_str.strip()
    if ticker_str.startswith("@"):
        filepath = ticker_str[1:]
        with open(filepath, "r") as f:
            return {line.strip() for line in f if line.strip()}
    return {t.strip() for t in ticker_str.split(",") if t.strip()}


def cmd_ingest(args: argparse.Namespace) -> None:
    if args.data_type not in VALID_DATA_TYPES:
        print(f"Error: --data-type must be one of {sorted(VALID_DATA_TYPES)}", file=sys.stderr)
        sys.exit(1)

    input_root = args.input_root
    output_root = args.output_root

    if args.filter_csv and args.data_type != "individual_stock":
        print("Error: --filter-csv is only supported with --data-type individual_stock", file=sys.stderr)
        sys.exit(1)

    if args.parallel and args.parallel > 1 and args.filter_csv:
        logger.warning(
            "--parallel does not apply to the event-window (--filter-csv) ingest; it runs sequentially"
        )

    if args.period is not None:
        mode_str = "full"
        if args.filter_csv:
            mode_str = "event-window"
        elif args.tickers:
            mode_str = f"ticker-filter ({len(_parse_tickers(args.tickers))} tickers)"
        print(f"Ingesting by period: {args.period}  [{mode_str}]")
        print(f"  Data type: {args.data_type}")
        print(f"  Output: {output_root}")

        if args.filter_csv:
            ingest_event_windows_period(
                input_root, output_root,
                period=args.period,
                filter_csv=args.filter_csv,
                window_minutes=args.window,
                resume=not args.no_resume,
                max_workers=args.parallel,
            )
            print("Done")
        else:
            ticker_filter = _parse_tickers(args.tickers) if args.tickers else None
            results = ingest_period(
                input_root, output_root,
                period=args.period,
                data_type=args.data_type,
                language=args.language,
                resume=not args.no_resume,
                max_workers=args.parallel,
                ticker_filter=ticker_filter,
            )
            success = sum(1 for r in results if "error" not in r)
            failed = sum(1 for r in results if "error" in r)
            print(f"Done: {success} succeeded, {failed} failed")
        return

    years = _parse_years(args.years) if args.years else [args.year] if args.year else None
    if years is None:
        print("Error: --years, --year, or --period is required", file=sys.stderr)
        sys.exit(1)

    ticker_filter = _parse_tickers(args.tickers) if args.tickers else None

    if args.flat:
        print(f"Ingesting all ZIPs from flat directory: {input_root}")
        print(f"  Data type: {args.data_type}")
        print(f"  Output: {output_root}")
        results = ingest_directory(
            input_root, output_root,
            data_type=args.data_type,
            language=args.language,
            max_workers=args.parallel,
            ticker_filter=ticker_filter,
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
                max_workers=args.parallel,
                ticker_filter=ticker_filter,
            )
            success = sum(1 for r in results if "error" not in r)
            failed = sum(1 for r in results if "error" in r)
            print(f"Year {year}: {success} succeeded, {failed} failed")


def cmd_export(args: argparse.Namespace) -> None:
    import tse_tick

    if args.data_type not in VALID_DATA_TYPES:
        print(f"Error: --data-type must be one of {sorted(VALID_DATA_TYPES)}", file=sys.stderr)
        sys.exit(1)

    ticker_filter = _parse_tickers(args.tickers) if args.tickers else None

    store = getattr(args, "store", None)
    if store and args.data_type == "individual_stock" and ticker_filter and len(ticker_filter) == 1:
        # Two-stage: build a reusable, part-pruned Parquet store then query it. The
        # store is left on disk for fast (sub-second) repeat queries.
        ticker = next(iter(ticker_filter))
        print(f"Building part-pruned store at {store}, then querying {ticker} (two-stage)...")
        df = tse_tick.extract_to_store(
            args.input_root,
            store,
            args.period,
            ticker,
            data_type=args.data_type,
            start_time=args.start_time,
            end_time=args.end_time,
            language=args.language,
        )
    else:
        if store:
            print("Note: --store supports a single individual_stock ticker; "
                  "doing a direct read instead.", file=sys.stderr)
        # One-shot direct read. For individual_stock + a ticker filter this is
        # automatically part-pruned (opens only the ticker's parts), so it's fast.
        print(f"Reading {args.data_type} from {args.input_root} (part-pruned raw scan)...")
        df = tse_tick.read_ticks(
            args.input_root,
            data_type=args.data_type,
            ticker_filter=ticker_filter,
            date=args.period,
            start_time=args.start_time,
            end_time=args.end_time,
            language=args.language,
        )
    if args.output.lower().endswith(".parquet"):
        df.write_parquet(args.output)
    else:
        df.write_csv(args.output)
    print(f"Wrote {df.height} rows x {df.width} cols -> {args.output}")


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
        choices=list(DATA_TYPES),
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
        "--period",
        type=str,
        default=None,
        help=(
            "Date range to process: YYYY (entire year), "
            "YYYYMM-YYYYMM (month range), or YYYYMMDD-YYYYMMDD (day range). "
            "Takes precedence over --years/--year."
        ),
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
        help="Parallel worker processes for per-date ingest (default: 1, serial; "
             "capped at the machine's logical core count). Applies to --period and "
             "--year (structured root) and --flat; not to --filter-csv event windows.",
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
    ingest_parser.add_argument(
        "--tickers",
        type=str,
        default=None,
        help=(
            "Comma-separated ticker codes to keep, or @file.txt with one per line. "
            "When provided, only these tickers are included in output."
        ),
    )
    ingest_parser.add_argument(
        "--filter-csv",
        type=str,
        default=None,
        help="Path to event filter CSV (enables event-window mode). Overrides --tickers.",
    )
    ingest_parser.add_argument(
        "--window",
        type=int,
        default=120,
        help="Window in minutes around each event (default: 120). Only used with --filter-csv.",
    )

    export_parser = subparsers.add_parser(
        "export",
        help="Read raw ZIPs and export a ticker/time slice to CSV or Parquet (no store needed)",
    )
    export_parser.add_argument(
        "--data-type", required=True,
        choices=list(DATA_TYPES),
        help="Type of NEEDS data to read",
    )
    export_parser.add_argument(
        "--input-root", required=True,
        help="Folder containing the NEEDS ZIPs. Any nesting works — files are located by type + date "
             "(e.g. point at G:/NEEDS even for 個別株式{year}/TICST120/{yyyymm}/).",
    )
    export_parser.add_argument(
        "--output", required=True,
        help="Output file path; format is chosen by extension (.csv or .parquet).",
    )
    export_parser.add_argument(
        "--tickers", default=None,
        help="Comma-separated codes (or @file.txt) to keep; omit for all tickers.",
    )
    export_parser.add_argument(
        "--period", default=None,
        help="YYYYMMDD-YYYYMMDD day range, YYYYMM month, YYYY year, or a single YYYYMMDD.",
    )
    export_parser.add_argument(
        "--start-time", default=None, help="HH:MM:SS lower bound (tick types only)",
    )
    export_parser.add_argument(
        "--end-time", default=None, help="HH:MM:SS upper bound (tick types only)",
    )
    export_parser.add_argument(
        "--language", default="en", choices=["en", "jp"],
        help="Column name language (default: en)",
    )
    export_parser.add_argument(
        "--store", default=None,
        help="Optional Parquet store dir. If given for a single individual_stock "
             "ticker, build a reusable, part-pruned store here then query it "
             "(two-stage) — best when you will read the data more than once. Omit "
             "for a one-off direct read (also part-pruned). Requires the [query] extra.",
    )
    export_parser.add_argument(
        "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity level",
    )

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    # Route CLI progress to stdout (not stderr) so it doesn't surface as red
    # NativeCommandError lines under PowerShell.
    level = getattr(logging, getattr(args, "log_level", "INFO"), logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", "%Y-%m-%d %H:%M:%S")
    )
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)

    if args.command == "ingest":
        cmd_ingest(args)
    elif args.command == "export":
        cmd_export(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
