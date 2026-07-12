# tse_tick/enhanced.py
import datetime
import gc
import re
import zipfile
import io
import glob
import logging
import warnings
from collections import defaultdict
from itertools import groupby
from pathlib import Path
from typing import Optional, Literal, Tuple, List, Dict, Union

import polars as pl

from .core import clean_data, parse_line, _tick_datetime_expr
from .constants import INDEX_TYPES, SUMMARY_TYPES, validate_data_type
from .schemas import (
    get_schema_individual_stock_95,
    get_schema_summary_83,
    get_schema_indices_15,
    get_schema_indices_23,
    get_japanese_column_mapping,
)
from .partscan import select_parts_for_day

logger = logging.getLogger(__name__)


class NoDataWarning(UserWarning):
    """Warned when a read resolves to zero rows (a typed-empty frame is returned).

    Emitted by :func:`read_ticks` whenever nothing matches — a non-trading day
    (e.g. a holiday), a ticker/index code not present, or filters that exclude
    every row — for all four data types alike. Being a ``UserWarning`` subclass
    it is capturable with ``warnings.catch_warnings(record=True)`` and silenceable
    with ``warnings.filterwarnings("ignore", category=tse_tick.NoDataWarning)``.
    """


def _warn_no_data(message: str) -> None:
    # stacklevel=3: report the user's read_ticks(...) call site, not this helper
    # or its internal caller.
    warnings.warn(message, NoDataWarning, stacklevel=3)


class TruncationWarning(UserWarning):
    """Warned when :func:`read_ticks` hits its ``rows`` cap and truncates output.

    A ``UserWarning`` subclass so it surfaces through the same ``warnings``
    channel as :class:`NoDataWarning` (capturable with
    ``warnings.catch_warnings()``, silenceable / escalatable by category) rather
    than via ``logging`` — hitting the cap means "build a Parquet store and use
    :func:`tse_tick.query_ticks` instead".
    """


class LargeResultWarning(UserWarning):
    """Warned when :func:`extract_to_store` is about to materialize a very large
    result (past ~10M rows) as one in-memory DataFrame.

    ``extract_to_store`` deliberately returns **all** matching rows (no cap — a
    whole month of an active ticker exceeds ``query_ticks``'s default 10M), so a
    multi-year period can be tens of GB in RAM. The store is already built by the
    time this warns; the memory-safe pattern is to ignore the returned frame and
    read the store in bounded slices with :func:`tse_tick.query_ticks` (per day /
    per month). Capturable with ``warnings.catch_warnings()``, silenceable with
    ``warnings.filterwarnings("ignore", category=tse_tick.LargeResultWarning)``.
    """


class PartialIngestWarning(UserWarning):
    """Warned when :func:`extract_to_store`'s Stage-1 ingest lost data on the way
    to the store it is about to query — a corrupt/unreadable ZIP part or a whole
    failed date — so the returned DataFrame may be missing those rows.

    The affected dates are named in the message. Days that lost parts are left
    resume-eligible (no coverage marker / an incomplete one), so re-running the
    same call after fixing the raw files re-ingests exactly those days.
    Capturable with ``warnings.catch_warnings()``, silenceable with
    ``warnings.filterwarnings("ignore", category=tse_tick.PartialIngestWarning)``.
    """


class OneShotMemoryError(MemoryError):
    """Raised when a one-shot read (:func:`create_df` / :func:`read_ticks`) would
    exhaust memory — either the cumulative decompressed size crossed the ceiling,
    or a Polars load panicked (an uncatchable ``BaseException``) and was converted.

    Subclasses :class:`MemoryError`, so callers can ``except MemoryError`` (or this
    type) to catch it and fall back to the two-stage ingest+query path. The
    ``ingest_*`` functions deliberately **re-raise** it rather than swallowing it
    via their broad ``except Exception`` handlers, so a too-large read aborts
    loudly instead of silently persisting a partial day.
    """


class SuspiciousZipError(ValueError):
    """Raised when a ZIP trips one of the zip-bomb guards: too many members, an
    implausible compression ratio, or an oversized decompressed member.

    Subclasses :class:`ValueError` (what these guards historically raised) but is
    re-raised past :func:`get_1y_dataframe`'s per-ZIP skip-and-continue handler —
    the generic ``except Exception`` used to catch the guards' own errors two
    lines below where they were raised, silently turning "abort a suspicious ZIP"
    into "skip it and keep going" (audit finding M1).
    """


_MAX_DECOMPRESSED_BYTES = 5 * 1024 * 1024 * 1024
_MAX_ZIP_ENTRIES = 5

# Cumulative decompressed-size ceiling for the one-shot (create_df) path. The
# per-entry _MAX_DECOMPRESSED_BYTES guard above is checked per ZIP *member*, so it
# can't see memory adding up across the many numbered parts of one trading day (a
# normal individual_stock day is ~9 parts / tens of millions of rows). When the
# running total of decompressed bytes crosses this ceiling we raise a catchable
# MemoryError *before* the load rather than letting Polars panic uncatchably
# (PanicException subclasses BaseException). Default 5 GB; override by setting
# tse_tick.enhanced._MAX_ONESHOT_DECOMPRESSED_BYTES.
_MAX_ONESHOT_DECOMPRESSED_BYTES = 5 * 1024 * 1024 * 1024

# Shared tail for the one-shot OOM guidance: the two-stage escape hatch.
_TWO_STAGE_GUIDANCE = (
    "Use the two-stage path instead: ingest_single_zip() then query_ticks()."
)

# Sentinel for the create_df/read_ticks ``max_oneshot_bytes`` default: resolved to
# _MAX_ONESHOT_DECOMPRESSED_BYTES at call time (so the module constant stays
# monkeypatchable), while an explicit ``None`` disables the ceiling entirely.
_DEFAULT_ONESHOT = object()


def _resolve_oneshot_bytes(max_oneshot_bytes):
    """Resolve ``max_oneshot_bytes``: the sentinel -> the module default; otherwise
    the value as given (an ``int`` ceiling, or ``None`` to disable the guard)."""
    if max_oneshot_bytes is _DEFAULT_ONESHOT:
        return _MAX_ONESHOT_DECOMPRESSED_BYTES
    return max_oneshot_bytes


def _oneshot_limit_message(total_bytes: int, limit_bytes: int) -> str:
    return (
        f"Estimated decompressed size ({total_bytes / 1024**3:.1f} GB) exceeds the "
        f"{limit_bytes / 1024**3:.0f} GB one-shot limit. {_TWO_STAGE_GUIDANCE}"
    )


_CODE_TYPE_MAP = {
    "individual_stock": "HTICST120",
    "stock_summary": "HTICSS110",
    "indices": "HTICIT110",
    "indices_summary": "HTICIS110",
}


def _expand_date_range(from_date: str, to_date: str) -> List[str]:
    start = datetime.datetime.strptime(from_date, "%Y%m%d")
    end = datetime.datetime.strptime(to_date, "%Y%m%d")
    if start > end:
        raise ValueError(f"Start date {from_date} is after end date {to_date}")
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y%m%d"))
        current += datetime.timedelta(days=1)
    return dates


def _expand_month_range(from_str: str, to_str: str) -> List[Tuple[int, int]]:
    from_year, from_month = int(from_str[:4]), int(from_str[4:6])
    to_year, to_month = int(to_str[:4]), int(to_str[4:6])
    if (from_year, from_month) > (to_year, to_month):
        raise ValueError(f"Start month {from_str} is after end month {to_str}")
    months = []
    year, month = from_year, from_month
    while (year < to_year) or (year == to_year and month <= to_month):
        months.append((year, month))
        month += 1
        if month > 12:
            month = 1
            year += 1
    return months


# Single source of truth for the accepted period forms, so every parse_period
# error message lists the same complete set (incl. the bare YYYYMM / YYYYMMDD
# forms the code accepts and read_ticks documents).
_PERIOD_FORMATS_HELP = (
    "Expected YYYY, a single YYYYMM or YYYYMMDD, or a "
    "YYYY-YYYY / YYYYMM-YYYYMM / YYYYMMDD-YYYYMMDD range"
)


def parse_period(period_str: str) -> Dict[str, Union[str, List[int], Dict[int, List[int]], List[str]]]:
    """Parse a period string into structured parameters.

    Accepted formats:
        YYYY                         -> entire year
        YYYYMM                       -> a single month
        YYYYMMDD                     -> a single trading day
        YYYY-YYYY                    -> all years from start year to end year
        YYYYMM-YYYYMM                -> all trading days from start month to end month
        YYYYMMDD-YYYYMMDD            -> all trading days from start date to end date

    The single ``YYYYMM`` and ``YYYYMMDD`` forms are the same ones
    ``read_ticks(date=…)`` accepts — a lone month or day needs no ``start-end``
    range.

    Returns dict with keys:
        granularity: "year" | "month" | "date"
        years: list of years covered
        months_by_year (month): {year: [month, ...]}
        dates (date): list of YYYYMMDD strings
    """
    period_str = str(period_str).strip()

    if "-" in period_str:
        parts = period_str.split("-")
        if len(parts) != 2:
            raise ValueError(f"Invalid period format: {period_str!r}. {_PERIOD_FORMATS_HELP}")
        from_part, to_part = parts[0].strip(), parts[1].strip()

        if len(from_part) == 8 and len(to_part) == 8 and from_part.isdigit() and to_part.isdigit():
            dates = _expand_date_range(from_part, to_part)
            years = sorted(set(int(d[:4]) for d in dates))
            return {"granularity": "date", "years": years, "dates": dates}

        elif len(from_part) == 6 and len(to_part) == 6 and from_part.isdigit() and to_part.isdigit():
            months = _expand_month_range(from_part, to_part)
            months_by_year: Dict[int, List[int]] = defaultdict(list)
            for y, m in months:
                months_by_year[y].append(m)
            years = sorted(months_by_year.keys())
            return {"granularity": "month", "years": years, "months_by_year": dict(months_by_year)}

        elif len(from_part) == 4 and len(to_part) == 4 and from_part.isdigit() and to_part.isdigit():
            from_year, to_year = int(from_part), int(to_part)
            if from_year > to_year:
                raise ValueError(f"Start year {from_part} is after end year {to_part}")
            return {"granularity": "year", "years": list(range(from_year, to_year + 1))}

        else:
            raise ValueError(f"Invalid period format: {period_str!r}. {_PERIOD_FORMATS_HELP}")
    else:
        if len(period_str) == 4 and period_str.isdigit():
            return {"granularity": "year", "years": [int(period_str)]}
        elif len(period_str) == 6 and period_str.isdigit():
            y, m = int(period_str[:4]), int(period_str[4:6])
            return {"granularity": "month", "years": [y], "months_by_year": {y: [m]}}
        elif len(period_str) == 8 and period_str.isdigit():
            return {"granularity": "date", "years": [int(period_str[:4])], "dates": [period_str]}
        else:
            raise ValueError(f"Invalid period: {period_str!r}. {_PERIOD_FORMATS_HELP}")


def _detect_year_from_path(folder_path: str) -> Optional[int]:
    """The first ``20xx`` year token found in the path parts, or ``None``."""
    for part in Path(folder_path).parts:
        match = re.search(r"(20\d{2})", part)
        if match:
            return int(match.group(1))
    return None


def _require_year_from_path(folder_path: str) -> int:
    """The year detected from the path, or raise the canonical 'could not detect'
    error. Single source for the message shared by :func:`create_df` and
    :func:`detect_data_type_and_year`."""
    year = _detect_year_from_path(folder_path)
    if year is None:
        raise ValueError(f"Could not detect year from path: {folder_path}")
    return year


def _detect_data_type_from_path(folder_path: str) -> str:
    """Detect the NEEDS ``data_type`` from path keywords, or a sample filename.

    Raises ``ValueError`` (with the same messages :func:`detect_data_type_and_year`
    has always used) when the type can't be determined.
    """
    path = Path(folder_path)
    path_str = str(path).lower()

    if any(kw in path_str for kw in ["individual_stock", "ticst", "stock_tick"]):
        return "individual_stock"
    elif any(kw in path_str for kw in ["stock_summary", "ticss", "stock_daily"]):
        return "stock_summary"
    elif any(kw in path_str for kw in ["indices_tick", "ticit", "index_tick"]) and "summary" not in path_str:
        return "indices"
    elif any(kw in path_str for kw in ["indices_summary", "ticis", "index_daily", "index_summary"]):
        return "indices_summary"

    if path.exists() and path.is_dir():
        files = list(path.glob("*.zip")) + list(path.glob("*.csv"))
        if files:
            sample_file = files[0].name.upper()
            if "TICST" in sample_file:
                return "individual_stock"
            elif "TICSS" in sample_file:
                return "stock_summary"
            elif "TICIT" in sample_file:
                return "indices"
            elif "TICIS" in sample_file:
                return "indices_summary"
            raise ValueError(f"Could not detect data type from files in: {folder_path}")
        raise ValueError(f"No ZIP or CSV files found in: {folder_path}")
    raise ValueError(f"Could not detect data type from path: {folder_path}")


def detect_data_type_and_year(folder_path: str) -> Tuple[str, int]:
    """Detect both the NEEDS ``data_type`` and ``year`` from a path.

    Year comes from a ``20xx`` token in the path; data type from path keywords or
    a sample filename. Raises ``ValueError`` when either can't be determined (year
    is reported first, preserving the original behavior). :func:`create_df` calls
    the two underlying detectors independently so an explicitly-passed
    ``year=``/``data_type=`` is honored without forcing the other to be detectable.
    """
    year = _require_year_from_path(folder_path)
    data_type = _detect_data_type_from_path(folder_path)
    return data_type, year


def _zip_date_token(name: str) -> Optional[str]:
    """The YYYYMMDD (daily) or YYYYMM (monthly) date token in a NEEDS filename."""
    for part in name.split("."):
        if part.isdigit() and part.startswith("20") and len(part) in (6, 8):
            return part
    return None


def _zip_sort_key(path) -> Tuple[str, int]:
    """Chronological natural sort key: (date token, part number) from the filename.

    Sorts ``…20240104.1.zip``, ``…20240104.2.zip``, ``…20240104.10.zip`` in numeric
    order rather than lexical (1, 10, 2, …).
    """
    parts = Path(path).name.split(".")
    date_tok, part_num = "", 0
    for i, p in enumerate(parts):
        if p.isdigit() and p.startswith("20") and len(p) in (6, 8):
            date_tok = p
            if i + 1 < len(parts) and parts[i + 1].isdigit():
                part_num = int(parts[i + 1])
            break
    return (date_tok, part_num)


def _prune_parts_by_ticker(zips: List[Path], ticker_filter: set) -> List[Path]:
    """Restrict a (possibly multi-day) TICST120 zip list to the parts that hold
    ``ticker_filter``, per day.

    A high-volume code straddles a contiguous run of consecutive parts, so this
    delegates to :func:`tse_tick.partscan.select_parts_for_day` (probe + backward
    run scan). Keeps ALL of a day's parts whenever pruning can't be confirmed
    (``select`` returns ``None``), so results never change — only the I/O shrinks.
    """
    out: List[Path] = []
    ordered = sorted(zips, key=_zip_sort_key)
    for _day, grp in groupby(ordered, key=lambda p: _zip_sort_key(p)[0]):
        parts = list(grp)
        chosen = select_parts_for_day(parts, ticker_filter)
        out.extend(parts if chosen is None else chosen)
    return out


def _discover_zips_recursive(
    root: Path,
    prefixes: Union[str, List[str]],
    years: List[int],
    months: Optional[List[int]],
    dates: Optional[List[str]],
) -> List[Path]:
    """Fallback discovery: recursively find ``<PREFIX>.*.zip`` anywhere under
    ``root`` (robust to nested delivery trees such as
    ``個別株式{year}/TICST120/{yyyymm}/`` and the 2016 ``…010`` index code),
    filtered by the requested year/month/date tokens parsed from each filename.
    """
    if isinstance(prefixes, str):  # tolerate a single prefix
        prefixes = [prefixes]
    year_set = {str(y) for y in years}
    month_set = {f"{m:02d}" for m in (months if months is not None else range(1, 13))}
    date_set = set(dates) if dates else None
    out: List[Path] = []
    for prefix in prefixes:
        for p in glob.glob(str(root / "**" / f"{prefix}.*.zip"), recursive=True):
            tok = _zip_date_token(Path(p).name)
            if tok is None or tok[:4] not in year_set or tok[4:6] not in month_set:
                continue
            # Daily files must match a requested date; monthly files match by month.
            if date_set is not None and len(tok) == 8 and tok not in date_set:
                continue
            out.append(Path(p))
    return sorted(out)


def discover_zips(
    input_root: str,
    data_type: str,
    years: List[int],
    months: Optional[List[int]] = None,
    dates: Optional[List[str]] = None,
) -> List[Path]:
    """Find NEEDS ZIPs for the requested year/month/date under ``input_root``.

    Tries two fast-path layouts — the documented ``{year}/{yearmonth}/`` tree and
    a ``{yearmonth}/`` directory directly under ``input_root`` (e.g. when it
    already points at a ``.../TICST120`` type folder) — then, if neither matched,
    falls back to a recursive search that handles deeper real-world delivery
    trees such as ``個別株式{year}/TICST120/{yyyymm}/``. Uses the data type's
    record prefix (e.g. ``HTICST120`` for ``individual_stock``).

    Args:
        input_root: Root of (or a folder above) the NEEDS delivery tree.
        data_type: One of the four NEEDS types (selects the filename prefix).
        years: Years to scan.
        months: Months (1-12) to scan; ``None`` means all twelve.
        dates: Restrict to these exact ``"YYYYMMDD"`` days; ``None`` for all.

    Returns:
        A sorted list of matching ZIP paths (``pathlib.Path``).
    """
    prefix = _CODE_TYPE_MAP.get(data_type)
    if prefix is None:
        raise ValueError(
            f"Unknown data_type {data_type!r}. Must be one of {list(_CODE_TYPE_MAP.keys())}"
        )
    # Index types changed record code across eras: 2017+ uses …110, 2016 uses
    # …010 (HTICIT010 / HTICIS010). Search both so 2016 index data is reachable.
    prefixes = [prefix]
    if data_type in INDEX_TYPES:
        prefixes.append(prefix[:-3] + "010")

    if months is None:
        months = list(range(1, 13))

    root = Path(input_root)
    all_zips: List[Path] = []

    for year in years:
        for month in months:
            month_str = f"{year}{month:02d}"
            # Two fast-path layouts: the documented {year}/{yearmonth}/ tree, and
            # a {yearmonth}/ folder directly under root (e.g. input_root already
            # points at a .../TICST120 type folder, as in G:\NEEDS\個別株式2023\TICST120).
            subdirs = [Path(str(year)) / month_str, Path(month_str)]
            targets = [d for d in dates if d[:6] == month_str] if dates is not None else [None]
            for sub in subdirs:
                for date_str in targets:
                    for pfx in prefixes:
                        fname = f"{pfx}.{date_str}.*.zip" if date_str else f"{pfx}.*.zip"
                        all_zips.extend(Path(p) for p in glob.glob(str(root / sub / fname)))

    # The fast paths cover the common deliveries; if none matched, fall back to a
    # full recursive search so deeper nested trees still work
    # (e.g. 個別株式{year}/TICST120/{yyyymm}/).
    if not all_zips:
        all_zips = _discover_zips_recursive(root, prefixes, years, months, dates)

    # Dedupe (a file can match more than one fast-path subdir) and natural-sort.
    seen: set = set()
    unique: List[Path] = []
    for z in all_zips:
        if str(z) not in seen:
            seen.add(str(z))
            unique.append(z)
    return sorted(unique, key=_zip_sort_key)


def _raw_width(kind: str, year: int) -> int:
    """Expected raw (pre-clean) column count for a NEEDS data type/era."""
    if kind == "indices":
        return 15 if year == 2016 else 23
    if kind in SUMMARY_TYPES:
        return 83
    return 95  # individual_stock


def _guard_polars_oom(load):
    """Run a Polars load, converting an *uncatchable* panic into a catchable error.

    A failed allocation on a huge one-shot read surfaces as a Polars
    ``PanicException``, which subclasses ``BaseException`` (not ``Exception``) — so
    ordinary ``except Exception`` can't catch it and it tears the caller down. Run
    ``load`` and convert any such non-``Exception`` ``BaseException`` into a
    ``MemoryError`` carrying the two-stage guidance; ordinary exceptions and real
    interrupts (``KeyboardInterrupt`` / ``SystemExit``) pass through untouched.
    """
    try:
        return load()
    except (Exception, KeyboardInterrupt, SystemExit):
        raise
    except BaseException as exc:  # PanicException & co. — not an Exception subclass
        raise OneShotMemoryError(
            f"A Polars load panicked, most often from running out of memory "
            f"(underlying error: {type(exc).__name__}: {exc}). {_TWO_STAGE_GUIDANCE}"
        ) from exc


# Field-5 (stock code) vectorized filter for the individual_stock ticker fast path.
# ``bytes.strip()`` in ``extract_stock_code`` strips exactly this ASCII set, so the
# Polars ``strip_chars`` below agrees with it byte-for-byte (Unicode ``strip`` would
# also drop e.g. U+00A0, which ``bytes.strip`` does not).
_FIELD5_WS = " \t\n\r\x0b\x0c"
# One decompressed block held at a time — keeps peak RAM proportional to the matched
# rows plus a bounded block, never the whole part (issue #38 memory constraint).
_FILTER_BLOCK_BYTES = 16 * 1024 * 1024


def _field5_codes(lines: "pl.Series") -> "pl.Series":
    """Vectorized field-5 stock code for a Series of raw TICST120 lines.

    The Polars equivalent of :func:`tse_tick.partscan.extract_stock_code` applied
    per line, matching it byte-for-byte on every input: split on the ``","`` field
    delimiter, take field index 5, then read only up to the next ``"`` (so a field-5
    that is the record's terminal field, or contains an embedded ``"``, parses the
    same as ``extract_stock_code``'s ``find('"')`` — not reachable in a real 95-field
    TICST120 record, but kept exact), then ``.strip()[:4]``. An empty/absent field-5
    is mapped to ``null`` — exactly as ``extract_stock_code`` returns ``None`` — so
    such a line is never kept, even for a degenerate ``ticker_filter`` containing
    ``""``. Pinned by ``tests/test_field5_filter.py``.
    """
    return (
        lines.str.split('","')
        .list.get(5, null_on_oob=True)
        .str.split('"')
        .list.get(0, null_on_oob=True)
        .str.strip_chars(_FIELD5_WS)
        .str.slice(0, 4)
        .replace("", None)
    )


def _append_field5_matches(chunk: bytes, tickers: list, out: bytearray) -> None:
    """Append every line of ``chunk`` (bytes, ``\\n``-separated) whose field-5 code
    is in ``tickers`` — each with a trailing ``\\n`` — to ``out``, in order."""
    # latin-1 is a lossless byte<->codepoint bijection, so arbitrary record bytes
    # round-trip and Polars string ops apply without any UTF-8 decode risk. Splitting
    # on "\n" reproduces the byte-loop's line boundaries exactly (a trailing "" from a
    # final "\n" has a null code and is dropped, so it is never kept).
    lines = pl.Series("raw", chunk.decode("latin-1").split("\n"), dtype=pl.String)
    matched = lines.filter(_field5_codes(lines).is_in(tickers))
    if matched.len():
        out += ("\n".join(matched.to_list()) + "\n").encode("latin-1")


def _read_individual_stock_matches(
    f, ticker_filter: set, block_bytes: int = _FILTER_BLOCK_BYTES
) -> bytes:
    """Vectorized, bounded-memory replacement for the per-line field-5 byte-loop.

    Streams the open part ``f`` in blocks, keeping only lines whose field-5 stock
    code is in ``ticker_filter``, and returns the kept lines concatenated as raw
    bytes ready for :func:`polars.read_csv`. The kept bytes are built incrementally
    per block, so peak memory stays proportional to the matched rows plus one block —
    a full decompressed part is never materialized (the
    ``pl.read_csv(whole_part).filter(...)`` trap the byte-loop deliberately avoids).
    Byte-identical kept-line set to the ``extract_stock_code`` loop it replaces
    (issue #38), at ~2x throughput per opened part.

    Reconstruction note: kept lines are re-joined with ``\\n`` and a trailing ``\\n``.
    For the near-universal ``\\n``/``\\r\\n``-terminated part this is byte-for-byte the
    original; a part whose final line lacks a terminator gains one trailing ``\\n``,
    which :func:`polars.read_csv` ignores — the parsed rows are unchanged.
    """
    tickers = list(ticker_filter)
    out = bytearray()
    tail = b""
    while True:
        block = f.read(block_bytes)
        if not block:
            break
        data = tail + block
        cut = data.rfind(b"\n")
        if cut == -1:
            # No newline yet: a single line longer than a block — carry it whole.
            tail = data
            continue
        complete, tail = data[: cut + 1], data[cut + 1:]
        _append_field5_matches(complete, tickers, out)
    if tail:
        _append_field5_matches(tail, tickers, out)
    return bytes(out)


def _read_zip_member(
    zf: "zipfile.ZipFile",
    file_name: str,
    year: int,
    kind: str,
    rows_to_read: Optional[int],
    ticker_filter: Optional[set],
    schema_override: dict,
) -> pl.DataFrame:
    """Parse one ZIP member into a raw string-typed frame (era/type dispatch).

    The member-parsing body of :func:`get_1y_dataframe`, factored out so the ZIP
    loop can read every file member of a ZIP rather than only ``namelist()[0]``
    (audit finding M4). ``rows_to_read`` caps the rows taken from this member;
    ``None`` reads it whole.
    """
    with zf.open(file_name) as f:
        if (year == 2016) and (kind == "indices_summary"):
            parsed_rows = []
            n_lines = 0
            for line in f:
                if rows_to_read is not None and n_lines >= rows_to_read:
                    break
                parsed_rows.append(parse_line(line))
                n_lines += 1
            return _guard_polars_oom(lambda: pl.DataFrame(parsed_rows))

        if (year == 2016) and (kind == "indices"):
            parsed_rows = []
            n_lines = 0
            for line in f:
                if rows_to_read is not None and n_lines >= rows_to_read:
                    break
                parsed_rows.append(parse_line(line, kind="indices"))
                n_lines += 1
            return _guard_polars_oom(lambda: pl.DataFrame(parsed_rows))

        if ticker_filter and kind == "individual_stock":
            # Vectorized field-5 filter (issue #38): byte-identical
            # kept-line set to the old ``extract_stock_code`` per-line
            # loop, ~2x faster per part, and still bounded-memory (only
            # matching lines are handed to Polars, never a full part).
            raw_bytes = _read_individual_stock_matches(f, ticker_filter)
            if not raw_bytes:
                return pl.DataFrame()
            df_chunk = _guard_polars_oom(
                lambda: pl.read_csv(
                    io.BytesIO(raw_bytes),
                    has_header=False,
                    schema_overrides=schema_override,
                    truncate_ragged_lines=True,
                )
            )
            if rows_to_read is not None:
                df_chunk = df_chunk.slice(0, rows_to_read)
            return df_chunk

        df_chunk = _guard_polars_oom(
            lambda: pl.read_csv(
                f,
                has_header=False,
                schema_overrides=schema_override,
                truncate_ragged_lines=True,
            )
        )
        if rows_to_read is not None:
            df_chunk = df_chunk.slice(0, rows_to_read)
        return df_chunk


def get_1y_dataframe(
    folder_path: str,
    year: int,
    kind: str,
    rows: Optional[int] = None,
    ticker_filter: Optional[set] = None,
    max_oneshot_bytes: Optional[int] = None,
) -> pl.DataFrame:
    path = Path(folder_path)

    if not path.exists():
        raise FileNotFoundError(f"Path does not exist: {folder_path}")

    if path.is_file() and path.suffix.lower() == ".zip":
        zip_files = [path]
    elif path.is_dir():
        zip_files = sorted(path.glob("*.zip"), key=_zip_sort_key)
    else:
        raise ValueError(
            f"Path must be either a directory containing ZIP files or a ZIP file: {folder_path}"
        )

    if not zip_files:
        raise FileNotFoundError(f"No ZIP files found in: {folder_path}")

    logger.debug("Found %d ZIP file(s) in %s", len(zip_files), folder_path)

    dfs = []
    total_rows_read = 0
    cumulative_decompressed = 0
    # The individual_stock ticker fast path keeps only matching lines (bounded
    # memory), so the cumulative size ceiling — meant for full-frame loads — must
    # not block it (alpha-review finding 4). ``None`` disables the guard entirely.
    guard_bytes = None if (ticker_filter and kind == "individual_stock") else max_oneshot_bytes

    schema_override = {f"column_{col+1}": pl.String for col in range(95)}

    for zip_file in zip_files:
        try:
            with zipfile.ZipFile(zip_file, "r") as zf:
                # Read EVERY file member, not just namelist()[0]: the entry cap
                # admits up to _MAX_ZIP_ENTRIES members, and only parsing the
                # first silently dropped the rest (audit finding M4). Directory
                # entries (trailing "/") carry no data and are skipped.
                member_names = [n for n in zf.namelist() if not n.endswith("/")]
                if len(member_names) > _MAX_ZIP_ENTRIES:
                    raise SuspiciousZipError(
                        f"ZIP has {len(member_names)} entries, max {_MAX_ZIP_ENTRIES}"
                    )
                if not member_names:
                    logger.warning("No file members in %s; skipping", zip_file)
                    continue
                for file_name in member_names:
                    info = zf.getinfo(file_name)
                    decompressed_size = info.file_size
                    compressed_size = info.compress_size
                    if compressed_size > 0 and decompressed_size / compressed_size > 100:
                        raise SuspiciousZipError(
                            f"Suspicious compression ratio ({decompressed_size / compressed_size:.0f}:1) "
                            f"in {zip_file}"
                        )
                    if decompressed_size > _MAX_DECOMPRESSED_BYTES:
                        raise SuspiciousZipError(
                            f"ZIP entry decompressed size ({decompressed_size:,} bytes) "
                            f"exceeds max ({_MAX_DECOMPRESSED_BYTES:,} bytes)"
                        )
                    # Cumulative across parts: the per-entry guard above can't see
                    # memory accumulating over a day's many numbered ZIPs. Stop with a
                    # clear, catchable error *before* the load rather than letting the
                    # concat OOM-panic uncatchably.
                    if guard_bytes is not None:
                        cumulative_decompressed += decompressed_size
                        if cumulative_decompressed > guard_bytes:
                            raise OneShotMemoryError(
                                _oneshot_limit_message(cumulative_decompressed, guard_bytes)
                            )
                    rows_to_read = None
                    if rows is not None:
                        remaining_rows = rows - total_rows_read
                        if remaining_rows <= 0:
                            break
                        rows_to_read = remaining_rows
                    df_chunk = _read_zip_member(
                        zf, file_name, year, kind, rows_to_read,
                        ticker_filter, schema_override,
                    )

                    if not df_chunk.is_empty():
                        dfs.append(df_chunk)
                        total_rows_read += len(df_chunk)

                if rows is not None and total_rows_read >= rows:
                    break

        except (zipfile.BadZipFile, EOFError, OneShotMemoryError, SuspiciousZipError):
            # OneShotMemoryError (the cumulative guard and converted Polars panics)
            # and the zip-bomb guards' SuspiciousZipError propagate; an incidental
            # plain MemoryError still falls through to the skip-and-continue below,
            # as it did before (alpha-review finding 11).
            raise
        except Exception as e:
            logger.warning("Error reading %s: %s", zip_file, e)
            continue

    if not dfs:
        if ticker_filter:
            # No matching rows: return a 0-row frame of the correct raw width so
            # the cleaning pipeline still yields a fully-typed empty result.
            return pl.DataFrame(
                schema={f"column_{i + 1}": pl.String for i in range(_raw_width(kind, year))}
            )
        raise ValueError("No data was successfully read")

    result = _guard_polars_oom(lambda: pl.concat(dfs, how="vertical"))
    logger.debug("Total rows read: %d", len(result))

    return result


def set_columns(df: pl.DataFrame, kind: str, language: Literal["en", "jp"] = "en") -> pl.DataFrame:
    if kind == "individual_stock":
        if len(df.columns) == 23:
            col_names_en = get_schema_indices_23()
        elif len(df.columns) == 95:
            col_names_en = get_schema_individual_stock_95()
        else:
            raise ValueError(f"Unexpected number of columns for {kind}: {len(df.columns)}")
    elif kind in SUMMARY_TYPES:
        if len(df.columns) == 83:
            col_names_en = get_schema_summary_83()
            if kind == "indices_summary":
                # The shared 83-column summary layout names column 5
                # "Stock Code", but for index summary data that field holds
                # the index identifier; get_final_columns() and the Parquet
                # partition key select it as "Index Code".
                col_names_en = [
                    "Index Code" if c == "Stock Code" else c for c in col_names_en
                ]
        else:
            raise ValueError(
                f"Unexpected number of columns for {kind}: {len(df.columns)}, expected 83"
            )
    elif kind == "indices":
        if len(df.columns) == 23:
            col_names_en = get_schema_indices_23()
        elif len(df.columns) == 15:
            col_names_en = get_schema_indices_15()
        else:
            raise ValueError(
                f"Unexpected number of columns for {kind}: {len(df.columns)}, expected 15 or 23"
            )
    else:
        raise ValueError(f"Unknown kind: {kind}")

    if len(df.columns) != len(col_names_en):
        raise ValueError(
            f"Column count mismatch: DataFrame has {len(df.columns)} columns but schema has {len(col_names_en)}"
        )

    rename_map = dict(zip(df.columns, col_names_en))
    df = df.rename(rename_map)

    if language == "jp":
        jp_mapping = get_japanese_column_mapping()
        col_names_jp = [jp_mapping.get(col, col) for col in col_names_en]
        rename_jp = dict(zip(col_names_en, col_names_jp))
        df = df.rename(rename_jp)

    return df


def get_final_columns(data_type):
    if data_type == "indices_summary":
        return [
            "Record Type", "Data Date", "Exchange Code", "Security Type", "Index Code",
            "AM Opening Price", "AM Opening Time", "AM High Price", "AM Low Price",
            "AM Close Price", "AM Close Time", "PM Opening Price", "PM Opening Time",
            "PM High Price", "PM Low Price", "PM Close Price", "PM Close Time",
        ]
    elif data_type == "indices":
        return [
            "Record Type", "Data Date", "Exchange Code", "Security Type", "Session",
            "Index Code", "Execution Time", "Index Value", "Execution Type", "Ayumi Flag",
        ]
    else:
        return [
            "Record Type", "Data Date", "Exchange Code", "Security Type", "Stock Code",
            "Trading Unit", "Issued Shares", "Executions ≤3 units",
            "Executions 3<x≤6 units", "Executions 6<x≤9 units", "Executions 9<x≤29 units",
            "Executions 29<x≤49 units", "Executions 49<x≤99 units", "Executions 99<x≤199 units",
            "Executions 199<x≤299 units", "AM Opening Price", "AM Opening Time",
            "AM Opening Volume", "AM High Price", "AM Low Price", "AM Close Price",
            "AM Close Time", "AM Close Volume", "AM UpTick Volume", "AM UpTick Amount",
            "AM UpTick Count", "AM DownTick Volume", "AM DownTick Amount", "AM DownTick Count",
            "AM Total Volume", "AM Total Amount", "AM Execution Count", "AM VWAP", "AM Std Dev",
            "AM Sell Quote Time", "AM Buy Quote Time", "AM Spread Time", "AM Avg Sell Quote Vol",
            "AM Avg Buy Quote Vol", "AM Avg Spread", "PM Opening Price", "PM Opening Time",
            "PM Opening Volume", "PM High Price", "PM Low Price", "PM Close Price",
            "PM Close Time", "PM Close Volume", "PM UpTick Volume", "PM UpTick Amount",
            "PM UpTick Count", "PM DownTick Volume", "PM DownTick Amount", "PM DownTick Count",
            "PM Total Volume", "PM Total Amount", "PM Execution Count", "PM VWAP", "PM Std Dev",
            "PM Sell Quote Time", "PM Buy Quote Time", "PM Spread Time", "PM Avg Sell Quote Vol",
            "PM Avg Buy Quote Vol", "PM Avg Spread", "Daily VWAP", "Daily Std Dev",
            "Daily Weighted Avg Sell Quote", "Daily Weighted Avg Buy Quote", "Daily Avg Spread",
            "AM Sell Quote Execution Vol", "AM Sell Quote Execution Amt",
            "AM Sell Quote Execution Cnt", "AM Buy Quote Execution Vol",
            "AM Buy Quote Execution Amt", "AM Buy Quote Execution Cnt",
            "PM Sell Quote Execution Vol", "PM Sell Quote Execution Amt",
            "PM Sell Quote Execution Cnt", "PM Buy Quote Execution Vol",
            "PM Buy Quote Execution Amt", "PM Buy Quote Execution Cnt",
        ]


def _finalize_raw(
    df_raw: pl.DataFrame,
    data_type: str,
    language: Literal["en", "jp"] = "en",
) -> pl.DataFrame:
    """Name, clean, and project a raw NEEDS frame into the final cleaned frame.

    Shared by :func:`create_df` (populated frames) and :func:`_empty_typed_frame`
    (a 0-row frame), so a no-data read carries a schema byte-identical to a
    populated read instead of drifting from it.
    """
    df_with_columns = set_columns(df_raw, data_type, language)

    if language == "jp":
        jp_mapping = get_japanese_column_mapping()
        en_to_jp = {v: k for k, v in jp_mapping.items()}
        jp_cols = df_with_columns.columns
        en_cols = [en_to_jp.get(col, col) for col in jp_cols]
        df_with_columns = df_with_columns.rename(dict(zip(jp_cols, en_cols)))
        df_cleaned = clean_data(df_with_columns, data_type, language)
        df_cleaned = df_cleaned.rename(dict(zip(en_cols, jp_cols)))
        if data_type != "individual_stock":
            final_cols = get_final_columns(data_type)
            final_cols_jp = [jp_mapping.get(c, c) for c in final_cols]
            available = [c for c in final_cols_jp if c in df_cleaned.columns]
            return df_cleaned.select(available)
        return df_cleaned

    df_cleaned = clean_data(df_with_columns, data_type, language)
    if data_type != "individual_stock":
        final_cols = get_final_columns(data_type)
        available = [c for c in final_cols if c in df_cleaned.columns]
        return df_cleaned.select(available)
    return df_cleaned


def _empty_typed_frame(
    data_type: str,
    language: Literal["en", "jp"] = "en",
    year: Optional[int] = None,
) -> pl.DataFrame:
    """A 0-row frame whose schema matches :func:`create_df`'s output for
    ``data_type`` — so a "no ZIPs found" read returns full typed columns rather
    than a schemaless ``(0, 0)`` frame (consistent with the no-match path).
    """
    if year is None:
        year = datetime.datetime.now().year
    width = _raw_width(data_type, year)
    raw = pl.DataFrame(schema={f"column_{i + 1}": pl.String for i in range(width)})
    return _finalize_raw(raw, data_type, language)


def _year_hint_from_date(date: Optional[str]) -> Optional[int]:
    """A representative 4-digit year parsed from a date/period string, used only
    to size an empty frame; ``None`` when none is present."""
    if date is None:
        return None
    for tok in re.split(r"[-\s]+", str(date).strip()):
        if len(tok) >= 4 and tok[:4].isdigit() and tok.startswith("20"):
            return int(tok[:4])
    return None


def _normalize_ticker_filter(ticker_filter) -> Optional[set]:
    """Normalize ``ticker_filter`` to a set of string codes (or ``None``).

    A bare single code is an easy novice mistake: a ``str`` like ``"101"`` would be
    iterated into characters (``{'1', '0', '1'}`` — matching nothing, a silent
    empty result) and an ``int`` like ``101`` would raise ``TypeError`` when
    iterated. Treat a lone ``str``/``int`` as a one-element filter; otherwise
    coerce each item of the iterable to a stripped string.
    """
    if ticker_filter is None:
        return None
    if isinstance(ticker_filter, (str, int)):  # a single bare code, not an iterable of codes
        ticker_filter = {ticker_filter}
    return {str(t).strip() for t in ticker_filter}


def _stock_family_roots(tickers) -> Optional[set]:
    """Map each requested ``individual_stock`` code to its 4-char family root.

    NEEDS appends a share-class digit to the parent's 4-char code (``"72031"`` =
    Toyota New Shares), and the whole read path — the raw-byte field-5 filter,
    the part probes, the store's coverage markers — operates on the first 4
    chars. A raw 5-char request therefore used to match NOTHING (the filter
    compared 4-char codes against the 5-char request). Rooting the requested
    codes once here gives family semantics end-to-end: a 4-char code selects the
    parent plus its share classes; a longer code selects its family. Codes of 4
    chars or fewer (incl. alphanumeric like ``"130A"``) pass through unchanged.
    ``individual_stock`` only — index display names must not be sliced.
    """
    if tickers is None:
        return None
    return {s[:4] if len(s) > 4 else s for s in (str(t).strip() for t in tickers)}


def _code_matches_family(stem_code: str, requested: str) -> bool:
    """True when a store file's ``ticker=`` code satisfies a requested code.

    A 4-char request selects its whole share-class family (prefix match:
    ``"7203"`` ⇒ ``7203``, ``72031``, …) — mirroring what Stage 1's field-5
    filter ingests for that request, so no ingested file is unreachable. Any
    other request length must match the stem exactly (the documented escape
    hatch for reading a single share class off a built store).
    """
    return stem_code == requested or (len(requested) == 4 and stem_code[:4] == requested)


def create_df(
    folder_path: str,
    language: Literal["en", "jp"] = "en",
    rows: Optional[int] = None,
    auto_detect: bool = True,
    data_type: Optional[str] = None,
    year: Optional[int] = None,
    ticker_filter: Optional[set] = None,
    max_oneshot_bytes: Optional[int] = _DEFAULT_ONESHOT,
) -> pl.DataFrame:
    """Read raw NEEDS ZIP(s) into a cleaned Polars DataFrame.

    Loads a single ``.zip`` or every ``.zip`` in a directory, parses the
    headerless NEEDS CSV, applies column names and categorical decoding, and
    returns the cleaned frame. This reads **raw files** — no Parquet store.

    Args:
        folder_path: A ``.zip`` file, or a directory of NEEDS ZIPs.
        language: Output column-name language, ``"en"`` or ``"jp"`` (a
            :class:`Language` works too).
        rows: Optional cap on rows read (a fast sample of the first N).
        auto_detect: When ``True`` (default) detect ``data_type``/``year`` from
            the path — but only for whichever you leave as ``None``. An explicitly
            passed ``data_type=``/``year=`` is always honored (no longer
            overwritten by detection), so a correctly-named ZIP in a folder whose
            path has no year still reads when you pass ``year=``. When ``False``
            you **must** pass both ``data_type`` and ``year``.
        data_type: One of the four NEEDS types (a :class:`DataType` works too).
            Auto-detected from the path when omitted; required when
            ``auto_detect=False``.
        year: Selects era-specific parsing. Auto-detected from the path when
            omitted; required when ``auto_detect=False``.
        ticker_filter: A ``set`` of **string** stock codes (e.g. ``{"7203"}``)
            kept via the raw-byte fast path; a bare ``"7203"`` / ``7203`` is also
            accepted as a single code. Applied for ``individual_stock`` **only** —
            ignored for the other data types. Note a single numbered ZIP holds
            only part of a day's code range, so filtering a lone part may yield 0
            rows; pass the day's directory for complete coverage.
        max_oneshot_bytes: Cumulative decompressed-size ceiling for this one-shot
            read. Defaults to 5 GB; pass a larger ``int`` for a high-RAM machine,
            or ``None`` to disable the guard. Crossing it raises
            :class:`OneShotMemoryError` (a :class:`MemoryError`) *before* the load —
            the signal to switch to the two-stage ingest+query path. Not applied to
            the bounded ``individual_stock`` ``ticker_filter`` fast path.

    Returns:
        The cleaned DataFrame (empty if ``ticker_filter`` matched no rows).
    """
    ticker_filter = _normalize_ticker_filter(ticker_filter)

    # Explicit year/data_type always win; auto-detection (when on) only fills in
    # whichever was left as None. This lets a correctly-named ZIP in a year-less
    # folder be read by passing year= even under the default auto_detect=True (it
    # previously raised "Could not detect year from path": alpha-test Bug #3).
    if auto_detect:
        if year is None:
            year = _require_year_from_path(folder_path)
        if data_type is None:
            data_type = _detect_data_type_from_path(folder_path)
        logger.debug("Resolved: %s, Year: %s", data_type, year)
    else:
        if data_type is None or year is None:
            raise ValueError(
                "When auto_detect=False, data_type and year must be explicitly provided"
            )
        logger.debug("Manual: %s, Year: %s", data_type, year)

    df_raw = get_1y_dataframe(
        folder_path,
        year,
        data_type,
        rows,
        # Root each requested code to its 4-char family (72031 -> 7203) so the
        # field-5 fast path — which compares 4-char codes — matches it; a raw
        # 5-char request used to silently return nothing.
        ticker_filter=_stock_family_roots(ticker_filter) if data_type == "individual_stock" else None,
        max_oneshot_bytes=_resolve_oneshot_bytes(max_oneshot_bytes),
    )

    df_final = _finalize_raw(df_raw, data_type, language)
    logger.debug("Data successfully created")
    return df_final


def export_to_csv(
    folder_path: str,
    output_path: Optional[str] = None,
    language: Literal["en", "jp"] = "en",
    rows: Optional[int] = None,
) -> str:
    """Read raw NEEDS ZIP(s) with :func:`create_df` and write the result to CSV.

    Convenience wrapper that cleans the **whole** file/directory and writes it
    out. There is **no** ``ticker_filter`` parameter — call :func:`create_df`
    (or :func:`tse_tick.read_ticks`) directly when you need ticker pruning.

    Args:
        folder_path: A ``.zip`` file, or a directory of NEEDS ZIPs.
        output_path: Destination ``.csv``; when ``None`` a name is derived as
            ``<data_type>_<year>_{en|jp}_cleaned.csv``.
        language: Output column-name language, ``"en"`` or ``"jp"``. The ``"jp"``
            file is written as UTF-8 **with a BOM** (``utf-8-sig``) so Excel on a
            Japanese Windows locale renders it correctly instead of as mojibake;
            ``"en"`` output is ASCII and stays BOM-free.
        rows: Optional cap on rows read.

    Returns:
        The path the CSV was written to.
    """
    df = create_df(folder_path, language, rows)

    if output_path is None:
        data_type, year = detect_data_type_and_year(folder_path)
        lang_suffix = "_jp" if language == "jp" else "_en"
        output_path = f"{data_type}_{year}{lang_suffix}_cleaned.csv"

    if language == "jp":
        # Excel on a Japanese Windows locale defaults to Shift-JIS and shows
        # BOM-less UTF-8 (レコード種別 / 東証 …) as mojibake; prepend a UTF-8 BOM so
        # it opens correctly. Polars/pandas readers strip the BOM transparently
        # (run14 Finding 3). Streamed via a binary handle so a large export is not
        # materialised as a Python string.
        with open(output_path, "wb") as fh:
            fh.write(b"\xef\xbb\xbf")
            df.write_csv(fh)
    else:
        df.write_csv(output_path)
    logger.info("Data exported to: %s", output_path)
    logger.debug("Shape: %s", df.shape)

    return output_path


# --------------------------------------------------------------------------- #
# read_ticks — one-shot path: raw ZIPs -> ticker/time-filtered DataFrame
# --------------------------------------------------------------------------- #

def _zip_year(zip_path: Path) -> int:
    """Best-effort era year for a ZIP: the YYYY of the date token in its name."""
    for part in zip_path.name.split("."):
        if len(part) == 8 and part.isdigit() and part.startswith("20"):
            return int(part[:4])
    # Fall back to scanning the path (handles {year}/{yearmonth}/ roots). Only the
    # year is needed, so use the year detector directly rather than full
    # data-type+year detection (which would also glob the dir for a sample file).
    year = _detect_year_from_path(str(zip_path))
    if year is None:
        raise ValueError(
            f"read_ticks: could not determine the year for {zip_path} "
            f"(needed for era-specific parsing)"
        )
    return year


def _zip_decompressed_size(zip_path) -> int:
    """Total decompressed size of a ZIP's members (central-directory read, no
    decompression). Returns 0 if the ZIP can't be opened — the actual read then
    surfaces the real error rather than this sizing probe."""
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            return sum(info.file_size for info in zf.infolist())
    except (zipfile.BadZipFile, EOFError, OSError):
        return 0


def _requested_days(date: Optional[str]) -> Optional[set]:
    """The exact ``YYYYMMDD`` days a date request resolves to, or ``None`` when
    the request is month/year-level (no per-day pruning intended).

    Monthly NEEDS ZIPs (the summary and index types) hold a whole month, so a
    single-day or day-range request must prune the result to those days —
    otherwise the caller silently gets the whole month, inconsistent with the
    daily ``individual_stock`` files and with ``query_ticks`` (both day-scoped).
    """
    if date is None:
        return None
    token = str(date).strip()
    if "-" not in token and token.isdigit() and len(token) == 8:
        return {token}
    parsed = parse_period(token)
    if parsed.get("granularity") == "date":
        return set(parsed["dates"])
    return None


def _discover_root_zips(input_root: str, data_type: str, date: Optional[str]) -> List[Path]:
    """Discover ZIPs under a structured ``{year}/{yearmonth}/`` NEEDS root."""
    if date is None:
        raise ValueError(
            "read_ticks: 'date' is required when 'source' is a structured NEEDS "
            "root (a day 'YYYYMMDD', month 'YYYYMM', year 'YYYY', or a "
            "'start-end' range)"
        )
    token = str(date).strip()
    if "-" not in token and token.isdigit() and len(token) == 8:
        return discover_zips(input_root, data_type, [int(token[:4])],
                             months=[int(token[4:6])], dates=[token])
    if "-" not in token and token.isdigit() and len(token) == 6:
        return discover_zips(input_root, data_type, [int(token[:4])], months=[int(token[4:6])])

    parsed = parse_period(token)
    gran = parsed["granularity"]
    zips: List[Path] = []
    if gran == "year":
        for y in parsed["years"]:
            zips.extend(discover_zips(input_root, data_type, [y]))
    elif gran == "month":
        for y, months in parsed["months_by_year"].items():
            zips.extend(discover_zips(input_root, data_type, [y], months=list(months)))
    else:  # gran == "date"
        dates = parsed["dates"]
        for y in sorted({int(d[:4]) for d in dates}):
            ydates = [d for d in dates if d.startswith(str(y))]
            ymonths = sorted({int(d[4:6]) for d in ydates})
            zips.extend(discover_zips(input_root, data_type, [y], months=ymonths, dates=ydates))
    return zips


def _resolve_source_zips(source: str, data_type: str, date: Optional[str]) -> List[Path]:
    """Resolve a ``source`` (single ZIP, flat dir, or structured root) to ZIPs."""
    p = Path(source)
    if not p.exists():
        raise FileNotFoundError(f"read_ticks: source does not exist: {source}")
    if p.is_file():
        if p.suffix.lower() != ".zip":
            raise ValueError(f"read_ticks: source file must be a .zip, got {source!r}")
        return [p]
    if p.is_dir():
        # With no date, a flat folder reads all its ZIPs (a structured root needs a
        # date — the call below then raises a clear message).
        if date is None:
            direct = sorted(p.glob("*.zip"), key=_zip_sort_key)
            if direct:
                return direct
        # With a date, resolve via discover_zips for BOTH a flat folder and a
        # structured root: it maps a single *day* onto its containing *monthly*
        # file and matches daily-packaged files by day. (The old flat-folder branch
        # matched the date token as a filename substring, so a single-day date never
        # matched a "…YYYYMM.zip" monthly file — a month-folder + single-day read
        # came back falsely empty: run11 Finding 1.)
        return _discover_root_zips(str(p), data_type, date)
    raise ValueError(f"read_ticks: source must be a .zip file or a directory: {source!r}")


def _parse_hms(value: str) -> datetime.time:
    try:
        return datetime.time.fromisoformat(str(value))
    except ValueError as exc:
        raise ValueError(f"Invalid time format (expected HH:MM:SS): {value!r}") from exc


def _resolve_col(df: pl.DataFrame, en_name: str) -> Optional[str]:
    """Return the column matching an English concept, honoring jp output.

    ``read_ticks(language="jp")`` renames columns to Japanese, so ticker- and
    time-filters keyed on the English name must also accept the Japanese
    equivalent (otherwise the filter silently finds no column and is skipped).
    """
    if en_name in df.columns:
        return en_name
    jp_name = get_japanese_column_mapping().get(en_name)  # mapping is {en: jp}
    if jp_name is not None and jp_name in df.columns:
        return jp_name
    return None


def _filter_time_window(
    df: pl.DataFrame, start_time: Optional[str], end_time: Optional[str]
) -> pl.DataFrame:
    time_col = _resolve_col(df, "Execution Time")
    if time_col is None:
        raise ValueError(
            "read_ticks: start_time/end_time require an 'Execution Time' column"
        )
    date_col = _resolve_col(df, "Data Date") or "Data Date"

    # individual_stock quote-only book updates have a blank Execution Time but a
    # real Update Time; fall back to it so a time window keeps in-window order-book
    # rows, not just trade-coincident snapshots (~94% of a liquid day is quotes).
    # Scoped automatically: only individual_stock carries an Update Time column
    # (the indices/summary projections drop it), so the fallback is inactive there.
    update_col = _resolve_col(df, "Update Time")
    work = df
    eff_col = time_col
    if update_col is not None:
        exec_raw = pl.col(time_col).cast(pl.String).str.strip_chars()
        work = df.with_columns(
            pl.when(exec_raw.is_null() | (exec_raw == ""))
            .then(pl.col(update_col))
            .otherwise(pl.col(time_col))
            .alias("__eff_time")
        )
        eff_col = "__eff_time"

    time_of_day = _tick_datetime_expr(date_col, eff_col).dt.time()
    expr = None
    if start_time is not None:
        cond = time_of_day >= pl.lit(_parse_hms(start_time))
        expr = cond if expr is None else (expr & cond)
    if end_time is not None:
        cond = time_of_day <= pl.lit(_parse_hms(end_time))
        expr = cond if expr is None else (expr & cond)

    result = work.filter(expr) if expr is not None else work
    if eff_col == "__eff_time":
        result = result.drop("__eff_time")
    return result


def _filter_codes(df: pl.DataFrame, data_type: str, wanted: set) -> pl.DataFrame:
    """Post-parse ticker filter for the non-individual_stock types (en or jp)."""
    if data_type == "stock_summary":
        col = _resolve_col(df, "Stock Code")
        if col is None:
            return df
        codes = pl.col(col).cast(pl.String).str.strip_chars().str.slice(0, 4)
        return df.filter(codes.is_in(list(wanted)))
    # indices / indices_summary: Index Code is categorically decoded to a display
    # name (e.g. "101" -> "Nikkei 225" / "日経平均株価"); accept the raw code or
    # either-language name. _index_code_lookup() maps both en and jp names -> code.
    col = _resolve_col(df, "Index Code")
    if col is None:
        return df
    from .io.parquet import _index_code_lookup

    # Index Code is now the raw code in-column, but accept a display name as
    # input too (and still match old stores that hold names): map names->code
    # and code->name(s) so wanted matches whichever form the column holds.
    lookup = _index_code_lookup()  # display name (en+jp) -> raw code
    expanded = set(wanted)
    for w in list(wanted):
        if w in lookup:
            expanded.add(lookup[w])
    for display, code in lookup.items():
        if code in expanded:
            expanded.add(display)
    disp = pl.col(col).cast(pl.String).str.strip_chars()
    return df.filter(disp.is_in(list(expanded)))


def read_ticks(
    source: str,
    *,
    data_type: str = "individual_stock",
    ticker_filter: Optional[set] = None,
    date: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    columns: Optional[List[str]] = None,
    rows: Optional[int] = 10_000_000,
    language: Literal["en", "jp"] = "en",
    prune_parts: bool = True,
    max_oneshot_bytes: Optional[int] = _DEFAULT_ONESHOT,
) -> pl.DataFrame:
    """One-shot read: raw NEEDS ZIPs -> a ticker/time-filtered DataFrame (no store).

    Answers the common exploratory question — *"ticker 7203 on 2024-02-01,
    09:00-11:30"* — in a single call, with no Parquet store to build first. It
    composes the existing pipeline (:func:`create_df`'s ``individual_stock``
    raw-byte ticker fast path, :func:`discover_zips` / :func:`parse_period`
    discovery, and the shared tick-timestamp helper); it does not reimplement
    parsing, ticker pruning, or time filtering.

    Args:
        source: A single ``.zip``, a flat folder of ZIPs, or a structured NEEDS
            root (``{year}/{yearmonth}/…``) — the same inputs the ``ingest_*``
            functions accept.
        data_type: One of the four NEEDS types (a :class:`DataType` works too).
        ticker_filter: A ``set`` of codes (e.g. ``{"7203"}``); ``int`` codes
            such as ``{7203}`` are accepted too and coerced to strings — so the
            output of :func:`tse_tick.get_available_tickers` feeds straight in. A
            **bare single code** (``"7203"`` or ``7203``) is also accepted and
            treated as a one-element filter (so a stray ``"7203"`` is not split
            into characters). For ``individual_stock`` this drives the
            bounded-memory raw-byte fast path; for ``indices`` it matches the
            index code (``"101"`` == Nikkei 225) after parsing.
        date: A day ``"YYYYMMDD"``, month ``"YYYYMM"``, year ``"YYYY"``, or a
            ``"start-end"`` range. Selects which ZIPs to open. **Required** when
            ``source`` is a structured root; optional for a single ZIP/flat dir.
        start_time: Inclusive lower bound on time-of-day (``"HH:MM:SS"``).
        end_time: Inclusive upper bound on time-of-day (``"HH:MM:SS"``).
        columns: Column projection; ``None`` selects all columns.
        rows: Cap on returned rows (default 10,000,000). On hitting the cap the
            result is truncated **and a** :class:`TruncationWarning` **is
            emitted** (capturable via ``warnings``) — including when the total
            lands exactly on the cap with ZIPs still unread (detected by reading
            one row past the cap, as :func:`tse_tick.query_ticks` does); an
            exact-fit result with nothing left unread does not warn. A whole **month** of a couple
            of *active* tickers can exceed 10M (e.g. 7203 + 9984 for one January is
            ~25M rows), so a single monthly call would stop partway. To read it all,
            use the two-stage ``ingest_period`` -> :func:`tse_tick.query_ticks` path
            (or :func:`tse_tick.extract_to_store` for a single ticker), loop
            per-day, or pass ``rows=None`` (bounded only by memory).
        language: Output column-name language, ``"en"`` or ``"jp"``.
        prune_parts: For ``individual_stock`` + a ``ticker_filter``, open only the
            short contiguous run of numbered parts that actually holds the
            ticker(s) instead of every part of the day (NEEDS numbers parts by
            ascending code but a busy code spans a few consecutive parts). Falls
            back to opening all parts if the ascending-code layout can't be
            confirmed, so the result is identical — only faster. Default ``True``.
        max_oneshot_bytes: Cumulative decompressed-size ceiling across the ZIPs this
            read opens (default 5 GB; pass a larger ``int`` or ``None`` to disable).
            Crossing it raises :class:`OneShotMemoryError` — the signal to switch to
            the two-stage ingest+query path. Exempt for the bounded
            ``individual_stock`` ``ticker_filter`` fast path.

    Returns:
        The cleaned Polars DataFrame — empty but fully typed if nothing matches.
        Its columns match :func:`create_df`. Note :func:`tse_tick.query_ticks`
        returns these columns **plus** an extra ``date`` partition column
        (``i64`` ``YYYYMMDD``, added by the store's Hive partitioning), so the
        store path has one more column than this one-shot path.

    Caveats:
        * **Time filtering applies to tick types only.** ``individual_stock`` /
          ``indices`` have ``Execution Time``; the two ``*_summary`` types are
          daily aggregates, so passing ``start_time``/``end_time`` for them
          raises — filter on ``date`` only. For ``individual_stock``, quote-only
          book updates have a blank ``Execution Time`` but a real ``Update Time``,
          so the time window falls back to ``Update Time`` for those rows — an
          in-window order book is kept whole, not reduced to trade-coincident
          snapshots.
        * **The fast path is ``individual_stock``-only.** Other types parse in
          full then filter (fine — those files are far smaller).
        * **Not a store replacement at scale.** Even one ticker-day opens **every
          ZIP part** of each requested day, so a one-shot read can take tens of
          seconds per day (the raw-byte fast path still scans all parts). With no
          ``ticker_filter`` over a wide span it re-scans raw ZIPs on every call;
          for repeated or large analyses — or just faster single-ticker lookups —
          ``ingest_*`` once + :func:`tse_tick.query_ticks` is far faster
          (sub-second).
        * **A single numbered ZIP is only part of a day.** NEEDS splits each day
          across numbered parts by ascending code, so one
          ``HTICST120.<date>.N.zip`` holds a code subset (part ``1`` starts at
          1301; Toyota 7203 is in a later part). Pass the directory or structured
          root — not a lone part — for complete ticker coverage.

    Example:
        >>> df = read_ticks("G:/NEEDS_root", ticker_filter={"7203"},
        ...                 date="20240201", start_time="09:00:00",
        ...                 end_time="11:30:00")
    """
    validate_data_type(data_type)

    is_summary = data_type in SUMMARY_TYPES
    if (start_time is not None or end_time is not None) and is_summary:
        raise ValueError(
            f"read_ticks: start_time/end_time are not supported for {data_type!r} "
            f"(daily aggregates have no Execution Time); filter on 'date' only"
        )

    norm_filter = _normalize_ticker_filter(ticker_filter)

    zips = _resolve_source_zips(source, data_type, date)
    if not zips:
        _warn_no_data(
            f"read_ticks: no ZIP files found for data_type={data_type!r} on the "
            f"requested date(s) in {source!r}. Likely causes: a non-trading day "
            "(e.g. an exchange holiday), or data_type not matching the files in "
            "this folder (e.g. pointing 'individual_stock' at an index folder)."
        )
        return _empty_typed_frame(data_type, language, _year_hint_from_date(date))

    # Part-pruning: a single ticker's rows sit in a short contiguous run of parts,
    # so open only that run instead of every part of the day. Degrades to all parts
    # when the ascending-code layout can't be confirmed, so results are identical.
    if prune_parts and data_type == "individual_stock" and norm_filter:
        zips = _prune_parts_by_ticker(zips, norm_filter)

    # create_df reaches the raw-byte fast path only for individual_stock.
    cdf_filter = norm_filter if data_type == "individual_stock" else None

    # One-shot size ceiling across every ZIP this read accumulates into `parts`.
    # The individual_stock ticker fast path is bounded-memory, so it's exempt;
    # create_df is called with its own guard off (max_oneshot_bytes=None) because
    # read_ticks does the cross-ZIP accounting here (alpha-review finding 5).
    guard_bytes = None if cdf_filter is not None else _resolve_oneshot_bytes(max_oneshot_bytes)

    parts: List[pl.DataFrame] = []
    total = 0
    truncated = False
    cumulative_bytes = 0
    schema_frame: Optional[pl.DataFrame] = None
    for zip_path in zips:
        if guard_bytes is not None:
            cumulative_bytes += _zip_decompressed_size(zip_path)
            if cumulative_bytes > guard_bytes:
                raise OneShotMemoryError(
                    _oneshot_limit_message(cumulative_bytes, guard_bytes)
                )
        df = None
        try:
            year = _zip_year(zip_path)
            df = create_df(
                str(zip_path),
                language=language,
                auto_detect=False,
                data_type=data_type,
                year=year,
                ticker_filter=cdf_filter,
                max_oneshot_bytes=None,
            )
            if norm_filter is not None and data_type != "individual_stock":
                df = _filter_codes(df, data_type, norm_filter)

            if start_time is not None or end_time is not None:
                df = _filter_time_window(df, start_time, end_time)

            # Capture the typed schema from the first part so a no-match read
            # returns an empty-but-typed frame, not a schemaless (0, 0).
            if schema_frame is None:
                schema_frame = df.clear()
            if df.height:
                parts.append(df)
                total += df.height
        finally:
            # One ZIP at a time, collected between, to bound memory on big files.
            del df
            gc.collect()

        # Strictly-greater, mirroring query_ticks's LIMIT+1 trick: keep reading
        # until the cap is EXCEEDED by at least one row, so an exact-fit total
        # with ZIPs still unread can't masquerade as a complete result — the old
        # `>= rows` break dropped the remaining ZIPs with no TruncationWarning
        # (audit finding L1). Costs at most one extra ZIP on an exact boundary.
        if rows is not None and total > rows:
            truncated = True
            break

    if parts:
        result = _guard_polars_oom(lambda: pl.concat(parts, how="vertical"))
    elif schema_frame is not None:
        result = schema_frame
    else:
        result = _empty_typed_frame(data_type, language, _year_hint_from_date(date))

    # Monthly ZIPs hold a whole month; prune to the requested day(s) so a
    # single-day/day-range request doesn't silently return the entire month.
    days = _requested_days(date)
    if days is not None and result.height:
        date_col = _resolve_col(result, "Data Date")
        if date_col is not None:
            dd = pl.col(date_col).cast(pl.Date).cast(pl.String).str.replace_all("-", "")
            result = result.filter(dd.is_in(list(days)))

    # Project AFTER all filtering — code, time, AND the monthly day-prune above.
    # Projecting per-part earlier let a `columns` subset that drops 'Data Date'
    # skip the day-prune and silently return the whole month (the run10 indices
    # ~20x inflation bug); doing it here keeps every filter on the full schema.
    if columns:
        missing = [c for c in columns if c not in result.columns]
        if missing:
            raise ValueError(f"read_ticks: requested columns not present: {missing}")
        result = result.select(columns)

    # A zero-row result is the same "no data" condition for every data type —
    # signal it the same capturable way (the ZIPs existed but a holiday/unknown
    # code/over-tight filter left nothing), not silently as before.
    if result.height == 0:
        # `is not None` (not truthiness) so an *empty* filter is still named — an
        # accidentally-empty set matches nothing, and omitting it from the message
        # wrongly implied no filter was applied (run10 F4).
        ft = f", ticker_filter={sorted(norm_filter)}" if norm_filter is not None else ""
        _warn_no_data(
            f"read_ticks: 0 rows for the requested filters "
            f"(data_type={data_type!r}, date={date!r}{ft}). Possible causes: a "
            "non-trading day (e.g. a holiday), a ticker/index code not present, or "
            "time/column filters that exclude every row (e.g. start_time after "
            "end_time)."
        )

    # Warn on `truncated` too, not just on the final height: the day-prune /
    # column filters above can shrink an early-broken result back under the cap,
    # but ZIPs were still left unread — coverage is incomplete either way (L1).
    if rows is not None and (truncated or result.height > rows):
        warnings.warn(
            f"read_ticks: row cap ({rows}) reached; result truncated. "
            "Build a Parquet store (ingest_*) and use query_ticks for full coverage.",
            TruncationWarning,
            stacklevel=2,
        )
        result = result.head(rows)
    return result
