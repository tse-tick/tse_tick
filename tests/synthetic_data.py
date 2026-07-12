# tests/synthetic_data.py
"""Synthetic NEEDS-format raw data generators for the Stage-2 test fixture.

These helpers produce *obviously fake* data shaped exactly like the headerless
NEEDS CSV that the real ingester consumes: the correct positional field layout
and field count (95 fields for ``individual_stock`` / TICST120), quoted
comma-separated values, timestamps spanning the TSE trading day with a genuine
lunch break, and plainly synthetic prices/volumes.

The output is written to ZIPs and run through the *real* ingest pipeline
(``ingest_single_zip``) so the resulting Parquet store is produced by the same
code path as production. No proprietary NEEDS data is read or written.
"""
from __future__ import annotations

import zipfile
from pathlib import Path

from tse_tick.schemas import (
    get_schema_individual_stock_95,
    get_schema_indices_23,
    get_schema_summary_83,
)

_SCHEMA_95 = get_schema_individual_stock_95()
_SCHEMA_INDICES_23 = get_schema_indices_23()
_SCHEMA_83 = get_schema_summary_83()

# TSE session boundaries, in minutes since midnight.
_AM_OPEN, _AM_CLOSE = 540, 690   # 09:00 - 11:30
_PM_OPEN, _PM_CLOSE = 750, 900   # 12:30 - 15:00


def _hhmmss(minute_of_day: int) -> str:
    hh, mm = divmod(minute_of_day, 60)
    return f"{hh:02d}{mm:02d}00"


def _field(name: str, *, date: str, ticker: str, hhmmss: str, session: str, price: int) -> str:
    """Return one synthetic field value, selected by its schema column name."""
    fixed = {
        "Record Type": "1200",        # -> "Stocks - Multiple Quote"
        "Data Date": date,
        "Exchange Code": "11",         # -> Tokyo Stock Exchange (TSE)
        "Security Type": "1",          # -> First Section
        "Session": session,            # "1" = morning, "2" = afternoon
        "Stock Code": ticker,
        "Execution Time": hhmmss,
        "Sell Quote Time": hhmmss,
        "Buy Quote Time": hhmmss,
        "Update Time": hhmmss + "000000",
        "Management Number": "0001",
        "Execution Type": "32",        # -> "Between Quotes"
        "Ayumi Flag": "0",             # -> "Regular"
        "Volume": "100",
        "Volume Flag": "0",            # -> "Final"
        "Close Quote Flag": "0",
        "Execution Price": str(price),
        "Sell Quote 1 Best": str(price + 1),
        "Buy Quote 1 Best": str(price - 1),
    }
    if name in fixed:
        return fixed[name]
    if "Vol" in name:          # quote-volume columns (e.g. "Sell Quote Vol 3")
        return "500"
    if "Flag" in name:         # quote-flag columns -> "Regular Quote"
        return "128"
    if name.startswith("Sell"):  # deeper sell quote price levels / market/limit
        return str(price + 2)
    if name.startswith("Buy"):   # deeper buy quote price levels / market/limit
        return str(price - 2)
    return "0"


def individual_stock_csv(
    date: str,
    tickers: list[str],
    rows_per_ticker: int = 40,
    base_prices: dict[str, int] | None = None,
    minute_offsets: dict[str, int] | None = None,
) -> bytes:
    """Build a headerless TICST120 (95-field) CSV as raw bytes.

    Rows are split between the morning and afternoon sessions with a real
    11:30-12:30 lunch gap, and prices vary row-to-row so order-book features
    (spread, imbalance, volatility) have something to compute.

    ``minute_offsets`` shifts a ticker's whole schedule by N minutes. DuckDB's
    parallel sort has an arbitrary order within a same-``(date, time)`` tie, so
    tests that compare frames containing several tickers (e.g. a parent code and
    its suffixed share class) must de-tie their timestamps to be deterministic.
    """
    base_prices = base_prices or {}
    minute_offsets = minute_offsets or {}
    half = max(rows_per_ticker // 2, 1)
    lines: list[str] = []

    for ticker in tickers:
        base = base_prices.get(ticker, 1500)
        shift = minute_offsets.get(ticker, 0)
        am_minutes = [shift + _AM_OPEN + round(i * (149 / max(half - 1, 1))) for i in range(half)]
        pm_minutes = [shift + _PM_OPEN + round(i * (149 / max(half - 1, 1))) for i in range(half)]
        schedule = [(m, "1") for m in am_minutes] + [(m, "2") for m in pm_minutes]

        for i, (minute, session) in enumerate(schedule):
            price = base + (i % 11)
            row = [
                _field(name, date=date, ticker=ticker, hhmmss=_hhmmss(minute),
                       session=session, price=price)
                for name in _SCHEMA_95
            ]
            lines.append(",".join('"' + v + '"' for v in row))

    return ("\n".join(lines) + "\n").encode("ascii")


def _index_field(name: str, *, date: str, code: str, hhmmss: str) -> str:
    """One synthetic TICIT110 (index tick, 23-field) field value, by column name."""
    fixed = {
        "Record Type": "2100",     # -> "Indices - Execution"
        "Data Date": date,
        "Exchange Code": "11",      # -> Tokyo Stock Exchange (TSE)
        "Security Type": "10",      # -> Cash Index
        "Session": "1",
        "Index Code": code,
        "Execution Time": hhmmss,
        "Update Time": hhmmss + "000000",
        "Management Number": "0001",
        "Index Value": "2850000",
        "Execution Type": "0",      # -> "Other"
        "Ayumi Flag": "0",          # -> "Regular"
    }
    return fixed.get(name, "0")     # Reserved 1..11 default to "0"


def indices_csv(date: str, codes: list[str], rows_per_code: int = 16) -> bytes:
    """Build a headerless TICIT110 (23-field index tick) CSV as raw bytes.

    ``codes`` are raw numeric index codes (e.g. "101" Nikkei 225, "113" TOPIX);
    they are categorically decoded to display names on ingest, so this exercises
    the raw-code partitioning fix.
    """
    lines: list[str] = []
    for code in codes:
        minutes = [_AM_OPEN + round(i * (359 / max(rows_per_code - 1, 1))) for i in range(rows_per_code)]
        for minute in minutes:
            row = [
                _index_field(name, date=date, code=code, hhmmss=_hhmmss(minute))
                for name in _SCHEMA_INDICES_23
            ]
            lines.append(",".join('"' + v + '"' for v in row))
    return ("\n".join(lines) + "\n").encode("ascii")


def indices_2016_csv(date: str, codes: list[str], times: list[str] | None = None) -> bytes:
    """Build a headerless TICIT010 (2016, 15-field) index record as raw bytes.

    Unlike the 2017+ quoted CSV, 2016 index records are **fixed-width** (parsed
    positionally by :func:`tse_tick.core.parse_line`) and store ``Execution Time``
    as 4-char ``HHMM`` with no seconds — the era quirk behind the width
    normalization. Each line is exactly 69 characters; numeric fields are
    zero-padded (so the integer/float casts succeed) and categorical fields are
    space-padded (so a strip yields the single-char code).
    """
    times = times or ["0900", "1030", "1515"]
    lines: list[str] = []
    for code in codes:
        for hhmm in times:
            line = (
                "2100"               # Record Type (4)   -> "Indices - Execution"
                + date               # Data Date (8)
                + "0"                # Identification Flag (1)
                + "11"               # Exchange Code (2)  -> TSE
                + "10"               # Security Type (2)  -> Cash Index
                + "1"                # Session (1)
                + code.rjust(12)     # Index Code (12)
                + hhmm               # Execution Time (4) HHMM — no seconds
                + "01"               # Record Type (Executions/Quotes) (2)
                + "0001"             # Management Number (4)
                + "001688500"        # Index Value (9)    -> 16885.00 after *0.01
                + "0".rjust(3)       # Execution Type (3) -> "Other"
                + "0".rjust(3)       # Ayumi Flag (3)     -> "Regular"
                + "0".zfill(11)      # Volume (11)
                + "0".rjust(3)       # Volume Flag (3)    -> "Final"
            )
            lines.append(line)
    return ("\n".join(lines) + "\n").encode("ascii")


def individual_stock_with_quote_rows_csv(
    date: str,
    ticker: str,
    *,
    trade_times: list[str],
    quote_times: list[str],
    base_price: int = 2100,
) -> bytes:
    """Build a TICST120 (95-field) CSV mixing trade rows and quote-only rows.

    Trade rows carry an ``Execution Time`` (``"HHMMSS"``) and ``Volume>0``;
    quote-only book updates carry a **blank** ``Execution Time`` but a real
    ``Update Time`` and ``Volume=0`` — the shape behind the time-filter
    ``Update Time`` fallback. ``trade_times`` / ``quote_times`` are ``"HHMMSS"``.
    """
    lines: list[str] = []

    def _row(overrides: dict, hhmmss: str) -> str:
        cells = [
            overrides[name]
            if name in overrides
            else _field(name, date=date, ticker=ticker, hhmmss=hhmmss,
                        session="1", price=base_price)
            for name in _SCHEMA_95
        ]
        return ",".join('"' + c + '"' for c in cells)

    for hhmmss in trade_times:
        lines.append(_row({}, hhmmss))
    for hhmmss in quote_times:
        lines.append(_row(
            {"Execution Time": "", "Update Time": hhmmss + "000000", "Volume": "0"},
            hhmmss,
        ))
    return ("\n".join(lines) + "\n").encode("ascii")


def stock_summary_csv(
    date: str,
    tickers: list[str],
    *,
    vwap: float = 1855.88528,
    volume: int = 9565000,
    time_value: str = "090000",
) -> bytes:
    """Build a headerless TICSS110 (83-field) daily-summary CSV as raw bytes.

    Quoted, comma-separated, one row per ticker. Every *measure* column carries
    an obviously-fake but valid numeric string (prices/VWAP as floats, volumes/
    counts/amounts as integers) so the numeric cast can be exercised; id/code
    columns carry codes and every time column carries ``time_value`` (default
    ``"090000"``; pass an era-specific width like ``"0900"`` (2016 ``HHMM``) or
    ``"090005000000"`` (2017+ ``HHMMSSffffff``) to exercise time normalization).
    """
    lines: list[str] = []
    for ticker in tickers:
        values: list[str] = []
        for name in _SCHEMA_83:
            if name == "Record Type":
                v = "DB13"            # -> "Stocks"
            elif name == "Data Date":
                v = date
            elif name == "Identification Flag":
                v = "0"
            elif name == "Exchange Code":
                v = "11"              # -> TSE
            elif name == "Security Type":
                v = "1"               # -> First Section
            elif name == "Stock Code":
                v = ticker
            elif "Time" in name:
                v = time_value
            elif ("Price" in name or "VWAP" in name or "Std Dev" in name
                  or "Spread" in name or "Avg" in name or "Quote" in name):
                v = f"{vwap:.5f}"     # float-valued measures
            else:
                v = str(volume)       # volumes / counts / amounts / units / shares
            values.append(v)
        lines.append(",".join('"' + v + '"' for v in values))
    return ("\n".join(lines) + "\n").encode("ascii")


def write_zip(zip_path: Path, member_name: str, payload: bytes) -> Path:
    """Write ``payload`` as a single CSV member inside a ZIP, NEEDS-style."""
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(member_name, payload)
    return zip_path


def seed_structured_day(
    root: Path,
    day: str,
    mapping: dict[int, list[str]],
    *,
    rows_per_ticker: int = 6,
    minute_offsets: dict[str, int] | None = None,
) -> None:
    """Seed one trading day of TICST120 parts under a nested NEEDS delivery tree.

    ``mapping`` is ``{part_number: [stock codes]}`` — one ZIP part per entry,
    ascending part numbers, exactly the multi-part-day layout the structured
    ingest paths consume (``個別株式{year}/TICST120/{yyyymm}/``). The shared
    helper behind the two-stage extraction tests (suffixed share-class families
    and zero-row days are built by choosing the codes per part).
    """
    leaf = Path(root) / f"個別株式{day[:4]}" / "TICST120" / day[:6]
    leaf.mkdir(parents=True, exist_ok=True)
    for n, codes in mapping.items():
        write_zip(
            leaf / f"HTICST120.{day}.{n}.zip",
            f"HTICST120.{day}.{n}.csv",
            individual_stock_csv(
                day, codes, rows_per_ticker=rows_per_ticker, minute_offsets=minute_offsets
            ),
        )
