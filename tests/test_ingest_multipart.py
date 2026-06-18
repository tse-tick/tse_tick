# tests/test_ingest_multipart.py
"""CLI/period ingest must collect ALL ZIP parts of a trading day (BUG 1 + BUG 2).

NEEDS splits a day across multiple parts by ticker range, plus a closing tail.
The store must end up with every ticker, and a ticker that appears in two parts
must keep both parts' rows (not be overwritten or skipped).
"""
import polars as pl

import tse_tick
from tests.synthetic_data import individual_stock_csv, write_zip

DATE = "20240104"


def _build_two_part_day(root):
    """Part 1: tickers 1301 + 1305. Part 2: ticker 7203 + a closing tail for 1301."""
    month_dir = root / "2024" / "202401"
    month_dir.mkdir(parents=True, exist_ok=True)
    write_zip(
        month_dir / f"HTICST120.{DATE}.1.zip", f"HTICST120.{DATE}.1.csv",
        individual_stock_csv(DATE, ["1301", "1305"], rows_per_ticker=40,
                             base_prices={"1301": 2000, "1305": 3000}),
    )
    part2 = (
        individual_stock_csv(DATE, ["7203"], rows_per_ticker=40, base_prices={"7203": 2100})
        + individual_stock_csv(DATE, ["1301"], rows_per_ticker=4, base_prices={"1301": 2000})
    )
    write_zip(month_dir / f"HTICST120.{DATE}.2.zip", f"HTICST120.{DATE}.2.csv", part2)


def _rows(date_dir, ticker):
    return pl.read_parquet(date_dir / f"ticker={ticker}.parquet").height


def test_ingest_collects_all_parts_of_a_day(tmp_path):
    root = tmp_path / "in"
    _build_two_part_day(root)
    store = tmp_path / "store"

    tse_tick.ingest_period(str(root), str(store), f"{DATE}-{DATE}", "individual_stock")

    date_dir = store / "individual_stock" / f"date={DATE}"
    tickers = {p.stem for p in date_dir.glob("ticker=*.parquet")}
    assert tickers == {"ticker=1301", "ticker=1305", "ticker=7203"}   # 7203 (part 2) NOT skipped — BUG 1
    assert _rows(date_dir, 7203) == 40
    assert _rows(date_dir, 1305) == 40
    assert _rows(date_dir, 1301) == 44   # part 1 (40) + part 2 tail (4), NOT overwritten — BUG 2


def test_ingest_resume_is_idempotent(tmp_path):
    root = tmp_path / "in"
    _build_two_part_day(root)
    store = tmp_path / "store"
    date_dir = store / "individual_stock" / f"date={DATE}"

    tse_tick.ingest_period(str(root), str(store), f"{DATE}-{DATE}", "individual_stock")
    first = _rows(date_dir, 1301)
    tse_tick.ingest_period(str(root), str(store), f"{DATE}-{DATE}", "individual_stock", resume=True)
    assert _rows(date_dir, 1301) == first   # re-run does not duplicate
