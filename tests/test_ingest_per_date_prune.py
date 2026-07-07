"""Per-date part-pruning in _ingest_grouped (issue #39).

Pruning moved from a whole-period pass BEFORE the loop to a per-date step INSIDE
the loop, AFTER the resume-skip check. These tests verify the three guarantees:
(a) the store is unchanged (per-date pruning selects the same parts), (b) a resumed
run skips already-written dates WITHOUT re-pruning them, and (c) a partition lands
per day (pruning interleaves with writes -> incremental progress), rather than the
whole period being pruned before the first partition is written.
"""
from pathlib import Path

import polars as pl
import pytest

import tse_tick
import tse_tick.ingest as ingest_mod
from tse_tick.enhanced import _prune_parts_by_ticker
from tests.synthetic_data import individual_stock_csv, write_zip

DAYS = ["20240104", "20240105", "20240109"]
TICKER = "7203"


def _seed_day(root, day, mapping):  # mapping: {part_no: [codes]}
    leaf = root / f"個別株式{day[:4]}" / "TICST120" / day[:6]
    leaf.mkdir(parents=True, exist_ok=True)
    for n, codes in mapping.items():
        write_zip(leaf / f"HTICST120.{day}.{n}.zip", f"HTICST120.{day}.{n}.csv",
                  individual_stock_csv(day, codes, rows_per_ticker=6))


def _seed_days(root, days=DAYS):
    for day in days:
        # 7203 overflows parts 2 AND 3; part 4 (the last) carries a 7203 appendix
        # tail -> exercises "contiguous run union last part".
        _seed_day(root, day, {1: ["1301"], 2: ["7203"], 3: ["7203"], 4: ["9999", "7203"]})


def _date_dir(store, day):
    return Path(store) / "individual_stock" / f"date={day}"


def _ticker_rows(store, day, ticker):
    p = _date_dir(store, day) / f"ticker={ticker}.parquet"
    return pl.read_parquet(p).height if p.exists() else 0


# ---------------------------------------------------------------------------
# (a) store unchanged: per-date pruning yields the same rows as an unpruned
#     read of the same tickers.
# ---------------------------------------------------------------------------
def test_pruned_store_matches_full_read_multiday(tmp_path):
    src = tmp_path / "src"
    _seed_days(src)
    store = tmp_path / "store"
    tse_tick.ingest_period(str(src), str(store), f"{DAYS[0]}-{DAYS[-1]}",
                           "individual_stock", ticker_filter={TICKER})
    for day in DAYS:
        ref = tse_tick.read_ticks(str(src), ticker_filter={TICKER}, date=day, prune_parts=False)
        assert _ticker_rows(store, day, TICKER) == ref.height > 0, day
        # 7203 lives in parts 2, 3 and the appendix in part 4 -> 3 * 6 rows.
        assert ref.height == 18, day


# ---------------------------------------------------------------------------
# (b) resume skips already-written dates WITHOUT re-pruning.
# ---------------------------------------------------------------------------
def test_resume_skips_without_repruning(tmp_path, monkeypatch):
    src = tmp_path / "src"
    _seed_days(src)
    store = tmp_path / "store"

    calls = {"dates": []}
    real = _prune_parts_by_ticker

    def spy(parts, ticker_filter):
        calls["dates"].append(Path(parts[0]).name.split(".")[1])
        return real(parts, ticker_filter)

    monkeypatch.setattr(ingest_mod, "_prune_parts_by_ticker", spy)

    # First run: every date is pruned once.
    tse_tick.ingest_period(str(src), str(store), f"{DAYS[0]}-{DAYS[-1]}",
                           "individual_stock", ticker_filter={TICKER})
    assert sorted(calls["dates"]) == sorted(DAYS)

    # Resume: all dates already written -> skipped before pruning -> zero prune calls.
    calls["dates"].clear()
    tse_tick.ingest_period(str(src), str(store), f"{DAYS[0]}-{DAYS[-1]}",
                           "individual_stock", ticker_filter={TICKER}, resume=True)
    assert calls["dates"] == []


def test_resume_prunes_only_remaining_dates(tmp_path, monkeypatch):
    src = tmp_path / "src"
    _seed_days(src)
    store = tmp_path / "store"

    # Pre-ingest the first day only, so a subsequent full-period resume must prune
    # exactly the remaining days.
    tse_tick.ingest_period(str(src), str(store), DAYS[0],
                           "individual_stock", ticker_filter={TICKER})

    calls = {"dates": []}
    real = _prune_parts_by_ticker

    def spy(parts, ticker_filter):
        calls["dates"].append(Path(parts[0]).name.split(".")[1])
        return real(parts, ticker_filter)

    monkeypatch.setattr(ingest_mod, "_prune_parts_by_ticker", spy)
    tse_tick.ingest_period(str(src), str(store), f"{DAYS[0]}-{DAYS[-1]}",
                           "individual_stock", ticker_filter={TICKER}, resume=True)
    # DAYS[0] already written -> not re-pruned; only the remaining days are pruned.
    assert sorted(calls["dates"]) == sorted(DAYS[1:])


# ---------------------------------------------------------------------------
# (c) a partition lands per day: pruning interleaves with writes, so by the time a
#     later day is pruned the earlier day's partition already exists on disk
#     (incremental progress -- not "prune the whole period, then start writing").
# ---------------------------------------------------------------------------
def test_partition_lands_per_day_incrementally(tmp_path, monkeypatch):
    src = tmp_path / "src"
    _seed_days(src)
    store = tmp_path / "store"

    observed = []  # (date being pruned, dates already written to the store)
    real = _prune_parts_by_ticker

    def spy(parts, ticker_filter):
        day = Path(parts[0]).name.split(".")[1]
        written = sorted(d for d in DAYS if (_date_dir(store, d)).exists()
                         and any(_date_dir(store, d).glob("ticker=*.parquet")))
        observed.append((day, written))
        return real(parts, ticker_filter)

    monkeypatch.setattr(ingest_mod, "_prune_parts_by_ticker", spy)
    tse_tick.ingest_period(str(src), str(store), f"{DAYS[0]}-{DAYS[-1]}",
                           "individual_stock", ticker_filter={TICKER})

    # Every day is pruned once, and prior days are already written when each later
    # day is pruned -> pruning is interleaved with per-day writes (incremental).
    assert [d for d, _ in observed] == DAYS
    for i, (_, written) in enumerate(observed):
        assert written == DAYS[:i], f"at prune #{i}, written={written}"
    # and every day ended up with a partition.
    for day in DAYS:
        assert _ticker_rows(store, day, TICKER) == 18, day


def test_query_ticks_store_equals_read_ticks_multiday(tmp_path):
    pytest.importorskip("duckdb")
    src = tmp_path / "src"
    _seed_days(src)
    store = tmp_path / "store"
    tse_tick.ingest_period(str(src), str(store), f"{DAYS[0]}-{DAYS[-1]}",
                           "individual_stock", ticker_filter={TICKER})
    for day in DAYS:
        got = tse_tick.query_ticks(str(store), data_type="individual_stock",
                                   ticker=TICKER, date=day)
        ref = tse_tick.read_ticks(str(src), ticker_filter={TICKER}, date=day)
        assert got.height == ref.height > 0, day
