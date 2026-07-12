"""Integration tests for part-pruning in read_ticks.

Guards that pruning opens only the holding run of parts AND returns byte-identical
results to a full scan — including the code-overflow case (a ticker straddling two
consecutive parts) that the Phase 0 spike found breaks a naive single-part prune.
"""
import tse_tick
from tse_tick.enhanced import _prune_parts_by_ticker
from tests.synthetic_data import individual_stock_csv, write_zip


def _mk(tmp, day, n, codes):
    return write_zip(
        tmp / f"HTICST120.{day}.{n}.zip", f"HTICST120.{day}.{n}.csv",
        individual_stock_csv(day, codes, rows_per_ticker=6))


def test_prune_keeps_only_holding_run_across_two_days(tmp_path):
    # Four parts per day; 7203 sits strictly INSIDE part 2's code range (no part
    # start equals it), so the arithmetic run is exactly part 2 — parts 1 and 3
    # are excluded without opening them; part 4 is the always-kept appendix.
    day_parts = {1: ["1301"], 2: ["7000", "7203", "7500"], 3: ["8001"], 4: ["9999"]}
    zips = [
        _mk(tmp_path, day, n, codes)
        for day in ("20240104", "20240105")
        for n, codes in day_parts.items()
    ]
    kept = sorted(p.name for p in _prune_parts_by_ticker(zips, {"7203"}))
    assert kept == [
        "HTICST120.20240104.2.zip", "HTICST120.20240104.4.zip",
        "HTICST120.20240105.2.zip", "HTICST120.20240105.4.zip",
    ]


def _seed_day(root, day, mapping):   # mapping: {part_no: [codes]}
    leaf = root / f"個別株式{day[:4]}" / "TICST120" / day[:6]
    leaf.mkdir(parents=True, exist_ok=True)
    for n, codes in mapping.items():
        write_zip(leaf / f"HTICST120.{day}.{n}.zip", f"HTICST120.{day}.{n}.csv",
                  individual_stock_csv(day, codes, rows_per_ticker=6))


def test_read_ticks_prune_equals_full(tmp_path):
    _seed_day(tmp_path, "20240104", {1: ["1301"], 2: ["7203"], 3: ["9999"]})
    src = str(tmp_path)
    full = tse_tick.read_ticks(src, ticker_filter={"7203"}, date="20240104", prune_parts=False)
    pruned = tse_tick.read_ticks(src, ticker_filter={"7203"}, date="20240104", prune_parts=True)
    assert pruned.height == full.height > 0
    assert pruned.equals(full)


def test_read_ticks_prune_equals_full_with_overflow(tmp_path):
    # 7203 straddles parts 2 AND 3 — the case that broke the naive single-part prune.
    _seed_day(tmp_path, "20240104", {1: ["1301"], 2: ["7203"], 3: ["7203"], 4: ["9999"]})
    src = str(tmp_path)
    full = tse_tick.read_ticks(src, ticker_filter={"7203"}, date="20240104", prune_parts=False)
    pruned = tse_tick.read_ticks(src, ticker_filter={"7203"}, date="20240104", prune_parts=True)
    assert pruned.height == full.height > 0
    assert pruned.equals(full)


def test_read_ticks_prune_absent_ticker_empty(tmp_path):
    # ticker present in the data-type folder but not that day -> typed-empty, no crash
    _seed_day(tmp_path, "20240104", {1: ["1301"], 2: ["7203"]})
    src = str(tmp_path)
    pruned = tse_tick.read_ticks(src, ticker_filter={"6758"}, date="20240104", prune_parts=True)
    full = tse_tick.read_ticks(src, ticker_filter={"6758"}, date="20240104", prune_parts=False)
    assert pruned.height == full.height == 0
    assert pruned.columns == full.columns   # both typed-empty, same schema


def test_ingest_pruned_store_equals_read_ticks_with_overflow(tmp_path):
    """Ticker-filtered ingest is part-pruned in _ingest_grouped; the store must
    still hold every row (overflow: 7203 in parts 2 AND 3)."""
    import pytest
    pytest.importorskip("duckdb")
    _seed_day(tmp_path / "src", "20240104", {1: ["1301"], 2: ["7203"], 3: ["7203"], 4: ["9999"]})
    src = str(tmp_path / "src")
    store = str(tmp_path / "store")
    tse_tick.ingest_period(src, store, "20240104", "individual_stock", ticker_filter={"7203"})
    got = tse_tick.query_ticks(store, data_type="individual_stock", ticker="7203", date="20240104")
    ref = tse_tick.read_ticks(src, ticker_filter={"7203"}, date="20240104")
    assert got.height == ref.height > 0
