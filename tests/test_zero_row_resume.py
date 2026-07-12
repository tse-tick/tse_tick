"""Resume behavior for dates that yield zero rows for the requested filter.

A filtered ingest of a day on which the ticker never traded used to write
nothing at all — no partition, no coverage marker — so every resumed run
re-probed and re-scanned that day's parts forever. A cleanly-read zero-row day
now records its coverage in the marker (in an otherwise-empty date dir) and
resume skips it; a day that LOST parts still writes no marker and stays fully
re-ingestable.
"""
import json
import zipfile

import pytest

import tse_tick
import tse_tick.ingest as ingest_mod
from tse_tick.ingest import _COVERAGE_MARKER
from tests.synthetic_data import seed_structured_day

pytest.importorskip("duckdb")

_DAY1, _DAY2 = "20240104", "20240105"


def _seed_two_days(root):
    # Day 1 holds the ticker; day 2's parts hold only an unrelated code.
    seed_structured_day(root, _DAY1, {1: ["1301"], 2: ["7203"]})
    seed_structured_day(root, _DAY2, {1: ["9999"], 2: ["9999"]})


def _spy_prunes(monkeypatch):
    calls = []
    real = ingest_mod._prune_parts_by_ticker

    def spy(zips, tickers):
        calls.append([str(z) for z in zips])
        return real(zips, tickers)

    monkeypatch.setattr(ingest_mod, "_prune_parts_by_ticker", spy)
    return calls


def test_zero_row_day_writes_marker_and_resume_skips(tmp_path, monkeypatch):
    _seed_two_days(tmp_path / "src")
    src, store = str(tmp_path / "src"), str(tmp_path / "store")
    period = f"{_DAY1}-{_DAY2}"

    tse_tick.ingest_period(src, store, period, "individual_stock", ticker_filter={"7203"})

    day2_dir = tmp_path / "store" / "individual_stock" / f"date={_DAY2}"
    marker = json.loads((day2_dir / _COVERAGE_MARKER).read_text())
    assert marker == {"full": False, "tickers": ["7203"], "complete": True}
    assert list(day2_dir.glob("*.parquet")) == []  # marker only, no data files

    calls = _spy_prunes(monkeypatch)
    results = tse_tick.ingest_period(
        src, store, period, "individual_stock", ticker_filter={"7203"}
    )
    assert calls == [], "resumed run must not re-prune (or re-read) any day"
    assert results == []  # both days resume-skipped


def test_marker_accumulates_and_other_ticker_reingests(tmp_path, monkeypatch):
    _seed_two_days(tmp_path / "src")
    src, store = str(tmp_path / "src"), str(tmp_path / "store")

    tse_tick.ingest_period(src, store, _DAY2, "individual_stock", ticker_filter={"7203"})
    # A different ticker is NOT covered by the zero-row marker -> re-ingested once.
    calls = _spy_prunes(monkeypatch)
    tse_tick.ingest_period(src, store, _DAY2, "individual_stock", ticker_filter={"9984"})
    assert len(calls) == 1

    day2_dir = tmp_path / "store" / "individual_stock" / f"date={_DAY2}"
    marker = json.loads((day2_dir / _COVERAGE_MARKER).read_text())
    assert marker["tickers"] == ["7203", "9984"]  # coverage accumulated

    # The union request is now fully covered -> skip.
    calls.clear()
    tse_tick.ingest_period(
        src, store, _DAY2, "individual_stock", ticker_filter={"7203", "9984"}
    )
    assert calls == []


def test_lost_parts_day_writes_no_marker_and_reingests(tmp_path):
    _seed_two_days(tmp_path / "src")
    # Corrupt one of day 2's parts: the day loses parts, so it must stay
    # re-ingestable — no marker.
    leaf = tmp_path / "src" / "個別株式2024" / "TICST120" / _DAY2[:6]
    bad = leaf / f"HTICST120.{_DAY2}.1.zip"
    bad.write_bytes(b"not a zip")
    src, store = str(tmp_path / "src"), str(tmp_path / "store")

    results = tse_tick.ingest_period(
        src, store, _DAY2, "individual_stock", ticker_filter={"7203"}
    )
    assert any(r.get("errors") for r in results)
    day2_dir = tmp_path / "store" / "individual_stock" / f"date={_DAY2}"
    assert not (day2_dir / _COVERAGE_MARKER).exists()

    # Repair the part: the resumed run re-ingests the day (no marker to skip on).
    seed_structured_day(tmp_path / "src", _DAY2, {1: ["9999"]})
    results = tse_tick.ingest_period(
        src, store, _DAY2, "individual_stock", ticker_filter={"7203"}
    )
    assert results and "errors" not in results[0]
    assert (day2_dir / _COVERAGE_MARKER).exists()


def test_legacy_markerless_empty_dir_still_reingests(tmp_path, monkeypatch):
    _seed_two_days(tmp_path / "src")
    src, store = str(tmp_path / "src"), str(tmp_path / "store")

    tse_tick.ingest_period(src, store, _DAY2, "individual_stock", ticker_filter={"7203"})
    day2_dir = tmp_path / "store" / "individual_stock" / f"date={_DAY2}"
    (day2_dir / _COVERAGE_MARKER).unlink()  # simulate a legacy (pre-marker) store

    calls = _spy_prunes(monkeypatch)
    tse_tick.ingest_period(src, store, _DAY2, "individual_stock", ticker_filter={"7203"})
    assert len(calls) == 1, "a marker-less empty date dir must re-ingest (legacy rule)"


def test_zero_row_day_not_listed_and_query_shape_unchanged(tmp_path):
    _seed_two_days(tmp_path / "src")
    src, store = str(tmp_path / "src"), str(tmp_path / "store")
    tse_tick.ingest_period(
        src, store, f"{_DAY1}-{_DAY2}", "individual_stock", ticker_filter={"7203"}
    )
    # The marker-only day is not a trading day of this store...
    assert tse_tick.get_available_dates(store) == [_DAY1]
    # ...and querying it returns the usual typed-empty shape.
    df = tse_tick.query_ticks(store, ticker="7203", date=_DAY2)
    assert df.height == 0
