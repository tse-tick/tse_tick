# tests/test_alpha_fixes.py
"""Regression tests for the alpha-test bug report.

Bug #1 — One-shot OOM on large multi-part ``individual_stock`` days.
    ``create_df`` accumulates every numbered part of a day in memory before the
    final concat. The existing 5 GB guard is *per ZIP entry*, so it can't see
    memory adding up across the parts (a normal day is ~9 parts / tens of
    millions of rows), and the eventual failure is an uncatchable Polars
    ``PanicException`` (a ``BaseException``, not an ``Exception``). The fix tracks
    the cumulative decompressed size and raises a catchable ``MemoryError`` before
    the load, and converts any Polars panic during the read into a ``MemoryError``.

Bug #2 — ``query_ticks`` silently truncates at the row limit (covered below).

Bug #3 — ``create_df`` ignores an explicit ``year=`` under ``auto_detect=True``
    (covered below).
"""
import warnings

import pytest
import polars as pl

import tse_tick.enhanced as enhanced
from tse_tick import create_df, read_ticks, TruncationWarning, OneShotMemoryError
from tse_tick.enhanced import detect_data_type_and_year
from tse_tick.query import query_ticks
from tests.synthetic_data import (
    individual_stock_csv,
    indices_csv,
    stock_summary_csv,
    write_zip,
)


# --------------------------------------------------------------------------- #
# Bug #1 — cumulative decompressed-size guard + panic-to-MemoryError conversion
# --------------------------------------------------------------------------- #
def _make_parts(folder, date, n_parts, rows_per_ticker=10):
    """Write ``n_parts`` numbered TICST120 ZIPs for one day; return each part's
    decompressed member size (identical across parts)."""
    payload = individual_stock_csv(date, ["7203"], rows_per_ticker=rows_per_ticker)
    for part in range(1, n_parts + 1):
        write_zip(
            folder / f"HTICST120.{date}.{part}.zip",
            f"HTICST120.{date}.{part}.csv",
            payload,
        )
    return len(payload)


def test_bug1_cumulative_size_guard_raises_memoryerror(tmp_path, monkeypatch):
    """Cumulative size across parts crossing the ceiling raises a clear, catchable
    MemoryError *before* the load — not an uncatchable Polars panic."""
    part_bytes = _make_parts(tmp_path, "20230703", n_parts=3)

    # Ceiling just above ONE part, so a single part is fine but the cumulative
    # size of three parts blows past it — exactly the accumulation the per-entry
    # guard misses.
    monkeypatch.setattr(enhanced, "_MAX_ONESHOT_DECOMPRESSED_BYTES", part_bytes + 10)

    with pytest.raises(MemoryError, match="one-shot limit"):
        create_df(
            str(tmp_path), auto_detect=False, data_type="individual_stock", year=2023
        )


def test_bug1_memoryerror_points_to_two_stage_path(tmp_path, monkeypatch):
    """The raised error names the two-stage escape hatch (ingest + query)."""
    part_bytes = _make_parts(tmp_path, "20230703", n_parts=2)
    monkeypatch.setattr(enhanced, "_MAX_ONESHOT_DECOMPRESSED_BYTES", part_bytes + 10)

    with pytest.raises(MemoryError, match="ingest_single_zip.*query_ticks"):
        create_df(
            str(tmp_path), auto_detect=False, data_type="individual_stock", year=2023
        )


def test_bug1_single_part_under_ceiling_still_reads(tmp_path, monkeypatch):
    """A single part under the ceiling must still load — the guard is cumulative,
    not a blanket block."""
    part_bytes = _make_parts(tmp_path, "20230703", n_parts=1, rows_per_ticker=6)
    monkeypatch.setattr(enhanced, "_MAX_ONESHOT_DECOMPRESSED_BYTES", part_bytes + 10)

    df = create_df(
        str(tmp_path), auto_detect=False, data_type="individual_stock", year=2023
    )
    assert df.height == 6


def test_bug1_real_polars_panic_becomes_catchable_oneshot_error(tmp_path, monkeypatch):
    """A *real* Polars ``PanicException`` during the read is converted into a
    catchable ``OneShotMemoryError`` (a ``MemoryError``) instead of tearing the
    process down as an uncatchable ``BaseException``."""
    from polars.exceptions import PanicException

    # The load-bearing assumption the whole guard rests on: a panic is a
    # BaseException, NOT an Exception, so a plain `except Exception` can't catch it.
    assert issubclass(PanicException, BaseException)
    assert not issubclass(PanicException, Exception)

    _make_parts(tmp_path, "20230703", n_parts=1, rows_per_ticker=6)

    def _boom(*args, **kwargs):
        raise PanicException("simulated polars OOM panic")

    monkeypatch.setattr(enhanced.pl, "read_csv", _boom)

    # OneShotMemoryError IS a MemoryError (an Exception), so the panic is now
    # catchable via ordinary `except MemoryError` (it was not before the fix).
    with pytest.raises(OneShotMemoryError):
        create_df(
            str(tmp_path), auto_detect=False, data_type="individual_stock", year=2023
        )


# --------------------------------------------------------------------------- #
# Bug #2 — query_ticks must warn (not silently truncate) when it hits the limit
# --------------------------------------------------------------------------- #
def test_bug2_query_ticks_warns_when_result_hits_limit(stock_store):
    """Hitting ``limit`` emits a capturable TruncationWarning (the store holds far
    more than 10 rows), the same signal read_ticks already gives on its row cap."""
    with pytest.warns(TruncationWarning, match="truncated at 10 rows"):
        df = query_ticks(stock_store, "individual_stock", limit=10)
    assert df.height == 10


def test_bug2_query_ticks_truncationwarning_is_the_shared_class(stock_store):
    """It reuses the exact class read_ticks uses, so one filter silences both."""
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        query_ticks(stock_store, "individual_stock", limit=10)
    assert any(isinstance(w.message, TruncationWarning) for w in rec)
    assert issubclass(TruncationWarning, UserWarning)


def test_bug2_query_ticks_no_warn_when_under_limit(stock_store):
    """A result smaller than ``limit`` must not warn (one ticker/day < limit)."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", TruncationWarning)  # any -> test failure
        df = query_ticks(
            stock_store, "individual_stock", ticker=7203, date="20230704", limit=10_000
        )
    assert 0 < df.height < 10_000


def test_bug2_query_ticks_no_warn_when_limit_none(stock_store):
    """``limit=None`` means 'all rows' — never a truncation, never a warning."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", TruncationWarning)
        df = query_ticks(stock_store, "individual_stock", limit=None)
    assert df.height > 10


# --------------------------------------------------------------------------- #
# Bug #3 — create_df must honor an explicit year=/data_type= under auto_detect
# --------------------------------------------------------------------------- #
def test_bug3_explicit_year_honored_when_path_has_no_year(tmp_path):
    """A correctly-named ZIP in a folder with no year token must read when year=
    is passed, even under the default auto_detect=True (data_type still detected)."""
    date = "20230703"
    folder = tmp_path / "individual_stock_no_year_here"  # type hint, but NO 20xx
    folder.mkdir()
    write_zip(
        folder / f"HTICST120.{date}.1.zip",
        f"HTICST120.{date}.1.csv",
        individual_stock_csv(date, ["7203"], rows_per_ticker=4),
    )

    # Precondition: auto-detection genuinely can't find a year in this folder path,
    # so the old behavior raised even with a valid year passed explicitly.
    with pytest.raises(ValueError, match="detect year"):
        detect_data_type_and_year(str(folder))

    # The fix: explicit year wins; data_type is still auto-detected from the path.
    df = create_df(str(folder), year=2023)
    assert df.height == 4


def test_bug3_explicit_year_overrides_detected_year(tmp_path):
    """An explicit year must win over a *different* year present in the path.

    The ZIP is a 2017+ (23-col quoted) index file living in a folder named
    ``2016``; if the path's 2016 were used, the 2016 fixed-width parser would
    mangle it. Passing year=2023 must parse it as the 2017+ format."""
    date = "20230704"
    folder = tmp_path / "TICIT110" / "2016"  # misleading 2016 in the path
    folder.mkdir(parents=True)
    write_zip(
        folder / f"HTICIT110.{date}.1.zip",
        f"HTICIT110.{date}.1.csv",
        indices_csv(date, ["101"], rows_per_code=4),
    )

    df = create_df(str(folder), year=2023)  # data_type auto-detected as "indices"
    assert df.height == 4


def test_bug3_explicit_data_type_overrides_detected_type(tmp_path):
    """An explicit data_type must win over the one auto-detected from the path.

    Summary (83-col) data sits in a folder named to look like individual_stock;
    without honoring the explicit data_type the summary frame is parsed as
    individual_stock and the column-count check raises."""
    date = "20230508"
    folder = tmp_path / "individual_stock_2023"  # path screams individual_stock
    folder.mkdir()
    write_zip(
        folder / f"HTICSS110.{date}.zip",
        f"HTICSS110.{date}.csv",
        stock_summary_csv(date, ["7203"]),
    )

    df = create_df(str(folder), data_type="stock_summary")  # year auto-detected
    assert df.height == 1
    assert "Stock Code" in df.columns


def test_bug3_full_auto_detect_still_works(tmp_path):
    """Regression guard: passing neither year nor data_type still auto-detects both
    from a normally-named path."""
    date = "20230703"
    folder = tmp_path / "individual_stock" / "2023" / "202307"
    folder.mkdir(parents=True)
    write_zip(
        folder / f"HTICST120.{date}.1.zip",
        f"HTICST120.{date}.1.csv",
        individual_stock_csv(date, ["7203"], rows_per_ticker=4),
    )

    df = create_df(str(folder))  # both auto-detected
    assert df.height == 4


# --------------------------------------------------------------------------- #
# Alpha-review rework — regressions for the code-review findings on PR #28
# --------------------------------------------------------------------------- #
def test_bug1_ticker_filter_fast_path_exempt_from_size_guard(tmp_path, monkeypatch):
    """The bounded individual_stock ticker fast path keeps only matching lines, so
    the cumulative size guard must NOT block it even when the parts' total
    decompressed size dwarfs the ceiling (review finding 4)."""
    part_bytes = _make_parts(tmp_path, "20230703", n_parts=3, rows_per_ticker=10)
    # A ceiling below even one part would trip a full-frame read immediately.
    monkeypatch.setattr(enhanced, "_MAX_ONESHOT_DECOMPRESSED_BYTES", part_bytes // 2)

    df = create_df(
        str(tmp_path), auto_detect=False, data_type="individual_stock",
        year=2023, ticker_filter={"7203"},
    )
    assert df.height == 30  # 3 parts x 10 rows of 7203, all kept, no MemoryError


def test_bug1_max_oneshot_bytes_none_disables_guard(tmp_path, monkeypatch):
    """``max_oneshot_bytes=None`` disables the ceiling even when the module default
    is tiny — the opt-out the review asked for (Q2)."""
    _make_parts(tmp_path, "20230703", n_parts=3, rows_per_ticker=8)
    part_bytes = len(individual_stock_csv("20230703", ["7203"], rows_per_ticker=8))
    monkeypatch.setattr(enhanced, "_MAX_ONESHOT_DECOMPRESSED_BYTES", part_bytes // 2)

    df = create_df(
        str(tmp_path), auto_detect=False, data_type="individual_stock",
        year=2023, max_oneshot_bytes=None,
    )
    assert df.height == 24  # 3 parts x 8 rows, fully loaded despite the tiny default


def test_bug1_max_oneshot_bytes_custom_value_trips(tmp_path):
    """A custom (low) ``max_oneshot_bytes`` trips without monkeypatching the default."""
    part_bytes = _make_parts(tmp_path, "20230703", n_parts=3, rows_per_ticker=8)
    with pytest.raises(OneShotMemoryError, match="one-shot limit"):
        create_df(
            str(tmp_path), auto_detect=False, data_type="individual_stock",
            year=2023, max_oneshot_bytes=part_bytes + 10,
        )


def test_bug1_ingest_reraises_oneshot_oom_not_partial_write(tmp_path, monkeypatch):
    """A one-shot OOM during ingest must ABORT (raise), not be swallowed by ingest's
    broad ``except Exception`` into a silent partial-day write (review finding 1)."""
    from polars.exceptions import PanicException
    from tse_tick.ingest import _ingest_date_group

    date = "20230703"
    _make_parts(tmp_path, date, n_parts=1, rows_per_ticker=6)
    zip_path = tmp_path / f"HTICST120.{date}.1.zip"

    def _boom(*args, **kwargs):
        raise PanicException("simulated polars OOM panic")

    monkeypatch.setattr(enhanced.pl, "read_csv", _boom)

    store = tmp_path / "store"
    with pytest.raises(OneShotMemoryError):
        _ingest_date_group(
            date, [str(zip_path)], str(store), "individual_stock", 2023, "en", None
        )


def test_bug1_read_ticks_one_shot_path_is_guarded(tmp_path):
    """read_ticks accumulates one frame per ZIP and is now guarded: a no-filter read
    whose cumulative size crosses the ceiling raises OneShotMemoryError before the
    concat can OOM (review finding 5)."""
    date = "20230703"
    part_bytes = _make_parts(tmp_path, date, n_parts=3, rows_per_ticker=10)
    with pytest.raises(OneShotMemoryError, match="one-shot limit"):
        read_ticks(
            str(tmp_path), data_type="individual_stock",
            max_oneshot_bytes=part_bytes + 10,
        )


def test_bug1_read_ticks_ticker_filter_exempt_from_guard(tmp_path):
    """read_ticks' individual_stock ticker fast path is bounded, so the ceiling does
    not apply even on a big multi-part day (review findings 4 + 5)."""
    date = "20230703"
    part_bytes = _make_parts(tmp_path, date, n_parts=3, rows_per_ticker=10)
    df = read_ticks(
        str(tmp_path), data_type="individual_stock", ticker_filter={"7203"},
        max_oneshot_bytes=part_bytes // 2,  # below one part; would trip a full read
    )
    assert df.height == 30


def test_bug2_query_ticks_no_warn_on_exact_fit(stock_store):
    """A result that exactly fills ``limit`` with nothing dropped must NOT warn — the
    limit+1 probe distinguishes exact-fit from real truncation (review finding 7)."""
    full = query_ticks(
        stock_store, "individual_stock", ticker=7203, date="20230704", limit=None
    )
    n = full.height
    assert n > 0
    with warnings.catch_warnings():
        warnings.simplefilter("error", TruncationWarning)  # any warning -> failure
        df = query_ticks(
            stock_store, "individual_stock", ticker=7203, date="20230704", limit=n
        )
    assert df.height == n
