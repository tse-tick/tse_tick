"""Round-16 post-deployment bug-hunt fixes (0.14.1).

Synthetic-first regression tests for four findings against tse-tick 0.14.0:

- **F1 (Major)** — the ``*_summary`` types are daily aggregates with no
  ``Execution Time``; ``start_time``/``end_time`` must be rejected with a clear
  ``ValueError`` at *every* entry point (``read_ticks`` already did — now
  ``query_ticks``, ``_query_extract_batch`` and ``extract_to_store`` too), and
  ``extract_to_store`` must fail **before** any ingest work, not after with a
  raw DuckDB binder error and a partial store on disk.
- **F2 (Minor)** — ``query_ticks`` must emit the same capturable
  ``NoDataWarning`` on a zero-row result that ``read_ticks`` does.
- **F4 (UX)** — an unrecognized ``language`` (``"ja"``, ``"fr"``, …) must raise,
  not silently pass raw undecoded codes through; ``"jp"`` still decodes to
  Japanese.
- **F5 (docs)** — ``discover_zips``'s docstring must mention the legacy 2016
  ``…010`` record-code prefix it actually searches.
"""
import warnings

import pytest

from tse_tick import create_df, read_ticks
from tse_tick.enhanced import NoDataWarning, discover_zips
from tse_tick.ingest import extract_to_store, ingest_single_zip
from tse_tick.query import query_ticks, _query_extract_batch

from tests.synthetic_data import individual_stock_csv, stock_summary_csv, write_zip


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #
@pytest.fixture
def summary_store(tmp_path):
    """A tiny ``stock_summary`` store built through the real ingest pipeline."""
    store = tmp_path / "sumstore"
    for date in ("20240104", "20240105"):
        zp = tmp_path / f"HTICSS110.{date}.1.zip"
        write_zip(zp, f"HTICSS110.{date}.1.csv",
                  stock_summary_csv(date, ["7203", "6758"], time_value="090005000000"))
        ingest_single_zip(str(zp), str(store), data_type="stock_summary", year=2024)
    return str(store)


@pytest.fixture
def stock_zip(tmp_path):
    """A single-part ``individual_stock`` ZIP for the one-shot read paths."""
    zp = tmp_path / "HTICST120.20240104.1.zip"
    write_zip(zp, "HTICST120.20240104.1.csv",
              individual_stock_csv("20240104", ["7203"], rows_per_ticker=4))
    return zp


# --------------------------------------------------------------------------- #
# F1 — summary types reject intraday time filters at every entry point
# --------------------------------------------------------------------------- #
def test_f1_guard_helper_only_fires_for_summary_types():
    from tse_tick.constants import validate_time_filter_support

    # Both summary types raise; the message names the type and points at 'date'.
    for dt in ("stock_summary", "indices_summary"):
        with pytest.raises(ValueError, match="not supported"):
            validate_time_filter_support(dt, "09:00:00", None)
        with pytest.raises(ValueError, match="not supported"):
            validate_time_filter_support(dt, None, "15:00:00")
    # Tick types and the no-filter case are untouched.
    validate_time_filter_support("individual_stock", "09:00:00", "15:00:00")
    validate_time_filter_support("indices", "09:00:00", "15:00:00")
    validate_time_filter_support("stock_summary", None, None)


def test_f1_query_ticks_summary_time_filter_raises(summary_store):
    # Was: _duckdb.BinderException leaking internal column names.
    with pytest.raises(ValueError, match="not supported"):
        query_ticks(summary_store, data_type="stock_summary", ticker="7203",
                    start_time="09:00:00", end_time="15:00:00")


def test_f1_query_extract_batch_summary_time_filter_raises(summary_store):
    with pytest.raises(ValueError, match="not supported"):
        _query_extract_batch(summary_store, "stock_summary", {"7203"},
                             date_from="20240104", date_to="20240105",
                             start_time="09:00:00", end_time="15:00:00")


def test_f1_extract_to_store_summary_time_filter_raises_before_ingest(tmp_path):
    """The guard must fire upfront — no ingest work, no store written."""
    out = tmp_path / "out"
    with pytest.raises(ValueError, match="not supported"):
        extract_to_store(str(tmp_path / "empty_root"), str(out), "202401", "7203",
                         data_type="stock_summary", start_time="09:00:00")
    # Nothing was ingested: the store dir was never populated.
    assert not out.exists() or not any(out.rglob("*.parquet"))


def test_f1_summary_without_time_filter_still_works(summary_store):
    df = query_ticks(summary_store, data_type="stock_summary", ticker="7203")
    assert df.height > 0
    df2 = _query_extract_batch(summary_store, "stock_summary", {"7203"},
                               date_from="20240104", date_to="20240105")
    assert df2.height > 0


def test_f1_read_ticks_summary_message_is_unified(tmp_path):
    # read_ticks keeps raising (via the shared helper); the message is the
    # unified one shared with the store paths.
    with pytest.raises(ValueError, match="not supported for 'indices_summary'"):
        read_ticks(str(tmp_path / "nope.zip"), data_type="indices_summary",
                   start_time="09:00:00")


# --------------------------------------------------------------------------- #
# F2 — query_ticks emits NoDataWarning on zero rows, like read_ticks
# --------------------------------------------------------------------------- #
def test_f2_query_ticks_warns_on_unknown_ticker(stock_store):
    with pytest.warns(NoDataWarning):
        df = query_ticks(stock_store, ticker=9999)
    assert df.height == 0
    assert df.width > 0  # typed-empty, columns intact


def test_f2_query_ticks_warns_on_absent_date(stock_store):
    with pytest.warns(NoDataWarning):
        df = query_ticks(stock_store, ticker=7203, date="20200101")
    assert df.height == 0


def test_f2_query_ticks_populated_read_does_not_warn(stock_store):
    with warnings.catch_warnings():
        warnings.simplefilter("error", NoDataWarning)  # any NoDataWarning -> failure
        df = query_ticks(stock_store, ticker=7203, date="20230704")
    assert df.height > 0


def test_f2_query_ticks_warning_is_capturable(stock_store):
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        query_ticks(stock_store, ticker=9999)
    assert any(isinstance(w.message, NoDataWarning) for w in rec)


# --------------------------------------------------------------------------- #
# F4 — language is validated; 'jp' (not 'ja') is the Japanese value
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("bad", ["ja", "fr", "JP", "english", ""])
def test_f4_invalid_language_raises_create_df(stock_zip, bad):
    with pytest.raises(ValueError, match="Unknown language"):
        create_df(str(stock_zip), auto_detect=False, data_type="individual_stock",
                  year=2024, language=bad)


def test_f4_invalid_language_raises_read_ticks(stock_zip):
    with pytest.raises(ValueError, match="Unknown language"):
        read_ticks(str(stock_zip), ticker_filter={"7203"}, language="ja")


def test_f4_valid_languages_still_work(stock_zip):
    en = create_df(str(stock_zip), auto_detect=False, data_type="individual_stock",
                   year=2024, language="en")
    jp = create_df(str(stock_zip), auto_detect=False, data_type="individual_stock",
                   year=2024, language="jp")
    # 'en' decodes to English display strings...
    assert "Tokyo Stock Exchange (TSE)" in en["Exchange Code"].to_list()
    # ...'jp' renames headers to Japanese AND decodes values to Japanese.
    assert "取引所コード" in jp.columns
    assert "東証" in jp["取引所コード"].to_list()


# --------------------------------------------------------------------------- #
# F5 — discover_zips docstring documents the legacy 2016 …010 prefix
# --------------------------------------------------------------------------- #
def test_f5_discover_zips_docstring_mentions_legacy_prefix():
    doc = discover_zips.__doc__ or ""
    assert "010" in doc
