# tests/test_round17_fixes.py
"""Round-17 fixes — package-integrity + real-data bug-hunt batch (shipped 0.14.2).

Covers the five open items from the hand-off bug report:

- **A1** (packaging): the declared dependency floors were far below what the code
  actually uses (``pl.String``, ``list.get(null_on_oob=)``,
  ``read_csv(schema_overrides=)``, the partitioned-parquet writer, DuckDB's
  hive-partition behaviour). Pins ``polars>=1.0.0`` / ``duckdb>=1.1.0`` so they
  are not quietly lowered again.
- **B1** (major): ``read_ticks(individual_stock, ticker_filter=set())`` silently
  returned the whole unfiltered market. An empty set now matches *nothing* — a
  typed-empty frame + ``NoDataWarning`` — mirroring the ``indices`` sibling.
- **B2** (cosmetic): ``OneShotMemoryError`` rendered a sub-GB limit as ``"0 GB"``.
- **A2** (hardening): a stray Hive ``ticker`` column (older DuckDB derived one from
  the ``ticker=NNNN.parquet`` *filename*) is defensively dropped.
- **B3** (UX): ``query_ticks`` / ``get_available_tickers`` now accept the same
  flexible date forms as ``read_ticks`` / ``ingest_period`` (``YYYY`` / ``YYYYMM``
  / ``YYYYMMDD`` / ``start-end``), not just a single exact day.

Synthetic NEEDS-format data only — no proprietary NEEDS files.
"""
import re
from pathlib import Path

import polars as pl
import pytest

import tse_tick
from tse_tick import create_df, read_ticks
from tse_tick.enhanced import _oneshot_limit_message
from tests.synthetic_data import individual_stock_csv, indices_csv, write_zip

_BASE = {"7203": 2100, "6758": 13000}


# --------------------------------------------------------------------------- #
# A1 — declared dependency floors stay >= what the code requires
# --------------------------------------------------------------------------- #
def _declared_floor(text: str, pkg: str):
    m = re.search(rf"{pkg}>=(\d+)\.(\d+)(?:\.(\d+))?", text)
    assert m, f"no declared >= floor for {pkg!r} in pyproject.toml"
    return tuple(int(g or 0) for g in m.groups())


def test_a1_dependency_floors_enforced():
    """polars>=1.0.0 (pl.String, list.get(null_on_oob=), read_csv(schema_overrides=),
    the partitioned-parquet writer) and duckdb>=1.1.0 (hive partitioning that does
    not leak the ticker= filename key) are the real floors — guard against a
    re-lowering (report A1)."""
    text = (
        Path(__file__).resolve().parents[1].joinpath("pyproject.toml").read_text(encoding="utf-8")
    )
    assert _declared_floor(text, "polars") >= (1, 0, 0)
    assert _declared_floor(text, "duckdb") >= (1, 1, 0)


# --------------------------------------------------------------------------- #
# B1 — an empty ticker_filter matches NOTHING (not the whole market)
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def stock_zip(tmp_path_factory):
    d = tmp_path_factory.mktemp("r17_stock")
    zp = d / "HTICST120.20230704.1.zip"
    write_zip(
        zp,
        "HTICST120.20230704.1.csv",
        individual_stock_csv("20230704", ["7203", "6758"], rows_per_ticker=20, base_prices=_BASE),
    )
    return zp


def test_b1_individual_stock_empty_set_matches_nothing(stock_zip):
    """The core bug: an empty set fell through the truthiness gates to the
    'no filter' branch and returned every row. It must match nothing."""
    with pytest.warns(tse_tick.NoDataWarning, match=r"ticker_filter=\[\]"):
        df = read_ticks(str(stock_zip), data_type="individual_stock", ticker_filter=set())
    assert df.shape == (0, 95)


def test_b1_symmetry_with_indices(stock_zip, tmp_path_factory):
    """individual_stock's empty-set behaviour now matches the indices sibling:
    both return 0 rows + a NoDataWarning naming ``ticker_filter=[]``."""
    d = tmp_path_factory.mktemp("r17_idx")
    izp = d / "HTICIT110.20230704.1.zip"
    write_zip(izp, "HTICIT110.20230704.1.csv", indices_csv("20230704", ["101"], rows_per_code=8))
    with pytest.warns(tse_tick.NoDataWarning, match=r"ticker_filter=\[\]"):
        sdf = read_ticks(str(stock_zip), data_type="individual_stock", ticker_filter=set())
    with pytest.warns(tse_tick.NoDataWarning, match=r"ticker_filter=\[\]"):
        idf = read_ticks(str(izp), data_type="indices", ticker_filter=set())
    assert sdf.height == 0 and idf.height == 0


def test_b1_create_df_empty_set_is_typed_empty(stock_zip):
    """create_df (under read_ticks) returns a fully-typed empty frame for an empty
    filter — not a raised 'No data was successfully read' (the third truthiness
    gate, at enhanced.py get_1y_dataframe, that the report did not name)."""
    df = create_df(
        str(stock_zip),
        data_type="individual_stock",
        year=2023,
        auto_detect=False,
        ticker_filter=set(),
    )
    assert df.shape == (0, 95)


def test_b1_nonempty_filter_unaffected(stock_zip):
    """Regression: a normal non-empty filter still selects exactly its rows."""
    df = read_ticks(str(stock_zip), data_type="individual_stock", ticker_filter={"7203"})
    codes = set(df["Stock Code"].cast(pl.String).str.strip_chars().str.slice(0, 4).to_list())
    assert df.height == 20 and codes == {"7203"}


# --------------------------------------------------------------------------- #
# B2 — a sub-GB one-shot limit does not render as "0 GB"
# --------------------------------------------------------------------------- #
def test_b2_small_limit_not_rendered_as_zero_gb():
    msg = _oneshot_limit_message(150_000_000, 1000)
    assert "0 GB" not in msg
    assert "1000 B" in msg  # the tiny limit shown in bytes, not rounded away
    assert "one-shot limit" in msg  # unchanged tail the existing alpha tests match on


def test_b2_gb_scale_limit_still_reads_in_gb():
    msg = _oneshot_limit_message(6 * 1024**3, 5 * 1024**3)
    assert "5 GB" in msg and "6 GB" in msg


# --------------------------------------------------------------------------- #
# A2 — a stray Hive ticker= filename column is dropped defensively
# --------------------------------------------------------------------------- #
def test_a2_drop_partition_ticker_column_helper():
    from tse_tick.query import _drop_partition_ticker_column

    leaked = pl.DataFrame({"Stock Code": ["7203"], "ticker": ["7203"], "date": [20230704]})
    out = _drop_partition_ticker_column(leaked)
    assert "ticker" not in out.columns
    assert out.columns == ["Stock Code", "date"]
    # A frame without the stray column is returned unchanged (no output schema has
    # a literal 'ticker' column — codes are Stock Code / Index Code).
    clean = pl.DataFrame({"Stock Code": ["7203"], "date": [20230704]})
    assert _drop_partition_ticker_column(clean).columns == ["Stock Code", "date"]


# --------------------------------------------------------------------------- #
# B3 — query_ticks / get_available_tickers accept flexible date forms
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def multi_month_store(tmp_path_factory):
    pytest.importorskip("duckdb")
    from tse_tick.ingest import ingest_single_zip

    raw = tmp_path_factory.mktemp("r17_raw")
    store = tmp_path_factory.mktemp("r17_store")
    # Two July days and one August day of the same two codes, so a month / range
    # query must include or exclude the right days (not the whole store).
    for date in ("20230703", "20230704", "20230801"):
        payload = individual_stock_csv(
            date, ["7203", "6758"], rows_per_ticker=10, base_prices=_BASE
        )
        zp = raw / f"HTICST120.{date}.1.zip"
        write_zip(zp, f"HTICST120.{date}.1.csv", payload)
        ingest_single_zip(str(zp), str(store), data_type="individual_stock", year=2023)
    return str(store)


def _dates(df):
    return set(str(d) for d in df["date"].to_list())


def test_b3_query_ticks_exact_day_unchanged(multi_month_store):
    df = tse_tick.query_ticks(multi_month_store, ticker=7203, date="20230703")
    assert df.height == 10 and _dates(df) == {"20230703"}


def test_b3_query_ticks_month(multi_month_store):
    df = tse_tick.query_ticks(multi_month_store, ticker=7203, date="202307")
    assert _dates(df) == {"20230703", "20230704"}  # excludes August
    assert df.height == 20


def test_b3_query_ticks_year(multi_month_store):
    df = tse_tick.query_ticks(multi_month_store, ticker=7203, date="2023")
    assert _dates(df) == {"20230703", "20230704", "20230801"}


def test_b3_query_ticks_range(multi_month_store):
    df = tse_tick.query_ticks(multi_month_store, ticker=7203, date="20230704-20230801")
    assert _dates(df) == {"20230704", "20230801"}  # excludes 0703


def test_b3_get_available_tickers_flexible(multi_month_store):
    gat = tse_tick.get_available_tickers
    assert set(gat(multi_month_store, date="202307")) == {"6758", "7203"}
    assert set(gat(multi_month_store, date="202308")) == {"6758", "7203"}
    assert set(gat(multi_month_store, date="20230703")) == {"6758", "7203"}
    assert set(gat(multi_month_store, date="2023")) == {"6758", "7203"}


def test_b3_invalid_date_still_raises(multi_month_store):
    with pytest.raises(ValueError):
        tse_tick.query_ticks(multi_month_store, ticker=7203, date="2023-07")
    with pytest.raises(ValueError):
        tse_tick.query_ticks(multi_month_store, ticker=7203, date="notadate")
    with pytest.raises(ValueError):
        tse_tick.get_available_tickers(multi_month_store, date="2023-07")
