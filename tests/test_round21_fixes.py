"""Round-21 (issue #65): the materialized ``Effective Time`` key.

``query_ticks`` / ``_query_extract_batch`` filtered and ordered ``individual_stock``
on a CASE over two columns (``Execution Time``, falling back to
``substr(Update Time, 1, 6)`` for the quote-only book rows that are ~94% of a
liquid day). A scalar expression cannot be matched against Parquet row-group
min/max statistics, so a time window read every row group of every selected file.

The fix stores that value as an ``Int32`` column at write time. The change is
**additive**: stores written before it keep working on the CASE fallback, so no
re-ingest is required. The column is an internal index — it is EXCLUDEd from
every documented output, so ``individual_stock`` keeps its locked 95 columns.

Measured against a pre-#65 store on real NEEDS data (7203 + 9984, 2025-04-09;
the 7203 day is 2,564,238 rows in 19 row groups), interleaved A/B: 1-minute
slice 7.64x, 09:00-09:05 window 5.65x, README 09:00-11:30 session window 1.27x,
unfiltered whole day unchanged (0.9 sigma), for +0.52% store bytes. All six
real-data filter shapes returned identical frames to the pre-#65 store.
"""

from pathlib import Path

import polars as pl
import pytest

import tse_tick
from tse_tick.io.parquet import (
    EFFECTIVE_TIME_COL,
    PartitionedParquetAppender,
    _add_effective_time,
    write_partitioned_parquet,
)
from tse_tick.ingest import ingest_single_zip
from tse_tick.query import _query_extract_batch, _store_has_effective_time, query_ticks
from tests.synthetic_data import individual_stock_csv, write_zip

# Field 6 of the 95-field TICST120 layout is Execution Time (0-based; the byte
# filter already keys on field 5, Stock Code).
_EXEC_TIME_FIELD = 6


def _make_quote_only(payload: bytes, every: int = 2) -> bytes:
    """Blank Execution Time on every Nth row, leaving Update Time intact.

    Reproduces the quote-only order-book rows that dominate a real liquid day —
    the rows the Update Time fallback exists for (0.9.0). The synthetic generator
    emits none, so a store built from it alone would never exercise the fallback.
    """
    lines = payload.decode("ascii").rstrip("\n").split("\n")
    out = []
    for i, line in enumerate(lines):
        if i % every == 0:
            fields = line.split(",")
            fields[_EXEC_TIME_FIELD] = '""'
            line = ",".join(fields)
        out.append(line)
    return ("\n".join(out) + "\n").encode("ascii")


def _case_expr() -> pl.Expr:
    """The pre-#65 CASE, as a Polars expression — the oracle for the stored column."""
    return (
        pl.when(pl.col("Execution Time").is_null() | (pl.col("Execution Time") == ""))
        .then(pl.col("Update Time").str.slice(0, 6))
        .otherwise(pl.col("Execution Time"))
    )


@pytest.fixture()
def quote_store(tmp_path: Path) -> str:
    """A real-ingest store whose rows are half executions, half quote-only."""
    raw, store = tmp_path / "raw", tmp_path / "store"
    raw.mkdir()
    for date in ("20230703", "20230704"):
        payload = _make_quote_only(individual_stock_csv(date, ["7203", "6758"], rows_per_ticker=40))
        zp = raw / f"HTICST120.{date}.1.zip"
        write_zip(zp, f"HTICST120.{date}.1.csv", payload)
        ingest_single_zip(str(zp), str(store), data_type="individual_stock", year=2023)
    return str(store)


def _legacy_copy(store: str, dest: Path) -> str:
    """Rewrite a store without the stored key — a pre-#65 store, byte-for-byte."""
    src_type = Path(store) / "individual_stock"
    for f in src_type.glob("**/*.parquet"):
        rel = f.relative_to(Path(store))
        out = dest / rel
        out.parent.mkdir(parents=True, exist_ok=True)
        df = pl.read_parquet(f)
        assert EFFECTIVE_TIME_COL in df.columns
        df.drop(EFFECTIVE_TIME_COL).write_parquet(out, compression="zstd")
    return str(dest)


# --------------------------------------------------------------------------- #
# The column itself
# --------------------------------------------------------------------------- #
def test_effective_time_written_as_int32(quote_store):
    f = next((Path(quote_store) / "individual_stock").glob("**/*.parquet"))
    schema = pl.read_parquet(f, n_rows=0).schema
    assert EFFECTIVE_TIME_COL in schema
    assert schema[EFFECTIVE_TIME_COL] == pl.Int32


def test_stored_value_equals_the_case_expression_on_every_row(quote_store):
    """The stored key must be exactly what the CASE would have computed."""
    for f in (Path(quote_store) / "individual_stock").glob("**/*.parquet"):
        df = pl.read_parquet(f)
        expected = df.select(_case_expr().cast(pl.Int32).alias("x"))["x"]
        assert df[EFFECTIVE_TIME_COL].equals(expected), f
        # The fixture must actually exercise the quote-only fallback.
        assert (df["Execution Time"] == "").sum() > 0


def test_helper_is_scoped_to_individual_stock():
    df = pl.DataFrame({"Execution Time": ["090000"], "Update Time": ["090000000000"]})
    for other in ("stock_summary", "indices", "indices_summary"):
        assert EFFECTIVE_TIME_COL not in _add_effective_time(df, other).columns
    assert EFFECTIVE_TIME_COL in _add_effective_time(df, "individual_stock").columns


def test_helper_is_idempotent_and_tolerates_missing_sources():
    df = pl.DataFrame({"Execution Time": ["090000"], "Update Time": ["090000000000"]})
    once = _add_effective_time(df, "individual_stock")
    assert _add_effective_time(once, "individual_stock").columns == once.columns
    # No Update Time (e.g. a projected frame) must not raise.
    bare = pl.DataFrame({"Execution Time": ["090000"]})
    assert EFFECTIVE_TIME_COL not in _add_effective_time(bare, "individual_stock").columns


def test_malformed_time_becomes_null_not_an_exception():
    df = pl.DataFrame({"Execution Time": ["bogus", ""], "Update Time": ["xxxxxxxxxxxx", ""]})
    out = _add_effective_time(df, "individual_stock")
    assert out[EFFECTIVE_TIME_COL].to_list() == [None, None]


# --------------------------------------------------------------------------- #
# The key never leaks into a documented output
# --------------------------------------------------------------------------- #
def test_query_ticks_keeps_the_locked_95_columns(quote_store):
    df = query_ticks(quote_store, "individual_stock", ticker=7203, date="20230703")
    assert EFFECTIVE_TIME_COL not in df.columns
    # 95 output columns + the Hive `date` column this store path adds.
    assert df.width == 96


def test_read_parquet_partition_does_not_leak_the_key(quote_store):
    df = tse_tick.read_parquet_partition(quote_store, "individual_stock", date="20230703")
    assert EFFECTIVE_TIME_COL not in df.columns


def test_typed_empty_frames_do_not_leak_the_key(quote_store):
    with pytest.warns(tse_tick.NoDataWarning):
        df = query_ticks(quote_store, "individual_stock", ticker=1111, date="20230703")
    assert df.height == 0
    assert EFFECTIVE_TIME_COL not in df.columns
    empty_batch = _query_extract_batch(quote_store, "individual_stock", tickers={"1111"})
    assert empty_batch.height == 0
    assert EFFECTIVE_TIME_COL not in empty_batch.columns


def test_explicit_column_projection_is_unaffected(quote_store):
    df = query_ticks(
        quote_store, "individual_stock", ticker=7203, columns=["Execution Time", "Volume"]
    )
    assert df.columns == ["Execution Time", "Volume"]


def test_export_query_output_has_no_key(quote_store, tmp_path):
    out = tmp_path / "export.parquet"
    tse_tick.export_query(quote_store, str(out), data_type="individual_stock", ticker=7203)
    assert EFFECTIVE_TIME_COL not in pl.read_parquet(out, n_rows=0).columns


# --------------------------------------------------------------------------- #
# Identity: the stored key must change nothing a caller can observe
# --------------------------------------------------------------------------- #
def _canon(df: pl.DataFrame) -> pl.DataFrame:
    """Total order over every column — immune to the documented tie-order
    non-determinism of DuckDB's parallel sort (PR #45)."""
    return df.sort(by=df.columns)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"ticker": 7203},
        {"ticker": 7203, "date": "20230703"},
        {"ticker": 7203, "start_time": "09:00:00", "end_time": "11:30:00"},
        {"ticker": 7203, "start_time": "09:30:00", "end_time": "09:31:00"},
        {"ticker": 6758, "date": "20230704", "start_time": "13:00:00"},
        {"date": "20230703"},
        {},
    ],
)
def test_stored_key_and_legacy_case_agree(quote_store, tmp_path, kwargs):
    """The load-bearing test: a #65 store and a pre-#65 store must return the
    same rows for every filter shape, including the quote-only fallback."""
    legacy = _legacy_copy(quote_store, tmp_path / "legacy")
    assert not _store_has_effective_time(
        str(next((Path(legacy) / "individual_stock").glob("**/*.parquet")))
    )
    new = query_ticks(quote_store, "individual_stock", **kwargs)
    old = query_ticks(legacy, "individual_stock", **kwargs)
    assert list(new.columns) == list(old.columns)
    assert new.height == old.height
    assert _canon(new).equals(_canon(old))


def test_time_window_still_keeps_quote_only_rows(quote_store):
    """The 0.9.0 guarantee: a window keeps in-window order-book rows, not just
    trade-coincident ones. Regressing to a bare Execution Time filter would drop
    every quote-only row."""
    df = query_ticks(
        quote_store,
        "individual_stock",
        ticker=7203,
        date="20230703",
        start_time="09:00:00",
        end_time="11:30:00",
    )
    assert (df["Execution Time"] == "").sum() > 0


def test_extract_batch_matches_legacy(quote_store, tmp_path):
    legacy = _legacy_copy(quote_store, tmp_path / "legacy2")
    new = _query_extract_batch(
        quote_store,
        "individual_stock",
        tickers={"7203", "6758"},
        start_time="09:00:00",
        end_time="11:30:00",
    )
    old = _query_extract_batch(
        legacy,
        "individual_stock",
        tickers={"7203", "6758"},
        start_time="09:00:00",
        end_time="11:30:00",
    )
    assert list(new.columns) == list(old.columns)
    assert EFFECTIVE_TIME_COL not in new.columns
    assert _canon(new).equals(_canon(old))


# --------------------------------------------------------------------------- #
# MIXED stores: resume ingests new (keyed) dates into a store whose older dates
# predate the key. DuckDB rejects a file list whose first file has a column a
# later one lacks, so this crashed until the fallback existed. Both orderings
# must work — only one of them trips the engine, so test both deliberately.
# --------------------------------------------------------------------------- #
def _downgrade_date(store: str, date: str) -> None:
    """Strip the key from one date, making the store mixed."""
    f = Path(store) / "individual_stock" / f"date={date}" / "ticker=7203.parquet"
    df = pl.read_parquet(f)
    assert EFFECTIVE_TIME_COL in df.columns
    df.drop(EFFECTIVE_TIME_COL).write_parquet(f, compression="zstd")


@pytest.mark.parametrize("stale_date", ["20230703", "20230704"])
def test_mixed_store_is_readable_and_correct(quote_store, tmp_path, stale_date):
    """Sorted file order decides which file DuckDB binds first, so downgrading
    the FIRST date and downgrading the LAST exercise different engine paths."""
    reference = query_ticks(
        quote_store,
        "individual_stock",
        ticker=7203,
        start_time="09:00:00",
        end_time="11:30:00",
    )
    _downgrade_date(quote_store, stale_date)
    mixed = query_ticks(
        quote_store,
        "individual_stock",
        ticker=7203,
        start_time="09:00:00",
        end_time="11:30:00",
    )
    assert list(mixed.columns) == list(reference.columns)
    assert EFFECTIVE_TIME_COL not in mixed.columns
    assert _canon(mixed).equals(_canon(reference))


@pytest.mark.parametrize("stale_date", ["20230703", "20230704"])
def test_mixed_store_extract_batch_is_correct(quote_store, stale_date):
    reference = _query_extract_batch(
        quote_store,
        "individual_stock",
        tickers={"7203"},
        start_time="09:00:00",
        end_time="11:30:00",
    )
    _downgrade_date(quote_store, stale_date)
    mixed = _query_extract_batch(
        quote_store,
        "individual_stock",
        tickers={"7203"},
        start_time="09:00:00",
        end_time="11:30:00",
    )
    assert list(mixed.columns) == list(reference.columns)
    assert EFFECTIVE_TIME_COL not in mixed.columns
    assert _canon(mixed).equals(_canon(reference))


def test_mixed_store_unfiltered_query_is_readable(quote_store):
    """No time filter still binds every file — the schema clash is in the scan,
    not the predicate."""
    _downgrade_date(quote_store, "20230703")
    df = query_ticks(quote_store, "individual_stock", ticker=7203)
    assert df.height > 0
    assert EFFECTIVE_TIME_COL not in df.columns


# --------------------------------------------------------------------------- #
# Interaction with PR #68's streaming (morsel) write path
# --------------------------------------------------------------------------- #
def test_streaming_appender_and_concat_writer_agree(tmp_path):
    """#68 streams cleaned morsels straight to one writer per (date, ticker) and
    never materializes the day. The key is element-wise, so appending it per
    morsel must equal computing it on the whole concatenated day."""
    payload = _make_quote_only(individual_stock_csv("20230703", ["7203"], rows_per_ticker=40))
    zp = write_zip(tmp_path / "HTICST120.20230703.1.zip", "HTICST120.20230703.1.csv", payload)
    frame = tse_tick.read_ticks(str(zp))

    concat_dir, stream_dir = tmp_path / "concat", tmp_path / "stream"
    write_partitioned_parquet(frame, str(concat_dir), "individual_stock")

    app = PartitionedParquetAppender(str(stream_dir), "individual_stock")
    third = max(frame.height // 3, 1)
    for i in range(0, frame.height, third):  # simulate morsels
        app.write(frame.slice(i, third))
    app.commit()

    a = pl.read_parquet(next(concat_dir.glob("**/*.parquet")))
    b = pl.read_parquet(next(stream_dir.glob("**/*.parquet")))
    assert a.schema == b.schema
    assert EFFECTIVE_TIME_COL in b.columns
    assert a.equals(b)


def test_appender_key_is_stable_across_morsels(tmp_path):
    """A per-morsel schema drift would abort the whole day in _append."""
    payload = _make_quote_only(individual_stock_csv("20230703", ["7203"], rows_per_ticker=40))
    zp = write_zip(tmp_path / "HTICST120.20230703.2.zip", "HTICST120.20230703.2.csv", payload)
    frame = tse_tick.read_ticks(str(zp))
    app = PartitionedParquetAppender(str(tmp_path / "s2"), "individual_stock")
    # A morsel of only quote-only rows, then only execution rows: both must type
    # the key identically (Int32), or the second write would raise.
    quotes = frame.filter(pl.col("Execution Time") == "")
    execs = frame.filter(pl.col("Execution Time") != "")
    assert quotes.height and execs.height
    app.write(quotes)
    app.write(execs)
    app.commit()
    out = pl.read_parquet(next((tmp_path / "s2").glob("**/*.parquet")))
    assert out.schema[EFFECTIVE_TIME_COL] == pl.Int32
    assert out.height == frame.height


# --------------------------------------------------------------------------- #
# Other data types must be untouched
# --------------------------------------------------------------------------- #
def test_indices_store_has_no_key_and_still_filters(indices_store):
    f = next((Path(indices_store) / "indices").glob("**/*.parquet"))
    assert EFFECTIVE_TIME_COL not in pl.read_parquet(f, n_rows=0).columns
    df = query_ticks(indices_store, "indices", ticker=101, start_time="09:00:00")
    assert df.height > 0
    assert EFFECTIVE_TIME_COL not in df.columns
