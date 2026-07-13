# tests/test_round18_fixes.py
"""Round-18 fix — a catchable memory guard on the DuckDB query path.

`query_ticks(..., limit=None)` over a multi-year range of an active ticker asks
DuckDB to sort and hand back the whole result as one Polars frame — hundreds of
millions of rows / tens-to-hundreds of GB — which overflows RAM at the Arrow
conversion. DuckDB then raised a raw ``duckdb.OutOfMemoryException`` whose advice
(``SET threads=…`` / ``SET memory_limit=…``) the caller cannot reach through this
API, while the *read* path (``read_ticks``) already converts the same situation into
a catchable :class:`OneShotMemoryError`. This adds the symmetric query-path guard:

- a catchable :class:`tse_tick.QueryMemoryError` (subclass of ``MemoryError``, like
  ``OneShotMemoryError``) carrying tse_tick's own slice-the-store remedy;
- ``_execute_to_polars`` converts ``duckdb.OutOfMemoryException`` / bare
  ``MemoryError`` at every high-level ``.pl()`` site (``query_ticks`` and the
  batched ``_query_extract_batch``), leaving other DuckDB errors untouched;
- ``_duckdb_connect`` disables insertion-order preservation (safe — both builders
  always ``ORDER BY``) to lower peak memory on big scans.

Synthetic NEEDS-format data only — no proprietary NEEDS files.
"""
import pytest

import tse_tick
from tse_tick import OneShotMemoryError, QueryMemoryError

duckdb = pytest.importorskip("duckdb")

from tse_tick.query import (  # noqa: E402  (after importorskip)
    _duckdb_connect,
    _execute_to_polars,
    _QUERY_MEMORY_GUIDANCE,
    query_ticks,
)
from tse_tick.ingest import ingest_single_zip  # noqa: E402
from tests.synthetic_data import individual_stock_csv, write_zip  # noqa: E402


# --------------------------------------------------------------------------- #
# QueryMemoryError — the catchable type
# --------------------------------------------------------------------------- #
def test_query_memory_error_is_catchable_memory_error():
    """QueryMemoryError is a distinct, top-level, MemoryError-based type — so one
    ``except MemoryError`` covers an over-large read (OneShotMemoryError) AND an
    over-large query, and it is importable regardless of the [query] extra (it lives
    in enhanced.py, not query.py, so a core install without DuckDB can still name
    it)."""
    assert issubclass(QueryMemoryError, MemoryError)
    assert QueryMemoryError is not OneShotMemoryError
    assert not issubclass(QueryMemoryError, OneShotMemoryError)
    assert tse_tick.QueryMemoryError is QueryMemoryError
    assert "QueryMemoryError" in tse_tick.__all__
    # `except MemoryError` catches it.
    with pytest.raises(MemoryError):
        raise QueryMemoryError("x")


# --------------------------------------------------------------------------- #
# _execute_to_polars — the conversion boundary
# --------------------------------------------------------------------------- #
class _FakePl:
    def __init__(self, exc):
        self._exc = exc

    def pl(self):
        raise self._exc


class _FakeCon:
    """Minimal DuckDB-connection stand-in whose result conversion raises ``exc``."""

    def __init__(self, exc):
        self._exc = exc
        self.closed = False

    def execute(self, sql):
        return _FakePl(self._exc)

    def close(self):
        self.closed = True


def test_execute_to_polars_converts_duckdb_oom():
    """A duckdb.OutOfMemoryException at .pl() becomes a QueryMemoryError carrying the
    slice-the-store guidance, with the original chained as __cause__ (not DuckDB's
    un-actionable SET threads/memory_limit advice as the top-level error)."""
    raw = duckdb.OutOfMemoryException(
        "Out of Memory Error: ArrowBuffer: failed to allocate 8388608 bytes"
    )
    with pytest.raises(QueryMemoryError) as ei:
        _execute_to_polars(_FakeCon(raw), "SELECT 1")
    msg = str(ei.value)
    assert _QUERY_MEMORY_GUIDANCE.split(" (")[0][:40] in msg
    assert "read it back in bounded slices" in msg
    assert "OutOfMemoryException" in msg  # underlying error named
    assert ei.value.__cause__ is raw  # chained, not swallowed


def test_execute_to_polars_converts_bare_memory_error():
    """A bare host MemoryError (e.g. the Arrow buffer allocation failing as a Python
    MemoryError) is also converted, so callers get the same catchable type + remedy."""
    raw = MemoryError("cannot allocate")
    with pytest.raises(QueryMemoryError) as ei:
        _execute_to_polars(_FakeCon(raw), "SELECT 1")
    assert ei.value.__cause__ is raw


def test_execute_to_polars_passes_other_errors_through():
    """Only out-of-memory failures are converted — an unrelated DuckDB/other error
    must propagate unchanged, so genuine query bugs are not masked as memory errors."""
    raw = duckdb.InvalidInputException("some binder error")
    with pytest.raises(duckdb.InvalidInputException):
        _execute_to_polars(_FakeCon(raw), "SELECT 1")

    class _Boom(RuntimeError):
        pass

    with pytest.raises(_Boom):
        _execute_to_polars(_FakeCon(_Boom("x")), "SELECT 1")


# --------------------------------------------------------------------------- #
# _duckdb_connect — memory hardening
# --------------------------------------------------------------------------- #
def test_duckdb_connect_disables_insertion_order():
    """The connection lowers peak memory on large scans by not preserving insertion
    order (safe: query_ticks / _query_extract_batch always impose an ORDER BY)."""
    con = _duckdb_connect()
    try:
        val = con.execute("SELECT current_setting('preserve_insertion_order')").fetchone()[0]
    finally:
        con.close()
    assert val is False or str(val).lower() == "false"


# --------------------------------------------------------------------------- #
# query_ticks — end-to-end wiring over a real (synthetic) store
# --------------------------------------------------------------------------- #
@pytest.fixture
def stock_store(tmp_path):
    store = tmp_path / "store"
    for date in ("20240104", "20240105"):
        zp = tmp_path / f"HTICST120.{date}.1.zip"
        write_zip(
            zp,
            f"HTICST120.{date}.1.csv",
            individual_stock_csv(date, ["7203", "6758"], rows_per_ticker=12,
                                 base_prices={"7203": 2100, "6758": 13000}),
        )
        ingest_single_zip(str(zp), str(store), data_type="individual_stock", year=2024)
    return str(store)


def test_query_ticks_converts_oom_to_query_memory_error(stock_store, monkeypatch):
    """query_ticks surfaces a DuckDB OOM as the catchable QueryMemoryError (not the
    raw duckdb.OutOfMemoryException) — the exact failure the run-16 notebook hit at
    query.py's `.pl()`. Deterministic: the real query is built and reached, but the
    connection's result conversion is forced to raise the OOM."""
    raw = duckdb.OutOfMemoryException("Out of Memory Error: failed to allocate")
    monkeypatch.setattr("tse_tick.query._duckdb_connect", lambda: _FakeCon(raw))
    with pytest.raises(QueryMemoryError):
        query_ticks(stock_store, data_type="individual_stock", ticker="7203", limit=None)
    # And a caller that only knows MemoryError still catches it.
    with pytest.raises(MemoryError):
        query_ticks(stock_store, data_type="individual_stock", ticker="7203", limit=None)


def test_query_ticks_still_orders_after_insertion_order_off(stock_store):
    """Disabling insertion-order preservation must not weaken the documented ordering:
    a normal multi-day read still comes back ascending by Data Date (the explicit
    ORDER BY governs), so the memory tweak is output-preserving."""
    df = query_ticks(stock_store, data_type="individual_stock", ticker="7203", limit=None)
    assert df.height > 0
    dd = df["Data Date"].to_list()
    assert dd == sorted(dd), "rows must stay ordered by Data Date"
