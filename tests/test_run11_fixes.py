# tests/test_run11_fixes.py
"""Regression tests for the run10 bug report (fixed in run11).

F1 (Major) — `read_ticks(data_type="indices", columns=<subset>)` returned the
whole month (~20x inflated) when the projection dropped `Data Date`, because the
monthly day-prune ran after the per-part projection and could no longer find the
column. F2 — `tse_tick.ingest` (the submodule) is hidden from `dir()` and has a
docstring. F3 — `get_info(path)` gives a guiding error, not a raw TypeError.
F4 — an empty `ticker_filter` is named in the no-data warning. F5 — author
metadata lists the full team consistently.
"""
import polars as pl
import pytest

import tse_tick
from tse_tick import read_ticks
from tests.synthetic_data import indices_csv, write_zip


def _two_day_indices_root(tmp_path, codes=("101",), rows_per_code=6):
    """A monthly indices ZIP holding TWO trading days (the day-prune target)."""
    payload = (
        indices_csv("20230508", list(codes), rows_per_code=rows_per_code)
        + indices_csv("20230509", list(codes), rows_per_code=rows_per_code)
    )
    zp = tmp_path / "個別株式2023" / "TICIT110" / "202305" / "HTICIT110.202305.zip"
    zp.parent.mkdir(parents=True)
    write_zip(zp, "HTICIT110.202305.csv", payload)
    return tmp_path


# --------------------------------------------------------------------------- #
# F1 (MAJOR) — a columns= subset must not skip the monthly day-prune
# --------------------------------------------------------------------------- #
def test_f1_indices_columns_subset_still_prunes_to_day(tmp_path):
    root = _two_day_indices_root(tmp_path)
    full = read_ticks(str(root), data_type="indices", ticker_filter={"101"}, date="20230508")
    proj = read_ticks(str(root), data_type="indices", ticker_filter={"101"}, date="20230508",
                      columns=["Index Value"])
    assert full.height == 6
    assert proj.height == 6                       # was 12 (whole month) before the fix
    assert proj.columns == ["Index Value"]


def test_f1_indices_columns_subset_keeps_only_requested_day(tmp_path):
    root = _two_day_indices_root(tmp_path)
    proj = read_ticks(str(root), data_type="indices", ticker_filter={"101"}, date="20230508",
                      columns=["Data Date", "Index Value"])
    dates = {str(d)[:10] for d in proj["Data Date"].to_list()}
    assert dates == {"2023-05-08"}               # only the requested day, not both


def test_f1_indices_columns_subset_with_time_window(tmp_path):
    root = _two_day_indices_root(tmp_path, rows_per_code=16)
    common = dict(data_type="indices", ticker_filter={"101"}, date="20230508",
                  start_time="09:00:00", end_time="11:30:00")
    full = read_ticks(str(root), **common)
    proj = read_ticks(str(root), columns=["Execution Time", "Index Value"], **common)
    assert 0 < proj.height == full.height         # same rows, just fewer columns
    assert proj.columns == ["Execution Time", "Index Value"]


def test_f1_full_column_projection_unchanged(tmp_path):
    # The full-column list (the case that already worked) must still work.
    root = _two_day_indices_root(tmp_path)
    cols = ["Record Type", "Data Date", "Exchange Code", "Security Type", "Session",
            "Index Code", "Execution Time", "Index Value", "Execution Type", "Ayumi Flag"]
    df = read_ticks(str(root), data_type="indices", ticker_filter={"101"}, date="20230508",
                    columns=cols)
    assert df.height == 6
    assert df.columns == cols


# --------------------------------------------------------------------------- #
# F2 — tse_tick.ingest (submodule) hidden from dir(), documented, still usable
# --------------------------------------------------------------------------- #
def test_f2_ingest_submodule_has_docstring():
    assert (tse_tick.ingest.__doc__ or "").strip()


def test_f2_ingest_module_hidden_from_dir_but_entry_points_shown():
    names = dir(tse_tick)
    assert "ingest" not in names                  # the confusing bare submodule
    assert "ingest_period" in names               # the real entry points remain
    assert "read_ticks" in names


def test_f2_public_api_present_in_dir():
    for name in ("read_ticks", "query_ticks", "get_info", "get_version",
                 "get_supported_data_types"):
        assert name in dir(tse_tick)


def test_f2_submodule_still_importable():
    # Hiding from dir() must not break attribute access.
    assert tse_tick.ingest.ingest_period is tse_tick.ingest_period


# --------------------------------------------------------------------------- #
# F3 — get_info(path) gives a guiding error instead of a raw TypeError
# --------------------------------------------------------------------------- #
def test_f3_get_info_no_args_returns_string():
    out = tse_tick.get_info()
    assert isinstance(out, str) and "tse_tick" in out


def test_f3_get_info_with_path_raises_clear_error(tmp_path):
    with pytest.raises(ValueError, match="dataset path|read_ticks"):
        tse_tick.get_info(str(tmp_path))


# --------------------------------------------------------------------------- #
# F4 — an empty ticker_filter is named in the no-data warning
# --------------------------------------------------------------------------- #
def test_f4_empty_ticker_filter_named_in_warning(tmp_path):
    zp = tmp_path / "HTICIT110.20230508.1.zip"
    write_zip(zp, "HTICIT110.20230508.1.csv", indices_csv("20230508", ["101"], rows_per_code=4))
    with pytest.warns(tse_tick.NoDataWarning, match=r"ticker_filter=\[\]"):
        df = read_ticks(str(zp), data_type="indices", ticker_filter=set(), date="20230508")
    assert df.height == 0


# --------------------------------------------------------------------------- #
# F5 — author metadata lists the full team consistently
# --------------------------------------------------------------------------- #
def test_f5_author_lists_full_team():
    info = tse_tick.get_info()
    for name in ("Kazumi Li", "Masataka Hayashi", "Peter Romero"):
        assert name in tse_tick.__author__
        assert name in info
