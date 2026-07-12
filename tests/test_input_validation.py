"""First-run guardrails: a wrong input root must not look like a successful ingest."""
import logging

import pytest

import tse_tick
from tests.synthetic_data import seed_structured_day

pytest.importorskip("duckdb")


def test_ingest_period_missing_root_raises(tmp_path):
    with pytest.raises(FileNotFoundError, match="Input root not found"):
        tse_tick.ingest_period(
            str(tmp_path / "nope"), str(tmp_path / "store"), "20240104", "individual_stock"
        )


def test_ingest_year_from_root_missing_root_raises(tmp_path):
    with pytest.raises(FileNotFoundError, match="Input root not found"):
        tse_tick.ingest_year_from_root(
            str(tmp_path / "nope"), str(tmp_path / "store"), 2024, "individual_stock"
        )


def test_extract_to_store_missing_root_raises(tmp_path):
    # extract_to_store inherits the guard through Stage 1.
    with pytest.raises(FileNotFoundError, match="Input root not found"):
        tse_tick.extract_to_store(
            str(tmp_path / "nope"), str(tmp_path / "store"), "20240104", "7203"
        )


def test_zero_discovery_warns_not_raises(tmp_path):
    # An existing root with no matching ZIPs (wrong data_type, root one level too
    # deep, or an all-holiday period) warns capturably and returns [] — it is not
    # an error, but it must not be silent either.
    (tmp_path / "empty_root").mkdir()
    with pytest.warns(tse_tick.NoDataWarning, match="individual_stock"):
        results = tse_tick.ingest_period(
            str(tmp_path / "empty_root"), str(tmp_path / "store"),
            "20240104", "individual_stock",
        )
    assert results == []
    # month-granularity period warns too
    with pytest.warns(tse_tick.NoDataWarning, match="202401"):
        tse_tick.ingest_period(
            str(tmp_path / "empty_root"), str(tmp_path / "store"),
            "202401", "individual_stock",
        )
    # year granularity (delegates per year)
    with pytest.warns(tse_tick.NoDataWarning, match="year 2024"):
        tse_tick.ingest_period(
            str(tmp_path / "empty_root"), str(tmp_path / "store"),
            "2024", "individual_stock",
        )


def test_matching_root_does_not_warn(tmp_path, recwarn):
    seed_structured_day(tmp_path / "src", "20240104", {1: ["7203"]})
    tse_tick.ingest_period(
        str(tmp_path / "src"), str(tmp_path / "store"), "20240104", "individual_stock"
    )
    assert not [w for w in recwarn if issubclass(w.category, tse_tick.NoDataWarning)]


def test_progress_lines_have_counters(tmp_path, caplog):
    seed_structured_day(tmp_path / "src", "20240104", {1: ["7203"]})
    seed_structured_day(tmp_path / "src", "20240105", {1: ["7203"]})
    src, store = str(tmp_path / "src"), str(tmp_path / "store")
    with caplog.at_level(logging.INFO, logger="tse_tick.ingest"):
        tse_tick.ingest_period(src, store, "20240104-20240105", "individual_stock")
    msgs = [r.getMessage() for r in caplog.records]
    assert any("[1/2]" in m for m in msgs) and any("[2/2]" in m for m in msgs)

    # a resumed run reports how many dates it skipped
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="tse_tick.ingest"):
        tse_tick.ingest_period(src, store, "20240104-20240105", "individual_stock")
    assert any("skipped 2 of 2" in r.getMessage() for r in caplog.records)
