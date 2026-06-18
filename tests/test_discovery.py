# tests/test_discovery.py
"""discover_zips must find ZIPs under the real NEEDS delivery tree
(個別株式{year}/TICST120/{yyyymm}/…), not only the documented {year}/{yearmonth}/."""
from pathlib import Path

import tse_tick
from tse_tick.enhanced import discover_zips
from tests.synthetic_data import individual_stock_csv, write_zip


def _make_zip(path: Path, date: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    write_zip(
        path,
        f"HTICST120.{date}.1.csv",
        individual_stock_csv(date, ["7203"], rows_per_ticker=4, base_prices={"7203": 2100}),
    )


def test_strict_layout_still_works(tmp_path):
    _make_zip(tmp_path / "2024" / "202402" / "HTICST120.20240201.1.zip", "20240201")
    found = discover_zips(str(tmp_path), "individual_stock", [2024], months=[2], dates=["20240201"])
    assert len(found) == 1


def test_nested_needs_layout_found_via_fallback(tmp_path):
    # the real delivery tree: 個別株式{year}/TICST120/{yyyymm}/...
    _make_zip(tmp_path / "個別株式2024" / "TICST120" / "202402" / "HTICST120.20240201.1.zip", "20240201")
    assert len(discover_zips(str(tmp_path), "individual_stock", [2024], months=[2], dates=["20240201"])) == 1
    assert len(discover_zips(str(tmp_path), "individual_stock", [2024])) == 1            # whole year
    # a different month must not match
    assert len(discover_zips(str(tmp_path), "individual_stock", [2024], months=[3])) == 0


def test_read_ticks_structured_root_nested(tmp_path):
    _make_zip(tmp_path / "個別株式2024" / "TICST120" / "202402" / "HTICST120.20240201.1.zip", "20240201")
    df = tse_tick.read_ticks(str(tmp_path), ticker_filter={"7203"}, date="20240201")
    assert df.height == 4


def test_yearmonth_under_root_uses_fast_path(tmp_path, monkeypatch):
    # input_root points straight at the type dir, so {yyyymm}/ sits directly under
    # it (the report's G:\NEEDS\個別株式2023\TICST120 usage). This must hit the
    # fast path, not the full recursive tree walk.
    _make_zip(tmp_path / "202305" / "HTICST120.20230502.1.zip", "20230502")
    import tse_tick.enhanced as enh

    calls = {"n": 0}
    orig = enh._discover_zips_recursive

    def spy(*a, **k):
        calls["n"] += 1
        return orig(*a, **k)

    monkeypatch.setattr(enh, "_discover_zips_recursive", spy)
    found = enh.discover_zips(str(tmp_path), "individual_stock", [2023], months=[5], dates=["20230502"])
    assert len(found) == 1
    assert calls["n"] == 0     # fast path handled it; no recursive walk
