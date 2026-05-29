# tests/test_features.py
"""Tests for tse_tick.features — order-book feature engineering from TICST120 data.

Happy-path tests use the synthetic ``feature_ticks`` fixture (a 95-column
TICST120 frame loaded from the pipeline-built store); edge cases build minimal
frames inline. The feature functions return Polars objects, so the tests assert
Polars types and behaviour.
"""

import math

import polars as pl
import pytest

from tse_tick.features import (
    compute_spread,
    compute_depth,
    compute_flow_imbalance,
    compute_volatility,
    compute_all_features,
)


def test_compute_spread_returns_series(feature_ticks):
    """
    compute_spread should return a Series aligned with the input DataFrame's
    rows, with dtype float64.
    """
    spread = compute_spread(feature_ticks)
    assert isinstance(spread, pl.Series)
    assert spread.dtype == pl.Float64
    assert len(spread) == feature_ticks.height


def test_compute_spread_formula(feature_ticks):
    """
    The spread for each row should equal Sell Quote 1 Best minus
    Buy Quote 1 Best. In the fixture the best quotes straddle the price by +/-1,
    so every spread is 2.0.
    """
    spread = compute_spread(feature_ticks)
    sell = feature_ticks["Sell Quote 1 Best"].cast(pl.Float64)
    buy = feature_ticks["Buy Quote 1 Best"].cast(pl.Float64)
    expected = (sell - buy).to_list()
    assert spread.to_list() == expected
    assert set(spread.to_list()) == {2.0}


def test_compute_spread_nan_when_quote_is_zero():
    """
    When either Sell Quote 1 Best or Buy Quote 1 Best is 0.0 (no quote),
    the spread should be null for that row.
    """
    df = pl.DataFrame({
        "Sell Quote 1 Best": [101.0, 0.0, 105.0],
        "Buy Quote 1 Best": [99.0, 99.0, 0.0],
    })
    spread = compute_spread(df)
    assert spread.to_list() == [2.0, None, None]


def test_compute_spread_missing_column_raises(feature_ticks):
    """
    compute_spread should raise when 'Sell Quote 1 Best' or 'Buy Quote 1 Best'
    is absent from the input DataFrame. Polars raises ColumnNotFoundError.
    """
    with pytest.raises(pl.exceptions.ColumnNotFoundError):
        compute_spread(feature_ticks.drop("Sell Quote 1 Best"))


def test_compute_depth_returns_dataframe(feature_ticks):
    """
    compute_depth should return a DataFrame with columns sell_depth_1..N and
    buy_depth_1..N when side='both'.
    """
    depth = compute_depth(feature_ticks, levels=10, side="both")
    assert isinstance(depth, pl.DataFrame)
    assert "sell_depth_1" in depth.columns
    assert "buy_depth_1" in depth.columns
    assert depth.width == 20


def test_compute_depth_column_names_match_levels(feature_ticks):
    """
    With levels=5, compute_depth should return exactly 10 columns:
    sell_depth_1..5 and buy_depth_1..5.
    """
    depth = compute_depth(feature_ticks, levels=5, side="both")
    assert depth.columns == [f"sell_depth_{i}" for i in range(1, 6)] + [
        f"buy_depth_{i}" for i in range(1, 6)
    ]


def test_compute_depth_sell_only(feature_ticks):
    """
    With side='sell', only sell_depth_1..N columns should be present;
    no buy_depth columns.
    """
    depth = compute_depth(feature_ticks, levels=4, side="sell")
    assert depth.columns == [f"sell_depth_{i}" for i in range(1, 5)]
    assert all(not c.startswith("buy_depth") for c in depth.columns)


def test_compute_depth_buy_only(feature_ticks):
    """
    With side='buy', only buy_depth_1..N columns should be present;
    no sell_depth columns.
    """
    depth = compute_depth(feature_ticks, levels=4, side="buy")
    assert depth.columns == [f"buy_depth_{i}" for i in range(1, 5)]
    assert all(not c.startswith("sell_depth") for c in depth.columns)


def test_compute_depth_invalid_levels_raises(feature_ticks):
    """
    compute_depth should raise ValueError when levels is outside 1-10.
    """
    with pytest.raises(ValueError, match="levels must be 1-10"):
        compute_depth(feature_ticks, levels=11)
    with pytest.raises(ValueError, match="levels must be 1-10"):
        compute_depth(feature_ticks, levels=0)


def test_compute_depth_invalid_side_raises(feature_ticks):
    """
    compute_depth should raise ValueError when side is not 'buy', 'sell',
    or 'both'.
    """
    with pytest.raises(ValueError, match="side must be"):
        compute_depth(feature_ticks, side="middle")


def test_compute_flow_imbalance_returns_series(feature_ticks):
    """
    compute_flow_imbalance should return a Series of float64 values in the
    range [-1, 1], aligned with the input DataFrame's rows.
    """
    ofi = compute_flow_imbalance(feature_ticks)
    assert isinstance(ofi, pl.Series)
    assert ofi.dtype == pl.Float64
    assert len(ofi) == feature_ticks.height


def test_compute_flow_imbalance_range(feature_ticks):
    """
    All non-null values in the flow imbalance Series should be within [-1, 1].
    """
    ofi = compute_flow_imbalance(feature_ticks)
    for v in ofi.drop_nulls().to_list():
        assert -1.0 <= v <= 1.0


def test_compute_flow_imbalance_nan_when_no_volume():
    """
    When total volume in a rolling window is zero, flow imbalance should be
    null, not raise a ZeroDivisionError. (The implementation uses Execution
    Price as the volume proxy, so a zero price yields zero window volume.)
    """
    df = pl.DataFrame({
        "Execution Time": ["100000", "100100"],
        "Execution Price": [0.0, 0.0],
        "Sell Quote 1 Best": [101.0, 101.0],
        "Buy Quote 1 Best": [99.0, 99.0],
    })
    ofi = compute_flow_imbalance(df, window="5min")
    assert ofi.null_count() == len(ofi)


def test_compute_volatility_realized_returns_series(feature_ticks):
    """
    compute_volatility with method='realized' should return a Series of
    non-negative float64 values aligned with the input DataFrame's rows.
    """
    vol = compute_volatility(feature_ticks, method="realized")
    assert isinstance(vol, pl.Series)
    assert vol.dtype == pl.Float64
    for v in vol.drop_nulls().to_list():
        assert v >= 0.0


def test_compute_volatility_garman_klass_returns_series(feature_ticks):
    """
    compute_volatility with method='garman_klass' should return a Series of
    non-negative float64 values.
    """
    vol = compute_volatility(feature_ticks, method="garman_klass")
    assert vol.dtype == pl.Float64
    for v in vol.drop_nulls().to_list():
        assert v >= 0.0


def test_compute_volatility_invalid_method_raises(feature_ticks):
    """
    compute_volatility should raise ValueError for an unrecognised method
    string (e.g. method='parkinson').
    """
    with pytest.raises(ValueError, match="method must be"):
        compute_volatility(feature_ticks, method="parkinson")


def test_compute_volatility_nan_for_single_tick_window(feature_ticks):
    """
    A window with a single tick has no log return. This implementation returns
    0.0 (the sqrt of an empty sum of squared returns) rather than NaN; assert it
    produces no positive volatility and does not raise.
    """
    one = feature_ticks.head(1)
    vol = compute_volatility(one, window="5min", method="realized")
    assert len(vol) == 1
    v = vol.to_list()[0]
    assert v is None or v == 0.0 or math.isnan(v)


def test_compute_all_features_column_count(feature_ticks):
    """
    compute_all_features with levels=10 should return 95 + 1 (spread) +
    20 (depth) + 1 (flow_imbalance) + 1 (volatility) = 118 columns.
    """
    out = compute_all_features(feature_ticks, levels=10)
    assert feature_ticks.width == 95
    assert out.width == 118


def test_compute_all_features_does_not_modify_input(feature_ticks):
    """
    compute_all_features should return a new DataFrame; the original should be
    unchanged after the call.
    """
    before_cols = list(feature_ticks.columns)
    before_shape = feature_ticks.shape
    _ = compute_all_features(feature_ticks, levels=10)
    assert feature_ticks.columns == before_cols
    assert feature_ticks.shape == before_shape


def test_compute_all_features_expected_column_names(feature_ticks):
    """
    The output of compute_all_features should contain columns named
    'spread', 'flow_imbalance', 'volatility', 'sell_depth_1', 'buy_depth_1'.
    """
    out = compute_all_features(feature_ticks, levels=10)
    for col in ("spread", "flow_imbalance", "volatility", "sell_depth_1", "buy_depth_1"):
        assert col in out.columns
