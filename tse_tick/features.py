# tse_tick/features.py
import datetime

import polars as pl


def _polars_period(window: str) -> str:
    """Normalize a window string to a Polars duration.

    Polars ``rolling`` uses ``m`` for minutes; this package documents windows
    such as ``"5min"``. Accept both spellings by mapping a trailing ``min`` to
    ``m`` (``"5min"`` -> ``"5m"``); already-valid units pass through unchanged.
    """
    if window.endswith("min"):
        return window[:-3] + "m"
    return window


def _exec_time_index(df: pl.DataFrame) -> pl.Series:
    et_col = df["Execution Time"]
    if len(et_col) == 0:
        return pl.Series([], dtype=pl.Datetime)

    sample = et_col.drop_nulls()
    if len(sample) == 0:
        return pl.Series([None] * len(df), dtype=pl.Datetime)

    first = sample[0]

    if isinstance(first, datetime.time):
        time_strs = et_col.cast(pl.String)
        return time_strs.str.to_datetime("%H:%M:%S", strict=False)
    elif isinstance(first, str) and len(first) <= 6:
        time_strs = et_col.cast(pl.String).str.zfill(6)
        return time_strs.str.to_datetime("%H%M%S", strict=False)
    else:
        return et_col.cast(pl.Datetime)


def compute_spread(df: pl.DataFrame) -> pl.Series:
    """Best-quote spread (``Sell Quote 1 Best`` − ``Buy Quote 1 Best``) per row.

    Expects **English** column names (the ``create_df`` / ``query_ticks``
    default). Rows missing either side (zero or null) yield ``None``.

    Args:
        df: An ``individual_stock`` tick frame.

    Returns:
        A ``Float64`` Series named ``"spread"``, aligned to ``df``'s rows.
    """
    sell = df["Sell Quote 1 Best"].cast(pl.Float64)
    buy = df["Buy Quote 1 Best"].cast(pl.Float64)
    spread_series = sell - buy

    result = []
    for sell_val, buy_val, spread_val in zip(
        sell.to_list(), buy.to_list(), spread_series.to_list()
    ):
        if sell_val is not None and buy_val is not None and sell_val != 0 and buy_val != 0:
            result.append(spread_val)
        else:
            result.append(None)

    return pl.Series("spread", result, dtype=pl.Float64)


def compute_depth(
    df: pl.DataFrame,
    levels: int = 10,
    side: str = "both",
) -> pl.DataFrame:
    """Order-book depth: quote volumes for levels 1..``levels``.

    Expects **English** column names (``Sell Quote Vol N`` / ``Buy Quote Vol N``).

    Args:
        df: An ``individual_stock`` tick frame.
        levels: How many price levels to include (1-10).
        side: ``"sell"``, ``"buy"``, or ``"both"``.

    Returns:
        A DataFrame of ``sell_depth_i`` / ``buy_depth_i`` columns (only those
        present in ``df``).
    """
    if not (1 <= levels <= 10):
        raise ValueError(f"levels must be 1-10, got {levels}")
    if side not in ("sell", "buy", "both"):
        raise ValueError(f"side must be 'sell', 'buy', or 'both', got {side!r}")

    cols = {}

    if side in ("sell", "both"):
        for i in range(1, levels + 1):
            col_name = f"Sell Quote Vol {i}"
            if col_name in df.columns:
                cols[f"sell_depth_{i}"] = df[col_name].cast(pl.Float64)

    if side in ("buy", "both"):
        for i in range(1, levels + 1):
            col_name = f"Buy Quote Vol {i}"
            if col_name in df.columns:
                cols[f"buy_depth_{i}"] = df[col_name].cast(pl.Float64)

    return pl.DataFrame(cols)


def compute_flow_imbalance(
    df: pl.DataFrame,
    window: str = "5min",
) -> pl.Series:
    """Rolling order-flow imbalance ``(buy − sell) / (buy + sell)`` over ``window``.

    Trades are signed by comparing ``Execution Price`` to the best-quote mid,
    then summed in a time-based rolling window. Expects **English** column names.

    Args:
        df: An ``individual_stock`` tick frame.
        window: Rolling window, e.g. ``"5min"`` (``"5m"`` is also accepted).

    Returns:
        A ``Float64`` Series named ``"flow_imbalance"``.
    """
    sell_best = df["Sell Quote 1 Best"].cast(pl.Float64)
    buy_best = df["Buy Quote 1 Best"].cast(pl.Float64)
    mid = (sell_best + buy_best) * 0.5

    price = df["Execution Price"].cast(pl.Float64)
    volume = df["Execution Price"].cast(pl.Float64)

    time_idx = _exec_time_index(df)

    trade_df = pl.DataFrame({
        "time": time_idx,
        "price": price,
        "volume": volume,
        "mid": mid,
    })

    trade_df = trade_df.with_columns(
        pl.when(pl.col("price") >= pl.col("mid"))
        .then(pl.col("volume"))
        .otherwise(0.0)
        .alias("buy_vol"),
        pl.when(pl.col("price") < pl.col("mid"))
        .then(pl.col("volume"))
        .otherwise(0.0)
        .alias("sell_vol"),
    )

    trade_df = trade_df.sort("time")

    # Time-based rolling in polars
    rolling = trade_df.rolling(index_column="time", period=_polars_period(window))
    buy_rolling = rolling.agg(pl.col("buy_vol").sum().alias("buy_roll"))
    sell_rolling = rolling.agg(pl.col("sell_vol").sum().alias("sell_roll"))

    ofi_col = []
    buy_list = buy_rolling["buy_roll"].to_list()
    sell_list = sell_rolling["sell_roll"].to_list()

    for b, s in zip(buy_list, sell_list):
        denom = b + s
        if denom is not None and denom != 0:
            ofi_col.append((b - s) / denom)
        else:
            ofi_col.append(None)

    return pl.Series("flow_imbalance", ofi_col, dtype=pl.Float64)


def compute_volatility(
    df: pl.DataFrame,
    window: str = "5min",
    method: str = "realized",
) -> pl.Series:
    """Rolling volatility over ``window``.

    Expects **English** column names (uses ``Execution Price`` / ``Execution
    Time``).

    Only real trades contribute. ``individual_stock`` frames carry
    ``Execution Price = 0`` (and a blank ``Execution Time``) on quote-only book
    rows — the vast majority of a liquid day — so those rows are excluded before
    any log-return / OHLC is computed. A log-return taken over a zero price is
    ``inf``/``NaN`` and would otherwise poison every rolling window it lands in,
    inflating the finite outputs too. The result is aligned to ``df``'s rows (the
    same convention as :func:`compute_spread`), with ``null`` — not ``NaN`` — for
    non-trade rows and for warm-up positions whose window holds no return.

    Args:
        df: An ``individual_stock`` tick frame.
        window: Rolling window, e.g. ``"5min"``.
        method: ``"realized"`` (sqrt of summed squared log returns) or
            ``"garman_klass"`` (OHLC range estimator).

    Returns:
        A ``Float64`` Series named ``"volatility"``, aligned to ``df``'s rows.
    """
    if method not in ("realized", "garman_klass"):
        raise ValueError(f"method must be 'realized' or 'garman_klass', got {method!r}")

    n = df.height
    out = pl.Series("volatility", [None] * n, dtype=pl.Float64)
    if n == 0:
        return out

    # Restrict to real trades (Execution Price > 0 and a parseable time), keeping
    # each row's original position so the rolling result can be scattered back
    # aligned to df. Quote-only book rows (price 0 / blank Execution Time) are
    # dropped here rather than nulled later, so log-returns never see a zero price.
    work = pl.DataFrame({
        "__row": pl.int_range(0, n, eager=True),
        "time": _exec_time_index(df),
        "price": df["Execution Price"].cast(pl.Float64),
    })
    trades = work.filter((pl.col("price") > 0) & pl.col("time").is_not_null()).sort("time")
    if trades.height == 0:
        return out

    period = _polars_period(window)

    if method == "realized":
        trades = trades.with_columns(
            (pl.col("price") / pl.col("price").shift(1)).log().alias("log_ret")
        )
        agg = trades.rolling(index_column="time", period=period).agg(
            pl.col("log_ret").pow(2).sum().sqrt().alias("vol"),
            pl.col("log_ret").count().alias("n_ret"),
        )
        # A window with no realised return (the leading warm-up tick) is undefined
        # -> null, consistent with compute_spread / compute_flow_imbalance.
        vol = agg.select(
            pl.when(pl.col("n_ret") > 0).then(pl.col("vol")).otherwise(None)
        ).to_series()
        return out.scatter(trades["__row"], vol)

    # garman_klass: OHLC range estimator over the same trade-only rolling windows.
    agg = trades.rolling(index_column="time", period=period).agg(
        pl.col("price").max().alias("high"),
        pl.col("price").min().alias("low"),
        pl.col("price").first().alias("open"),
        pl.col("price").last().alias("close"),
    )

    import math

    ln2 = math.log(2)
    gk_vals: list[float | None] = []
    for hi, lo, op, cl in zip(
        agg["high"].to_list(),
        agg["low"].to_list(),
        agg["open"].to_list(),
        agg["close"].to_list(),
    ):
        if None in (hi, lo, op, cl) or lo == 0 or op == 0:
            gk_vals.append(None)
        else:
            hl_term = math.log(hi / lo) ** 2
            oc_term = math.log(cl / op) ** 2
            gk_vals.append(math.sqrt(max(0.0, 0.5 * hl_term - (2 * ln2 - 1) * oc_term)))

    return out.scatter(trades["__row"], pl.Series(gk_vals, dtype=pl.Float64))


def compute_all_features(
    df: pl.DataFrame,
    levels: int = 10,
    volatility_window: str = "5min",
    imbalance_window: str = "5min",
) -> pl.DataFrame:
    """Append spread, depth, flow-imbalance and volatility columns to ``df``.

    Runs :func:`compute_spread`, :func:`compute_depth`,
    :func:`compute_flow_imbalance` and :func:`compute_volatility` and returns a
    copy of ``df`` with their outputs added. Expects **English** column names.

    Args:
        df: An ``individual_stock`` tick frame.
        levels: Depth levels to include (1-10).
        volatility_window: Window for the realized-volatility column.
        imbalance_window: Window for the flow-imbalance column.

    Returns:
        ``df`` plus ``spread``, ``sell_depth_*`` / ``buy_depth_*``,
        ``flow_imbalance`` and ``volatility`` columns.
    """
    result = df.clone()
    result = result.with_columns(compute_spread(df).alias("spread"))

    depth = compute_depth(df, levels=levels, side="both")
    for col in depth.columns:
        result = result.with_columns(depth[col].alias(col))

    result = result.with_columns(
        compute_flow_imbalance(df, window=imbalance_window).alias("flow_imbalance")
    )
    result = result.with_columns(
        compute_volatility(df, window=volatility_window, method="realized").alias("volatility")
    )

    return result
