"""Residual momentum after removing market component."""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

from ._base import (
    finalize,
    hour_bucket,
    prepare_minute_frame,
    to_float64,
)

REQUIRED_FIELDS: Iterable[str] = ("symbol", "timestamp", "Close")
FACTOR_NAME = "resmom_hourly"
DESCRIPTION = "Residual momentum after regressing on market returns"
CATEGORY = "momentum"
DEFAULT_FREQUENCY = "min"


def compute(
    data: pd.DataFrame,
    market_symbol: str = "BTCUSDT",
    regression_window: int = 120,
    momentum_window: int = 12,
    eps: float = 1e-12,
) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", FACTOR_NAME])
    if regression_window <= 1 or momentum_window <= 0:
        raise ValueError("regression_window must be >1 and momentum_window positive")

    df, original_index = prepare_minute_frame(data)
    df = df[["symbol", "timestamp", "Close"]].copy()
    df["hour"] = hour_bucket(df["timestamp"])

    hourly_close = (
        df.sort_values(["symbol", "hour", "timestamp"])
        .groupby(["symbol", "hour"], sort=False)["Close"]
        .last()
        .astype("float64")
        .clip(lower=eps)
    )

    log_close = np.log(hourly_close)
    hourly_ret = log_close.groupby(level=0, sort=False).diff().rename("ret")

    returns_matrix = hourly_ret.unstack(level=0)
    if market_symbol not in returns_matrix.columns:
        raise ValueError(f"market_symbol '{market_symbol}' not found in data")

    market_ret = returns_matrix[market_symbol].copy()
    var_market = market_ret.rolling(regression_window, min_periods=regression_window).var() + eps

    residuals = {}
    for symbol in returns_matrix.columns:
        asset_ret = returns_matrix[symbol]
        cov = asset_ret.rolling(regression_window, min_periods=regression_window).cov(market_ret)
        beta = cov / var_market
        resid = asset_ret - beta * market_ret
        residuals[symbol] = resid

    residual_df = pd.DataFrame(residuals)
    res_mom = (
        residual_df.shift(1)
        .rolling(momentum_window, min_periods=momentum_window)
        .sum()
    )

    res_mom_series = (
        res_mom.stack().rename(FACTOR_NAME).reorder_levels([1, 0]).sort_index()
    )

    keys = list(zip(df["symbol"].tolist(), df["hour"].tolist()))
    resmom_dict = res_mom_series.to_dict()
    aligned = np.array([resmom_dict.get(key, np.nan) for key in keys])
    out = pd.DataFrame(
        {
            "symbol": df["symbol"].values,
            "timestamp": df["timestamp"].values,
            FACTOR_NAME: aligned,
        },
        index=df.index,
    )
    return finalize(out, original_index)


def register(registry) -> None:
    kwargs = dict(
        name=FACTOR_NAME,
        factor_func=compute,
        description=DESCRIPTION,
        category=CATEGORY,
        preferred_batch_mode="date",
    )
    try:
        registry.register(
            **kwargs,
            frequency=DEFAULT_FREQUENCY,
            fields=list(REQUIRED_FIELDS),
            type_="alpha",
            window_size=0,
        )
    except TypeError:
        registry.register(
            **kwargs,
            default_frequency=DEFAULT_FREQUENCY,
            default_fields=list(REQUIRED_FIELDS),
        )
