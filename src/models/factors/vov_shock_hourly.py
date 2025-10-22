"""Volatility-of-volatility shock factor."""

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
FACTOR_NAME = "vov_shock_hourly"
DESCRIPTION = "Negative z-scored innovation in realized volatility"
CATEGORY = "volatility"
DEFAULT_FREQUENCY = "min"


def _mad(arr: np.ndarray) -> float:
    median = np.median(arr)
    return np.median(np.abs(arr - median))


def compute(
    data: pd.DataFrame,
    rv_window: int = 6,
    shock_window: int = 96,
    eps: float = 1e-12,
) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", FACTOR_NAME])
    if rv_window <= 0 or shock_window <= rv_window:
        raise ValueError("rv_window must be >0 and shock_window > rv_window")

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
    hourly_ret = log_close.groupby(level=0, sort=False).diff()

    rv = (
        hourly_ret.pow(2)
        .groupby(level=0, sort=False)
        .rolling(rv_window, min_periods=rv_window)
        .sum()
        .rename("rv")
    )

    rv_series = rv.reset_index(level=0, drop=True)
    symbol_index = rv.index.get_level_values(0)

    def compute_shock(series: pd.Series) -> pd.Series:
        lagged = series.shift(1)
        median = lagged.rolling(shock_window, min_periods=shock_window).median()
        mad = lagged.rolling(shock_window, min_periods=shock_window).apply(
            lambda x: _mad(x), raw=True
        )
        shock_vals = -((series - median) / (mad + eps))
        return shock_vals

    shock = (
        rv_series.groupby(symbol_index, group_keys=False)
        .apply(compute_shock)
        .rename(FACTOR_NAME)
    )

    shock.index = rv.index
    keys = list(zip(df["symbol"].tolist(), df["hour"].tolist()))
    shock_dict = shock.to_dict()
    aligned = np.array([shock_dict.get(key, 0.0) for key in keys])
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
            type_="risk",
            window_size=0,
        )
    except TypeError:
        registry.register(
            **kwargs,
            default_frequency=DEFAULT_FREQUENCY,
            default_fields=list(REQUIRED_FIELDS),
        )
