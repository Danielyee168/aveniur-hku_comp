"""Hourly low-volatility cross-sectional factor (BAB-style)."""

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
FACTOR_NAME = "bab_hourly"
DESCRIPTION = "Negative cross-sectional z-score of hourly volatility"
CATEGORY = "risk"
DEFAULT_FREQUENCY = "min"


def compute(
    data: pd.DataFrame,
    vol_window: int = 96,
    eps: float = 1e-12,
) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", FACTOR_NAME])
    if vol_window <= 1:
        raise ValueError("vol_window must be greater than 1")

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

    close_matrix = hourly_close.unstack(level=0)
    log_close = np.log(close_matrix)
    ret_matrix = log_close.diff()

    vol_matrix = ret_matrix.rolling(vol_window, min_periods=vol_window).std(ddof=0)
    cs_mean = vol_matrix.mean(axis=1)
    cs_std = vol_matrix.std(axis=1)
    score = (vol_matrix.sub(cs_mean, axis=0)).div(cs_std.replace(0, np.nan) + eps, axis=0)

    bab_matrix = (-score).fillna(0.0)
    bab_series = (
        bab_matrix.stack().rename(FACTOR_NAME).reorder_levels([1, 0]).sort_index()
    )

    keys = list(zip(df["symbol"].tolist(), df["hour"].tolist()))
    bab_dict = bab_series.to_dict()
    aligned = np.array([bab_dict.get(key, 0.0) for key in keys])
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
