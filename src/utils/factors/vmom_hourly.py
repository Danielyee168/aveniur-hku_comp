"""Volatility-managed momentum (hourly)."""

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
FACTOR_NAME = "vmom_hourly"
DESCRIPTION = "Volatility-managed hourly momentum"
CATEGORY = "momentum"
DEFAULT_FREQUENCY = "min"


def compute(
    data: pd.DataFrame,
    lookback: int = 8,
    vol_window: int = 8,
    eps: float = 1e-12,
) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", FACTOR_NAME])
    if lookback <= 0 or vol_window <= 0:
        raise ValueError("lookback and vol_window must be positive")

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
    momentum_matrix = log_close - log_close.shift(lookback)
    ret_matrix = log_close.diff()
    rv_matrix = ret_matrix.pow(2).rolling(vol_window, min_periods=vol_window).sum()
    sigma_matrix = np.sqrt(rv_matrix) + eps
    vmom_matrix = momentum_matrix / sigma_matrix
    vmom_series = (
        vmom_matrix.stack().rename(FACTOR_NAME).reorder_levels([1, 0]).sort_index()
    )

    keys = list(zip(df["symbol"].tolist(), df["hour"].tolist()))
    vmom_dict = vmom_series.to_dict()
    aligned = np.array([vmom_dict.get(key, np.nan) for key in keys])
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
