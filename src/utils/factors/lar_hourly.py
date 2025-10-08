"""Liquidity-adjusted reversal factor."""

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

REQUIRED_FIELDS: Iterable[str] = ("symbol", "timestamp", "Close", "Volume")
FACTOR_NAME = "lar_hourly"
DESCRIPTION = "Liquidity-adjusted hourly reversal"
CATEGORY = "mean_reversion"
DEFAULT_FREQUENCY = "min"


def _zscore(series: pd.Series, window: int, eps: float) -> pd.Series:
    mean = series.rolling(window, min_periods=window).mean()
    std = series.rolling(window, min_periods=window).std(ddof=0)
    return (series - mean) / (std + eps)


def compute(
    data: pd.DataFrame,
    reversal_window: int = 6,
    illiq_window: int = 48,
    eps: float = 1e-12,
) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", FACTOR_NAME])
    if reversal_window <= 0 or illiq_window <= 0:
        raise ValueError("windows must be positive")

    df, original_index = prepare_minute_frame(data)
    df = df[["symbol", "timestamp", "Close", "Volume"]].copy()
    df["hour"] = hour_bucket(df["timestamp"])

    grouped = df.sort_values(["symbol", "hour", "timestamp"])
    hourly_close = grouped.groupby(["symbol", "hour"], sort=False)["Close"].last().astype("float64")
    hourly_volume = grouped.groupby(["symbol", "hour"], sort=False)["Volume"].sum().astype("float64")

    close_matrix = hourly_close.unstack(level=0)
    volume_matrix = hourly_volume.unstack(level=0)

    log_close = np.log(close_matrix.clip(lower=eps))
    ret_matrix = log_close.diff()

    reversal = -ret_matrix.shift(1).rolling(reversal_window, min_periods=reversal_window).sum()
    dollar_vol = close_matrix * volume_matrix + eps
    illiq = (
        (ret_matrix.abs() / dollar_vol)
        .shift(1)
        .rolling(illiq_window, min_periods=illiq_window)
        .mean()
    )

    rev_mean = reversal.rolling(illiq_window, min_periods=illiq_window).mean()
    rev_std = reversal.rolling(illiq_window, min_periods=illiq_window).std(ddof=0)
    rev_z = (reversal - rev_mean) / (rev_std + eps)

    illiq_mean = illiq.rolling(illiq_window, min_periods=illiq_window).mean()
    illiq_std = illiq.rolling(illiq_window, min_periods=illiq_window).std(ddof=0)
    illiq_z = (illiq - illiq_mean) / (illiq_std + eps)

    lar_matrix = rev_z * illiq_z
    lar_series = (
        lar_matrix.stack().rename(FACTOR_NAME).reorder_levels([1, 0]).sort_index()
    )

    keys = list(zip(df["symbol"].tolist(), df["hour"].tolist()))
    lar_dict = lar_series.to_dict()
    aligned = np.array([lar_dict.get(key, 0.0) for key in keys])
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
