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
TYPE = "risk"
CATEGORY = "volatility"
DEFAULT_FREQUENCY = "hour"


def compute(
    data: pd.DataFrame,
    vol_window: int = 96,
    eps: float = 1e-12,
    col_name: str = 'factor',
) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", col_name])
    if vol_window <= 1:
        raise ValueError("vol_window must be greater than 1")

    df, original_index = prepare_minute_frame(data)
    df = df[["symbol", "timestamp", "Close"]].copy()

    df.set_index(["symbol", "timestamp"], inplace=True)

    close_matrix = df['Close'].unstack(level=0)
    log_close = np.log(close_matrix)
    ret_matrix = log_close.diff()

    vol_matrix = ret_matrix.rolling(vol_window, min_periods=vol_window).std(ddof=0)
    cs_mean = vol_matrix.mean(axis=1)
    cs_std = vol_matrix.std(axis=1)
    score = (vol_matrix.sub(cs_mean, axis=0)).div(cs_std.replace(0, np.nan) + eps, axis=0)
    score.ffill(inplace=True)

    bab_matrix = (-score)
    bab_series = bab_matrix.stack().rename(FACTOR_NAME).reorder_levels([1, 0]).sort_index()
    df[col_name] = bab_series
    df.dropna(inplace=True)
    out = df[col_name]
    return out

DEFAULT_CONFIG = {'name': FACTOR_NAME + '_96', 'factor_func': compute, 'frequency': DEFAULT_FREQUENCY,
                   'fields': ["symbol", "timestamp", "Close"],
                   'type_': TYPE, 'category': CATEGORY, 'description': DESCRIPTION}
