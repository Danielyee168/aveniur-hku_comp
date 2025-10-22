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
FACTOR_NAME = "vmom_hour_8"
TYPE = 'alpha'
DESCRIPTION = "Volatility-managed hourly momentum"
CATEGORY = "momentum"
DEFAULT_FREQUENCY = "hour"


def compute(
    data: pd.DataFrame,
    window: int = 8,
    eps: float = 1e-12,
    col_name: str = FACTOR_NAME,
) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", FACTOR_NAME])
    if window <= 0:
        raise ValueError("lookback and vol_window must be positive")

    df = data.copy()
    df.set_index(["symbol", "timestamp"], inplace=True)

    close_matrix = df.unstack(level=0)
    log_close = np.log(close_matrix)
    momentum_matrix = log_close - log_close.shift(window)
    ret_matrix = log_close.diff()
    rv_matrix = ret_matrix.pow(2).rolling(window).sum()
    sigma_matrix = np.sqrt(rv_matrix) + eps
    vmom_matrix = momentum_matrix / sigma_matrix
    vmom_series = (
        vmom_matrix.stack().rename(col_name).reorder_levels([1, 0]).sort_index()
    )

    df[col_name] = vmom_series

    return df['symbol', 'timestamp', col_name]


DEFAULT_CONFIG = {'name': FACTOR_NAME, 'factor_func': compute, 'frequency': DEFAULT_FREQUENCY,
                   'fields': REQUIRED_FIELDS,
                   'type_': TYPE, 'category': CATEGORY, 'description': DESCRIPTION}