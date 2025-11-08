"""Realized volatility factor computed from aggregated OHLCV tables."""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd


REQUIRED_FIELDS: Iterable[str] = (
    "symbol",
    "timestamp",
    "Close",
)

FACTOR_NAME = "rv_hour_24"
DESCRIPTION = "realised variance computed from aggregated bars of last N (default 24) hours"
TYPE = 'alpha'
CATEGORY = "volatility"
DEFAULT_FREQUENCY = "hour"


def cal_period_rv(series: pd.Series) -> float:
    x = np.log(series)
    log_ret = np.diff(x)
    log_ret_sq = log_ret ** 2

    return float(np.sum(log_ret_sq))


def compute(
    data: pd.DataFrame,
    price_col: str = "Close",
    eps: float = 1e-12,
    col_name: str = "factor",
    window: int = 24
) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", col_name])

    df = data.sort_values(["symbol", "timestamp"]).copy()

    if price_col not in df.columns:
        raise ValueError(f"Column '{price_col}' not found in input data")
    df[col_name] = df.groupby('symbol')[price_col].apply(lambda x: x.astype("float64").clip(lower=eps).rolling(
        window).apply(cal_period_rv, raw=True)).reset_index(level=0, drop=True)

    return df


DEFAULT_CONFIG = {'name': FACTOR_NAME, 'factor_func': compute, 'frequency': DEFAULT_FREQUENCY,
                   'fields': REQUIRED_FIELDS,
                   'type_': TYPE, 'category': CATEGORY, 'description': DESCRIPTION}