"""Time-series momentum with volatility targeting (M3)."""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

from ._base import finalize, prepare_minute_frame, to_float64


REQUIRED_FIELDS: Iterable[str] = (
    "symbol",
    "timestamp",
    "Close",
)

FACTOR_NAME = "tsmomWeighted_hour_4"
DESCRIPTION = "Sign-based time-series momentum scaled to target volatility"
TYPE = "alpha"
CATEGORY = "momentum"
DEFAULT_FREQUENCY = "hour"


def compute(
    data: pd.DataFrame,
    window: int = 4,
    price_col: str = "Close",
    target_vol: float = 0.4,
    col_name: str = FACTOR_NAME,
) -> pd.DataFrame:
    """Compute time-series momentum signal with volatility scaling."""

    if window <= 0:
        raise ValueError("lookback must be positive")
    if target_vol <= 0:
        raise ValueError("target_vol must be positive")

    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", col_name])

    df = data.copy()
    price = to_float64(df[price_col])
    ret = price.groupby(df["symbol"], sort=False).pct_change()

    grouped = ret.groupby(df["symbol"], sort=False)
    cum_ret = grouped.transform(lambda s: s.rolling(window=window, min_periods=window).sum())
    sigma = grouped.transform(lambda s: s.rolling(window=window, min_periods=window).std(ddof=0))

    weight = np.sign(cum_ret)
    sigma = sigma.replace({0.0: np.nan})
    scaled = weight * (target_vol / (sigma * np.sqrt(window)))
    scaled = scaled.replace([np.inf, -np.inf], np.nan)

    df[col_name] = scaled
    return df


DEFAULT_CONFIG = {'name': FACTOR_NAME, 'factor_func': compute, 'frequency': DEFAULT_FREQUENCY,
                   'fields': REQUIRED_FIELDS,
                   'type_': TYPE, 'category': CATEGORY, 'description': DESCRIPTION}