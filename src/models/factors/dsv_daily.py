"""Downside semivariance factor broadcast to minute bars."""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

from ._base import ensure_trade_date, finalize, prepare_minute_frame, to_float64

REQUIRED_FIELDS: Iterable[str] = (
    "symbol",
    "timestamp",
    "Close",
)

FACTOR_NAME = "dsv_daily"
DESCRIPTION = "Daily downside semivariance of log returns"
TYPE = "alpha"
CATEGORY = "volatility"
DEFAULT_FREQUENCY = "min"


def compute(data: pd.DataFrame, price_col: str = "Close", eps: float = 1e-12, col_name: str = FACTOR_NAME) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", col_name])

    df = data.copy()
    trade_date = ensure_trade_date(df)
    trade_date.name = "trade_date"
    price = to_float64(df[price_col]).clip(lower=eps)

    log_ret = np.log(price).groupby(df["symbol"], sort=False).diff()
    downside = log_ret.fillna(0.0).clip(upper=0.0).pow(2)

    dsv = downside.groupby([df["symbol"], trade_date], sort=False).transform("sum")

    result = pd.DataFrame(
        {
            "symbol": df["symbol"].values,
            "timestamp": df["timestamp"].values,
            FACTOR_NAME: dsv.values,
            "trade_date": trade_date.values,
        },
        index=df.index,
    )

    return df


DEFAULT_CONFIG = {'name': FACTOR_NAME, 'factor_func': compute, 'frequency': DEFAULT_FREQUENCY,
                   'fields': REQUIRED_FIELDS,
                   'type_': TYPE, 'category': CATEGORY, 'description': DESCRIPTION}
