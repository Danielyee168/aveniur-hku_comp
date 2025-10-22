"""Garman-Klass daily volatility estimator."""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

from ._base import ensure_trade_date, finalize, prepare_minute_frame

REQUIRED_FIELDS: Iterable[str] = (
    "symbol",
    "timestamp",
    "Open",
    "High",
    "Low",
    "Close",
)

FACTOR_NAME = "garmanKlass_hour"
DESCRIPTION = "Hourly Garman–Klass volatility estimate"
TYPE = "risk"
CATEGORY = "volatility"
DEFAULT_FREQUENCY = "hour"

CONST = 2.0 * np.log(2.0) - 1.0


def compute(data: pd.DataFrame, col_name: str = FACTOR_NAME) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", col_name])

    df = data.copy()

    daily_high = df["High"]
    daily_low = df["Low"]
    daily_open = df["Open"]
    daily_close = df["Close"]

    log_hl = np.log(daily_high / daily_low)
    log_co = np.log(daily_close / daily_open)
    values = 0.5 * log_hl.pow(2) - CONST * log_co.pow(2)

    df[col_name] = values

    return df[["symbol", "timestamp", col_name]]


DEFAULT_CONFIG = {'name': FACTOR_NAME, 'factor_func': compute, 'frequency': DEFAULT_FREQUENCY,
                   'fields': REQUIRED_FIELDS,
                   'type_': TYPE, 'category': CATEGORY, 'description': DESCRIPTION}
