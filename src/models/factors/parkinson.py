"""Parkinson volatility estimator per day."""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

REQUIRED_FIELDS: Iterable[str] = (
    "symbol",
    "timestamp",
    "High",
    "Low",
)

FACTOR_NAME = "parkinson_hour"
DESCRIPTION = "Hourly Parkinson high-low volatility estimator"
TYPE = "risk"
CATEGORY = "volatility"
DEFAULT_FREQUENCY = "hour"

CONST = 4.0 * np.log(2.0)


def compute(data: pd.DataFrame, col_name: str = FACTOR_NAME) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", col_name])

    df = data.copy()

    daily_high = df["High"]
    daily_low = df["Low"]

    ratio = np.log(daily_high / daily_low).pow(2)
    values = ratio / CONST

    df[col_name] = values

    return df[["symbol", "timestamp", col_name]]


DEFAULT_CONFIG = {'name': FACTOR_NAME, 'factor_func': compute, 'frequency': DEFAULT_FREQUENCY,
                   'fields': REQUIRED_FIELDS,
                   'type_': TYPE, 'category': CATEGORY, 'description': DESCRIPTION}
