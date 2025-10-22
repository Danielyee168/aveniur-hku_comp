"""Time-series momentum factor (minute-level)."""

from __future__ import annotations

from typing import Iterable
import pandas as pd

REQUIRED_FIELDS: Iterable[str] = (
    "symbol",
    "timestamp",
    "Close",
)

FACTOR_NAME = "tsmom_hour_4"
DESCRIPTION = "L-period close-to-close momentum"
TYPE = "alpha"
CATEGORY = "momentum"
DEFAULT_FREQUENCY = "hour"


def compute(data: pd.DataFrame, window: int = 4, price_col: str = "Close", col_name:str = "factor") -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", FACTOR_NAME])

    if window <= 0:
        raise ValueError("lookback must be positive")

    df = data.copy()
    df.sort_values(["symbol", "timestamp"], inplace=True)
    df[col_name] = df.groupby("symbol")[price_col].pct_change(window)

    return df.dropna()

DEFAULT_CONFIG = {'name': FACTOR_NAME, 'factor_func': compute, 'frequency': DEFAULT_FREQUENCY,
                   'fields': REQUIRED_FIELDS,
                   'type_': TYPE, 'category': CATEGORY, 'description': DESCRIPTION}
