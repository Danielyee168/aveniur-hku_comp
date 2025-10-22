"""Short-horizon reversal factor."""

from __future__ import annotations

from typing import Iterable

import pandas as pd

from ._base import to_float64

REQUIRED_FIELDS: Iterable[str] = (
    "symbol",
    "timestamp",
    "Close",
)

FACTOR_NAME = "revShort_min_240"
DESCRIPTION = "Negative sum of recent minute returns"
CATEGORY = "mean_reversion"
TYPE = 'alpha'
DEFAULT_FREQUENCY = "min"


def compute(data: pd.DataFrame, window: int = 240, price_col: str = "Close", col_name: str = FACTOR_NAME) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", col_name])

    if window <= 0:
        raise ValueError("window must be positive")

    df = data.copy()
    price = to_float64(df[price_col])

    ret1m = price.groupby(df["symbol"], sort=False).pct_change()
    rev = -ret1m.rolling(window, min_periods=window).sum()
    df[col_name] = rev
    df['timestamp'] = pd.to_datetime(df.timestamp, unit='ms', utc=True)
    df.dropna(inplace=True)
    out = (df
           .set_index('timestamp')
           .groupby('symbol')
           .resample('H')[col_name]
           .agg('sum')
           .reset_index())
    out['timestamp'] = out['timestamp'].dt.tz_localize(None)
    return out


DEFAULT_CONFIG = {'name': FACTOR_NAME, 'factor_func': compute, 'frequency': DEFAULT_FREQUENCY,
                   'fields': REQUIRED_FIELDS,
                   'type_': TYPE, 'category': CATEGORY, 'description': DESCRIPTION}
