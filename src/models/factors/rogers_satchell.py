"""Rogers–Satchell daily volatility estimator."""

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

FACTOR_NAME = "rogersSatchell_hour"
DESCRIPTION = "Hourly Rogers–Satchell volatility estimate"
TYPE = "risk"
CATEGORY = "volatility"
DEFAULT_FREQUENCY = "hour"


def compute(data: pd.DataFrame, col_name: str = FACTOR_NAME) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", FACTOR_NAME])

    df = data.copy()

    log_hc = np.log(df['High'] / df['Close'])
    log_ho = np.log(df['High'] / df['Open'])
    log_lc = np.log(df['Low'] / df['Close'])
    log_lo = np.log(df['Low'] / df['Open'])

    df[col_name] = log_hc * log_ho + log_lc * log_lo

    return df[['symbol', 'timestamp', col_name]]


DEFAULT_CONFIG = {'name': FACTOR_NAME, 'factor_func': compute, 'frequency': DEFAULT_FREQUENCY,
                   'fields': REQUIRED_FIELDS,
                   'type_': TYPE, 'category': CATEGORY, 'description': DESCRIPTION}
