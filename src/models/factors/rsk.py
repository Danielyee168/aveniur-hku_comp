"""Weekly realized skewness factor broadcast to minute bars."""

from __future__ import annotations

from typing import Iterable

import numpy as np
from numpy.lib.stride_tricks import sliding_window_view as swv
import pandas as pd


REQUIRED_FIELDS: Iterable[str] = (
    "symbol",
    "timestamp",
    "Close",
)

FACTOR_NAME = "rsk_min_2880"
DESCRIPTION = "hourly realized skewness of minute log returns"
TYPE = "alpha"
CATEGORY = "volatility"
DEFAULT_FREQUENCY = "min"


def compute(data: pd.DataFrame, price_col: str = "Close", window: int = 48*60, col_name: str = FACTOR_NAME, input_type: str = DEFAULT_FREQUENCY) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", col_name])

    window = int(window / 60)

    df = data.copy()
    df = df.sort_values('timestamp')
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df['log_ret'] = df.groupby("symbol")[price_col].diff()

    if input_type == 'min':
        df['hour'] = df['timestamp'].dt.floor('H')
        df['min_in_h'] = df['timestamp'].dt.minute

        wide = (df.pivot_table(index=['symbol', 'hour'],
                             columns='min_in_h',
                             values='log_ret').sort_index())
        records = []
        for s, group in wide.groupby(level=0):
            vw = swv(group, (window, 60), axis=(0, 1))
            flat = vw.reshape(vw.shape[0], -1)
            mean = flat.mean(axis=1, keepdims=True)
            centered = flat - mean
            sum_sq = np.sum(centered ** 2, axis=1)
            sum_cu = np.sum(centered ** 3, axis=1)
            skew = np.where(sum_sq <= 1e-16, 0.0, sum_cu / (sum_sq ** 1.5))
            hours = wide.loc[s].index[window - 1:]
            records.append(
                pd.DataFrame({'symbol': s, 'timestamp': hours, col_name: skew})
            )

        re = pd.concat(records).dropna().reset_index(drop=True)

    else:
        skew_se = df.set_index(['symbol', 'timestamp']).head(1000).groupby(level=0)['log_ret'].rolling(window
                                                ).apply(lambda x: x.skew()).reset_index(level=0, drop=True)
        re = skew_se.reset_index().dropna()
        re.columns = ['symbol', 'timestamp', col_name]

    return re


DEFAULT_CONFIG = {'name': FACTOR_NAME, 'factor_func': compute, 'frequency': DEFAULT_FREQUENCY,
                   'fields': REQUIRED_FIELDS,
                   'type_': TYPE, 'category': CATEGORY, 'description': DESCRIPTION}