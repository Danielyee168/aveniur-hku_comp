"""Corwin–Schultz bid-ask spread estimator (daily)."""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

from ._base import ensure_trade_date, finalize, prepare_minute_frame

REQUIRED_FIELDS: Iterable[str] = (
    "symbol",
    "timestamp",
    "High",
    "Low",
)

FACTOR_NAME = "corwin_schultz"
DESCRIPTION = "Corwin–Schultz two-day spread estimator"
TYPE = "risk"
CATEGORY = "liquidity"
DEFAULT_FREQUENCY = "daily"

K = 2.0 - np.sqrt(2.0)


def compute(data: pd.DataFrame, col_name:str = 'factor') -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", col_name])

    data = data.copy()

    data.sort_values(["symbol", "timestamp"], inplace=True)
    data["prev_high"] = data.groupby("symbol")["High"].shift(1)
    data["prev_low"] = data.groupby("symbol")["Low"].shift(1)

    for col in ("prev_high", "prev_low"):
        data[col] = data[col].astype("float64")

    mask = data["prev_high"].notna() & data["prev_low"].notna()
    beta = (np.log(data.loc[mask, "High"] / data.loc[mask, "Low"])) ** 2
    beta += (np.log(data.loc[mask, "prev_high"] / data.loc[mask, "prev_low"])) ** 2
    gamma = np.log(
        np.maximum(data.loc[mask, "High"], data.loc[mask, "prev_high"]) /
        np.minimum(data.loc[mask, "Low"], data.loc[mask, "prev_low"])
    ) ** 2

    beta_adj = np.maximum(beta - gamma, 0.0)
    with np.errstate(invalid="ignore"):
        alpha = (np.sqrt(2.0 * beta_adj) - np.sqrt(beta_adj)) / K
        spread = 2.0 * (np.exp(alpha) - 1.0) / (1.0 + np.exp(alpha))

    result = pd.DataFrame(columns=["symbol", "timestamp", col_name])
    result['timestamp'] = data["timestamp"]
    result['symbol'] = data["symbol"]
    result.loc[mask, col_name] = spread.fillna(0.0)

    return result.dropna()

DEFAULT_CONFIG = {'name': FACTOR_NAME, 'factor_func': compute, 'frequency': DEFAULT_FREQUENCY,
                   'fields': REQUIRED_FIELDS,
                   'type_': TYPE, 'category': CATEGORY, 'description': DESCRIPTION}