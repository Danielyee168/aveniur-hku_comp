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
CATEGORY = "volatility"
DEFAULT_FREQUENCY = "min"


def compute(data: pd.DataFrame, price_col: str = "Close", eps: float = 1e-12) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", FACTOR_NAME])

    df, original_index = prepare_minute_frame(data)
    trade_date = ensure_trade_date(df)
    trade_date.name = "trade_date"
    price = to_float64(df[price_col]).clip(lower=eps)

    log_ret = np.log(price).groupby(df["symbol"], sort=False).diff()
    downside = log_ret.fillna(0.0).clip(upper=0.0).pow(2)

    dsv = downside.groupby([df["symbol"], trade_date], sort=False).transform("sum")
    dsv.name = FACTOR_NAME

    result = pd.DataFrame(
        {
            "symbol": df["symbol"].values,
            "timestamp": df["timestamp"].values,
            FACTOR_NAME: dsv.values,
            "trade_date": trade_date.values,
        },
        index=df.index,
    )

    return finalize(result, original_index)


def register(registry) -> None:
    kwargs = dict(
        name=FACTOR_NAME,
        factor_func=compute,
        description=DESCRIPTION,
        category=CATEGORY,
        preferred_batch_mode="date",
    )
    try:
        registry.register(
            **kwargs,
            frequency=DEFAULT_FREQUENCY,
            fields=list(REQUIRED_FIELDS),
            type_="alpha",
            window_size=0,
        )
    except TypeError:
        registry.register(
            **kwargs,
            default_frequency=DEFAULT_FREQUENCY,
            default_fields=list(REQUIRED_FIELDS),
        )
