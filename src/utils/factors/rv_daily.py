"""Realized volatility factor compatible with CalFactorFramework."""

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

FACTOR_NAME = "rv_daily"
DESCRIPTION = "Daily realised variance broadcast to minute bars"
CATEGORY = "volatility"
DEFAULT_FREQUENCY = "min"


def compute(data: pd.DataFrame, price_col: str = "Close", eps: float = 1e-12) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", FACTOR_NAME])

    df, original_index = prepare_minute_frame(data)
    trade_date = ensure_trade_date(df)

    price = to_float64(df[price_col]).clip(lower=eps)
    log_ret = np.log(price).groupby(df["symbol"], sort=False).diff()
    log_ret_sq = log_ret.fillna(0.0).pow(2)

    rv = log_ret_sq.groupby([df["symbol"], trade_date], sort=False).transform("sum")
    rv.name = FACTOR_NAME

    result = pd.DataFrame(
        {
            "symbol": df["symbol"].values,
            "timestamp": df["timestamp"].values,
            FACTOR_NAME: rv.values,
        },
        index=df.index,
    )
    if "trade_date" in df.columns:
        result["trade_date"] = df["trade_date"].values
    else:
        result["trade_date"] = trade_date.values

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
