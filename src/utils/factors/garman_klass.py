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

FACTOR_NAME = "garman_klass"
DESCRIPTION = "Daily Garman–Klass volatility estimate"
CATEGORY = "volatility"
DEFAULT_FREQUENCY = "min"

CONST = 2.0 * np.log(2.0) - 1.0


def compute(data: pd.DataFrame) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", FACTOR_NAME])

    df, original_index = prepare_minute_frame(data)
    trade_date = ensure_trade_date(df)
    trade_date.name = "trade_date"

    grouped = df.groupby([df["symbol"], trade_date], sort=False)
    daily_high = grouped["High"].transform("max")
    daily_low = grouped["Low"].transform("min")
    daily_open = grouped["Open"].transform("first")
    daily_close = grouped["Close"].transform("last")

    log_hl = np.log(daily_high / daily_low)
    log_co = np.log(daily_close / daily_open)
    values = 0.5 * log_hl.pow(2) - CONST * log_co.pow(2)

    result = pd.DataFrame(
        {
            "symbol": df["symbol"].values,
            "timestamp": df["timestamp"].values,
            FACTOR_NAME: values.values,
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
            type_="risk",
            window_size=0,
        )
    except TypeError:
        registry.register(
            **kwargs,
            default_frequency=DEFAULT_FREQUENCY,
            default_fields=list(REQUIRED_FIELDS),
        )
