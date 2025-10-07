"""Parkinson volatility estimator per day."""

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

FACTOR_NAME = "parkinson"
DESCRIPTION = "Daily Parkinson high-low volatility estimator"
CATEGORY = "volatility"
DEFAULT_FREQUENCY = "min"

CONST = 4.0 * np.log(2.0)


def compute(data: pd.DataFrame) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", FACTOR_NAME])

    df, original_index = prepare_minute_frame(data)
    trade_date = ensure_trade_date(df)
    trade_date.name = "trade_date"

    daily_high = df.groupby([df["symbol"], trade_date], sort=False)["High"].transform("max")
    daily_low = df.groupby([df["symbol"], trade_date], sort=False)["Low"].transform("min")

    ratio = np.log(daily_high / daily_low).pow(2)
    values = ratio / CONST

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
