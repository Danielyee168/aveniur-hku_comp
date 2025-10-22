"""Long-horizon reversal factor (R4)."""

from __future__ import annotations

from typing import Iterable

import pandas as pd

from ._base import finalize, prepare_minute_frame, to_float64


REQUIRED_FIELDS: Iterable[str] = (
    "symbol",
    "timestamp",
    "Close",
)

FACTOR_NAME = "rev_long_term"
DESCRIPTION = "Long-run reversal based on cumulative past returns"
CATEGORY = "reversal"
DEFAULT_FREQUENCY = "min"


def compute(
    data: pd.DataFrame,
    lookback: int = 1260,
    price_col: str = "Close",
) -> pd.DataFrame:
    """Return the negative cumulative return over a long horizon."""

    if lookback <= 0:
        raise ValueError("lookback must be positive")

    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", FACTOR_NAME])

    df, original_index = prepare_minute_frame(data)
    price = to_float64(df[price_col])
    ret = price.groupby(df["symbol"], sort=False).pct_change()

    cum_ret = ret.groupby(df["symbol"], sort=False).transform(
        lambda s: s.rolling(window=lookback, min_periods=lookback).sum()
    )

    signal = -cum_ret

    result = pd.DataFrame(
        {
            "symbol": df["symbol"].values,
            "timestamp": df["timestamp"].values,
            FACTOR_NAME: signal.values,
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
