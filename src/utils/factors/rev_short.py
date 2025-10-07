"""Short-horizon reversal factor."""

from __future__ import annotations

from typing import Iterable

import pandas as pd

from ._base import finalize, prepare_minute_frame, to_float64

REQUIRED_FIELDS: Iterable[str] = (
    "symbol",
    "timestamp",
    "Close",
)

FACTOR_NAME = "rev_short"
DESCRIPTION = "Negative sum of recent minute returns"
CATEGORY = "mean_reversion"
DEFAULT_FREQUENCY = "min"


def compute(data: pd.DataFrame, window: int = 60, price_col: str = "Close") -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", FACTOR_NAME])

    if window <= 0:
        raise ValueError("window must be positive")

    df, original_index = prepare_minute_frame(data)
    price = to_float64(df[price_col])

    ret1m = price.groupby(df["symbol"], sort=False).pct_change()
    rev = -ret1m.rolling(window, min_periods=window).sum()
    rev.name = FACTOR_NAME

    result = pd.DataFrame(
        {
            "symbol": df["symbol"].values,
            "timestamp": df["timestamp"].values,
            FACTOR_NAME: rev.fillna(0.0).values,
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
