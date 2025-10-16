"""Short-term reversal factors (R1/R3)."""

from __future__ import annotations

from typing import Iterable

import pandas as pd

from ._base import finalize, prepare_minute_frame, to_float64


REQUIRED_FIELDS: Iterable[str] = (
    "symbol",
    "timestamp",
    "Close",
)

FACTOR_NAME = "rev_short_term"
DESCRIPTION = "Short-horizon reversal using negative recent returns"
CATEGORY = "reversal"
DEFAULT_FREQUENCY = "min"


def compute(
    data: pd.DataFrame,
    lag: int = 1,
    agg: str = "last",
    price_col: str = "Close",
) -> pd.DataFrame:
    """Return negative of the last ``lag`` return (mean or sum)."""

    if lag <= 0:
        raise ValueError("lag must be positive")

    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", FACTOR_NAME])

    df, original_index = prepare_minute_frame(data)
    price = to_float64(df[price_col])
    ret = price.groupby(df["symbol"], sort=False).pct_change()

    grouped = ret.groupby(df["symbol"], sort=False)
    if agg == "sum":
        signal = -grouped.transform(lambda s: s.shift(1).rolling(window=lag, min_periods=lag).sum())
    else:  # default last-bar signal
        signal = -grouped.shift(1)
        if lag > 1:
            signal = -grouped.transform(lambda s: s.shift(1).rolling(window=lag, min_periods=lag).mean())

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
