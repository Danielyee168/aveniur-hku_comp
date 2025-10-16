"""Time-series momentum with volatility targeting (M3)."""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

from ._base import finalize, prepare_minute_frame, to_float64


REQUIRED_FIELDS: Iterable[str] = (
    "symbol",
    "timestamp",
    "Close",
)

FACTOR_NAME = "tsmom_weighted"
DESCRIPTION = "Sign-based time-series momentum scaled to target volatility"
CATEGORY = "momentum"
DEFAULT_FREQUENCY = "min"


def compute(
    data: pd.DataFrame,
    lookback: int = 252,
    price_col: str = "Close",
    target_vol: float = 0.4,
) -> pd.DataFrame:
    """Compute time-series momentum signal with volatility scaling."""

    if lookback <= 0:
        raise ValueError("lookback must be positive")
    if target_vol <= 0:
        raise ValueError("target_vol must be positive")

    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", FACTOR_NAME])

    df, original_index = prepare_minute_frame(data)
    price = to_float64(df[price_col])
    ret = price.groupby(df["symbol"], sort=False).pct_change()

    grouped = ret.groupby(df["symbol"], sort=False)
    cum_ret = grouped.transform(lambda s: s.rolling(window=lookback, min_periods=lookback).sum())
    sigma = grouped.transform(lambda s: s.rolling(window=lookback, min_periods=lookback).std(ddof=0))

    weight = np.sign(cum_ret)
    sigma = sigma.replace({0.0: np.nan})
    scaled = weight * (target_vol / (sigma * np.sqrt(lookback)))
    scaled = scaled.replace([np.inf, -np.inf], np.nan).fillna(0.0)

    result = pd.DataFrame(
        {
            "symbol": df["symbol"].values,
            "timestamp": df["timestamp"].values,
            FACTOR_NAME: scaled.values,
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
