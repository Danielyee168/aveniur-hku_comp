"""Volatility-managed momentum (M4)."""

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

FACTOR_NAME = "mom_vol_scaled"
DESCRIPTION = "Inverse-volatility scaling weights for momentum"
CATEGORY = "momentum"
DEFAULT_FREQUENCY = "min"


def compute(
    data: pd.DataFrame,
    lookback: int = 126,
    price_col: str = "Close",
    target_vol: float = 0.2,
    min_sigma: float = 1e-6,
    cap: float | None = None,
) -> pd.DataFrame:
    """Compute inverse-volatility weights for momentum-style signals."""

    if lookback <= 0:
        raise ValueError("lookback must be positive")
    if target_vol <= 0:
        raise ValueError("target_vol must be positive")

    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", FACTOR_NAME])

    df, original_index = prepare_minute_frame(data)
    price = to_float64(df[price_col])
    ret = price.groupby(df["symbol"], sort=False).pct_change()

    sigma = ret.groupby(df["symbol"], sort=False).transform(
        lambda s: s.rolling(window=lookback, min_periods=lookback).std(ddof=0)
    )

    sigma = sigma.clip(lower=min_sigma)
    weights = target_vol / (sigma * np.sqrt(lookback))

    if cap is not None:
        cap = abs(cap)
        weights = weights.clip(lower=-cap, upper=cap)

    weights = weights.replace([np.inf, -np.inf], np.nan).fillna(0.0)

    result = pd.DataFrame(
        {
            "symbol": df["symbol"].values,
            "timestamp": df["timestamp"].values,
            FACTOR_NAME: weights.values,
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
