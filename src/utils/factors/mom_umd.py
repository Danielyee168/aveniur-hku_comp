"""Carhart UMD-style momentum signal (M7)."""

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

FACTOR_NAME = "mom_umd"
DESCRIPTION = "UMD-style long/short indicators based on cross-sectional ranks"
CATEGORY = "momentum"
DEFAULT_FREQUENCY = "min"


def compute(
    data: pd.DataFrame,
    lookback: int = 252,
    skip: int = 21,
    price_col: str = "Close",
    quantile: float = 0.1,
) -> pd.DataFrame:
    """Return (+1, −1, 0) based on cross-sectional momentum ranks."""

    if lookback <= 0:
        raise ValueError("lookback must be positive")
    if skip < 0:
        raise ValueError("skip must be non-negative")
    if skip >= lookback:
        raise ValueError("skip must be smaller than lookback")
    if not (0 < quantile < 0.5):
        raise ValueError("quantile should be between 0 and 0.5")

    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", FACTOR_NAME])

    df, original_index = prepare_minute_frame(data)
    price = to_float64(df[price_col])
    ret = price.groupby(df["symbol"], sort=False).pct_change()
    log_ret = np.log1p(ret)

    grouped = log_ret.groupby(df["symbol"], sort=False)
    total = grouped.transform(lambda s: s.rolling(window=lookback, min_periods=lookback).sum())
    if skip > 0:
        recent = grouped.transform(lambda s: s.rolling(window=skip, min_periods=skip).sum())
        total = total - recent

    momentum = np.expm1(total)

    ranks = momentum.groupby(df["timestamp"], sort=False).transform(
        lambda x: x.rank(pct=True, method="average")
    )

    long_mask = ranks >= (1 - quantile)
    short_mask = ranks <= quantile

    signal = long_mask.astype(float) - short_mask.astype(float)

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
