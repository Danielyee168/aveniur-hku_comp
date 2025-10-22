"""Cross-sectional momentum style factors (M1/M2/M10)."""

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

FACTOR_NAME = "mom_cross_sectional"
DESCRIPTION = "Cross-sectional momentum with configurable lookback/skip"
CATEGORY = "momentum"
DEFAULT_FREQUENCY = "min"


def _rolling_sum(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window=window, min_periods=window).sum()


def compute(
    data: pd.DataFrame,
    lookback: int = 252,
    skip: int = 21,
    price_col: str = "Close",
) -> pd.DataFrame:
    """Cross-sectional cumulative return excluding the most recent ``skip`` bars."""

    if lookback <= 0:
        raise ValueError("lookback must be positive")
    if skip < 0:
        raise ValueError("skip must be non-negative")
    if skip >= lookback:
        raise ValueError("skip must be smaller than lookback")

    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", FACTOR_NAME])

    df, original_index = prepare_minute_frame(data)
    price = to_float64(df[price_col])
    ret = price.groupby(df["symbol"], sort=False).pct_change()
    log_ret = np.log1p(ret)

    grouped = log_ret.groupby(df["symbol"], sort=False)
    total = grouped.transform(lambda s: _rolling_sum(s, lookback))

    if skip > 0:
        recent = grouped.transform(lambda s: _rolling_sum(s, skip))
        total = total - recent

    momentum = np.expm1(total)

    result = pd.DataFrame(
        {
            "symbol": df["symbol"].values,
            "timestamp": df["timestamp"].values,
            FACTOR_NAME: momentum.values,
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
