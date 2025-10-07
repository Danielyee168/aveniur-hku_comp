"""Weekly realized skewness factor broadcast to minute bars."""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

from ._base import ensure_trade_date, ensure_week_key, finalize, prepare_minute_frame, to_float64

REQUIRED_FIELDS: Iterable[str] = (
    "symbol",
    "timestamp",
    "Close",
)

FACTOR_NAME = "rsk_weekly"
DESCRIPTION = "Weekly realized skewness of minute log returns"
CATEGORY = "volatility"
DEFAULT_FREQUENCY = "min"


def _skew_transform(group: pd.Series) -> pd.Series:
    if group.isna().all():
        return pd.Series(0.0, index=group.index)
    centered = group - group.mean()
    var = centered.pow(2).mean()
    if var <= 1e-16:
        val = 0.0
    else:
        val = centered.pow(3).mean() / (var ** 1.5)
    return pd.Series(val, index=group.index)


def compute(data: pd.DataFrame, price_col: str = "Close", eps: float = 1e-12) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", FACTOR_NAME])

    df, original_index = prepare_minute_frame(data)
    price = to_float64(df[price_col]).clip(lower=eps)
    log_ret = np.log(price).groupby(df["symbol"], sort=False).diff().fillna(0.0)

    week_key = ensure_week_key(df)
    skew = log_ret.groupby([df["symbol"], week_key], sort=False).transform(_skew_transform)
    skew.name = FACTOR_NAME

    result = pd.DataFrame(
        {
            "symbol": df["symbol"].values,
            "timestamp": df["timestamp"].values,
            FACTOR_NAME: skew.values,
            "trade_date": ensure_trade_date(df).values,
            "week": week_key.values,
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
