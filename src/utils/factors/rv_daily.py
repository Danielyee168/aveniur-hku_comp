"""Realized volatility factor computed from aggregated OHLCV tables."""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

from ._base import finalize

REQUIRED_FIELDS: Iterable[str] = (
    "symbol",
    "timestamp",
    "Close",
)

FACTOR_NAME = "rv_daily"
DESCRIPTION = "Daily realised variance computed from aggregated bars"
CATEGORY = "volatility"
DEFAULT_FREQUENCY = "hour"


def _to_utc_datetime(series: pd.Series) -> pd.Series:
    if np.issubdtype(series.dtype, np.integer):
        return pd.to_datetime(series, unit="ms", utc=True, errors="coerce")
    return pd.to_datetime(series, utc=True, errors="coerce")


def compute(
    data: pd.DataFrame,
    price_col: str = "Close",
    eps: float = 1e-12,
) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", FACTOR_NAME])

    df = data.sort_values([c for c in ("symbol", "timestamp") if c in data.columns]).copy()
    original_index = df.index

    if price_col not in df.columns:
        raise ValueError(f"Column '{price_col}' not found in input data")

    ts = _to_utc_datetime(df["timestamp"])
    trade_date = ts.dt.strftime("%Y%m%d")

    price = df[price_col].astype("float64").clip(lower=eps)
    log_ret = np.log(price).groupby(df["symbol"], sort=False).diff()
    log_ret_sq = log_ret.fillna(0.0).pow(2)

    rv = log_ret_sq.groupby([df["symbol"], trade_date], sort=False).transform("sum")
    rv.name = FACTOR_NAME

    result = pd.DataFrame(
        {
            "symbol": df["symbol"].values,
            "timestamp": df["timestamp"].values,
            FACTOR_NAME: rv.values,
            "trade_date": trade_date.values,
        },
        index=original_index,
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
