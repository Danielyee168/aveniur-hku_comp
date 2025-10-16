"""Residual momentum factor (M6)."""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

from ._base import finalize, prepare_minute_frame


REQUIRED_FIELDS: Iterable[str] = (
    "symbol",
    "timestamp",
    "residual",
)

FACTOR_NAME = "mom_residual"
DESCRIPTION = "Momentum on regression residuals with volatility scaling"
CATEGORY = "momentum"
DEFAULT_FREQUENCY = "min"


def compute(
    data: pd.DataFrame,
    residual_col: str = "residual",
    lookback: int = 252,
    skip: int = 21,
) -> pd.DataFrame:
    """Compute residual momentum with skip-period exclusion."""

    if lookback <= 0:
        raise ValueError("lookback must be positive")
    if skip < 0:
        raise ValueError("skip must be non-negative")
    if skip >= lookback:
        raise ValueError("skip must be smaller than lookback")

    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", FACTOR_NAME])

    if residual_col not in data.columns:
        raise KeyError(f"Data must contain column '{residual_col}' for residual momentum")

    df, original_index = prepare_minute_frame(data)
    resid = df[residual_col].astype(float)

    def _resid_mom(series: pd.Series) -> pd.Series:
        roll = series.rolling(window=lookback, min_periods=lookback).sum()
        if skip > 0:
            recent = series.rolling(window=skip, min_periods=skip).sum()
            roll = roll - recent
        sigma = series.rolling(window=lookback - skip, min_periods=lookback - skip).std(ddof=0)
        return roll / sigma

    signal = resid.groupby(df["symbol"], sort=False).transform(_resid_mom)
    signal = signal.replace([np.inf, -np.inf], np.nan)

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
