"""Residual-based short-term reversal (R2)."""

from __future__ import annotations

from typing import Iterable

import pandas as pd

from ._base import finalize, prepare_minute_frame


REQUIRED_FIELDS: Iterable[str] = (
    "symbol",
    "timestamp",
    "residual",
)

FACTOR_NAME = "rev_residual"
DESCRIPTION = "Short-term reversal based on regression residuals"
CATEGORY = "reversal"
DEFAULT_FREQUENCY = "min"


def compute(
    data: pd.DataFrame,
    residual_col: str = "residual",
    lag: int = 1,
) -> pd.DataFrame:
    """Return the negative of lagged residuals."""

    if lag <= 0:
        raise ValueError("lag must be positive")

    if residual_col not in data.columns:
        raise KeyError(f"Data must contain '{residual_col}' for residual reversal")

    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", FACTOR_NAME])

    df, original_index = prepare_minute_frame(data)
    resid = df[residual_col].astype(float)

    signal = -resid.groupby(df["symbol"], sort=False).shift(lag)

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
