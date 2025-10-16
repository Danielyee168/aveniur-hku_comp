"""Dynamic μ/σ² momentum weighting (M5)."""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

from ._base import finalize, prepare_minute_frame


REQUIRED_FIELDS: Iterable[str] = (
    "symbol",
    "timestamp",
    "mu_hat",
    "sigma_hat",
)

FACTOR_NAME = "mom_mu_over_sigma2"
DESCRIPTION = "Weights proportional to predicted mean divided by variance"
CATEGORY = "momentum"
DEFAULT_FREQUENCY = "min"


def compute(
    data: pd.DataFrame,
    mu_col: str = "mu_hat",
    sigma_col: str = "sigma_hat",
    min_sigma: float = 1e-6,
) -> pd.DataFrame:
    """Return μ̂/σ̂² weights given predicted mean/vol columns."""

    required = {mu_col, sigma_col}
    missing = required - set(data.columns)
    if missing:
        raise KeyError(f"Missing columns required for μ/σ² weighting: {missing}")

    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", FACTOR_NAME])

    df, original_index = prepare_minute_frame(data)

    mu = df[mu_col].astype(float)
    sigma = df[sigma_col].astype(float).replace([np.inf, -np.inf], np.nan)
    sigma = sigma.clip(lower=min_sigma)

    weight = mu / (sigma ** 2)
    weight = weight.replace([np.inf, -np.inf], np.nan).fillna(0.0)

    result = pd.DataFrame(
        {
            "symbol": df["symbol"].values,
            "timestamp": df["timestamp"].values,
            FACTOR_NAME: weight.values,
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
