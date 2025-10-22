"""Dynamic μ/σ² momentum weighting (M5)."""

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

FACTOR_NAME = "mom_mu_over_sigma2"
DESCRIPTION = "Weights proportional to predicted mean divided by variance"
CATEGORY = "momentum"
DEFAULT_FREQUENCY = "min"


def compute(
    data: pd.DataFrame,
    mu_col: str = "mu_hat",
    sigma_col: str = "sigma_hat",
    min_sigma: float = 1e-6,
    lookback_mu: int = 252,
    lookback_sigma: int = 252,
    price_col: str = "Close",
) -> pd.DataFrame:
    """Return μ̂/σ̂² weights given predicted mean/vol columns."""

    if lookback_mu <= 0 or lookback_sigma <= 0:
        raise ValueError("lookback_mu and lookback_sigma must be positive")

    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", FACTOR_NAME])

    df, original_index = prepare_minute_frame(data)

    if mu_col in df.columns and sigma_col in df.columns:
        mu = df[mu_col].astype(float)
        sigma = df[sigma_col].astype(float).replace([np.inf, -np.inf], np.nan)
    else:
        price = to_float64(df[price_col])
        ret = price.groupby(df["symbol"], sort=False).pct_change()

        grouped = ret.groupby(df["symbol"], sort=False)
        mu = grouped.transform(
            lambda s: s.rolling(window=lookback_mu, min_periods=lookback_mu).mean()
        )
        sigma = grouped.transform(
            lambda s: s.rolling(window=lookback_sigma, min_periods=lookback_sigma).std(ddof=0)
        )

    mu = mu.fillna(0.0)
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
