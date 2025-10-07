"""Corwin–Schultz bid-ask spread estimator (daily)."""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

from ._base import ensure_trade_date, finalize, prepare_minute_frame

REQUIRED_FIELDS: Iterable[str] = (
    "symbol",
    "timestamp",
    "High",
    "Low",
)

FACTOR_NAME = "corwin_schultz"
DESCRIPTION = "Corwin–Schultz two-day spread estimator"
CATEGORY = "liquidity"
DEFAULT_FREQUENCY = "min"

DENOM = 3.0 - 2.0 * np.sqrt(2.0)


def compute(data: pd.DataFrame) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", FACTOR_NAME])

    df, original_index = prepare_minute_frame(data)
    trade_date = ensure_trade_date(df)
    trade_date.name = "trade_date"
    group = df.groupby([df["symbol"], trade_date], sort=False)

    daily = group.agg({"High": "max", "Low": "min"}).reset_index()

    daily.sort_values(["symbol", "trade_date"], inplace=True)
    daily["prev_high"] = daily.groupby("symbol")["High"].shift(1)
    daily["prev_low"] = daily.groupby("symbol")["Low"].shift(1)

    for col in ("prev_high", "prev_low"):
        daily[col] = daily[col].astype("float64")

    mask = daily["prev_high"].notna() & daily["prev_low"].notna()
    beta = (np.log(daily.loc[mask, "High"] / daily.loc[mask, "Low"])) ** 2
    beta += (np.log(daily.loc[mask, "prev_high"] / daily.loc[mask, "prev_low"])) ** 2
    gamma = np.log(
        np.maximum(daily.loc[mask, "High"], daily.loc[mask, "prev_high"]) /
        np.minimum(daily.loc[mask, "Low"], daily.loc[mask, "prev_low"])
    ) ** 2

    with np.errstate(invalid="ignore"):
        alpha = (np.sqrt(2.0 * beta) - np.sqrt(beta)) / DENOM
        alpha -= np.sqrt(gamma / DENOM)
        spread = 2.0 * (np.exp(alpha) - 1.0) / (1.0 + np.exp(alpha))

    daily[FACTOR_NAME] = 0.0
    daily.loc[mask, FACTOR_NAME] = spread.fillna(0.0)

    merged = df[["symbol", "timestamp"]].copy()
    merged["trade_date"] = trade_date.values
    merged = merged.merge(
        daily[["symbol", "trade_date", FACTOR_NAME]],
        on=["symbol", "trade_date"],
        how="left",
    )

    result = merged
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
            type_="risk",
            window_size=1,
        )
    except TypeError:
        registry.register(
            **kwargs,
            default_frequency=DEFAULT_FREQUENCY,
            default_fields=list(REQUIRED_FIELDS),
        )
