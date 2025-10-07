"""MAX factor: rolling maximum daily return."""

from __future__ import annotations

from typing import Iterable

import pandas as pd

from ._base import ensure_trade_date, finalize, prepare_minute_frame

REQUIRED_FIELDS: Iterable[str] = (
    "symbol",
    "timestamp",
    "Close",
)

FACTOR_NAME = "max_daily"
DESCRIPTION = "Rolling maximum daily close-to-close return"
CATEGORY = "momentum"
DEFAULT_FREQUENCY = "min"


def compute(data: pd.DataFrame, window: int = 21) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", FACTOR_NAME])
    if window <= 0:
        raise ValueError("window must be positive")

    df, original_index = prepare_minute_frame(data)
    trade_date = ensure_trade_date(df)
    trade_date.name = "trade_date"

    daily = (
        df.groupby([df["symbol"], trade_date], sort=False)["Close"]
        .last()
        .reset_index()
    )
    daily.sort_values(["symbol", "trade_date"], inplace=True)
    daily["prev_close"] = daily.groupby("symbol")["Close"].shift(1)
    daily["daily_ret"] = daily["Close"] / daily["prev_close"] - 1.0

    daily[FACTOR_NAME] = (
        daily.groupby("symbol")["daily_ret"]
        .rolling(window, min_periods=window)
        .max()
        .reset_index(level=0, drop=True)
    )

    merged = df[["symbol", "timestamp"]].copy()
    merged["trade_date"] = trade_date.values
    merged = merged.merge(
        daily[["symbol", "trade_date", FACTOR_NAME]],
        on=["symbol", "trade_date"],
        how="left",
    )
    merged[FACTOR_NAME] = merged[FACTOR_NAME].fillna(0.0)

    return finalize(merged, original_index)


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
            window_size=21,
        )
    except TypeError:
        registry.register(
            **kwargs,
            default_frequency=DEFAULT_FREQUENCY,
            default_fields=list(REQUIRED_FIELDS),
        )
