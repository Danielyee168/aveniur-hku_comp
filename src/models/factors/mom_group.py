"""Group-level momentum factors (M8/M9)."""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

import re

from ._base import finalize, prepare_minute_frame, to_float64


REQUIRED_FIELDS: Iterable[str] = (
    "symbol",
    "timestamp",
    "Close",
)

FACTOR_NAME = "mom_group"
DESCRIPTION = "Momentum computed at group level and broadcast to members"
CATEGORY = "momentum"
DEFAULT_FREQUENCY = "min"


def _group_price(df: pd.DataFrame, group_col: str, price_col: str) -> pd.DataFrame:
    grouped = (
        df[[group_col, "timestamp", price_col]]
        .dropna()
        .groupby([group_col, "timestamp"], sort=False)[price_col]
        .mean()
        .reset_index()
    )
    grouped = grouped.sort_values([group_col, "timestamp"])
    grouped = grouped.rename(columns={group_col: "symbol", price_col: "Close"})
    return grouped


def _derive_group(symbols: pd.Series) -> pd.Series:
    base = symbols.astype(str)
    base = base.str.replace(r"^[0-9]+", "", regex=True)
    base = base.str.replace("USDT", "", regex=False)
    base = base.str.replace("USD", "", regex=False)
    base = base.str.replace("PERP", "", regex=False)
    base = base.str.strip()
    base = base.str.upper()
    base = base.replace("", pd.NA)
    return base.fillna(symbols.astype(str))


def compute(
    data: pd.DataFrame,
    group_col: str = "group",
    lookback: int = 252,
    skip: int = 21,
    price_col: str = "Close",
) -> pd.DataFrame:
    """Momentum computed on group-average prices and mapped back to constituents."""

    df, original_index = prepare_minute_frame(data)

    if group_col not in df.columns:
        df = df.copy()
        df[group_col] = _derive_group(df["symbol"])

    grouped_price = _group_price(df, group_col, price_col)
    grouped_result = mom_cross_sectional_compute(grouped_price, lookback, skip)

    merged = df[[group_col, "symbol", "timestamp"]].copy()
    merged = merged.merge(
        grouped_result.rename(columns={"symbol": group_col}),
        on=[group_col, "timestamp"],
        how="left",
    )

    if "timestamp_x" in merged.columns:
        merged = merged.rename(columns={"timestamp_x": "timestamp"})
    if "timestamp_y" in merged.columns:
        merged = merged.drop(columns=["timestamp_y"])

    merged.index = df.index

    merged = merged[["symbol", "timestamp", FACTOR_NAME]]
    return finalize(merged, original_index)


def mom_cross_sectional_compute(_data: pd.DataFrame, lookback: int, skip: int) -> pd.DataFrame:
    df, original_index = prepare_minute_frame(_data)
    price = to_float64(df["Close"])
    ret = price.groupby(df["symbol"], sort=False).pct_change()
    log_ret = np.log1p(ret)

    grouped = log_ret.groupby(df["symbol"], sort=False)
    total = grouped.transform(lambda s: s.rolling(window=lookback, min_periods=lookback).sum())
    if skip > 0:
        recent = grouped.transform(lambda s: s.rolling(window=skip, min_periods=skip).sum())
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
