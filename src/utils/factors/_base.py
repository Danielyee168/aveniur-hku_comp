"""Shared utilities for factor modules."""

from __future__ import annotations

from typing import Iterable, Tuple

import numpy as np
import pandas as pd


def prepare_minute_frame(data: pd.DataFrame, sort_cols: Iterable[str] = ("symbol", "timestamp")) -> Tuple[pd.DataFrame, pd.Index]:
    """Return a sorted copy of *data* and remember original order."""
    if data.empty:
        return pd.DataFrame(), data.index

    cols = [c for c in sort_cols if c in data.columns]
    if cols:
        df = data.sort_values(cols).copy()
    else:
        df = data.copy()
    return df, data.index


def ensure_trade_date(df: pd.DataFrame) -> pd.Series:
    """Return YYYYMMDD string per row, deriving from timestamp when absent."""
    if "trade_date" in df.columns:
        return df["trade_date"].astype(str)

    ts = pd.to_datetime(df["timestamp"], errors="coerce", utc=True, unit="ms")
    return ts.dt.strftime("%Y%m%d")


def ensure_week_key(df: pd.DataFrame) -> pd.Series:
    """Return ISO week key YYYYWW for each row."""
    ts = pd.to_datetime(df["timestamp"], errors="coerce", utc=True, unit="ms")
    return (ts.dt.isocalendar().year.astype(str) + ts.dt.isocalendar().week.astype(str).str.zfill(2))


def to_float64(series: pd.Series) -> pd.Series:
    """Safe float conversion with NaNs preserved."""
    return series.astype("float64")


def finalize(result: pd.DataFrame, original_index: pd.Index) -> pd.DataFrame:
    """Reindex *result* to the original ordering."""
    if result.empty:
        return result
    return result.loc[original_index]


def daily_group_keys(df: pd.DataFrame) -> Tuple[pd.Series, Tuple[pd.Series, pd.Series]]:
    """Return trade_date series and group key tuple (symbol, trade_date)."""
    trade_date = ensure_trade_date(df)
    key = (df["symbol"], trade_date)
    return trade_date, key
