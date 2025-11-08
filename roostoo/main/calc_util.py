from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

DATA_ROOT = Path(__file__).resolve().parent.parent / "data"


def _iter_parquet_files(root: Path) -> Iterable[Path]:
    if not root.exists():
        return []

    for day_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        for parquet_file in sorted(day_dir.glob("*.parquet")):
            yield parquet_file


def load_all_market_data(root: Path = DATA_ROOT) -> pd.DataFrame:
    """Load every parquet snapshot currently stored under the data directory."""
    frames = []
    for parquet_path in _iter_parquet_files(root):
        try:
            frames.append(pd.read_parquet(parquet_path))
        except FileNotFoundError:
            continue  # File was removed between listing and reading

    if not frames:
        return pd.DataFrame(columns=["symbol", "timestamp", "price"])

    combined = pd.concat(frames, ignore_index=True)
    combined["timestamp"] = pd.to_datetime(combined["timestamp"], utc=True)
    combined.sort_values(by=["timestamp", "symbol"], inplace=True, ignore_index=True)
    return combined


def load_minute_data(root: Path = DATA_ROOT) -> pd.DataFrame:
    """Return market data with timestamps normalized to the nearest minute."""
    df = load_all_market_data(root=root)
    if df.empty:
        return df

    df = df.copy()
    df["timestamp"] = df["timestamp"].dt.floor("T")
    return df


def load_hourly_data(root: Path = DATA_ROOT) -> pd.DataFrame:
    """Aggregate minute-level data into hourly OHLC candles."""
    minute_df = load_minute_data(root=root)
    if minute_df.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", "open", "high", "low", "close"])

    minute_df = minute_df.sort_values(["symbol", "timestamp"])
    minute_df["hour"] = minute_df["timestamp"].dt.floor("H")

    ohlc = (
        minute_df.groupby(["symbol", "hour"], sort=True)["price"]
        .agg(open="first", high="max", low="min", close="last")
        .reset_index()
        .rename(columns={"hour": "timestamp"})
    )

    ohlc = ohlc[["symbol", "timestamp", "open", "high", "low", "close"]]
    ohlc.sort_values(by=["timestamp", "symbol"], inplace=True, ignore_index=True)
    return ohlc


def load_data(frequency: str = "min", root: Path = DATA_ROOT) -> pd.DataFrame:
    """Load market data at minute or hourly frequency."""
    frequency = frequency.lower()
    if frequency == "min":
        return load_minute_data(root=root)
    if frequency == "hour":
        return load_hourly_data(root=root)
    raise ValueError("frequency must be either 'min' or 'hour'")
