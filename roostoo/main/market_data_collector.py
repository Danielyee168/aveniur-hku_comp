#!/usr/bin/env python3
"""Utilities for collecting Roostoo market tickers and storing parquet snapshots."""

from __future__ import annotations

import logging
import shutil
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

BASE_URL = "https://mock-api.roostoo.com"
DATA_ROOT = Path(__file__).resolve().parent.parent / "data"
REQUEST_TIMEOUT = 10

MAX_RETRIES = 15
RETRY_DELAY_SECONDS = 1
WAIT_EVENT = threading.Event()


def _current_timestamp_ms() -> int:
    return int(time.time() * 1000)


def _snapshot_time(server_time_ms: Optional[int]) -> datetime:
    if server_time_ms:
        seconds = int(server_time_ms) // 1000
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
    return datetime.now(timezone.utc).replace(microsecond=0)


def fetch_market_data(session: Optional[requests.Session] = None) -> pd.DataFrame:
    """ Using Roostoo API to fetch market data, dataframe include symbol, timestamp and price"""
    http = session or requests.Session()
    params = {"timestamp": _current_timestamp_ms()}

    response = http.get(f"{BASE_URL}/v3/ticker", params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    payload = response.json()
    if not payload.get("Success", False):
        message = payload.get("ErrMsg", "Unknown error")
        raise RuntimeError(f"Ticker request failed: {message}")

    data = payload.get("Data", {})
    if not isinstance(data, dict) or not data:
        raise ValueError("Ticker response contained no market data.")

    snapshot_ts = _snapshot_time(payload.get("ServerTime"))

    rows = []
    for pair, metrics in data.items():
        if not isinstance(metrics, dict):
            continue
        rows.append(
            {
                "pair": pair,
                "server_time": snapshot_ts,
                "LastPrice": metrics.get("LastPrice"),
            }
        )

    if not rows:
        raise ValueError("Ticker response contained no usable market data.")

    df = pd.DataFrame(rows, columns=["pair", "server_time", "LastPrice"])
    df.rename(
        columns={"pair": "symbol", "server_time": "timestamp", "LastPrice": "price"},
        inplace=True,
    )

    return df[["symbol", "timestamp", "price"]]


def persist_snapshot(df: pd.DataFrame, root: Path = DATA_ROOT) -> Path:
    """ Make consistant timestamp format and correct data folder """
    if df.empty:
        raise ValueError("Cannot persist an empty dataframe.")

    raw_timestamp = df["timestamp"].iloc[0]
    if isinstance(raw_timestamp, pd.Timestamp):
        timestamp = raw_timestamp.to_pydatetime()
    elif isinstance(raw_timestamp, datetime):
        timestamp = raw_timestamp
    else:
        timestamp = datetime.now(timezone.utc)

    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)

    local_timestamp = timestamp.astimezone()

    day_dir = root / local_timestamp.strftime("%Y-%m-%d")
    day_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{local_timestamp.strftime('%H:%M:%S')}.parquet"
    file_path = day_dir / filename
    df.to_parquet(file_path, index=False)

    return file_path


def next_minute_boundary(reference: datetime) -> datetime:
    truncated = reference.replace(second=0, microsecond=0)
    if truncated < reference:
        truncated += timedelta(minutes=1)
    return truncated


def wait_until(target: datetime) -> None:
    while True:
        now = datetime.now(timezone.utc)
        remaining = (target - now).total_seconds()
        if remaining <= 0:
            return
        WAIT_EVENT.wait(timeout=min(remaining, RETRY_DELAY_SECONDS))


def collect_once(session: requests.Session) -> None:
    """ Helper to collect market data with maximum retires, makes sure the collection is at whole minutes """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            dataframe = fetch_market_data(session=session)
            file_path = persist_snapshot(dataframe)
            logging.info("Stored market snapshot at %s", file_path)
            return
        except Exception as exc:
            if attempt >= MAX_RETRIES:
                logging.exception(
                    "Failed to store market snapshot after %s attempts.",
                    MAX_RETRIES,
                )
                return
            logging.warning(
                "Attempt %s/%s failed: %s. Retrying in %s seconds.",
                attempt,
                MAX_RETRIES,
                exc,
                RETRY_DELAY_SECONDS,
            )
            WAIT_EVENT.wait(timeout=RETRY_DELAY_SECONDS)


def delete_old_data(retention_days: int = 3, root: Path = DATA_ROOT) -> None:
    """Remove dated data directories older than the retention window."""
    if retention_days < 0:
        return

    cutoff_date = datetime.now().astimezone().date() - timedelta(days=retention_days)
    if not root.exists():
        return

    for child in root.iterdir():
        if not child.is_dir():
            continue
        try:
            folder_date = datetime.strptime(child.name, "%Y-%m-%d").date()
        except ValueError:
            continue

        if folder_date < cutoff_date:
            shutil.rmtree(child, ignore_errors=True)
