from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from main.market_data_collector import fetch_market_data, persist_snapshot


class DummyResponse:
    def __init__(self, payload: dict, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError("HTTP error")

    def json(self) -> dict:
        return self._payload


class DummySession:
    def __init__(self, payload: dict) -> None:
        self._payload = payload
        self.calls = []

    def get(self, url: str, params: dict | None = None, timeout: int | None = None) -> DummyResponse:
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        return DummyResponse(self._payload)


def test_fetch_market_data_returns_dataframe_with_expected_content():
    server_time_ms = 1_700_000_000_000
    payload = {
        "Success": True,
        "ServerTime": server_time_ms,
        "Data": {
            "BTC/USD": {
                "MaxBid": 100.1,
                "MinAsk": 100.2,
                "LastPrice": 100.15,
                "Change": 0.01,
                "CoinTradeValue": 1234.5,
                "UnitTradeValue": 123_456.78,
            }
        },
    }
    session = DummySession(payload)

    df = fetch_market_data(session=session)

    assert not df.empty
    assert session.calls, "Expected at least one HTTP call"

    expected_time = datetime.fromtimestamp(server_time_ms // 1000, tz=timezone.utc)
    row = df.iloc[0]

    assert list(df.columns) == ["symbol", "timestamp", "price"]
    assert row["symbol"] == "BTC/USD"
    assert row["timestamp"] == expected_time
    assert row["price"] == pytest.approx(100.15)


def test_persist_snapshot_creates_dated_parquet(tmp_path):
    pytest.importorskip("pyarrow")

    timestamp = datetime(2024, 1, 1, 12, 34, 56, tzinfo=timezone.utc)
    df = pd.DataFrame([{"symbol": "ETH/USD", "timestamp": timestamp, "price": 200.12}])

    file_path = persist_snapshot(df, root=tmp_path)

    assert file_path.exists()

    expected_local = timestamp.astimezone()
    assert file_path.parent == tmp_path / expected_local.strftime("%Y-%m-%d")
    assert file_path.name == f"market_data_{expected_local.strftime('%H:%M:%S')}.parquet"
    assert file_path.suffix == ".parquet"

    roundtrip = pd.read_parquet(file_path)
    assert list(roundtrip.columns) == ["symbol", "timestamp", "price"]
    assert roundtrip.iloc[0]["symbol"] == "ETH/USD"
    assert roundtrip.iloc[0]["price"] == pytest.approx(200.12)
