import numpy as np
import pandas as pd
from typing import (
    List,
    Optional,
)
from datetime import datetime as dt
from config import PARQUET_PATH, DATA_FREQUENCY

class Kline(object)：
    PATH = None  # Path to the parquet file repository
    Data = None

    def __init__(self, parquet_path: Optional[str] = PARQUET_PATH, codes: Optional[List[str]] = None, start: Optional[str] = None, end: Optional[str] = None):
        self.PATH = parquet_path
        self.Data = self.load_data(codes, start, end)
        pass

    def __getitem__(self, date: pd.Timestamp) -> pd.DataFrame:
        return self.Data.loc[date]

    @staticmethod
    def get_trading_days(start: Optional[str] = None, end: Optional[str] = None) -> Optional[List[str]]:
        """Generate trading days for crypto assets (all calendar timestamps between start and end)."""
        if start and end:
            start_date = pd.to_datetime(start)
            end_date = pd.to_datetime(end)
            trading_days = pd.date_range(start=start_date, end=end_date, freq=DATA_FREQUENCY)
            return trading_days.strftime('%Y%m%d %H:%M:%S').tolist()
        elif start:
            # When only start is provided, expand from start to the current timestamp
            start_date = pd.to_datetime(start)
            end_date = pd.Timestamp.now()
            trading_days = pd.date_range(start=start_date, end=end_date, freq=DATA_FREQUENCY)
            return trading_days.strftime('%Y%m%d %H:%M:%S').tolist()
        elif end:
            # When only end is provided, backfill from a default anchor (e.g. one year earlier)
            end_date = pd.to_datetime(end)
            start_date = end_date - pd.DateOffset(years=1)
            trading_days = pd.date_range(start=start_date, end=end_date, freq=DATA_FREQUENCY)
            return trading_days.strftime('%Y%m%d %H:%M:%S').tolist()
        else:
            return None


    def load_data(self, codes: Optional[List[str]] = None, start: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
        """Load data from parquet and filter it by codes and date range."""
        # Read the parquet file
        data = pd.read_parquet(self.PATH)
        
        # Convert timestamps if they are still stored as strings
        if data['timestamp'].dtype == 'object':
            data['timestamp'] = pd.to_datetime(data['timestamp'])
        
        # Obtain the trading calendar for crypto assets (all calendar dates)
        trading_days = self.get_trading_days(start, end)
        
        # Filter by the requested calendar range when trading_days is provided
        if trading_days:
            # Convert trading_days into datetime for comparison
            trading_dates = pd.to_datetime(trading_days, format='%Y%m%d %H:%M:%S')
            data = data[data['timestamp'].dt.date.isin(trading_dates.date)]
        
        # Filter by instrument codes when provided
        if codes:
            data = data[data['symbol'].isin(codes)]
        
        # Rename columns to match the downstream schema
        data = data.rename(columns={
            'symbol': 'st_code',
            'timestamp': 'trade_date',
            'Close': 'close',  # Closing price
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Volume': 'Volume'
        })
        
        # Create close_adj (no adjustment assumed for now)
        data['close_adj'] = data['close']
        
        # Add missing columns with sensible defaults when necessary
        if 'trade_state' not in data.columns:
            data['trade_state'] = 1  # Treat as actively trading by default
        if 'avg_price' not in data.columns:
            data['avg_price'] = (data['high'] + data['low'] + data['close']) / 3
        if 'Amount' not in data.columns:
            data['Amount'] = data.get('Quote asset volume', 0)
        
        data = data[['st_code', 'trade_date', 'close_adj', 'trade_state','open', 'close', 'avg_price','high','low','Volume','Amount']]
        return data

    def get_data(self, indicator: str) -> Optional[pd.DataFrame]:
        try:
            pivot_data = pd.pivot_table(data=self.Data, values=indicator, index='trade_date', columns='st_code')
            return pivot_data
        except Exception as e:
            print(f"Error in get_data method: {e}")
            print("Indicator must be one of 'close_adj','trade_state','open','close','avg_price','high','low','Volume','Amount'")
            return None

if __name__ == '__main__':
    my_data = Kline(start='20230101')
    data = my_data.Data
    print(my_data.get_data('trade_state'))

