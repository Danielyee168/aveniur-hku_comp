import pandas as pd
import numpy as np
import os
import warnings
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import matplotlib.pyplot as plt
import seaborn as sns
from tqdm import tqdm
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
import psutil
import time
from joblib import Parallel, delayed

warnings.filterwarnings('ignore')

stable_list = ['USDCUSDT', 'USUALUSDT', 'USUALUSDT', 'TUSDT']


def read_parquet_robust(file_path: str) -> pd.DataFrame:
    
    """
    Robust parquet file reader with multiple engine fallbacks
    
    Args:
        file_path (str): Path to the parquet file
        
    Returns:
        pd.DataFrame: Loaded dataframe or empty dataframe if failed
    """
    df = pd.read_parquet(file_path)
    if not df.empty:
        # Column mapping for actual parquet structure
        column_mapping = {
            'open_price': 'open',
            'high_price': 'high', 
            'low_price': 'low',
            'close_price': 'close',
            # timestamp and volume are already correctly named
        }

        # Apply column mapping
        df = df.rename(columns=column_mapping)

        # Required columns after mapping
        required_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']

        # Convert timestamp to datetime
        if df['timestamp'].dtype == 'object':
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        elif df['timestamp'].dtype in ['int64', 'int32']:
            # Handle millisecond timestamps
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

        # Convert price columns to float
        price_cols = ['open', 'high', 'low', 'close']
        for col in price_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Convert volume columns to numeric (handle object types)
        volume_cols = ['volume', 'amount', 'buy_volume', 'buy_amount']
        for col in volume_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Convert count to int
        if 'count' in df.columns:
            df['count'] = pd.to_numeric(df['count'], errors='coerce').fillna(0).astype('int64')

        # Sort by timestamp
        df = df.sort_values('timestamp').reset_index(drop=True)
        df['vwap'] = (df['amount'] / df['volume']).replace([np.inf, -np.inf], np.nan).ffill()

        # Keep only required columns (but also preserve additional useful columns)
        # Keep buy_volume and buy_amount for potential enhanced feature engineering
        available_extra_cols = [col for col in ['amount', 'count', 'buy_volume', 'buy_amount', 'vwap'] if col in df.columns]
        final_cols = required_cols + available_extra_cols
        df = df[final_cols]
    return df


def load_crypto_data(data_dir: str = '../data/raw') -> Dict[str, pd.DataFrame]:
    """
    Load all cryptocurrency parquet files with robust error handling
    
    Returns:
        Dict[str, pd.DataFrame]: Dictionary mapping symbol to dataframe
    """
    print(f" Loading cryptocurrency data from {data_dir}...")
    
    if not os.path.exists(data_dir):
        print(f"Data directory not found: {data_dir}")
        return {}
    
    STABLE = [x + '.parquet' for x in stable_list]
    
    parquet_files = [f for f in os.listdir(data_dir) if f.endswith('.parquet') and f not in STABLE]
    
    parquet_files
    print(f"Found {len(parquet_files)} parquet files")
    
    crypto_data = {}
    successful_loads = 0
    failed_loads = 0
    failed_symbols = []
    
    for filename in tqdm(parquet_files, desc="Loading files"):
        symbol = filename.replace('.parquet', '')
        file_path = os.path.join(data_dir, filename)
        
        try:
            # Load and standardize data
            df = read_parquet_robust(file_path)
            df['symbol'] = symbol
            
            if not df.empty and len(df) >= 100:  # Minimum data requirement
                crypto_data[symbol] = df
                successful_loads += 1
            else:
                failed_loads += 1
                failed_symbols.append(symbol)
                if len(df) < 100:
                    print(f"{symbol}: Insufficient data ({len(df)} records)")
                    
        except Exception as e:
            failed_loads += 1
            failed_symbols.append(symbol)
            print(f"Failed to load {symbol}: {str(e)[:50]}...")
    
    print(f"\\n Successfully loaded: {successful_loads} files")
    print(f"Failed to load: {failed_loads} files")
    
    if failed_symbols:
        print(f"\\n Failed symbols: {', '.join(failed_symbols[:10])}{'...' if len(failed_symbols) > 10 else ''}")
        print(f"Recommendation: These {failed_loads} coins will be excluded from AlphaNet training")
        print(f"This is normal - represents {failed_loads/len(parquet_files)*100:.1f}% failure rate")
    
    return crypto_data


def add_feat(df: pd.DataFrame) -> pd.DataFrame:
    """输入 pandas DataFrame，输出含特征 + y_raw 并 3-MAD 去极值"""
    df = df.sort_values(["symbol", "timestamp"]).reset_index(drop=True)

    df["future_vwap"] = df.groupby("symbol")["vwap"].shift(-96)
    df["y_raw"] = df["future_vwap"] / df["vwap"] - 1

    for span in [4, 16, 48, 96]:
        df[f"log_ret_{span}"] = np.log(df["close"]).groupby(df["symbol"]).diff(span)
        df[f"vol_log_ret_{span}"] = np.log(df["volume"]).groupby(df["symbol"]).diff(span)
        atr = (df["high"] - df["low"]) / df["close"]
        df[f"atr_norm_{span}"] = (
            atr.groupby(df["symbol"])
            .rolling(window=span, min_periods=1).mean()
            .reset_index(level=0, drop=True)
        )

    feature_cols = [c for c in df.columns if c.startswith(("log_ret", "vol_log", "atr_norm"))]
    
    def mad_clip(s: pd.Series, k: float = 3.0) -> pd.Series:
        median = s.median()
        mad = (s - median).abs().median()
        upper = median + k * mad * 1.4826   # 1.4826 使 MAD≈标准差
        lower = median - k * mad * 1.4826
        return s.clip(lower, upper)

    for col in feature_cols:
        df[col] = df.groupby("timestamp")[col].transform(mad_clip)    
    
    for col in feature_cols:
        mu = df.groupby("timestamp")[col].transform("mean")
        std = df.groupby("timestamp")[col].transform("std")
        df[col] = (df[col] - mu) / (std + 1e-12)

    return df.dropna().reset_index(drop=True)


def make_seq(df, seq_len=96):
    symbols = df["symbol"].unique()
    X, Y, ts = [], [], []
    for sym in symbols:
        sub = df.filter(pl.col("symbol")==sym)
        feats = sub.select(pl.all().exclude(["symbol","timestamp","y_raw"])).to_numpy()
        y_raw = sub["y_raw"].to_numpy()
        tim = sub["timestamp"].to_numpy()
        for i in range(seq_len, len(feats)):
            X.append(feats[i-seq_len:i])
            Y.append(y_raw[i])
            ts.append(tim[i])
    X, Y, ts = map(np.array, (X, Y, ts))
    return X.astype(np.float32), Y.astype(np.float32), ts


# Load all cryptocurrency data
crypto_data = load_crypto_data()

print(f"\nLoaded data for {len(crypto_data)} cryptocurrencies")

# Show sample data
if crypto_data:
    sample_symbol = list(crypto_data.keys())[0] 
    sample_df = crypto_data[sample_symbol]

abnormal_ones = []
for k in crypto_data:
    temp = crypto_data[k]
    if temp[temp['count'] == 0].shape[0] > 10:
        print(k)
        abnormal_ones.append(k)
        
for k in (abnormal_ones):
    crypto_data.pop(k)
    
test_data = pd.concat([crypto_data[k] for k in crypto_data])
test_data = add_feat(test_data)
X, Y, ts = make_seq(df)
pdf = pd.DataFrame({"ts": ts, "y": Y})
pdf["rank"] = pdf.groupby("ts")["y"].rank(pct=True)
Y_rank = pdf["rank"].values.astype(np.float32)
with open(OUT, "wb") as f:
    pickle.dump((X, Y_rank), f)
print("Saved", X.shape, Y_rank.shape)