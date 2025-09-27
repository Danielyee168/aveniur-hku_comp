import os
import glob
import pandas as pd
import multiprocessing as mp

os.makedirs('./data/min_data', exist_ok=True)
os.makedirs('./data/hour_data', exist_ok=True)
os.makedirs('./data/daily_data', exist_ok=True)

# ---------- hyper parameter ----------
MIN_DIR   = './data/min_data'
HOUR_OUT  = './data/hour_data/all_hour_data.parquet'
DAILY_OUT = './data/daily_data/all_daily_data.parquet'
CHUNK_DAYS = 30                # how many days to process in one chunk
MAX_WORKERS = mp.cpu_count()

# ---------- tool funcs ----------
def resample_data(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    """resample data to specified frequency"""
    if df.empty:
        return df
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df = df.set_index('timestamp')
    agg = {
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum',
        'Quote asset volume': 'sum',
        'Number of trades': 'sum',
        'Taker buy base asset volume': 'sum',
        'Taker buy quote asset volume': 'sum',
    }
    return (df.groupby('order_book_id')
              .resample(freq)
              .agg(agg)
              .reset_index())

def process_one_chunk(file_chunk: list) -> tuple[pd.DataFrame, pd.DataFrame]:
    """read & resample one chunk of files"""
    # read & concat
    df = pd.concat([pd.read_parquet(f) for f in file_chunk], ignore_index=True)
    # resample
    h = resample_data(df, '1H')
    d = resample_data(df, '1D')
    return h, d

# ---------- main process ----------
def main(wokers: int = MAX_WORKERS):
    files = sorted(glob.glob(os.path.join(MIN_DIR, '*.parquet')))
    if not files:
        print('no files found.')
        return

    # 1. divide into group：group by CHUNK_DAYS
    chunks = [files[i:i+CHUNK_DAYS] for i in range(0, len(files), CHUNK_DAYS)]

    # 2. multiprocessing
    hour_li, daily_li = [], []
    ctx = mp.get_context('spawn')
    with ctx.Pool(wokers) as pool:
        for h, d in pool.imap_unordered(process_one_chunk, chunks, chunksize=1):
            hour_li.append(h)
            daily_li.append(d)
            print(f'batch done：hour={len(h)} rows, daily={len(d)}rows')

    # 3. conccatenate & write into disk
    if hour_li:
        pd.concat(hour_li, ignore_index=True).to_parquet(HOUR_OUT)
    if daily_li:
        pd.concat(daily_li, ignore_index=True).to_parquet(DAILY_OUT)

    print('resample done.')

if __name__ == '__main__':
    main(3)  # set workers to 3 for testing
    # main()  # use all cpu cores for full speed
