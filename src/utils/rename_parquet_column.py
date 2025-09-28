import argparse
import os
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
from concurrent.futures import ProcessPoolExecutor, as_completed
import tqdm


def rename_obid_to_symbol(parquet_path: str) -> str:
    """单个文件处理函数，返回完成或错误信息"""
    path = '../../data/min_data/' + parquet_path
    try:
        tbl = pd.read_parquet(path)
    except Exception as e:
        return f'[WARN] 读取失败 {path}: {e}'

    if 'order_book_id' not in tbl.columns:
        return None  # 无需处理

    # 重命名列
    tbl.rename({'order_book_id':'symbol'}, inplace=True, axis=1)
    tbl.to_parquet(parquet_path)
    return f'[INFO] 已处理 {path}'


def main():
    root_dir = '../../data/min_data/'

    files = [str(p) for p in os.listdir(root_dir)]

    # 进度条 + 多进程
    with ProcessPoolExecutor(max_workers=os.cpu_count() - 1) as exe:
        futures = [exe.submit(rename_obid_to_symbol, fp) for fp in files]
        for f in tqdm.tqdm(as_completed(futures), total=len(files), unit='file'):
            msg = f.result()
            if msg:  # 跳过 None
                print(msg)

if __name__ == '__main__':
    main()