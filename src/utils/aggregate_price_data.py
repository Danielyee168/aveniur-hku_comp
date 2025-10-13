"""Aggregate minute-level parquet files into coarser OHLCV data.

The script scans a directory of ``dataYYYYMMDD.parquet`` files (typically the
output of :mod:`src.utils.convert_minute_pickle_to_parquet`) and resamples each
file to any number of pandas offset frequencies (e.g. ``15T``, ``1H``,
``1D``). Results are concatenated and written to
``<output-root>/<freq>_data/all_data.parquet``.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd

from .data_prepare import resample_data


logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aggregate minute parquet files into OHLCV tables")
    parser.add_argument("--min-dir", default="data/min_data", help="Directory containing dataYYYYMMDD.parquet")
    parser.add_argument("--freq", dest="frequencies", action="append",
                        help="Aggregation frequency (pandas offset alias). Repeat for multiples. Defaults to 1H and 1D.")
    parser.add_argument("--output-root", default="data", help="Base directory where aggregated files are stored")
    parser.add_argument("--pattern", default="data*.parquet", help="Glob pattern for minute parquet files")
    parser.add_argument("--start-date", help="Inclusive start date YYYYMMDD")
    parser.add_argument("--end-date", help="Inclusive end date YYYYMMDD")
    parser.add_argument("--limit", type=int, help="Keep only the latest N files after filtering")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    return parser.parse_args()


def filter_files(files: List[Path], start: Optional[str], end: Optional[str]) -> List[Path]:
    def extract_date(path: Path) -> Optional[str]:
        name = path.stem
        if name.startswith("data") and len(name) >= 12:
            return name[4:12]
        return None

    filtered: List[Path] = []
    for fp in files:
        date_str = extract_date(fp)
        if date_str is None:
            continue
        if start and date_str < start:
            continue
        if end and date_str > end:
            continue
        filtered.append(fp)
    return filtered


def normalise_freq(freq: str) -> str:
    return freq.lower().replace('/', '').replace(':', '').replace(' ', '')


def default_output_path(freq: str, root: Path) -> Path:
    norm = normalise_freq(freq)
    if norm == "1h":
        return root / "hour_data" / "all_data.parquet"
    if norm == "1d":
        return root / "daily_data" / "all_data.parquet"
    return root / f"{norm}_data" / "all_data.parquet"


def aggregate(files: Iterable[Path], freqs: List[str]) -> dict[str, pd.DataFrame]:
    frames: dict[str, List[pd.DataFrame]] = {freq: [] for freq in freqs}

    for fp in files:
        logger.info("Processing %s", fp)
        try:
            df = pd.read_parquet(fp)
        except Exception as exc:
            logger.error("Failed to read %s: %s", fp, exc)
            continue

        for freq in freqs:
            try:
                agg_df = resample_data(df.copy(), freq)
            except Exception as exc:
                logger.error("Failed to resample %s with freq %s: %s", fp, freq, exc)
                continue

            if agg_df is None or agg_df.empty:
                continue

            if 'order_book_id' in agg_df.columns:
                agg_df = agg_df.rename(columns={'order_book_id': 'symbol'})

            frames[freq].append(agg_df)

    return {
        freq: pd.concat(frames[freq], ignore_index=True) if frames[freq] else pd.DataFrame()
        for freq in freqs
    }


def main() -> None:
    args = parse_args()

    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")

    min_dir = Path(args.min_dir)
    if not min_dir.exists():
        raise FileNotFoundError(f"Minute directory not found: {min_dir}")

    files = sorted(min_dir.glob(args.pattern))
    files = filter_files(files, args.start_date, args.end_date)

    if args.limit is not None and args.limit > 0:
        files = files[-args.limit:]

    if not files:
        logger.warning("No files matched the provided filters")
        return

    freqs = args.frequencies or ["1H", "1D"]

    logger.info("Aggregating %d files for frequencies %s", len(files), freqs)
    agg_map = aggregate(files, freqs)

    output_root = Path(args.output_root)

    for freq, df in agg_map.items():
        out_path = default_output_path(freq, output_root)
        if df.empty:
            logger.warning("No data produced for freq %s", freq)
            continue
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out_path, index=False)
        logger.info("Wrote %s rows to %s", len(df), out_path)


if __name__ == "__main__":
    main()

"""
Examples
--------

    # 1. Take the entire history and build both hourly and daily tables
    /home/mfin7037_best_students/miniconda3/envs/nlp/bin/python -m src.utils.aggregate_price_data \\
        --min-dir data/min_data \\
        --output-root data \\
        --verbose

    # 2. Build 15-minute bars for the first month only
    /home/mfin7037_best_students/miniconda3/envs/nlp/bin/python -m src.utils.aggregate_price_data \\
        --min-dir data/min_data \\
        --start-date 20230101 --end-date 20230131 \\
        --freq 15t \\
        --output-root data \\
        --verbose

    # 3. Build hour/day aggregates for the most recent 14 files (approx two weeks)
    /home/mfin7037_best_students/miniconda3/envs/nlp/bin/python -m src.utils.aggregate_price_data \\
        --min-dir data/min_data \\
        --limit 14 \\
        --freq 1h --freq 1d \\
        --output-root data \\
        --verbose
"""