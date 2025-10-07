"""Utilities to convert Binance-style minute pickle files into canonical parquet.

Downstream utilities (`data_prepare.py`, `CalFactorFramework.py`) expect minute
bars to be stored as parquet with capitalised OHLCV columns, millisecond
`timestamp`, and both `symbol` and `order_book_id` identifiers. Historical raw
feeds are currently stored as `.pickle` dumps under `data/min_data/`.

Run this module as a script to batch convert the pickles:

```
python -m src.utils.convert_minute_pickle_to_parquet \
    --input data/min_data --output data/min_data
```

Optionally pass `--delete-pickle` to remove the source files after successful
conversion.
"""

from __future__ import annotations

import argparse
import pickle
from pathlib import Path
from typing import Dict, Iterable

import pandas as pd

# Mapping from the pickle column names to the canonical schema used throughout
# the repository.
RENAME_MAP: Dict[str, str] = {
    "open": "Open",
    "high": "High",
    "low": "Low",
    "close": "Close",
    "volume": "Volume",
    "quote_volume": "Quote asset volume",
    "count": "Number of trades",
    "taker_buy_volume": "Taker buy base asset volume",
    "taker_buy_quote_volume": "Taker buy quote asset volume",
}

# Target column order expected by resampling utilities.
COLUMN_ORDER: Iterable[str] = (
    "symbol",
    "order_book_id",
    "timestamp",
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
    "Quote asset volume",
    "Number of trades",
    "Taker buy base asset volume",
    "Taker buy quote asset volume",
)


def convert_pickle(path: Path, output_dir: Path, delete_pickle: bool = False) -> None:
    """Convert a single pickle file to parquet and write it to ``output_dir``."""
    with path.open("rb") as f:
        df = pickle.load(f)

    if not isinstance(df, pd.DataFrame):
        print(f"[WARN] {path.name}: object is not a DataFrame, skipping")
        return

    required = {"open_time", "order_book_id"}
    missing = required - set(df.columns)
    if missing:
        print(f"[WARN] {path.name}: missing required columns {sorted(missing)}, skipping")
        return

    df = df.rename(columns=RENAME_MAP).copy()
    df["timestamp"] = df.pop("open_time").astype("int64")
    df["symbol"] = df["order_book_id"].astype(str)

    missing_after = [col for col in COLUMN_ORDER if col not in df.columns]
    if missing_after:
        print(f"[WARN] {path.name}: missing renamed columns {missing_after}, skipping")
        return

    df = df[list(COLUMN_ORDER)]

    out_path = output_dir / (path.stem + ".parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    print(f"[INFO] wrote {out_path.relative_to(output_dir)} rows={len(df)}")

    if delete_pickle:
        path.unlink(missing_ok=True)
        print(f"[INFO] removed source pickle {path.name}")


def convert_directory(input_dir: Path, output_dir: Path, delete_pickle: bool = False) -> None:
    """Convert every ``data*.pickle`` under ``input_dir``."""
    pickle_paths = sorted(input_dir.glob("data*.pickle"))
    if not pickle_paths:
        print(f"[INFO] no pickle files found under {input_dir}")
        return

    for pickle_path in pickle_paths:
        try:
            convert_pickle(pickle_path, output_dir, delete_pickle=delete_pickle)
        except Exception as exc:  # noqa: BLE001 – log and continue
            print(f"[ERROR] failed to convert {pickle_path.name}: {exc}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert minute pickle files to parquet")
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/min_data"),
        help="Directory containing data*.pickle files (default: data/min_data)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/min_data"),
        help="Directory to write parquet files (default: same as input)",
    )
    parser.add_argument(
        "--delete-pickle",
        action="store_true",
        help="Remove the source pickle after a successful conversion",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    convert_directory(args.input, args.output, delete_pickle=args.delete_pickle)


if __name__ == "__main__":
    main()
