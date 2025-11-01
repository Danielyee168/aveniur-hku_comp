# --------------
# auth: NewbieWong
# version: 10/2/2025
# --------------

import pandas as pd
from pathlib import Path
from typing import List, Dict, Callable, Optional
import math
import json
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import pyarrow.parquet as pq
import os
import re
import gc
from collections import defaultdict
import logging
import warnings
from config import TRADE_LIST, TRADE_LIST_FILTERED

from nbclient.client import timestamp

warnings.filterwarnings("ignore")


# Configuration Log
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("FactorRegistry")


class FactorCalculator:
    """Factor Calculation Engine - Improved Version, Supports Batch Multi-Process Computing"""
    data_root = Path("./data")
    def __init__(self, data_root: str = "./data"):
        FactorCalculator.data_root = Path(data_root)

    def calculate_factor(self,
                         factor_func: Callable,
                         frequency: str = "daily",
                         fields: List[str] = None,
                         dates: List[str] = None,
                         symbols: List[str] = None,
                         parallel: bool = True,
                         n_jobs: int = 4,
                         parallel_batch_size: int = 30,
                         factor_name: str = None,
                         factor_type: str = None,
                         window_size: int = 0,
                         save_p: bool = True,
                         **factor_kwargs) -> pd.DataFrame:
        """
        Core factor calculation method

        Param:
            factor_func: Factor calculation function
            frequency: data frequency
            fields: Required data fields
            dates: Date List
            symbols: List of targets
            parallel: Is it parallel computing, True or False
            n_jobs: Number of parallel tasks
            batch_size: The batch size, None means determined automatically
            factor_kwargs: Parameters passed to the factor function
            factor_name: name of the factor,
            factor_type: 'alpha' or 'risk',
            window_size: Size of the lookback window
        """
        if symbols is None:
            symbols = TRADE_LIST_FILTERED

        if factor_name is None:
            factor_name = factor_func.__name__

        # Determine factor type
        if factor_type is None:
            factor_type = "alpha" if "alpha" in factor_name.lower() else "risk"

        if not parallel or frequency != "min":
            # Direct calculation
            # 1. Data Loading
            data = FactorCalculator._load_data(frequency, dates, fields)

            # 2. Data Validation and Preprocessing
            data = FactorCalculator._validate_and_preprocess(data, symbols)

            if data.empty:
                warnings.warn("The loaded data is empty. Please check the data path and date/target filter conditions.")
                return pd.DataFrame()

            factor_df = self._direct_calculation(factor_func, data, factor_kwargs)

        else:
            if dates is None:
                all_files = os.listdir(self.data_root / 'min_data')
                dates = []
                for name in all_files:
                    match = re.search(r'data(\d{8})\.parquet', name)
                    if match:
                        dates.append(match.group(1))
                dates.sort()

            # Parallel computing, using a batching strategy
            factor_df = self._parallel_calculation_with_batching(
                factor_func, dates, window_size, n_jobs,
                parallel_batch_size, frequency, fields, symbols, factor_kwargs)

        factor_df = factor_df.drop_duplicates()
        factor_df.dropna(inplace=True)
        # trend_regression_hourly may emit an empty frame after warm-up trimming;
        # guard against KeyError when timestamp/symbol columns are absent
        if {'timestamp', 'symbol'}.issubset(factor_df.columns):
            factor_df = factor_df.sort_values(['timestamp', 'symbol'])
        if save_p:
            factor_path = self.data_root / "factors" / f"{factor_type}" / f"factor_{factor_name}.parquet"
            factor_df.to_parquet(factor_path, index=False)
        return factor_df

    def calculate_factor_incremental(self,
                                     factor_func: Callable,
                                     frequency: str = "min",
                                     fields: List[str] = None,
                                     dates: List[str] = None,
                                     symbols: List[str] = None,
                                     batch_size: int = 180,
                                     parallel_batch_size: int = 30,
                                     n_jobs: int = 4,
                                     factor_name: str = None,
                                     factor_type: str = None,
                                     window_size: int = 0,
                                     **factor_kwargs) -> pd.DataFrame:
        """
        Incremental calculation factors, combined with batching and multiprocessing
        Param:
            factor_func: Factor calculation function
            frequency: data frequency
            fields: Required data fields
            dates: Date List
            symbols: List of targets
            n_jobs: Number of parallel tasks
            batch_size: The batch size, None means determined automatically
            parallel_batch_size: size of the data processed in each parallel tasks
            factor_kwargs: Parameters passed to the factor function
            factor_name: name of the factor,
            factor_type: 'alpha' or 'risk',
            window_size: Size of the lookback window
        """
        if symbols is None:
            symbols = TRADE_LIST_FILTERED

        if factor_name is None:
            factor_name = factor_func.__name__

        # Determine factor type
        if factor_type is None:
            factor_type = "alpha" if "alpha" in factor_name.lower() else "risk"

        if frequency != "min":
            # Non-minute data direct calculation
            return self.calculate_factor(
                factor_func, frequency, fields, dates, symbols,
                parallel=True, n_jobs=n_jobs, factor_name=factor_name, factor_type=factor_type,
                window_size=window_size, **factor_kwargs
            )

        # Confirm the date to process
        data_path = self.data_root / f"{frequency}_data"
        if dates:
            target_dates = dates
        else:
            target_dates = sorted([fp.stem.replace("data", "") for fp in data_path.glob("data*.parquet")])

        if not target_dates:
            return pd.DataFrame()

        # Batch by date, processing each batch using multiple processes
        all_results = []
        total_batches = (len(target_dates) - 1) // batch_size + 1
        start = 0
        batch_idx = 0

        while start < len(target_dates):
            end = min(start + batch_size, len(target_dates))
            chunk_dates = target_dates[start:end]
            if not chunk_dates:
                break

            batch_idx += 1
            print(f"Processing Date Batch {batch_idx}/{total_batches}: {chunk_dates[0]} to {chunk_dates[-1]}")

            chunk_result = self.calculate_factor(
                factor_func=factor_func,
                frequency=frequency,
                fields=fields,
                dates=chunk_dates,
                symbols=symbols,
                parallel=(n_jobs > 1),
                n_jobs=n_jobs,
                factor_name=factor_name,
                factor_type=factor_type,
                window_size=window_size,
                parallel_batch_size=parallel_batch_size,
                save_p=False,
                **factor_kwargs
            )

            if not chunk_result.empty:
                all_results.append(chunk_result)

            gc.collect()

            if end < len(target_dates):
                if window_size > 0:
                    start = max(end - window_size, start + 1)
                else:
                    start = end
            else:
                break

        factor_df = pd.concat(all_results, axis=0) if all_results else pd.DataFrame()
        factor_path = self.data_root / "factors" / f"{factor_type}" / f"factor_{factor_name}.parquet"
        if not factor_df.empty:
            factor_df = factor_df.drop_duplicates()
            factor_df.dropna(inplace=True)
            if {'timestamp', 'symbol'}.issubset(factor_df.columns):
                factor_df = factor_df.sort_values(['timestamp', 'symbol'])
            factor_path.parent.mkdir(parents=True, exist_ok=True)
            factor_df.reset_index(drop=True, inplace=True)
            factor_df.to_parquet(factor_path, index=False)
        else:
            warnings.warn("No factor results produced for the requested date range.")

        return factor_df

    def _parallel_calculation_with_batching(self,
                                            factor_func: Callable,
                                            dates: List[str],
                                            window_size: int,
                                            n_jobs: int,
                                            parallel_batch_size: int,
                                            frequency: str,
                                            fields: List[str],
                                            symbols: List[str],
                                            factor_kwargs: Dict) -> pd.DataFrame:
        """Parallel computing with batching"""
        batches = self._create_date_batches(dates, window_size, parallel_batch_size)

        # If there is no batch (small amount of data), compute directly.
        if len(batches) == 1:
            print('trigger', dates)
            # 1. Data Loading
            data = FactorCalculator._load_data(frequency, dates, fields)

            # 2. Data Validation and Preprocessing
            data = FactorCalculator._validate_and_preprocess(data, symbols)
            return self._direct_calculation(factor_func, data, factor_kwargs)

        # Set the number of processes
        if n_jobs == -1:
            n_jobs = min(len(batches), os.cpu_count())
        else:
            n_jobs = min(len(batches), n_jobs)

        # Parallel processing batch
        with ProcessPoolExecutor(max_workers=n_jobs) as executor:
            futures = []
            for i, batch_dates in enumerate(batches):
                # Submit Task
                future = executor.submit(FactorCalculator._calculate_batch, factor_func,
                                         batch_dates, i, fields, symbols, frequency, factor_kwargs)

                futures.append(future)

            # Collect results
            results = []
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if not result.empty:
                        results.append(result)
                except Exception as e:
                    warnings.warn(f"fail to calculate in batches mode: {str(e)}")

        # Merge results
        if results:
            return pd.concat(results, ignore_index=True)
        else:
            return pd.DataFrame({'symbol':'error', 'timestamp':'0', 'test':0}, index=[0])

    def _create_date_batches(self, dates: List[str], window_size: int, batch_size: int) -> List[List[str]]:
        """Create data batches by date"""

        if batch_size is None:
            # Automatically determine batch size
            n_dates = len(dates)
            batch_size = max(1, n_dates // (os.cpu_count() * 2))

        batches = []
        last = 0
        for i in range(batch_size, len(dates), batch_size):
            batch_dates = dates[last:i]
            batches.append(batch_dates)
            last = i - window_size

        if dates[last] != dates[-1]:
            batches.append(dates[last:])

        if len(batches) > 0:
            return batches
        else:
            return [dates]

    @staticmethod
    def _calculate_batch(factor_func: Callable, batch_dates: List[str],
                         batch_id: int, fields: List[str], symbols: List[str], frequency: str, factor_kwargs: Dict) -> pd.DataFrame:
        """Calculate a single batch"""
        try:
            data = FactorCalculator._load_data(frequency, batch_dates, fields)

            # 2. Data Validation and Preprocessing
            data = FactorCalculator._validate_and_preprocess(data, symbols)
            result = factor_func(data, **factor_kwargs)

            # Standardized result format
            if isinstance(result, pd.Series):
                result = result.reset_index()

            return result
        except Exception as e:
            warnings.warn(f"Batch {batch_id} calculation failed: {str(e)}")
            return pd.DataFrame({'symbol':'error', 'timestamp':'0', 'test':0}, index=[0])

    @staticmethod
    def _load_data(frequency: str, dates: List[str], fields: List[str]) -> pd.DataFrame:
        """Data Loading Main Function"""
        data_path = FactorCalculator.data_root / f"{frequency}_data"

        if frequency == "min":
            return FactorCalculator._load_minute_data_optimized(data_path, dates, fields)
        else:
            return FactorCalculator._load_other_frequency_data(data_path, dates, fields)

    @staticmethod
    def _load_minute_data_optimized(data_path: Path, dates: List[str], fields: List[str]) -> pd.DataFrame:
        """Optimized minute-level data loading"""
        # Confirm the file to be loaded
        if dates:
            file_paths = []
            for date in dates:
                file_path = data_path / f"data{date}.parquet"
                if file_path.exists():
                    file_paths.append(file_path)
                else:
                    warnings.warn(f"Minute-level data file does not exist: {file_path}")
        else:
            file_paths = list(data_path.glob("data*.parquet"))

        if not file_paths:
            warnings.warn(f"No minute data file found under {data_path}")
            return pd.DataFrame()

        # Select parallel or sequential reading based on the number of files
        if len(file_paths) > 60:  # Use parallel processing when there are many files
            return FactorCalculator._parallel_load_minute_data(file_paths, fields)
        else:
            return FactorCalculator._sequential_load_minute_data(file_paths, fields)

    @staticmethod
    def _parallel_load_minute_data(file_paths: List[Path], fields: List[str]) -> pd.DataFrame:
        """Parallel loading of minute-level data (I/O intensive task)"""

        def load_single_file(file_path):
            """load single file"""
            try:
                # only load columns that are required
                if fields:
                    # read schema to see what columns exit
                    schema = pq.read_schema(file_path)
                    available_fields = [col for col in fields if col in schema.names]
                    # add necessary columns
                    required_cols = ['symbol']
                    for col in required_cols:
                        if col in schema.names and col not in available_fields:
                            available_fields.append(col)

                    if available_fields:
                        df = pd.read_parquet(file_path, columns=available_fields)
                    else:
                        df = pd.read_parquet(file_path)
                else:
                    df = pd.read_parquet(file_path)

                # add date info
                date_str = file_path.stem.replace("data", "")
                df['trade_date'] = date_str

                return df
            except Exception as e:
                warnings.warn(f"raise error when load file {file_path} : {str(e)}")
                return pd.DataFrame()

        # Use a thread pool to read in parallel (IO-intensive tasks)
        all_data = []
        max_workers = min(len(file_paths), 8)  # Limit the maximum number of threads

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {executor.submit(load_single_file, fp): fp for fp in file_paths}

            for future in as_completed(future_to_file):
                try:
                    df = future.result()
                    if not df.empty:
                        all_data.append(df)
                except Exception as e:
                    file_path = future_to_file[future]
                    warnings.warn(f"Error processing file {file_path}: {str(e)}")

        if not all_data:
            return pd.DataFrame()

        return pd.concat(all_data, ignore_index=True)

    @staticmethod
    def _sequential_load_minute_data(file_paths: List[Path], fields: List[str]) -> pd.DataFrame:
        """Load minute data sequentially (used when there are few files)"""
        all_data = []

        for file_path in file_paths:
            try:
                # Read only the required columns
                if fields:
                    schema = pq.read_schema(file_path)
                    available_fields = [col for col in fields if col in schema.names]
                    required_cols = ['symbol']
                    for col in required_cols:
                        if col in schema.names and col not in available_fields:
                            available_fields.append(col)

                    if available_fields:
                        df = pd.read_parquet(file_path, columns=available_fields)
                    else:
                        df = pd.read_parquet(file_path)
                else:
                    df = pd.read_parquet(file_path)

                # Add date information without overwriting the original timestamp
                date_str = file_path.stem.replace("data", "")
                df['trade_date'] = date_str

                all_data.append(df)

            except Exception as e:
                warnings.warn(f"Failed to load file {file_path}: {str(e)}")
                continue

        if not all_data:
            return pd.DataFrame()

        return pd.concat(all_data, ignore_index=True)

    @staticmethod
    def _load_other_frequency_data(data_path: Path, dates: List[str], fields: List[str]) -> pd.DataFrame:
        """Load hourly and daily frequency data"""
        file_path = data_path / "all_data.parquet"
        if not file_path.exists():
            warnings.warn(f"The data file does not exist: {file_path}")
            return pd.DataFrame()

        try:
            # Read only the required columns
            if fields:
                schema = pq.read_schema(file_path)
                available_fields = [col for col in fields if col in schema.names]
                required_cols = ['symbol', 'timestamp']
                for col in required_cols:
                    if col in schema.names and col not in available_fields:
                        available_fields.append(col)

                if available_fields:
                    data = pd.read_parquet(file_path, columns=available_fields)
                else:
                    data = pd.read_parquet(file_path)
            else:
                data = pd.read_parquet(file_path)

            # Date Filter
            if dates and not data.empty:
                if 'timestamp' in data.columns:
                    data = data[data['timestamp'].isin(dates)]
                elif 'timestamp' in data.columns:
                    data = data[data['timestamp'].isin(dates)]

            return data

        except Exception as e:
            warnings.warn(f"Failed to load file {file_path}: {str(e)}")
            return pd.DataFrame()

    @staticmethod
    def _validate_and_preprocess(data: pd.DataFrame, symbols: List[str]) -> pd.DataFrame:
        """Data Validation and Preprocessing"""
        if data.empty:
            return data

        # Target Screening
        if symbols and 'symbol' in data.columns:
            data = data[data['symbol'].isin(symbols)]

        # Sort
        if 'timestamp' in data.columns and 'symbol' in data.columns:
            data = data.sort_values(['symbol', 'timestamp'])
        elif 'timestamp' in data.columns:
            data = data.sort_values('timestamp')
        elif 'symbol' in data.columns:
            data = data.sort_values('symbol')

        return data

    def _direct_calculation(self, factor_func: Callable, data: pd.DataFrame,
                            factor_kwargs: Dict) -> pd.DataFrame:
        """Direct calculation"""
        print(f'Calculating {factor_func.__name__}...')
        if data.empty:
            return pd.DataFrame()

        try:
            result = factor_func(data, **factor_kwargs)

            # Standardized result format
            if isinstance(result, pd.Series):
                result = result.reset_index()

            return result
        except Exception as e:
            warnings.warn(f"fail to calculate directly: {str(e)}")
            return pd.DataFrame()
