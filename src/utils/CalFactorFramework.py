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
        if factor_name is None:
            factor_name = factor_func.__name__

        # Determine factor type
        if factor_type is None:
            factor_type = "alpha" if "alpha" in factor_name.lower() else "risk"

        factor_path = self.data_root / "factors" / f"{factor_type}" / f"factor_{factor_name}.parquet"

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
        factor_df.sort_values(['timestamp', 'symbol'])
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
                # parallel=(n_jobs > 1) & (len(chunk_dates) > parallel_batch_size),
                parallel=(n_jobs > 1),
                n_jobs=n_jobs,
                factor_name=factor_name,
                factor_type=factor_type,
                window_size=window_size,
                parallel_batch_size=parallel_batch_size,
                **factor_kwargs
            )

            if not chunk_result.empty:
                all_results.append(chunk_result)

            gc.collect()

            if window_size > 0:
                start = max(end - window_size, start + 1)
            else:
                start = end

        factor_df = pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame()

        factor_path = self.data_root / f"{frequency}_data" / f"{factor_type}" / f"factor_{factor_name}.parquet"
        if not factor_df.empty:
            factor_df = factor_df.drop_duplicates()
            factor_df.dropna(inplace=True)
            if {'timestamp', 'symbol'}.issubset(factor_df.columns):
                factor_df = factor_df.sort_values(['timestamp', 'symbol'])
            factor_path.parent.mkdir(parents=True, exist_ok=True)
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
                result = result.to_frame(name=factor_func.__name__)

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
                result = result.to_frame(name=factor_func.__name__)

            return result
        except Exception as e:
            warnings.warn(f"fail to calculate directly: {str(e)}")
            return pd.DataFrame()


class FactorRegistryError(Exception): pass
class FactorNotFoundError(FactorRegistryError): pass
class FactorFunctionNotFound(FactorRegistryError): pass


class FactorRegistry:
    """Factor Registry - Focused on Factor Information Management and Persistence"""
    # Update Type Enumeration
    UPDATE_TYPE_DATA = "data_update"  # Category One Update: Only add the latest data
    UPDATE_TYPE_LOGIC = "logic_update"  # Category 2 update: logic changes, recalculation required
    ACTION_DELETE = "delete"
    def __init__(self, calculator=None, registry_file: str = './register/factor_registry.json'):
        self.calculator = calculator
        self._factors = {}  # Factor Registry: name -> info
        self._logs = defaultdict(list)  # name -> list of log entries
        self._registry_file = registry_file

        self.data_root = Path('./data')

        self._log_file = Path('./logs/factor_update_log.json')
        self._log_file.parent.mkdir(parents=True, exist_ok=True)

        # If a registry file is specified, attempt to load it
        if registry_file and Path(registry_file).exists():
            self.load_from_file(registry_file)

        if self._log_file.exists():
            try:
                with open(self._log_file, 'r', encoding='utf-8') as f:
                    raw_logs = json.load(f)
                    self._logs = defaultdict(list, {k: v for k, v in raw_logs.items()})
            except Exception as e:
                logger.warning(f"Failed to load logs from {self._log_file}: {e}")

    def register(self,
                 name: str,
                 factor_func: Callable,
                 frequency: str,
                 fields: List[str],
                 type_: str,
                 category: str,
                 description: str = "",
                 **kwargs):
        """
        Factor Registration
        Args:
            frequency: Data frequency
            name: Factor Name
            factor_func: Factor calculation function
            fields: Required data fields
            description: Factor Description
            category: Factor Subcategories
            type_: Factor categories, risk or alpha
            **kwargs: Other parameters (such as default parameters, etc.)
        """
        # Get the module path and full name of a function (supports nested functions)
        if name in self._factors:
            raise FactorRegistryError(f"Factor '{name}' already registered. Use 'update_factor' to update.")

        try:
            module_name = factor_func.__module__
            qualname = factor_func.__qualname__  # Support class methods and nested functions
        except AttributeError:
            raise ValueError(f"Function {factor_func} must have __module__ and __qualname__")

        # Basic Information
        factor_info = {
            'func_module': module_name,
            'func_qualname': qualname,
            'frequency': frequency,
            'fields': fields or [],
            'description': description,
            'created_time': datetime.now().isoformat(),
            'updated_time': datetime.now().isoformat(),
            'type': type_,
            'category': category
        }

        # Add other parameters
        factor_info.update(kwargs)

        # Log registration as logic update
        self._log_update(name, self.UPDATE_TYPE_LOGIC, f"Registered new factor: {module_name}.{qualname}")

        self._factors[name] = factor_info

        # Automatically save to file
        if self._registry_file:
            self._save_metadata(self._registry_file)

        logger.info(f"Factor '{name}' registered: {module_name}.{qualname}")

    def _save_metadata(self, file_path: str) -> bool:
        """Save factor metadata to a JSON file"""
        try:
            file_path = Path(file_path)
            file_path.parent.mkdir(parents=True, exist_ok=True)

            # 1. Write to a temporary file
            temp_file = file_path.with_suffix('.tmp')
            metadata = {
                name: {k: v for k, v in info.items() if not k.startswith('func_')}
                for name, info in self._factors.items()
            }
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)

            # 2. Atom replacement
            temp_file.replace(file_path)

            return True
        except Exception as e:
            logger.error(f"Failed to save metadata to {file_path}: {str(e)}", exc_info=True)
            return False

    def load_from_file(self, file_path: str) -> bool:
        """Load factor metadata from a JSON file"""
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                print(f"The registry file does not exist: {file_path}")
                return False

            with open(file_path, 'r', encoding='utf-8') as f:
                metadata = json.load(f)

            # Update the registry (only update metadata, do not overwrite functions)
            for name, info in metadata.items():
                if name in self._factors:
                    # Update the metadata of existing factors while retaining the functions
                    for k, v in info.items():
                        if not k.startswith('func_'):
                            self._factors[name][k] = v
                    self._factors[name]['updated_time'] = datetime.now(timezone.utc).isoformat()
                    self._log_update(name, self.UPDATE_TYPE_DATA, "Metadata updated from file")
                else:
                    # New factor, requires manual registration of functions later
                    info['func_module'] = None
                    info['func_qualname'] = None
                    info['created_time'] = datetime.now(timezone.utc).isoformat()
                    info['updated_time'] = datetime.now(timezone.utc).isoformat()
                    self._factors[name] = info
                    self._log_update(name, self.UPDATE_TYPE_DATA, "Loaded from file (function not bound)")

                logger.info(f"Loaded metadata for {len(metadata)} factors from {file_path}")

            print(f"Loaded metadata for {len(metadata)} factors from {file_path}")
            return True
        except Exception as e:
            print(f"Failed to load factor metadata: {str(e)}")
            return False

    def list_factors(self) -> Dict[str, Dict]:
        """List all registered factor information"""
        return {
            name: {
                'description': info['description'],
                'frequency': info['frequency'],
                'fields': info['fields'],
                'created_time': info['created_time'],
                'updated_time': info['updated_time'],
                'type': info['type'],
                'category': info['category'],
                'has_function': (info.get('func_module') is not None and info.get('func_qualname') is not None)
            }
            for name, info in self._factors.items()
        }

    def get_factor_info(self, name: str) -> Optional[Dict]:
        """Obtain detailed information about specific factors"""
        if name not in self._factors:
            return None
        return {k: v for k, v in self._factors[name].items() if k not in ['func']}

    def update_factor(self, name: str, update_type: str = UPDATE_TYPE_DATA, **kwargs) -> bool:
        """Update factor information"""
        if name not in self._factors:
            raise FactorNotFoundError(f"Factor '{name}' is not registered")

        # update info
        for k, v in kwargs.items():
            if not k.startswith('func_'):  # Cannot directly update the function
                self._factors[name][k] = v

        # update modification time
        self._factors[name]['updated_time'] = datetime.now(timezone.utc).isoformat()

        # Log the update
        reason = kwargs.pop('reason', 'Manual update')
        self._log_update(name, update_type, reason)

        # save to file
        if self._registry_file:
            self._save_metadata(self._registry_file)

        logger.info(f"Factor '{name}' updated [{update_type}]: {reason}")
        return True

    def _log_update(self, name: str, update_type: str, message: str):
        """Internal: append a single log entry safely"""
        log_entry = {
            'factor': name,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'type': update_type,
            'message': message
        }

        try:
            # Use 'a' mode to append, with each log on a separate line (JSON Lines format)
            with open(self._log_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(log_entry, ensure_ascii=False) + '\n')

            # Simultaneously update the logs in memory (for get_update_log)
            self._logs[name].append(log_entry)

        except Exception as e:
            logger.error(f"Failed to append log entry: {str(e)}", exc_info=True)

    def get_update_log(self, name: str) -> List[Dict]:
        """Get update history for a factor"""
        if name not in self._factors:
            raise FactorNotFoundError(f"Factor '{name}' not found")
        return self._logs.get(name, []).copy()

    def delete_factor(self, name: str, reason: str = "Manual deletion") -> bool:
        """Delete a factor and log it"""
        if name not in self._factors:
            raise FactorNotFoundError(f"Factor '{name}' not found")

        del self._factors[name]
        self._log_update(name, self.ACTION_DELETE, reason)

        if self._registry_file:
            self._save_metadata(self._registry_file)

        logger.info(f"Factor '{name}' deleted: {reason}")
        return True

    def _get_function(self, name: str) -> Callable:
        """Internal: resolve function from module and qualname"""
        info = self._factors.get(name)
        if not info:
            raise FactorNotFoundError(f"Factor '{name}' not found")

        module_name = info.get('func_module')
        qualname = info.get('func_qualname')

        if not module_name or not qualname:
            raise FactorFunctionNotFound(
                f"Function for factor '{name}' is not bound (module={module_name}, qualname={qualname})")

        try:
            module = __import__(module_name, fromlist=[qualname.split('.')[0]])
            func = module
            for attr in qualname.split('.'):
                func = getattr(func, attr)
            return func
        except Exception as e:
            raise FactorFunctionNotFound(f"Failed to import {module_name}.{qualname}: {str(e)}")

    def call_factor(self, name: str, **kwargs):
        """
        Call the registered factor function directly.
        Use this to invoke the function with arbitrary args.
        """
        func = self._get_function(name)
        logger.debug(f"Calling factor function '{name}'")
        try:
            return func(**kwargs)
        except Exception as e:
            logger.error(f"Error calling factor '{name}': {str(e)}")
            raise

    def calculate(self, name: str, is_batch: bool, is_parallel: bool = True,
                  dates: List[str] = None, symbols: List[str] = None,
                  batch_size: int = 180, parallel_batch_size: int = 20, n_jobs: int = 4,
                  **factor_kwargs) -> any:
        """Calculate the registered factors"""
        if name not in self._factors:
            raise FactorNotFoundError(f"Factor '{name}' is not registered")

        factor_info = self._factors[name]
        func = self._get_function(name)  # Will raise if not found

        if factor_info.get('func_qualname') is None:
            raise ValueError(f"Metadata for factor '{name}' has been loaded, but the function is not registered.")

        # calculate window size to fill data
        if factor_info['frequency'] == 'min':
            ws = math.ceil((factor_kwargs.get('window', 1) - 1) / 1440)
        else:
            ws = factor_kwargs.get('window', 1) - 1

        if ws < 0:
            ws = 0

        # If you have a calculator, use the calculator to calculate.
        if self.calculator:
            calculator_kwargs = {
                'factor_func': func,
                'frequency': factor_info['frequency'],
                'fields': factor_info['fields'],
                'dates': dates,
                'symbols': symbols,
                'n_jobs': n_jobs,
                'parallel_batch_size': parallel_batch_size,
                'factor_name': name,
                'factor_type': factor_info['type'],
                'window_size': ws,
            }

            if is_batch:
                calculator_kwargs['batch_size'] = batch_size
                return self.calculator.calculate_factor_incremental(
                    **calculator_kwargs,
                    **factor_kwargs
                )
            else:
                calculator_kwargs['parallel'] = is_parallel
                return self.calculator.calculate_factor(
                    **calculator_kwargs,
                    **factor_kwargs
                )
        else:
            # Directly call the function
            return func(**factor_kwargs)

    def get_factor_data(self, name: str, **query_kwargs):
        """
        Retrieve already calculated factor data.
        Assumes the calculator has a method `get_factor_data`.
        """
        try:
            pq_path = self.data_root / "factors" / f"{self._factors[name]['type']}" / f"factor_{name}.parquet"
            temp_factor = pd.read_parquet(pq_path)
            return temp_factor
        except Exception as e:
            logger.error(f"Failed to retrieve data for factor '{name}': {str(e)}")
            raise

    def set_registry_file(self, file_path: str):
        """Set registry file path"""
        self._registry_file = file_path

    def save(self) -> bool:
        """Manually save the registry"""
        if self._registry_file:
            return self._save_metadata(self._registry_file)
        else:
            print("Registry file path not set")
            return False

    def exists(self, name: str) -> bool:
        return name in self._factors
