import pandas as pd
from pathlib import Path
from typing import List, Dict, Callable, Optional
import json
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import pyarrow.parquet as pq
import os
import gc
import warnings


class FactorCalculator:
    """Factor Calculation Engine - Improved Version, Supports Batch Multi-Process Computing"""

    def __init__(self, data_root: str = "./data"):
        self.data_root = Path(data_root)

    def calculate_factor(self,
                         factor_func: Callable,
                         frequency: str = "daily",
                         fields: List[str] = None,
                         dates: List[str] = None,
                         symbols: List[str] = None,
                         parallel: bool = True,
                         n_jobs: int = 4,
                         batch_size: int = 30,
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

        # 1. Data Loading
        data = self._load_data(frequency, dates, fields)

        # 2. Data Validation and Preprocessing
        data = self._validate_and_preprocess(data, symbols)

        if data.empty:
            warnings.warn("The loaded data is empty. Please check the data path and date/target filter conditions.")
            return pd.DataFrame()

        factor_path = self.data_root / "factors" / f"{factor_type}" / f"factor_{factor_name}.parquet"

        # 3. Calculate
        if not parallel:
            # Direct calculation
            factor_df = self._direct_calculation(factor_func, data, factor_kwargs)

        else:
            # Parallel computing, using a batching strategy
            factor_df = self._parallel_calculation_with_batching(
                factor_func, data, window_size, n_jobs,
                batch_size, factor_kwargs)

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
        last = 0

        for i in range(batch_size, len(target_dates), batch_size):
            chunk_dates = target_dates[last:i]
            print(f"Processing Date Batch {i // batch_size + 1}/{total_batches}: {chunk_dates[0]} 到 {chunk_dates[-1]}")

            # Calculate the current batch using multiprocessing
            chunk_result = self.calculate_factor(
                factor_func=factor_func,
                frequency=frequency,
                fields=fields,
                dates=chunk_dates,
                symbols=symbols,
                parallel=True,
                n_jobs=n_jobs,
                factor_name=factor_name,
                factor_type=factor_type,
                window_size=window_size,
                batch_size=None,
                **factor_kwargs
            )
            last = max(last, i - window_size)
            if not chunk_result.empty:
                all_results.append(chunk_result)

            # Forced garbage collection
            gc.collect()

        factor_df = pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame()

        factor_path = self.data_root / f"{frequency}_data" / f"{factor_type}" / f"factor_{factor_name}.parquet"
        factor_df = factor_df.drop_duplicates()
        factor_df.dropna(inplace=True)
        factor_df.sort_values(['timestamp', 'symbol'])
        factor_df.to_parquet(factor_path, index=False)
        return factor_df

    def _parallel_calculation_with_batching(self,
                                            factor_func: Callable,
                                            data: pd.DataFrame,
                                            window_size: int,
                                            n_jobs: int,
                                            batch_size: int,
                                            factor_kwargs: Dict) -> pd.DataFrame:
        """Parallel computing with batching"""

        batches = self._create_date_batches(data, window_size, batch_size)

        # If there is no batch (small amount of data), compute directly.
        if len(batches) == 1:
            return self._direct_calculation(factor_func, data, factor_kwargs)

        # Set the number of processes
        if n_jobs == -1:
            n_jobs = min(len(batches), os.cpu_count())
        else:
            n_jobs = min(len(batches), n_jobs)

        # Parallel processing batch
        with ProcessPoolExecutor(max_workers=n_jobs) as executor:
            futures = []
            for i, batch_data in enumerate(batches):
                # Create a data copy for each batch to avoid data sharing issues between processes.
                batch_data_copy = batch_data.copy()

                # Submit Task
                future = executor.submit(FactorCalculator._calculate_batch, factor_func,
                                         batch_data_copy, i, factor_kwargs)

                futures.append(future)

            # Collect results
            results = []
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if not result.empty:
                        results.append(result)
                except Exception as e:
                    warnings.warn(f"批次计算失败: {str(e)}")

        # Merge results
        if results:
            return pd.concat(results, ignore_index=True)
        else:
            return pd.DataFrame()

    def _create_date_batches(self, data: pd.DataFrame, window_size: int, batch_size: int) -> List[pd.DataFrame]:
        """Create data batches by date"""
        dates = sorted(data['timestamp'].unique())

        if batch_size is None:
            # Automatically determine batch size
            n_dates = len(dates)
            batch_size = max(1, n_dates // (os.cpu_count() * 2))

        batches = []
        last = 0
        for i in range(batch_size, len(dates), batch_size):
            batch_dates = dates[last:i]
            batch_data = data[data['timestamp'].isin(batch_dates)].copy()
            batches.append(batch_data)
            last = i - window_size

        return batches

    @staticmethod
    def _calculate_batch(factor_func: Callable, batch_data: pd.DataFrame,
                         batch_id: int, factor_kwargs: Dict) -> pd.DataFrame:
        """Calculate a single batch"""
        try:
            result = factor_func(batch_data, **factor_kwargs)

            # Standardized result format
            if isinstance(result, pd.Series):
                result = result.to_frame(name=factor_func.__name__)

            return result
        except Exception as e:
            warnings.warn(f"Batch {batch_id} calculation failed: {str(e)}")
            return pd.DataFrame()

    def _load_data(self, frequency: str, dates: List[str], fields: List[str]) -> pd.DataFrame:
        """Data Loading Main Function"""
        data_path = self.data_root / f"{frequency}_data"

        if frequency == "min":
            return self._load_minute_data_optimized(data_path, dates, fields)
        else:
            return self._load_other_frequency_data(data_path, dates, fields)

    def _load_minute_data_optimized(self, data_path: Path, dates: List[str], fields: List[str]) -> pd.DataFrame:
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
            return self._parallel_load_minute_data(file_paths, fields)
        else:
            return self._sequential_load_minute_data(file_paths, fields)

    def _parallel_load_minute_data(self, file_paths: List[Path], fields: List[str]) -> pd.DataFrame:
        """Parallel loading of minute-level data (I/O intensive task)"""

        def load_single_file(file_path):
            """加载单个文件"""
            try:
                # 只读取需要的列
                if fields:
                    # 先读取schema确定哪些列存在
                    schema = pq.read_schema(file_path)
                    available_fields = [col for col in fields if col in schema.names]
                    # 添加必要列
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

                # 添加日期信息
                date_str = file_path.stem.replace("data", "")
                df['trade_date'] = date_str

                return df
            except Exception as e:
                warnings.warn(f"加载文件 {file_path} 失败: {str(e)}")
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

    def _sequential_load_minute_data(self, file_paths: List[Path], fields: List[str]) -> pd.DataFrame:
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

                # Add date information
                date_str = file_path.stem.replace("data", "")
                df['timestamp'] = date_str

                all_data.append(df)

            except Exception as e:
                warnings.warn(f"Failed to load file {file_path}: {str(e)}")
                continue

        if not all_data:
            return pd.DataFrame()

        return pd.concat(all_data, ignore_index=True)

    def _load_other_frequency_data(self, data_path: Path, dates: List[str], fields: List[str]) -> pd.DataFrame:
        """Load hourly and daily frequency data"""
        file_path = data_path / "all_data.parquet"
        if not file_path.exists():
            warnings.warn(f"The data file does not exist: {file_path}")
            return pd.DataFrame()

        try:
            # Read only the required columns
            if fields:
                import pyarrow.parquet as pq
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

    def _validate_and_preprocess(self, data: pd.DataFrame, symbols: List[str]) -> pd.DataFrame:
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
        if data.empty:
            return pd.DataFrame()

        try:
            result = factor_func(data, **factor_kwargs)

            # Standardized result format
            if isinstance(result, pd.Series):
                result = result.to_frame(name=factor_func.__name__)

            return result
        except Exception as e:
            warnings.warn(f"直接计算失败: {str(e)}")
            return pd.DataFrame()


class FactorRegistry:
    """Factor Registry - Focused on Factor Information Management and Persistence"""

    def __init__(self, calculator=None, registry_file: str = './register/factor_registry.json'):
        self.calculator = calculator
        self._factors = {}  # Factor Registry
        self._registry_file = registry_file

        # If a registry file is specified, attempt to load it
        if registry_file and Path(registry_file).exists():
            self.load_from_file(registry_file)

    def register(self,
                 name: str,
                 factor_func: Callable,
                 frequency: str,
                 fields: List[str],
                 type_: str,
                 category: str,
                 description: str = "",
                 window_size: int = 0,
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
            window_size: the rolling window size when calculate this factor
            **kwargs: Other parameters (such as default parameters, etc.)
        """
        # Basic Information
        factor_info = {
            'func': factor_func,
            'frequency': frequency,
            'fields': fields or [],
            'description': description,
            'created_time': datetime.now().isoformat(),
            'updated_time': datetime.now().isoformat(),
            'type': type_,
            'category': category,
            'window_size': window_size
        }

        # Add other parameters
        factor_info.update(kwargs)

        self._factors[name] = factor_info

        # Automatically save to file
        if self._registry_file:
            self._save_metadata(self._registry_file)

    def _save_metadata(self, file_path: str) -> bool:
        """Save factor metadata to a JSON file"""
        try:
            file_path = Path(file_path)
            file_path.parent.mkdir(parents=True, exist_ok=True)

            # Prepare serializable factor information (excluding function objects)
            metadata = {}
            for name, info in self._factors.items():
                metadata[name] = {
                    k: v for k, v in info.items()
                    if k != 'func'  # Exclusion function object
                }

            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)

            return True
        except Exception as e:
            print(f"Failed to save factor metadata: {str(e)}")
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
                        if k != 'func':
                            self._factors[name][k] = v
                    self._factors[name]['updated_time'] = datetime.now().isoformat()
                else:
                    # New factor, requires manual registration of functions later
                    self._factors[name] = info
                    self._factors[name]['func'] = None

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
                'has_function': info.get('func') is not None
            }
            for name, info in self._factors.items()
        }

    def get_factor_info(self, name: str) -> Optional[Dict]:
        """Obtain detailed information about specific factors"""
        if name not in self._factors:
            return None
        return {k: v for k, v in self._factors[name].items() if k != 'func'}

    def update_factor(self, name: str, **kwargs) -> bool:
        """Update factor information"""
        if name not in self._factors:
            print(f"Factor '{name}' is not registered")
            return False

        # 更新信息
        for k, v in kwargs.items():
            if k != 'func':  # Cannot directly update the function
                self._factors[name][k] = v

        # 更新修改时间
        self._factors[name]['updated_time'] = datetime.now().isoformat()

        # 保存到文件
        if self._registry_file:
            self._save_metadata(self._registry_file)

        return True

    def register_function(self, name: str, factor_func: Callable) -> bool:
        """Register a function for factors with already loaded metadata"""
        if name not in self._factors:
            print(f"Metadata for factor '{name}' not found")
            return False

        self._factors[name]['func'] = factor_func
        self._factors[name]['updated_time'] = datetime.now().isoformat()

        # Save to file
        if self._registry_file:
            self._save_metadata(self._registry_file)

        print(f"Function has been registered for factor '{name}'")
        return True

    def calculate(self, name: str, is_batch: bool, is_parallel: bool = True,
                  dates: List[str] = None, symbols: List[str] = None,
                  batch_size: int = 180, n_jobs: int = 4,
                  **factor_kwargs) -> any:
        """Calculate the registered factors"""
        if name not in self._factors:
            raise ValueError(f"Factor '{name}' is not registered")

        factor_info = self._factors[name]

        if factor_info.get('func') is None:
            raise ValueError(f"Metadata for factor '{name}' has been loaded, but the function is not registered.")

        # If you have a calculator, use the calculator to calculate.
        if self.calculator:
            calculator_kwargs = {
                'factor_func': factor_info['func'],
                'frequency': factor_info['frequency'],
                'fields': factor_info['fields'],
                'dates': dates,
                'symbols': symbols,
                'n_jobs': n_jobs,
                'batch_size': batch_size,
                'factor_name': name,
                'factor_type': factor_info['type'],
                'window_size': factor_info['window_size'],
            }

            if is_batch:
                return self.calculator.calculate_factor_incremental(
                    **calculator_kwargs,
                    **factor_kwargs
                )
            else:
                calculator_kwargs['parallel'] = is_parallel,
                return self.calculator.calculate_factor(
                    **calculator_kwargs,
                    **factor_kwargs
                )
        else:
            # Directly call the function
            return factor_info['func'](**factor_kwargs)

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


def momentum(data, window=20):
    """动量因子 - 需要rolling计算"""
    data.set_index('timestamp', inplace=True)
    data['factor'] = data.groupby('symbol')['Close'].pct_change(window)
    return data.reset_index()[['timestamp', 'symbol', 'factor']]


def volatility(data, window=30):
    """波动率因子 - 需要rolling计算"""
    data.set_index('timestamp', inplace=True)
    data = data.groupby('symbol')['Close'].rolling(window).std().reset_index()
    data.rename({'Close': 'factor'}, axis=1, inplace=True)
    return data


def simple_factor(data):
    """简单因子 - 不需要rolling计算"""
    data.set_index(['symbol', 'timestamp'], inplace=True)
    return (data['Close'] / data['Open'] - 1).reset_index().rename({0: 'factor'}, axis=1)

# 使用示例
if __name__ == "__main__":

    # 实例化计算框架
    factor_calculator = FactorCalculator("../../data/")

    # 创建注册表
    registry = FactorRegistry(factor_calculator, "../../data/register/factor_registry.json")

    # 注册因子
    registry.register(
        name="momentum",
        factor_func=momentum,
        frequency="daily",
        fields=["Close"],
        description="价格动量因子",
        category="trend",
        type_='alpha',
        window_size= 20
    )

    registry.register(
        name="volatility",
        factor_func=volatility,
        frequency="daily",
        fields=["Close"],
        description="历史波动率",
        category="risk",
        type_='alpha',
        window_size=30
    )

    registry.register(
        name="simple_factor",
        factor_func=simple_factor,
        frequency="daily",
        fields=["Close", "Open"],
        description="简单价差因子",
        category="general",
        type_='alpha'
    )

    # 使用
    symbols = ['BTCUSDT', 'ETHUSDT', 'BCHUSDT', 'XRPUSDT', 'LTCUSDT', 'TRXUSDT']

    # 方法1: 普通并行计算，自动分批
    result1 = registry.calculate(
        "momentum",
        symbols=symbols,
        is_batch=False,
        is_parallel=True,
        n_jobs=4,
        batch_size=60,
        window=20
    )
    print(f"动量因子结果形状: {result1.shape}")

    # 方法2: 增量计算，结合日期分批和进程内标的分批
    result2 = registry.calculate(
        "volatility",
        is_batch=True,
        symbols=symbols,
        batch_size=120,  # 每次处理2天数据
        n_jobs=2,  # 使用2个进程
        window=30
    )

    print(f"波动率因子结果形状: {result2.shape}")

    result3 = registry.calculate(
        "simple_factor",
        symbols=symbols,
        is_batch=False,
        is_parallel=True,
        n_jobs=2,
        batch_size=90  # 每批2天数据
    )

    print(f"简单因子结果形状: {result3.shape}")

    # 查看所有因子
    factors = registry.list_factors()
    print("已注册因子:", list(factors.keys()))
