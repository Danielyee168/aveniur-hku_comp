"""
AlphaNet 通用数据加载器 - 支持 Parquet 和 NPZ 格式
====================================================
自动优化数据加载，支持团队现有的 parquet 工作流
"""

import pandas as pd
import numpy as np
import torch
from torch.utils.data import Dataset, DataLoader
from typing import List, Tuple, Optional, Dict, Union
import os
from pathlib import Path
import time
import warnings
warnings.filterwarnings('ignore')

class AlphaNetUniversalDataset(Dataset):
    """
    通用 AlphaNet 数据集 - 智能处理 Parquet 和 NPZ 格式
    
    特点:
    1. 自动检测数据格式 (parquet 或 npz)
    2. 首次运行时自动将 parquet 转换为 npz 缓存
    3. 后续运行直接从 npz 加载 (快 10 倍)
    4. 完全兼容现有的 panel.parquet 工作流
    """
    
    def __init__(
        self,
        data_path: str = None,
        df: pd.DataFrame = None,
        seq_len: int = 96,
        cache_dir: str = 'cache',
        force_rebuild: bool = False,
        verbose: bool = True
    ):
        """
        初始化数据集
        
        参数:
            data_path: 数据文件路径 (支持 .parquet 或 .npz)
            df: pandas DataFrame (可选，直接传入数据)
            seq_len: 序列长度 (默认 96 = 24小时)
            cache_dir: NPZ 缓存目录
            force_rebuild: 强制重建缓存
            verbose: 显示详细信息
        """
        self.seq_len = seq_len
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.verbose = verbose
        
        # 确定缓存文件路径
        if data_path:
            base_name = Path(data_path).stem
            self.cache_path = self.cache_dir / f"{base_name}_cache.npz"
        else:
            self.cache_path = self.cache_dir / "alphanet_cache.npz"
        
        # 智能加载数据
        self._load_data(data_path, df, force_rebuild)
    
    def _load_data(self, data_path: str, df: pd.DataFrame, force_rebuild: bool):
        """
        智能加载数据 - 自动选择最优方式
        """
        # 情况 1: 直接传入 DataFrame
        if df is not None:
            if self.verbose:
                print("📊 从 DataFrame 处理数据...")
            self._process_dataframe(df)
            return
        
        # 情况 2: NPZ 缓存存在且不强制重建
        if self.cache_path.exists() and not force_rebuild:
            if self.verbose:
                print(f"⚡ 从 NPZ 缓存快速加载: {self.cache_path}")
            self._load_from_npz(self.cache_path)
            return
        
        # 情况 3: 提供了数据路径
        if data_path:
            data_path = Path(data_path)
            
            # 3.1: NPZ 文件
            if data_path.suffix == '.npz':
                if self.verbose:
                    print(f"📦 加载 NPZ 文件: {data_path}")
                self._load_from_npz(data_path)
                
            # 3.2: Parquet 文件
            elif data_path.suffix == '.parquet':
                if self.verbose:
                    print(f"📄 加载 Parquet 文件: {data_path}")
                    print("   首次加载会创建 NPZ 缓存，之后加载会快很多...")
                df = pd.read_parquet(data_path)
                self._process_dataframe(df)
                
            else:
                raise ValueError(f"不支持的文件格式: {data_path.suffix}")
        else:
            raise ValueError("请提供 data_path 或 df 参数")
    
    def _process_dataframe(self, df: pd.DataFrame):
        """
        处理 DataFrame 并创建 NPZ 缓存
        """
        start_time = time.time()
        
        # 确保数据按正确顺序排序
        df = df.sort_values(['timestamp', 'symbol']).reset_index(drop=True)
        
        # 获取唯一值
        self.symbols = sorted(df['symbol'].unique())
        self.timestamps = sorted(df['timestamp'].unique())
        self.n_symbols = len(self.symbols)
        
        # 自动检测特征列 (排除 timestamp, symbol, y_raw, future_vwap)
        exclude_cols = {'timestamp', 'symbol', 'y_raw', 'future_vwap'}
        self.feature_cols = [col for col in df.columns if col not in exclude_cols]
        self.n_features = len(self.feature_cols)
        
        if self.verbose:
            print(f"   📈 处理 {len(self.timestamps)} 个时间点 × {self.n_symbols} 个币种")
            print(f"   🔢 特征数量: {self.n_features}")
            print(f"   📝 特征列: {self.feature_cols[:5]}..." if len(self.feature_cols) > 5 else f"   📝 特征列: {self.feature_cols}")
        
        # 创建数组
        data = np.zeros((len(self.timestamps), self.n_symbols, self.n_features), dtype=np.float32)
        targets = np.zeros((len(self.timestamps), self.n_symbols), dtype=np.float32)
        masks = np.zeros((len(self.timestamps), self.n_symbols), dtype=bool)
        
        # 创建映射
        symbol_to_idx = {sym: idx for idx, sym in enumerate(self.symbols)}
        timestamp_to_idx = {ts: idx for idx, ts in enumerate(self.timestamps)}
        
        # 按币种批量处理 (更高效)
        for symbol in self.symbols:
            symbol_data = df[df['symbol'] == symbol].copy()
            if symbol_data.empty:
                continue
            
            s_idx = symbol_to_idx[symbol]
            
            # 映射时间戳到索引
            symbol_data['t_idx'] = symbol_data['timestamp'].map(timestamp_to_idx)
            valid_mask = symbol_data['t_idx'].notna()
            symbol_data = symbol_data[valid_mask]
            
            if not symbol_data.empty:
                t_indices = symbol_data['t_idx'].astype(int).values
                
                # 填充特征
                data[t_indices, s_idx, :] = symbol_data[self.feature_cols].values
                
                # 填充目标
                if 'y_raw' in symbol_data.columns:
                    targets[t_indices, s_idx] = symbol_data['y_raw'].values
                
                # 标记有效数据
                masks[t_indices, s_idx] = True
        
        # 找出有效的序列起始索引
        valid_indices = []
        for i in range(len(self.timestamps) - self.seq_len + 1):
            # 检查窗口内是否有足够的有效数据
            window_mask = masks[i:i+self.seq_len].any(axis=0)
            # 至少需要 10 个币种或 30% 的币种有数据
            if window_mask.sum() >= min(10, self.n_symbols * 0.3):
                valid_indices.append(i)
        
        valid_indices = np.array(valid_indices, dtype=np.int32)
        
        # 保存到 NPZ
        if self.verbose:
            print(f"   💾 保存 NPZ 缓存到: {self.cache_path}")
        
        # 转换时间戳为字符串 (NPZ 不能直接保存 datetime)
        timestamps_str = [str(ts) for ts in self.timestamps]
        
        np.savez_compressed(
            self.cache_path,
            data=data,
            targets=targets,
            masks=masks,
            valid_indices=valid_indices,
            symbols=np.array(self.symbols),
            timestamps=np.array(timestamps_str),
            feature_cols=np.array(self.feature_cols),
            n_features=self.n_features,
            seq_len=self.seq_len
        )
        
        # 存储在内存
        self.data = data
        self.targets = targets
        self.masks = masks
        self.valid_indices = valid_indices
        
        elapsed = time.time() - start_time
        file_size = self.cache_path.stat().st_size / (1024**2) if self.cache_path.exists() else 0
        
        if self.verbose:
            print(f"   ✅ 处理完成! 耗时: {elapsed:.1f} 秒")
            print(f"   📦 缓存大小: {file_size:.1f} MB")
            print(f"   🎯 有效序列数: {len(valid_indices)}")
    
    def _load_from_npz(self, npz_path: Path):
        """
        从 NPZ 文件加载数据
        """
        start_time = time.time()
        
        with np.load(npz_path, allow_pickle=True) as npz:
            self.data = npz['data']
            self.targets = npz['targets']
            self.masks = npz['masks']
            self.valid_indices = npz['valid_indices']
            self.symbols = npz['symbols'].tolist()
            self.n_symbols = len(self.symbols)
            self.n_features = int(npz['n_features'])
            self.seq_len = int(npz['seq_len'])
            
            # 加载特征列名
            if 'feature_cols' in npz:
                self.feature_cols = npz['feature_cols'].tolist()
            
            # 解析时间戳
            if 'timestamps' in npz:
                self.timestamps = [pd.Timestamp(ts) for ts in npz['timestamps']]
        
        elapsed = time.time() - start_time
        
        if self.verbose:
            print(f"   ✅ NPZ 加载完成! 耗时: {elapsed:.2f} 秒")
            print(f"   📊 数据形状: {self.data.shape}")
            print(f"   🎯 有效序列数: {len(self.valid_indices)}")
    
    def __len__(self):
        return len(self.valid_indices)
    
    def __getitem__(self, idx):
        """
        获取一个训练样本
        
        返回:
            X: [n_symbols, seq_len, n_features] - 特征序列
            y: [n_symbols] - 目标值
            mask: [n_symbols] - 有效性掩码
        """
        # 获取序列的起始索引
        start_idx = self.valid_indices[idx]
        end_idx = start_idx + self.seq_len
        
        # 提取序列: [seq_len, n_symbols, n_features]
        X_seq = self.data[start_idx:end_idx]
        
        # 转置为: [n_symbols, seq_len, n_features]
        # 这个格式与 CryptoTransformer 兼容
        X = X_seq.transpose(1, 0, 2)
        
        # 获取最后一个时间点的目标和掩码
        y = self.targets[end_idx - 1]
        mask = self.masks[end_idx - 1]
        
        # 转换为 PyTorch 张量
        return (
            torch.from_numpy(X.copy()).float(),
            torch.from_numpy(y.copy()).float(),
            torch.from_numpy(mask.copy()).float()
        )
    
    def get_info(self) -> Dict:
        """
        获取数据集信息
        """
        return {
            'n_symbols': self.n_symbols,
            'n_features': self.n_features,
            'seq_len': self.seq_len,
            'n_samples': len(self.valid_indices),
            'symbols': self.symbols[:10] if len(self.symbols) > 10 else self.symbols,
            'feature_cols': self.feature_cols[:10] if len(self.feature_cols) > 10 else self.feature_cols,
            'cache_path': str(self.cache_path) if self.cache_path.exists() else None
        }


def create_dataloaders(
    data_source: Union[str, pd.DataFrame] = 'panel.parquet',
    batch_size: int = 16,
    seq_len: int = 96,
    train_ratio: float = 0.8,
    val_ratio: float = 0.1,
    num_workers: int = 0,
    cache_dir: str = 'cache',
    force_rebuild: bool = False,
    verbose: bool = True
) -> Tuple[DataLoader, DataLoader, DataLoader, Dict]:
    """
    创建训练/验证/测试数据加载器
    
    参数:
        data_source: 数据源 (parquet文件路径、npz文件路径或DataFrame)
        batch_size: 批次大小
        seq_len: 序列长度
        train_ratio: 训练集比例
        val_ratio: 验证集比例
        num_workers: 并行加载进程数
        cache_dir: 缓存目录
        force_rebuild: 强制重建缓存
        verbose: 显示详细信息
    
    返回:
        (train_loader, val_loader, test_loader, info_dict)
    """
    
    if verbose:
        print("="*60)
        print("🚀 AlphaNet 数据加载器初始化")
        print("="*60)
    
    # 创建基础数据集
    if isinstance(data_source, pd.DataFrame):
        dataset = AlphaNetUniversalDataset(
            df=data_source,
            seq_len=seq_len,
            cache_dir=cache_dir,
            force_rebuild=force_rebuild,
            verbose=verbose
        )
    else:
        dataset = AlphaNetUniversalDataset(
            data_path=data_source,
            seq_len=seq_len,
            cache_dir=cache_dir,
            force_rebuild=force_rebuild,
            verbose=verbose
        )
    
    # 划分数据集
    n_samples = len(dataset)
    indices = np.arange(n_samples)
    
    train_end = int(n_samples * train_ratio)
    val_end = int(n_samples * (train_ratio + val_ratio))
    
    train_indices = indices[:train_end]
    val_indices = indices[train_end:val_end]
    test_indices = indices[val_end:]
    
    # 创建子集
    from torch.utils.data import Subset
    
    train_dataset = Subset(dataset, train_indices)
    val_dataset = Subset(dataset, val_indices)
    test_dataset = Subset(dataset, test_indices)
    
    # 创建数据加载器
    train_loader = DataLoader(
        train_dataset,
        batch_size=batch_size,
        shuffle=True,
        num_workers=num_workers,
        pin_memory=torch.cuda.is_available(),
        persistent_workers=(num_workers > 0),
        drop_last=True
    )
    
    val_loader = DataLoader(
        val_dataset,
        batch_size=batch_size,
        shuffle=False,
        num_workers=num_workers,
        pin_memory=torch.cuda.is_available(),
        persistent_workers=(num_workers > 0)
    )
    
    test_loader = DataLoader(
        test_dataset,
        batch_size=batch_size,
        shuffle=False,
        num_workers=num_workers,
        pin_memory=torch.cuda.is_available(),
        persistent_workers=(num_workers > 0)
    )
    
    # 获取数据集信息
    info = dataset.get_info()
    info.update({
        'train_samples': len(train_dataset),
        'val_samples': len(val_dataset),
        'test_samples': len(test_dataset),
        'batch_size': batch_size
    })
    
    if verbose:
        print("\n📊 数据集划分:")
        print(f"   训练集: {len(train_dataset)} 样本 ({len(train_loader)} 批次)")
        print(f"   验证集: {len(val_dataset)} 样本 ({len(val_loader)} 批次)")
        print(f"   测试集: {len(test_dataset)} 样本 ({len(test_loader)} 批次)")
        print("\n✅ 数据加载器创建成功!")
        print("="*60)
    
    return train_loader, val_loader, test_loader, info


# ============ 快速测试函数 ============
def test_dataloader():
    """
    测试数据加载器是否正常工作
    """
    print("🧪 测试数据加载器...")
    
    # 创建测试数据
    test_symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']
    test_timestamps = pd.date_range('2024-01-01', periods=200, freq='15min')
    
    test_data = []
    for symbol in test_symbols:
        for ts in test_timestamps:
            test_data.append({
                'timestamp': ts,
                'symbol': symbol,
                'open': np.random.randn() * 100 + 1000,
                'high': np.random.randn() * 100 + 1100,
                'low': np.random.randn() * 100 + 900,
                'close': np.random.randn() * 100 + 1000,
                'volume': np.random.exponential(1000),
                'vwap': np.random.randn() * 100 + 1000,
                'y_raw': np.random.randn() * 0.01
            })
    
    # test_df = pd.DataFrame(test_data)
    test_df = pd.read_parquet('./test.parquet')
    
    # 创建数据加载器
    train_loader, val_loader, test_loader, info = create_dataloaders(
        data_source=test_df,
        batch_size=2,
        seq_len=30,
        verbose=True
    )
    
    # 测试一个批次
    for X, y, mask in train_loader:
        print(f"\n✅ 测试成功!")
        print(f"   X 形状: {X.shape}")
        print(f"   y 形状: {y.shape}")
        print(f"   mask 形状: {mask.shape}")
        break
    
    return True


if __name__ == "__main__":
    test_dataloader()