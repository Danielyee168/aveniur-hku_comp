from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd
import statsmodels.api as sm
from numba import jit


REQUIRED_FIELDS: Iterable[str] = (
    "symbol",
    "timestamp",
    "Close",
    "Volume"
)

FACTOR_NAME = "trend_hour"
DESCRIPTION = "test"
TYPE = 'alpha'
CATEGORY = "momentum"
DEFAULT_FREQUENCY = "hour"


def winsorize_series(s, quantile=0.025):
    """对序列进行双边缩尾处理"""
    low = s.quantile(quantile)
    high = s.quantile(1 - quantile)
    return s.clip(lower=low, upper=high)


@jit(nopython=True)
def compute_llt_numba(price_array: np.ndarray, a: float) -> np.ndarray:
    n = len(price_array)
    llt = np.full(n, np.nan)
    if n < 3:
        return llt

    llt[0] = price_array[0]
    llt[1] = price_array[1]

    a2 = a ** 2
    coef1 = a - 0.25 * a2
    coef2 = 0.5 * a2
    coef3 = -(a - 0.75 * a2)
    coef4 = 2 * (1 - a)
    coef5 = -(1 - a) ** 2

    for i in range(2, n):
        if np.isnan(price_array[i]) or np.isnan(price_array[i - 1]) or np.isnan(price_array[i - 2]):
            continue
        llt[i] = (
                coef1 * price_array[i] +
                coef2 * price_array[i - 1] +
                coef3 * price_array[i - 2] +
                coef4 * llt[i - 1] +
                coef5 * llt[i - 2]
        )
    return llt


def compute_llt_custom(price: pd.Series, d: int, min_periods: int = 3) -> pd.Series:
    a = 2 / (1 + d)
    valid = price.dropna()
    if len(valid) < max(min_periods, 3):
        return pd.Series([np.nan] * len(price), index=price.index)

    # 转为数组
    idx = valid.index
    price_array = valid.values
    llt_array = compute_llt_numba(price_array, a)

    # 映射回原索引
    result = pd.Series(np.nan, index=price.index)
    result.loc[idx] = llt_array
    return result


def compute(data: pd.DataFrame, col_name: str = FACTOR_NAME) -> pd.DataFrame:
    """
    计算趋势因子

    参数:
        data (pd.DataFrame): 包含 ['symbol', 'timestamp', 'Close', 'Volume'] 的原始数据
            timestamp 为小时级别

    返回:
        pd.DataFrame: 包含 ['symbol', 'timestamp', 'factor'] 的趋势因子
    """
    # 确保时间列是 datetime 类型并排序
    data = data.copy()
    data = data.sort_values(['symbol', 'timestamp']).reset_index(drop=True)

    # 转为宽表格式
    close_1h = data.pivot(index='timestamp', columns='symbol', values='Close')
    volume_1h = data.pivot(index='timestamp', columns='symbol', values='Volume')

    # 创建计算用的 DataFrame
    cal_df = pd.DataFrame()

    # === 1. 计算 LLT 均线 ===
    llt_periods = [4, 8, 24, 48, 72, 120, 240, 720, 1200]
    min_periods_list = [3, 6, 18, 36, 60, 80, 160, 480, 720]

    for d, min_p in zip(llt_periods, min_periods_list):
        tcol_name = f'LLT{d}'
        cal_df[tcol_name] = close_1h.apply(
            lambda x: compute_llt_custom(x, d=d)
        ).stack()

    # === 2. 计算成交量均线 ===
    vol_fields = ['volume_8h', 'volume_24h', 'volume_72h', 'volume_120h']
    vol_windows = [8, 24, 72, 120]

    cal_df['volume'] = volume_1h.stack()
    cal_df['volume_4h'] = volume_1h.rolling(4, min_periods=3).mean().stack()

    for w, col in zip(vol_windows, vol_fields):
        cal_df[col] = volume_1h.rolling(w, min_periods=max(w // 3, 3)).mean().stack()

    # === 3. 未来8小时对数收益率 ===
    cal_df['log_ret_8h'] = np.log(close_1h / close_1h.shift(8)).shift(-8).stack()

    # 添加 close 用于后续归一化
    cal_df['close'] = close_1h.stack()

    # === 4. 特征列定义 ===
    L_fields = [f'LLT{d}' for d in llt_periods]
    V_fields = vol_fields
    feature_fields = L_fields + V_fields

    # === 5. 动态截面回归：每小时跑一次回归，获取 beta ===
    beta_8h_df = []

    # 回归时间段（确保有足够历史）
    reg_start = close_1h.index[1200]  # 至少需要 1200 小时数据
    reg_end = close_1h.index[-1]  # 至少需要 1200 小时数据
    reg_dates = pd.date_range(reg_start, reg_end, freq='h')

    for period in reg_dates:
        if period not in cal_df.index:
            continue

        tmp = cal_df.loc[period].copy()
        # 归一化：LLT / close
        tmp[L_fields] = tmp[L_fields].div(tmp['close'], axis=0)
        # 成交量比：/ 4h均量
        tmp[V_fields] = tmp[V_fields].div(tmp['volume_4h'], axis=0).replace([np.inf, -np.inf], np.nan)

        tmp = tmp.dropna(subset=feature_fields + ['log_ret_8h'])

        if len(tmp) < 10:  # 最少样本数
            continue

        X = tmp[feature_fields]
        X = sm.add_constant(X)
        y = winsorize_series(tmp['log_ret_8h'], 0.025)

        try:
            model = sm.OLS(y, X).fit()
            beta = model.params[feature_fields]  # 去掉常数项
            beta_8h_df.append(beta.to_frame(period))
        except:
            continue  # 回归失败则跳过

    if not beta_8h_df:
        raise ValueError("回归未生成任何 beta，检查数据完整性")

    beta_8h_df = pd.concat(beta_8h_df, axis=1).T
    beta_8h_df.index.name = 'timestamp'

    # === 6. Beta 平滑 + 滞后 8 小时 ===
    beta_roll_8h_df = beta_8h_df.shift(8).ewm(span=96).mean()

    # === 7. 计算最终因子得分 ===
    factor_list = []

    # 应用时间段
    for period in beta_roll_8h_df.index:

        tmp = cal_df.loc[period].copy()
        # 归一化（与训练一致）
        tmp[L_fields] = tmp[L_fields].div(tmp['close'], axis=0)
        tmp[V_fields] = tmp[V_fields].div(tmp['volume_4h'], axis=0).replace([np.inf, -np.inf], np.nan)

        beta = beta_roll_8h_df.loc[period]
        if beta.isna().any():
            continue

        # 计算趋势得分
        tmp[col_name] = tmp[feature_fields] @ beta

        # 标准化（Z-score，可选）
        tmp[col_name] = (tmp[col_name] - tmp[col_name].mean()) / (tmp[col_name].std() + 1e-8)

        tmp['timestamp'] = period
        factor_list.append(tmp[['timestamp', col_name]].reset_index())

    # 合并结果
    factor_df = pd.concat(factor_list, ignore_index=True)
    factor_df = factor_df.rename(columns={'index': 'symbol'})[['symbol', 'timestamp', col_name]]

    # 确保 timestamp 为 datetime
    factor_df['timestamp'] = pd.to_datetime(factor_df['timestamp'])

    return factor_df


DEFAULT_CONFIG = {'name': FACTOR_NAME, 'factor_func': compute, 'frequency': DEFAULT_FREQUENCY,
                   'fields': REQUIRED_FIELDS,
                   'type_': TYPE, 'category': CATEGORY, 'description': DESCRIPTION}
