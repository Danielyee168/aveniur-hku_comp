# --------------
# auth: NewbieWong
# version: 10/2/2025
# --------------

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Tuple, Union
from pathlib import Path
import warnings
from scipy import stats
import json
from datetime import datetime

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')


DEFAULT_RESULTS_FILE = "./results/backtest_results.json"

# -------------------------
# BaseBacktest
# -------------------------
class BaseBacktester(ABC):
    """
    Enhanced base class supporting flexible prediction horizon and offset alignment.
    """

    def __init__(self,
                 price_data: pd.DataFrame,
                 factor_data: pd.DataFrame,
                 frequency: str = 'D',
                 offset: Optional[pd.Timedelta] = None,
                 prediction_horizon: Optional[pd.Timedelta] = None,
                 benchmark: Optional[pd.Series] = None,
                 risk_free_rate: float = 0.0):
        """
        Args:
            prediction_horizon: How far into the future to predict (e.g., 4H, 6H). Default = frequency.
        """
        self.price_data = self._align_data(price_data)
        self.factor_data = self._align_data(factor_data)
        self.frequency = frequency
        self.offset = offset or pd.Timedelta(seconds=0)
        self.prediction_horizon = prediction_horizon or pd.Timedelta(self.frequency)
        self.benchmark = benchmark
        self.risk_free_rate = risk_free_rate

        # Align timestamps
        self.factor_timestamps = self._get_aligned_timestamps()
        self.returns = self._compute_forward_returns()

    def _get_aligned_timestamps(self) -> pd.DatetimeIndex:
        """Get factor timestamps after offset."""
        ts = self.factor_data.index.get_level_values('timestamp').unique()
        return (ts + self.offset)

    def _compute_forward_returns(self) -> pd.Series:
        """Compute forward returns over prediction_horizon."""
        # Pivot prices
        price_wide = self.price_data.unstack('symbol')
        if isinstance(price_wide, pd.Series):
            price_wide = price_wide.to_frame('price')
        if 'value' in price_wide.columns:
            price_wide = price_wide['value'].unstack('symbol')

        # Resample to desired frequency if needed
        price_wide = price_wide.resample(self.frequency).last()

        # Forward-looking horizon
        future_time = self.prediction_horizon
        future_prices = price_wide.shift(-1).resample(self.frequency).last()  # Assume aligned
        current_prices = price_wide

        # Forward return: (P[t+h] - P[t]) / P[t]
        forward_rets = (future_prices / current_prices - 1).stack()
        forward_rets.index.names = ['timestamp', 'symbol']
        forward_rets = forward_rets.swaplevel()

        # Only keep timestamps that exist in factor_data (after offset)
        factor_ts = self.factor_timestamps
        return forward_rets[forward_rets.index.get_level_values('timestamp').isin(factor_ts)]

    def _align_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure multi-index (symbol, timestamp)."""
        if isinstance(df.index, pd.MultiIndex):
            return df
        elif 'symbol' in df.columns and 'timestamp' in df.columns:
            return df.set_index(['symbol', 'timestamp'])
        else:
            # Wide format: index=timestamp, columns=symbols
            s = df.stack()
            s.index.names = ['timestamp', 'symbol']
            return s.to_frame('value').swaplevel().sort_index()

    def _validate_data(self):
        assert len(self.factor_data) > 0, "Empty factor data."
        assert len(self.returns) > 0, "Empty or misaligned returns."
        assert self.factor_data.index.is_monotonic_increasing, "Factor data not sorted."

    @abstractmethod
    def run(self, **kwargs):
        pass

    def _annualize(self, ratio: float, base_period: str = 'D') -> float:
        freq_map = {'D': 252, 'H': 252 * 24, '15T': 252 * 24 * 4}
        periods_per_year = freq_map.get(base_period, 252)
        return ratio * np.sqrt(periods_per_year)

    def _max_drawdown(self, cum_rets: pd.Series) -> float:
        rolling_max = cum_rets.expanding().max()
        drawdown = (cum_rets - rolling_max) / rolling_max
        return drawdown.min()

    def _sharpe_ratio(self, returns: pd.Series) -> float:
        excess = returns - self.risk_free_rate / 252  # Assume annualized rate
        sharpe = excess.mean() / excess.std()
        return self._annualize(sharpe, self.frequency[0])

    def _stats_report(self, returns: pd.Series, name: str = "Strategy") -> Dict[str, float]:
        cum_ret = (1 + returns).prod() - 1
        ann_ret = (1 + cum_ret) ** (252 / len(returns)) - 1 if len(returns) > 0 else 0
        ann_vol = returns.std() * np.sqrt(252)
        sharpe = self._sharpe_ratio(returns)
        mdd = self._max_drawdown((1 + returns).cumprod())
        calmar = ann_ret / abs(mdd) if mdd != 0 else np.inf

        return {
            'Cumulative Return': cum_ret,
            'Annualized Return': ann_ret,
            'Annualized Volatility': ann_vol,
            'Sharpe Ratio': sharpe,
            'Max Drawdown': mdd,
            'Calmar Ratio': calmar,
            'Win Rate': (returns > 0).mean() if len(returns) > 0 else 0.0
        }


# -------------------------
# AlphaFactorBacktest
# -------------------------
class AlphaBacktester(BaseBacktester):
    def __init__(self, *args, quantiles: int = 5, long_short: bool = True, **kwargs):
        super().__init__(*args, **kwargs)
        self.quantiles = quantiles
        self.long_short = long_short
        self.results_ = {}

    def _quantile_partition(self, date_group: pd.DataFrame) -> pd.Series:
        vals = date_group['value'].dropna()
        if len(vals) < self.quantiles:
            return pd.Series(dtype=int, index=vals.index)
        q_labels = pd.qcut(vals, q=self.quantiles, labels=False, duplicates='drop')
        return q_labels.reindex(vals.index)

    def _compute_ic(self) -> pd.DataFrame:
        merged = pd.merge(
            self.factor_data,
            self.returns,
            left_index=True,
            right_index=True,
            how='inner'
        )
        ic = merged.groupby('timestamp').apply(
            lambda x: stats.spearmanr(x['value'], x['return'])[0]
        )
        return ic.to_frame('IC')

    def _ic_analysis(self) -> Dict[str, float]:
        ic = self._compute_ic()['IC'].dropna()
        mean_ic = ic.mean()
        std_ic = ic.std()
        ir_ic = mean_ic / std_ic if std_ic != 0 else 0.0
        hit_rate = (ic > 0).mean()
        turnover = self._turnover()
        return {
            'Mean IC': mean_ic,
            'Std IC': std_ic,
            'IC IR': ir_ic,
            'IC Hit Rate': hit_rate,
            'Turnover': turnover
        }

    def _turnover(self) -> float:
        wide = self.factor_data.unstack('symbol').droplevel(0, axis=1)
        ranks = wide.rank(axis=1)
        return (ranks.diff().abs().mean(axis=1)).mean()

    def run(self, benchmark_returns: Optional[pd.Series] = None) -> Dict[str, Any]:
        signals = self.factor_data.copy()
        signals.index = signals.index.set_levels(signals.index.levels[1] + self.offset, level='timestamp')

        # Align with returns
        signals = signals[signals.index.get_level_values('timestamp').isin(self.returns.index.get_level_values('timestamp'))]

        signals['quantile'] = signals.groupby('timestamp').apply(self._quantile_partition).droplevel(0)

        test_data = pd.merge(signals, self.returns, left_index=True, right_index=True, how='left').dropna()
        group_rets = test_data.groupby(['timestamp', 'quantile'])['return'].mean().unstack()

        if self.long_short:
            ls_ret = group_rets.iloc[:, -1] - group_rets.iloc[:, 0]
        else:
            ls_ret = group_rets.mean(axis=1)

        perf = self._stats_report(ls_ret)
        ic_stats = self._ic_analysis()

        self.results_ = {
            'performance': perf,
            'ic_analysis': ic_stats,
            'group_returns': group_rets,
            'long_short_returns': ls_ret,
            'cumulative_returns': (1 + ls_ret).cumprod(),
            'benchmark_returns': benchmark_returns,
            'prediction_horizon': str(self.prediction_horizon)
        }
        return self.results_

    def plot(self, save_path: Optional[str] = None):
        plt.figure(figsize=(14, 10))
        cum_ls = self.results_['cumulative_returns']
        plt.subplot(2, 2, 1)
        plt.plot(cum_ls, label='Long-Short')
        if self.results_['benchmark_returns'] is not None:
            bench_cum = (1 + self.results_['benchmark_returns']).cumprod()
            plt.plot(bench_cum, label='Benchmark', ls='--')
        plt.title(f'Cumulative Returns ({self.results_["prediction_horizon"]} Horizon)')
        plt.legend(); plt.grid(True)

        plt.subplot(2, 2, 2)
        mean_quartile = self.results_['group_returns'].mean()
        plt.bar(range(len(mean_quartile)), mean_quartile, color='skyblue', edgecolor='black')
        plt.title('Avg Return by Quantile')
        plt.xlabel('Quantile')

        plt.subplot(2, 2, 3)
        ic = self._compute_ic()
        plt.plot(ic.index, ic['IC'], label='IC')
        plt.axhline(ic['IC'].mean(), color='r', ls='--')
        plt.title('Information Coefficient')
        plt.legend(); plt.grid(True)

        plt.subplot(2, 2, 4)
        turnover = self._turnover()
        plt.bar(['Turnover'], [turnover], color='orange')
        plt.title('Factor Turnover')

        plt.tight_layout()
        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
        plt.show()


# -------------------------
# RiskFactorBacktest (skeleton)
# -------------------------
class RiskBacktester(BaseBacktester):
    def __init__(self, *args, lookback: int = 21, stability_window: int = 63, **kwargs):
        super().__init__(*args, **kwargs)
        self.lookback = lookback
        self.stability_window = stability_window  # 用于滚动稳定性
        self.results_ = {}

    def _factor_autocorr(self, lag: int = 1) -> float:
        """因子自相关性（时间序列稳定性）"""
        wide = self.factor_data.unstack('symbol').droplevel(0, axis=1)
        mean_factor = wide.mean(axis=1)
        return mean_factor.autocorr(lag=lag)

    def _factor_volatility(self) -> float:
        """因子值变化率（标准差）"""
        wide = self.factor_data.unstack('symbol').droplevel(0, axis=1)
        return wide.std(axis=1).mean()

    def _rolling_stability(self, window: int = 21) -> pd.Series:
        """滚动相关性稳定性：相邻窗口因子值的相关性"""
        wide = self.factor_data.unstack('symbol').droplevel(0, axis=1)
        roll_corr = wide.rolling(window).corr(wide.shift(1))
        return roll_corr.mean(axis=1).rolling(5).mean()  # 平滑

    def _predict_volatility(self) -> pd.Series:
        factor_ret = self.factor_data.groupby('timestamp')['value'].mean()
        return factor_ret.rolling(self.lookback).std().shift(1)

    def _realized_volatility(self) -> pd.Series:
        asset_rets = self.returns.groupby('timestamp').mean()
        return asset_rets.rolling(self.lookback).std().shift(-self.lookback)

    def _var_backtest(self, confidence: float = 0.95) -> Dict[str, Any]:
        pred_vol = self._predict_volatility()
        if pred_vol.isna().all():
            return {'exceptions': 0, 'hit_rate': 0.0}
        z_score = stats.norm.ppf(1 - confidence)
        var = -z_score * pred_vol
        actual_ret = self.returns.groupby('timestamp').mean()
        exceptions = (actual_ret < -var).sum()
        total = len(actual_ret.dropna())
        hit_rate = exceptions / total if total > 0 else 0.0
        return {'exceptions': exceptions, 'total': total, 'hit_rate': hit_rate}

    def _r_squared(self) -> float:
        merged = pd.merge(self.factor_data, self.returns, left_index=True, right_index=True, how='inner')
        if len(merged) == 0:
            return 0.0
        corr = merged['value'].corr(merged['return'])
        return corr ** 2

    def run(self, confidence: float = 0.95) -> Dict[str, Any]:
        # 暴露稳定性
        autocorr = self._factor_autocorr(lag=1)
        volatility = self._factor_volatility()
        rolling_stab = self._rolling_stability(window=21)

        # 风险预测
        pred_vol = self._predict_volatility()
        real_vol = self._realized_volatility()
        common = pd.concat([pred_vol, real_vol], axis=1).dropna()
        common.columns = ['Predicted', 'Realized']
        mse = ((common['Predicted'] - common['Realized']) ** 2).mean()
        corr_vol = common['Predicted'].corr(common['Realized'])

        var_test = self._var_backtest(confidence)
        r2 = self._r_squared()

        self.results_ = {
            'exposure_stability': {
                'Autocorrelation (lag=1)': autocorr,
                'Volatility of Exposure': volatility,
                'Rolling Stability (avg)': rolling_stab.mean(),
                'Rolling Stability Series': rolling_stab
            },
            'vol_forecast': {
                'MSE': mse,
                'Correlation': corr_vol,
                'Predicted_Vol': pred_vol,
                'Realized_Vol': real_vol
            },
            'var_backtest': var_test,
            'r_squared': r2,
            'confidence': confidence
        }
        return self.results_

    def plot(self, save_path: Optional[str] = None):
        fig, axes = plt.subplots(3, 2, figsize=(14, 14))

        # 1. 暴露自相关
        stab = self.results_['exposure_stability']
        axes[0,0].bar(['Autocorr'], [stab['Autocorrelation (lag=1)']], color='purple')
        axes[0,0].set_title('Factor Autocorrelation')

        # 2. 暴露波动率
        axes[0,1].bar(['Volatility'], [stab['Volatility of Exposure']], color='brown')
        axes[0,1].set_title('Exposure Volatility')

        # 3. 滚动稳定性
        stab['Rolling Stability Series'].plot(ax=axes[1,0], title='Rolling Stability (Corr)')
        axes[1,0].grid(True)

        # 4. 预测 vs 实现波动率
        vol_df = pd.concat([stab['Predicted_Vol'], stab['Realized_Vol']], axis=1)
        vol_df.plot(ax=axes[1,1], title='Volatility Forecast')
        axes[1,1].grid(True)

        # 5. R²
        axes[2,0].bar(['R²'], [self.results_['r_squared']], color='green')
        axes[2,0].set_ylim(0,1)
        axes[2,0].set_title('Explained Variance (R²)')

        # 6. VaR 回测
        hit_rate = self.results_['var_backtest']['hit_rate']
        expected = 1 - self.results_['confidence']
        axes[2,1].bar(['Actual', 'Expected'], [hit_rate, expected], color=['orange', 'gray'])
        axes[2,1].set_title('VaR Backtest')

        plt.tight_layout()
        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
        plt.show()

# -------------------------
# Optional: High-level Runner
# -------------------------
def run_factor_evaluation(
        price_data: pd.DataFrame,
        factor_data: pd.DataFrame,
        factor_type: str,
        frequency: str = 'D',
        offset: Optional[pd.Timedelta] = None,
        benchmark_returns: Optional[pd.Series] = None,
        risk_free_rate: float = 0.0,
        output_dir: str = './results'
):
    """
    Unified entry point for factor evaluation.
    """
    Path(output_dir).mkdir(exist_ok=True)

    if factor_type.lower() == 'alpha':
        tester = AlphaBacktester(
            price_data=price_data,
            factor_data=factor_data,
            frequency=frequency,
            offset=offset,
            benchmark=benchmark_returns,
            risk_free_rate=risk_free_rate
        )
        results = tester.run(benchmark_returns=benchmark_returns)
        tester.plot(save_path=f"{output_dir}/alpha_report.png")

    elif factor_type.lower() == 'risk':
        tester = RiskBacktester(
            price_data=price_data,
            factor_data=factor_data,
            frequency=frequency,
            offset=offset,
            risk_free_rate=risk_free_rate
        )
        results = tester.run()
        tester.plot(save_path=f"{output_dir}/risk_report.png")
    else:
        raise ValueError("factor_type must be 'alpha' or 'risk'")

    # Save results
    with open(f"{output_dir}/results.json", 'w') as f:
        json.dump({k: {str(kk): float(vv) for kk, vv in v.items()}
        if isinstance(v, dict) else float(v) for k, v in results.items()}, f, indent=2)

    return results


# -------------------------
# Example usage (runnable)
# -------------------------




