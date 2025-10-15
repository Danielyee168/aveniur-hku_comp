# --------------
# auth: NewbieWong
# version: 10/2/2025
# --------------

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Union
import pandas as pd
import numpy as np
import logging
from dataclasses import dataclass
import seaborn as sns
import matplotlib.pyplot as plt
from pandas.tseries.frequencies import to_offset
import warnings
from scipy import stats
import json
from datetime import datetime
import statsmodels.api as sm
from statsmodels.stats.diagnostic import het_breuschpagan
from statsmodels.stats.stattools import durbin_watson
from sklearn.linear_model import LinearRegression
from dataclasses import dataclass

from config import TRADE_LIST

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class BacktestContext:
    """
    Enhanced context: Supports original DataFrame input and dynamic profit calculation
    """
    # raw input
    factor_df: pd.DataFrame  # required column: symbol, timestamp, {factor_name}
    factor_name: str         # specified factor column name
    price_data: pd.DataFrame # price data: symbol, timestamp, price

    # Backtesting Configuration
    rebalance_freq: str      # Portfolio adjustment frequency，like '4H', '1D'
    offset: Optional[str] = None  # Time offset, such as '2H' means that the 4H portfolio adjustment starts at 02:00
    data_frequency: str = 'H'
    # Calculate the returns after several periods (e.g., adjust positions every 4 hours,
    # forward_periods=1 means calculating the returns for the next 4 hours)
    forward_periods: int = 1
    # Preprocessing Configuration
    winsorize: bool = True
    winsorize_limits: tuple = (0.01, 0.99)
    standardize: bool = True
    min_data_points: int = 10
    handle_missing: str = "drop"  # "drop" or "interpolate"

    # Metadata
    universe: str = "all"
    asset_id_col: str = "symbol"
    timestamp_col: str = "timestamp"
    price_col: str = "Close"

# -------------------------
# BaseBacktest
# -------------------------
class BaseBacktester(ABC):
    """
    base class of backtesters
    """
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.logger = logger

    @abstractmethod
    def run(self, context: BacktestContext) -> Dict[str, Any]:
        pass

    # —————————————————— Data Preprocessing and Alignment —————————————————— #
    def _validate_input(self, context: BacktestContext):
        """Validate input data format"""
        required_cols = [context.asset_id_col, context.timestamp_col, context.factor_name]
        if not all(col in context.factor_df.columns for col in required_cols):
            raise ValueError(f"factor_df must contain columns: {required_cols}")

        if not all(col in context.price_data.columns for col in [context.asset_id_col, context.timestamp_col, context.price_col]):
            raise ValueError(f"price_data must contain columns: {context.asset_id_col}, {context.timestamp_col}, {context.price_col}")

    def _prepare_panel(self, context: BacktestContext) -> pd.DataFrame:
        """
        Convert raw data into aligned panel data
        return: MultiIndex [timestamp, symbol], columns: factor, return
        """
        self._validate_input(context)

        factor_df = context.factor_df.copy()
        price_df = context.price_data.copy()

        asset_col = context.asset_id_col
        time_col = context.timestamp_col

        # Set Index
        factor_df = factor_df.set_index([asset_col, time_col]).sort_index()
        price_df = price_df.set_index([asset_col, time_col])[context.price_col].sort_index()

        # Merger Factor and Price
        df = factor_df[[context.factor_name]].join(price_df, how='inner')
        if df.empty:
            raise ValueError("No overlapping data between factor and price")

        # Resample to rebalancing frequency (supports offset)
        freq = context.rebalance_freq
        if context.offset:
            freq = freq + " " + context.offset  # For example, '4H 2H' → every 4 hours, starting from 2 hours

        df = df[df.index.get_level_values(0).isin(TRADE_LIST)]  # filter coins

        factor_aligned = df[context.factor_name].groupby(level=0).apply(lambda x: x.reset_index(level=0, drop=True).resample(freq).last())
        price_aligned = df[context.price_col].groupby(level=0).apply(lambda x: x.reset_index(level=0, drop=True).resample(freq).last())

        # cal forward return
        holding_periods = context.forward_periods

        returns = price_aligned.groupby(level=0).apply(lambda x: x.shift(-holding_periods) / x - 1).reset_index(level=0, drop=True)


        # Alignment Factors and Returns
        panel = pd.concat([factor_aligned, returns], axis=1, keys=['factor', 'return']).dropna()
        panel = panel.replace([np.inf, -np.inf], np.nan).dropna()

        if len(panel) < context.min_data_points:
            raise ValueError(f"Insufficient data after alignment: {len(panel)} < {context.min_data_points}")

        return panel  # MultiIndex [timestamp, symbol]

    # —————————————————— General preprocessing —————————————————— #

    def _winsorize(self, series: pd.Series, limits: tuple) -> pd.Series:
        if not self.config.get("winsorize", True):
            return series
        low, high = series.quantile(limits[0]), series.quantile(limits[1])
        return series.clip(lower=low, upper=high)

    def _standardize(self, series: pd.Series) -> pd.Series:
        if not self.config.get("standardize", True):
            return series
        mean, std = series.mean(), series.std()
        return (series - mean) / (std + 1e-8)

    def _preprocess(self, panel: pd.DataFrame, context: BacktestContext) -> pd.DataFrame:
        """Unified preprocessing pipeline"""
        factor = panel['factor']
        returns = panel['return']

        # drop the extreme values
        factor = self._winsorize(factor, context.winsorize_limits)

        # standardization
        factor = self._standardize(factor)

        # return the panel data after process
        return pd.concat([factor, returns], axis=1, keys=['factor', 'return'])

    # —————————————————— tool function —————————————————— #

    def get_metadata(self, context: BacktestContext, panel: pd.DataFrame) -> Dict[str, Any]:
        """提取元数据"""
        return {
            "factor_name": context.factor_name,
            "rebalance_freq": context.rebalance_freq,
            "offset": context.offset,
            "forward_periods": context.forward_periods,
            "universe": context.universe,
            "n_assets": panel.index.get_level_values('symbol').nunique(),
            "n_dates": panel.index.get_level_values(context.timestamp_col).nunique(),
            "config": self.config
        }


# -------------------------
# AlphaFactorBacktest
# -------------------------
# core/alpha_single.py
class SingleAlphaFactorBacktester(BaseBacktester):
    """
    Single-factor backtester: supports IC analysis, stratified backtesting, and transaction cost sensitivity
    """
    def __init__(self, n_quantiles: int, config: Dict[str, Any] = None):
        super().__init__(config)

        self.default_config = {
            "n_quantiles": n_quantiles,
            **(config or {})
        }
        # Merge into self.config
        self.config.update(self.default_config)

        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self, context: BacktestContext) -> Dict[str, Any]:
        # 1. prepare panel data (Auto Alignment + Profit Calculation)
        panel = self._prepare_panel(context)
        processed = self._preprocess(panel, context)

        factor = processed['factor']
        returns = processed['return']

        # 2. Core Analysis Module
        ic_stats = self._compute_ic_analysis(factor, returns)
        factor_direction = 'positive' if ic_stats['mean_ic'] > 0 else 'negative'
        quantile_analysis, turnover_analysis = self._compute_quantile_analysis(factor, returns, factor_direction, n_quantiles=5)

        # 3. Summary of results
        result = {
            "ic": ic_stats["mean_ic"],
            "ic_ir": ic_stats["ic_ir"],
            "rank_ic": ic_stats["mean_rank_ic"],
            "ic_hit_rate": ic_stats["hit_rate"],
            "quantile_spread_return": quantile_analysis["spread_return"],
            "quantile_tstat": quantile_analysis["tstat"],
            "turnover_sensitivity": turnover_analysis,
            "detailed_results": {
                "ic_series": ic_stats["ic_series"],
                "rank_ic_series": ic_stats["rank_ic_series"],
                "quantile_returns": quantile_analysis["group_returns"],
                "quantile_acc_returns": quantile_analysis["group_acc_returns"],
                "turnover_analysis": turnover_analysis['turnover_series']
            },
            "metadata": self.get_metadata(context, processed),
            "summary_metrics": {
                "ic": float(ic_stats["mean_ic"]),
                "ic_ir": float(ic_stats["ic_ir"]),
                "sharpe_ratio": float(ic_stats["ic_ir"]),
                "hit_rate": float(ic_stats["hit_rate"]),
                "spread_return": float(quantile_analysis["spread_return"]),
                "tstat": float(quantile_analysis["tstat"]),

            }
        }

        return result

    # —————————————————— IC analyze —————————————————— #
    def _compute_ic_analysis(self, factor: pd.Series, returns: pd.Series) -> Dict[str, Any]:
        """Calculate IC-related indicators"""
        # IC time series (grouped by time)
        df = pd.DataFrame({'factor': factor, 'return': returns})
        ic_by_date = df[['factor','return']].groupby(level=1).corr().loc[pd.IndexSlice[:, 'factor'], 'return']
        ranked = df[['factor', 'return']].groupby(level=1).rank()
        rank_ic_by_date = (ranked.groupby(level=1).corr().loc[pd.IndexSlice[:, 'factor'], 'return'])

        mean_ic = ic_by_date.mean()
        std_ic = ic_by_date.std()
        ic_ir = mean_ic / std_ic if std_ic != 0 else 0.0
        hit_rate = (ic_by_date > 0).mean()

        return {
            "ic_series": ic_by_date.reset_index(level=1, drop=True),
            "rank_ic_series": rank_ic_by_date.reset_index(level=1, drop=True),
            "mean_ic": mean_ic,
            "std_ic": std_ic,
            "ic_ir": ic_ir,
            "mean_rank_ic": rank_ic_by_date.mean(),
            "rank_ic_ir": rank_ic_by_date.mean() / rank_ic_by_date.std(),
            "hit_rate": hit_rate
        }

    # —————————————————— Layered Backtesting (Quantile Analysis) —————————————————— #
    def _compute_quantile_analysis(self, factor: pd.Series, returns: pd.Series, factor_direction: str, n_quantiles: int = 5) -> \
    tuple[dict[str, int | Any], dict[str, Any]]:
        """Grouped backtesting: Calculate the average return of each quantile"""
        df = pd.DataFrame({'factor': factor, 'return': returns})
        df['quantile'] = df.groupby(level=0)['factor'].transform(
            lambda x: pd.qcut(x, n_quantiles, labels=False, duplicates='drop'))

        # Calculate the average revenue by date and group
        group_returns = df.groupby([df.index.get_level_values(1), 'quantile'])['return'].mean()
        if factor_direction == 'positive':
            long_group = n_quantiles - 1
            short_group = 0
        else:
            long_group = 0
            short_group = n_quantiles - 1

        long_short = (group_returns[group_returns.index.get_level_values(1) == long_group].reset_index(level=1, drop=True) -
                      group_returns[group_returns.index.get_level_values(1) == short_group].reset_index(level=1, drop=True))

        long_short.name = 'long_short'
        group_returns = group_returns.unstack(level=1)
        group_returns = group_returns.merge(long_short, right_index=True, left_index=True)
        group_returns.ffill(inplace=True)

        group_accumulated_returns = (group_returns + 1).cumprod() - 1

        # Calculate long-short returns (highest group - lowest group)
        spread_return = group_returns['long_short'].tail(1).values[0]
        tstat, p_one_sided = stats.ttest_1samp(group_returns['long_short'], 0)

        #
        df['position'] = np.where((df['quantile'] == n_quantiles - 1), 1, 0)
        df['position'] = np.where((df['quantile'] == 0), -1, df['position'])

        def scale_weight(g):
            k_l = sum(g['position'] == 1)
            k_s = sum(g['position'] == -1)
            g['weight'] = g['position'].map({1: 1.0 / k_l,
                                             -1: -1.0 / k_s,
                                             0: 0.0})
            return g

        df = df.groupby(level=1).apply(scale_weight).reset_index(level=0, drop=True)
        w = df['weight'].unstack(level=0)
        turn_sr = w.diff().abs().sum(axis=1) * 0.5
        turnover = turn_sr.mean()
        return {"group_returns": group_returns,
                "group_acc_returns": group_accumulated_returns,
                "spread_return": spread_return,
                "tstat": tstat,
                "p_value": p_one_sided}, {"mean_daily_turnover": turnover,
                "mean_yearly_turnover": turnover * 365,
                "turnover_series": turn_sr}

    # —————————————————— visualization —————————————————— #
    def plot_summary(self, result: Dict[str, Any], cost_Unilateral: float = 0.003, save_path: str = None):
        """plot analytical graphs"""
        ic = result['detailed_results']['ic_series']
        rank_ic = result['detailed_results']['rank_ic_series']
        cum_ic = ic.ffill().cumsum()
        cum_ric = rank_ic.ffill().cumsum()

        group_acc = result['detailed_results']['quantile_acc_returns']
        group_returns = result['detailed_results']['quantile_returns']
        cost_d = result['detailed_results']['turnover_analysis'] * cost_Unilateral
        ls_net = (group_returns['long_short'] - cost_d)
        ls_net = (ls_net + 1).cumprod() - 1

        # 1. IC time series
        fig = plt.figure(figsize=(16, 9))
        # 1. IC time series
        ax1 = plt.subplot(3, 2, 1)
        ax1_twin = ax1.twinx()
        ax1.plot(cum_ic.index, cum_ic.values, color='C0', label='Cum IC')
        ax1.set_ylabel('Cum IC', color='C0')
        ax1_twin.step(ic.index, ic.values, where='mid', color='grey', alpha=0.8)
        ax1_twin.set_ylabel('IC', color='grey')
        ax1.set_title('IC')

        ax2 = plt.subplot(3, 2, 2)
        ax2_twin = ax2.twinx()
        ax2.plot(cum_ric.index, cum_ric.values, color='C1', label='Cum RankIC')
        ax2.set_ylabel('Cum RankIC', color='C1')
        ax2_twin.step(ic.index, ic.values, where='mid', color='grey', alpha=0.8)
        ax2_twin.set_ylabel('Rank IC', color='grey')
        ax2.set_title('Rank IC')

        # 2. Layered backtesting
        ax3 = plt.subplot(3, 2, 3)
        for col in group_acc.columns:
            if col == 'long_short':
                continue
            ax3.plot(group_acc.index, group_acc[col], label=col)
        ax3.plot(group_acc.index, group_acc['long_short'], color='black', lw=2, label='Long-Short')
        ax3.legend()
        ax3.set_title('Group Cumulative Returns')

        ax4 = plt.subplot(3, 2, 4)
        ax4.plot(group_acc['long_short'], label='Gross')
        ax4.plot(ls_net, label=f'Net {cost_Unilateral * 10000} bp')
        ax4.legend()
        ax4.set_title('Long-Short Performance')

        # 3. meta data info
        ax5 = plt.subplot(3, 2, 5)
        plt.axis('off')

        meta = result['metadata']
        text = "\n".join([
            f"Factor: {meta['factor_name']}",
            f"Rebalance Freq: {meta['rebalance_freq']} (Offset: {meta['offset']})",
            f"Universe: {meta['universe']}",
            f"IC: {result['ic']:.4f}, IR: {result['ic_ir']:.4f}, Hit Rate: {result['ic_hit_rate']:.2%}",
            f"Spread Return: {result['quantile_spread_return']:.4f}, T-stat: {result['quantile_tstat']:.4f}"
        ])
        ax5.text(0.02, 0.9, text, fontsize=12, verticalalignment='top', bbox=dict(boxstyle="round", facecolor="wheat"))

        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
        else:
            plt.show()

        return fig


# -------------------------
# RiskFactorBacktest (skeleton)
# -------------------------
class RiskFactorBacktester(BaseBacktester):
    """
    Risk Factor Evaluator: Evaluates the quality of a risk factor from multiple dimensions:
    - Variance explanation (R²)
    - Factor loading stability
    - Residual autocorrelation
    - Residual heteroskedasticity
    - Optional: volatility forecasting ability
    """

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self.default_config = {
            "min_assets_per_period": 5,        # minimum number of assets for cross-sectional regression
            "min_periods_regression": 20,      # minimum periods for time-series regression per asset
            "vol_window": 20,                  # rolling window for realized volatility
            "resid_lag": 5,                    # number of lags for autocorrelation test
            **(config or {})
        }
        self.config.update(self.default_config)
        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self, context: BacktestContext) -> Dict[str, Any]:
        # 1. Prepare aligned panel data
        panel = self._prepare_panel(context)
        processed = self._preprocess(panel, context)

        factor = processed['factor']
        returns = processed['return']

        # 2. Core Analysis Modules
        variance_explained = self._analyze_variance_explanation(factor, returns)
        loading_stability = self._analyze_loading_stability(factor, returns)
        residual_autocorr = self._analyze_residual_autocorrelation(factor, returns)
        residual_hetero = self._analyze_residual_heteroskedasticity(factor, returns)

        # 3. Summary Results
        result = {
            "variance_explanation": variance_explained,
            "loading_stability": loading_stability,
            "residual_autocorrelation": residual_autocorr,
            "residual_heteroskedasticity": residual_hetero,
            "detailed_results": {
                "ts_r2_series": variance_explained["ts_r2_by_asset"],
                "cs_r2_series": variance_explained["cs_r2_series"],
                "loadings": loading_stability["loadings_df"],
                "autocorr_stats": residual_autocorr["autocorr_stats"],
                "bp_pvalues": residual_hetero["bp_pvalues"],
            },
            "metadata": self.get_metadata(context, processed),
            "summary_metrics": {
                "avg_ts_r2": float(variance_explained["avg_ts_r2"]),
                "median_ts_r2": float(variance_explained["median_ts_r2"]),
                "avg_cs_r2": float(variance_explained["avg_cs_r2"]),
                "loading_cv": float(loading_stability["loading_cv"]),
                "dw_avg": float(residual_autocorr["dw_avg"]),
                "bp_reject_ratio": float(residual_hetero["bp_reject_ratio"]),
            }
        }

        return result

    # —————————————————— 1. Variance Explanation —————————————————— #
    def _analyze_variance_explanation(self, factor: pd.Series, returns: pd.Series) -> Dict[str, Any]:
        """Analyze how well the factor explains return variance via TS and CS R²"""
        df = pd.DataFrame({'factor': factor, 'return': returns}).dropna()

        # Time-Series R²: For each asset, run regression over time
        ts_r2_list = {}
        for symbol, group in df.groupby(level='symbol'):
            if len(group) < self.config["min_periods_regression"]:
                continue
            X = sm.add_constant(group['factor'])
            y = group['return']
            try:
                model = sm.OLS(y, X).fit()
                ts_r2_list[symbol] = model.rsquared
            except:
                continue

        avg_ts_r2 = np.mean(list(ts_r2_list.values())) if ts_r2_list else 0.0
        median_ts_r2 = np.median(list(ts_r2_list.values())) if ts_r2_list else 0.0

        # Cross-Sectional R²: For each timestamp, run regression across assets
        cs_r2_list = []
        for ts, group in df.groupby(level='timestamp'):
            if len(group) < self.config["min_assets_per_period"]:
                continue
            X = sm.add_constant(group['factor'])
            y = group['return']
            try:
                model = sm.OLS(y, X).fit()
                cs_r2_list.append((ts, model.rsquared))
            except:
                continue

        cs_r2_series = pd.Series(dict(cs_r2_list)).sort_index()
        avg_cs_r2 = cs_r2_series.mean() if len(cs_r2_series) > 0 else 0.0

        return {
            "ts_r2_by_asset": pd.Series(ts_r2_list),
            "cs_r2_series": cs_r2_series,
            "avg_ts_r2": avg_ts_r2,
            "median_ts_r2": median_ts_r2,
            "avg_cs_r2": avg_cs_r2
        }

    # —————————————————— 2. Factor Loading Stability —————————————————— #
    def _analyze_loading_stability(self, factor: pd.Series, returns: pd.Series) -> Dict[str, Any]:
        """Evaluate stability of factor loadings over time (cross-sectional view)"""
        df = pd.DataFrame({'factor': factor, 'return': returns}).dropna()

        loadings = []
        for ts, group in df.groupby(level='timestamp'):
            if len(group) < self.config["min_assets_per_period"]:
                continue
            X = group['factor'].values.reshape(-1, 1)
            y = group['return'].values
            try:
                reg = LinearRegression().fit(X, y)
                loadings.append((ts, reg.coef_[0]))
            except:
                continue

        if not loadings:
            return {"loadings_df": pd.Series(), "loading_cv": np.nan}

        loading_series = pd.Series(dict(loadings))
        loading_cv = loading_series.std() / (abs(loading_series.mean()) + 1e-8)  # Coefficient of variation

        return {
            "loadings_df": loading_series,
            "loading_cv": loading_cv
        }

    # —————————————————— 3. Residual Autocorrelation —————————————————— #
    def _analyze_residual_autocorrelation(self, factor: pd.Series, returns: pd.Series) -> Dict[str, Any]:
        """Test for autocorrelation in residuals (Durbin-Watson test)"""
        df = pd.DataFrame({'factor': factor, 'return': returns}).dropna()
        dw_stats = []

        for ts, group in df.groupby(level='timestamp'):
            if len(group) < self.config["min_assets_per_period"]:
                continue
            X = sm.add_constant(group['factor'])
            y = group['return']
            try:
                model = sm.OLS(y, X).fit()
                dw = durbin_watson(model.resid)
                dw_stats.append((ts, dw))
            except:
                continue

        if not dw_stats:
            return {"autocorr_stats": pd.Series(), "dw_avg": np.nan}

        dw_series = pd.Series(dict(dw_stats))
        dw_avg = dw_series.mean()

        return {
            "autocorr_stats": dw_series,
            "dw_avg": dw_avg
        }

    # —————————————————— 4. Residual Heteroskedasticity —————————————————— #
    def _analyze_residual_heteroskedasticity(self, factor: pd.Series, returns: pd.Series) -> Dict[str, Any]:
        """Test for heteroskedasticity using Breusch-Pagan test"""
        df = pd.DataFrame({'factor': factor, 'return': returns}).dropna()
        bp_pvalues = []

        for ts, group in df.groupby(level='timestamp'):
            if len(group) < self.config["min_assets_per_period"]:
                continue
            X = sm.add_constant(group[['factor']])
            y = group['return']
            try:
                model = sm.OLS(y, X).fit()
                _, pval, _, _ = het_breuschpagan(model.resid, model.model.exog)
                bp_pvalues.append((ts, pval))
            except:
                continue

        if not bp_pvalues:
            return {"bp_pvalues": pd.Series(), "bp_reject_ratio": np.nan}

        bp_pval_series = pd.Series(dict(bp_pvalues))
        # Proportion of rejections at 5% level (i.e., p < 0.05 → heteroskedastic)
        bp_reject_ratio = (bp_pval_series < 0.05).mean()

        return {
            "bp_pvalues": bp_pval_series,
            "bp_reject_ratio": bp_reject_ratio
        }

    # —————————————————— Visualization —————————————————— #
    def plot_summary(self, result: Dict[str, Any], save_path: Optional[str] = None):
        """Plot comprehensive risk factor diagnostics"""
        fig = plt.figure(figsize=(16, 11))

        # 1. Variance Explanation
        ax1 = plt.subplot(3, 3, 1)
        ts_r2 = result['detailed_results']['ts_r2_series']
        if not ts_r2.empty:
            sns.histplot(ts_r2, kde=True, ax=ax1, color='skyblue')
            ax1.set_title(f'TS R² Distribution (avg={ts_r2.mean():.3f})')
            ax1.set_xlabel('Time-Series R²')

        ax2 = plt.subplot(3, 3, 2)
        cs_r2 = result['detailed_results']['cs_r2_series']
        if not cs_r2.empty:
            cs_r2.plot(ax=ax2, color='coral')
            ax2.set_title(f'CS R² Over Time (avg={cs_r2.mean():.3f})')
            ax2.set_ylabel('R²')

        # 2. Factor Loadings Stability
        ax3 = plt.subplot(3, 3, 3)
        loadings = result['detailed_results']['loadings']
        if not loadings.empty:
            loadings.plot(ax=ax3, color='green', alpha=0.8)
            loadings.rolling(10).mean().plot(ax=ax3, color='darkgreen', lw=2, label='MA(10)')
            ax3.legend()
            ax3.set_title(f'Factor Loadings Over Time (CV={result["loading_stability"]["loading_cv"]:.3f})')

        # 3. Residual Autocorrelation
        ax4 = plt.subplot(3, 3, 4)
        dw_stats = result['detailed_results']['autocorr_stats']
        if not dw_stats.empty:
            dw_stats.plot(ax=ax4, color='orange')
            ax4.axhline(2.0, color='red', linestyle='--', label='No Autocorr')
            ax4.set_title(f'Durbin-Watson Statistic (avg={dw_stats.mean():.3f})')
            ax4.legend()

        # 4. Heteroskedasticity (p-values)
        ax5 = plt.subplot(3, 3, 5)
        bp_pvals = result['detailed_results']['bp_pvalues']
        if not bp_pvals.empty:
            bp_pvals.plot(ax=ax5, color='purple', alpha=0.7)
            ax5.axhline(0.05, color='red', linestyle='--', label='α=0.05')
            ax5.set_title(f'Breusch-Pagan p-values (rej. ratio={result["residual_heteroskedasticity"]["bp_reject_ratio"]:.3f})')
            ax5.legend()

        # 5. Combined Diagnostic Radar (Summary)
        ax6 = plt.subplot(3, 3, 6, projection='polar')
        categories = ['TS R²', 'CS R²', 'Loading Stability', 'No Autocorr', 'Homoskedastic']
        values = [
            result['summary_metrics']['avg_ts_r2'],
            result['summary_metrics']['avg_cs_r2'],
            1 / (1 + result['summary_metrics']['loading_cv']),  # inverse of CV
            max(0, 1 - abs(result['summary_metrics']['dw_avg'] - 2) / 2),  # proximity to 2
            1 - result['summary_metrics']['bp_reject_ratio']  # lower rejection = better
        ]
        values += values[:1]  # close the circle
        angles = [n / float(len(categories)) * 2 * np.pi for n in range(len(categories))]
        angles += angles[:1]

        ax6.fill(angles, values, color='teal', alpha=0.25)
        ax6.plot(angles, values, color='teal', marker='o')
        ax6.set_xticks(angles[:-1])
        ax6.set_xticklabels(categories)
        ax6.set_ylim(0, 1)
        ax6.set_title("Risk Factor Quality Radar", size=12, pad=20)

        # 6. Metadata & Summary
        ax7 = plt.subplot(3, 1, 3)
        plt.axis('off')
        meta = result['metadata']
        summary = result['summary_metrics']
        text = "\n".join([
            f" Risk Factor Evaluation Report",
            f"Factor: {meta['factor_name']}",
            f"Rebalance Freq: {meta['rebalance_freq']} (Offset: {meta['offset']})",
            f"Universe: {meta['universe']}",
            "",
            f" Variance Explanation",
            f"  • Avg TS R²: {summary['avg_ts_r2']:.4f}",
            f"  • Avg CS R²: {summary['avg_cs_r2']:.4f}",
            "",
            f"  Factor Loading",
            f"  • Stability (CV): {summary['loading_cv']:.4f}",
            "",
            f" Residual Diagnostics",
            f"  • Avg DW Stat: {summary['dw_avg']:.4f}",
            f"  • Hetero Reject Ratio: {summary['bp_reject_ratio']:.4f}"
        ])
        ax7.text(0.02, 0.95, text, fontsize=11, verticalalignment='top',
                 family='monospace', bbox=dict(boxstyle="round", facecolor="wheat"))

        plt.tight_layout()
        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
            plt.close(fig)
            self.logger.info(f"Risk factor analysis plot saved to {save_path}")
        else:
            plt.show()

        return fig


# -------------------------
# Optional: High-level Runner
# -------------------------
