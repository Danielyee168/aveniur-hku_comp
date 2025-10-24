"""Hourly trend factor via cross-sectional LLT/volume regression."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Sequence, Tuple

import numpy as np
import pandas as pd

DEFAULT_LOOKAHEAD_HOURS = 8
DEFAULT_BETA_EMA_SPAN = 48
FACTOR_NAME = f"trend_hour_{DEFAULT_LOOKAHEAD_HOURS}h"
DESCRIPTION = (
    "Trend factor using LLT and volume signals with "
    f"{DEFAULT_LOOKAHEAD_HOURS}-hour forward returns"
)
CATEGORY = "trend"
DEFAULT_FREQUENCY = "hour"
REQUIRED_FIELDS: Iterable[str] = ("symbol", "timestamp", "Close", "Volume")


@dataclass(frozen=True)
class _LLTSpec:
    window: int
    min_periods: int


@dataclass(frozen=True)
class _RollingSpec:
    window: int
    min_periods: int


LLT_SPECS: Sequence[_LLTSpec] = (
    _LLTSpec(4, 3),
    _LLTSpec(8, 6),
    _LLTSpec(24, 18),
    _LLTSpec(48, 36),
    _LLTSpec(72, 60),
    _LLTSpec(120, 80),
    _LLTSpec(240, 160),
    _LLTSpec(720, 480),
    _LLTSpec(1200, 720),
)

VOLUME_SPECS: Dict[int, _RollingSpec] = {
    4: _RollingSpec(4, 3),
    8: _RollingSpec(8, 6),
    24: _RollingSpec(24, 20),
    72: _RollingSpec(72, 60),
    120: _RollingSpec(120, 72),
}

VOLUME_FEATURE_WINDOWS: Tuple[int, ...] = (8, 24, 72, 120)

REG_MIN_OBS = 15
WINSOR_LIMIT = 0.025


def _compute_llt(series: pd.Series, length: int, min_periods: int) -> pd.Series:
    """Compute linear lag trend smoothing per Trend_Factor notebook."""
    arr = series.astype("float64").to_numpy()
    n = arr.size
    out = np.full(n, np.nan, dtype="float64")
    valid_idx = np.flatnonzero(~np.isnan(arr))
    if valid_idx.size < max(min_periods, 3):
        return pd.Series(out, index=series.index)

    a = 2.0 / (1.0 + length)
    first, second = valid_idx[0], valid_idx[1]
    out[first] = arr[first]
    out[second] = arr[second]

    prev2, prev1 = first, second
    for idx in valid_idx[2:]:
        p_t = arr[idx]
        p_t1 = arr[prev1]
        p_t2 = arr[prev2]
        llt_t1 = out[prev1]
        llt_t2 = out[prev2]

        out[idx] = (
            (a - 0.25 * a**2) * p_t
            + 0.5 * a**2 * p_t1
            - (a - 0.75 * a**2) * p_t2
            + 2 * (1 - a) * llt_t1
            - (1 - a) ** 2 * llt_t2
        )
        prev2, prev1 = prev1, idx

    return pd.Series(out, index=series.index)


def _winsorize(series: pd.Series, limit: float) -> pd.Series:
    if series.empty:
        return series
    lower = series.quantile(limit)
    upper = series.quantile(1 - limit)
    return series.clip(lower=lower, upper=upper)


def _prepare_panel(
    data: pd.DataFrame,
    price_col: str,
    volume_col: str,
    lookahead_hours: int,
) -> Tuple[pd.DataFrame, pd.Series]:
    """Return stacked panel with LLT / volume / return features."""
    df = data.loc[:, ["symbol", "timestamp", price_col, volume_col]].copy()
    df["timestamp_dt"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)

    close_wide = df.pivot(index="timestamp_dt", columns="symbol", values=price_col)
    volume_wide = df.pivot(index="timestamp_dt", columns="symbol", values=volume_col)

    llt_frames: Dict[str, pd.DataFrame] = {}
    for spec in LLT_SPECS:
        name = f"LLT{spec.window}"
        llt_frames[name] = close_wide.apply(
            _compute_llt, axis=0, length=spec.window, min_periods=spec.min_periods
        )

    volume_ma: Dict[int, pd.DataFrame] = {}
    for key, spec in VOLUME_SPECS.items():
        volume_ma[key] = volume_wide.rolling(
            spec.window, min_periods=spec.min_periods
        ).mean()

    future_log_ret = np.log(close_wide.shift(-lookahead_hours) / close_wide)

    base_panel = close_wide.stack(future_stack=True).rename("close").to_frame()
    index = base_panel.index

    for name, frame in llt_frames.items():
        base_panel[name] = frame.stack(future_stack=True).reindex(index)

    for key, frame in volume_ma.items():
        base_panel[f"volume_{key}h"] = frame.stack(future_stack=True).reindex(index)

    base_panel["future_log_ret"] = future_log_ret.stack(future_stack=True).reindex(index)
    base_panel["volume"] = volume_wide.stack(future_stack=True).reindex(index)

    timestamp_map = (
        df[["timestamp_dt", "timestamp"]].drop_duplicates().set_index("timestamp_dt")["timestamp"]
    )

    return base_panel, timestamp_map


def _normalize_features(panel: pd.DataFrame) -> pd.DataFrame:
    panel = panel.copy()
    close = panel["close"].replace(0, np.nan)
    for spec in LLT_SPECS:
        name = f"LLT{spec.window}"
        if name in panel:
            panel[name] = panel[name] / close

    vol_ref = panel["volume_4h"].replace(0, np.nan)
    for key in VOLUME_FEATURE_WINDOWS:
        col = f"volume_{key}h"
        if col in panel:
            panel[col] = panel[col] / vol_ref

    panel.replace([np.inf, -np.inf], np.nan, inplace=True)
    return panel


def _run_cross_sectional_regression(
    panel: pd.DataFrame,
    feature_cols: List[str],
) -> pd.DataFrame:
    betas: List[pd.Series] = []
    timestamps: List[pd.Timestamp] = []

    grouped = panel.groupby(level=0, sort=True)
    for ts, frame in grouped:
        X = frame[feature_cols]
        y = frame["future_log_ret"]
        mask = (~X.isna().any(axis=1)) & y.notna()
        if mask.sum() < REG_MIN_OBS:
            continue

        X = X.loc[mask]
        y = _winsorize(y.loc[mask], WINSOR_LIMIT)

        X_values = X.to_numpy(dtype="float64", copy=True)
        y_values = y.to_numpy(dtype="float64", copy=True)
        ones = np.ones((X_values.shape[0], 1), dtype="float64")
        design = np.hstack([ones, X_values])
        try:
            sol, *_ = np.linalg.lstsq(design, y_values, rcond=None)
        except np.linalg.LinAlgError:
            continue

        params = pd.Series(sol[1:], index=feature_cols, dtype="float64")
        betas.append(params)
        timestamps.append(ts)

    if not betas:
        return pd.DataFrame(columns=feature_cols)

    beta_df = pd.DataFrame(betas, index=pd.Index(timestamps, name="timestamp"))
    return beta_df.sort_index()


def _apply_beta_to_panel(
    panel: pd.DataFrame,
    betas: pd.DataFrame,
    feature_cols: List[str],
    col_name: str,
) -> pd.Series:
    factor_chunks: List[pd.Series] = []

    for ts, frame in panel.groupby(level=0, sort=True):
        if ts not in betas.index:
            continue
        beta_ts = betas.loc[ts]
        if beta_ts.isna().all():
            continue

        exposures = frame[feature_cols].dropna(how="any")
        if exposures.empty:
            continue

        aligned_beta = beta_ts.reindex(feature_cols).fillna(0.0)
        factor_values = exposures.mul(aligned_beta, axis=1).sum(axis=1)
        factor_chunks.append(factor_values)

    if not factor_chunks:
        return pd.Series(dtype="float64")

    return pd.concat(factor_chunks).rename(col_name)


def compute(
    data: pd.DataFrame,
    price_col: str = "Close",
    volume_col: str = "Volume",
    *,
    lookahead_hours: int = DEFAULT_LOOKAHEAD_HOURS,
    beta_span: int | None = None,
    col_name: str | None = None,
) -> pd.DataFrame:
    if data.empty:
        final_col = col_name or f"trend_hour_{lookahead_hours}h"
        return pd.DataFrame(columns=["symbol", "timestamp", final_col])

    if col_name is None:
        col_name = f"trend_hour_{lookahead_hours}h"
    if beta_span is None:
        beta_span = max(int(lookahead_hours * 12), 1)

    panel, timestamp_map = _prepare_panel(data, price_col, volume_col, lookahead_hours)
    panel = _normalize_features(panel)

    feature_cols = [f"LLT{spec.window}" for spec in LLT_SPECS] + [
        f"volume_{w}h" for w in VOLUME_FEATURE_WINDOWS
    ]

    beta_raw = _run_cross_sectional_regression(panel, feature_cols)
    if beta_raw.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", col_name])

    full_index = panel.index.get_level_values(0).unique()
    beta_aligned = beta_raw.reindex(full_index)
    beta_smoothed = (
        beta_aligned.shift(lookahead_hours)
        .ewm(span=beta_span, adjust=False, min_periods=1)
        .mean()
    )

    factor_series = _apply_beta_to_panel(panel, beta_smoothed, feature_cols, col_name)
    if factor_series.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", col_name])

    factor_df = factor_series.reset_index()
    ts_level, symbol_level = factor_series.index.names
    if ts_level is None:
        ts_level = "timestamp_dt"
    if symbol_level is None:
        symbol_level = "symbol"
    factor_df = factor_df.rename(columns={ts_level: "timestamp_dt", symbol_level: "symbol"})
    factor_df["timestamp"] = factor_df["timestamp_dt"].map(timestamp_map)
    factor_df = factor_df.dropna(subset=["timestamp"])
    factor_df = factor_df.drop(columns=["timestamp_dt"])

    result = factor_df.loc[:, ["symbol", "timestamp", col_name]].dropna()
    result = result.sort_values(["timestamp", "symbol"]).reset_index(drop=True)
    return result


def register(registry, *, lookahead_hours: int = DEFAULT_LOOKAHEAD_HOURS) -> None:
    name = f"trend_hour_{lookahead_hours}h"
    description = (
        "Trend factor using LLT and volume signals with "
        f"{lookahead_hours}-hour forward returns"
    )
    registry.register(
        name=name,
        factor_func=compute,
        frequency=DEFAULT_FREQUENCY,
        fields=list(REQUIRED_FIELDS),
        type_="alpha",
        category=CATEGORY,
        description=description,
        lookahead_hours=lookahead_hours,
    )
