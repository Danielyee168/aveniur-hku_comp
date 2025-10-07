# Factor Reference

## rv_daily
Daily realised variance. Uses minute log returns squared and summed per symbol/trade_date. Columns: `Close`, `timestamp`, `symbol`.

## tsmom
L-period time-series momentum. Computes `pct_change(lookback)` on `Close`. Columns: `Close`, `timestamp`, `symbol`.

## rev_short
Short-horizon reversal. Negative rolling sum of 1-minute returns over `window`. Columns: `Close`, `timestamp`, `symbol`.

## dsv_daily
Daily downside semivariance. Sum of squared negative minute log returns per day. Columns: `Close`, `timestamp`, `symbol`.

## rsk_weekly
Weekly realised skewness. Minute log returns aggregated by ISO week to produce skewness (`E[(r-mean)^3] / var^(3/2)`). Columns: `Close`, `timestamp`, `symbol`.

## parkinson
Parkinson high-low volatility estimator. Uses daily `log(H/L)^2 / (4 ln 2)` from intraday extremes. Columns: `High`, `Low`.

## garman_klass
Garman–Klass volatility estimator. Combines daily high, low, open, close: `0.5 ln(H/L)^2 - (2 ln 2 - 1) ln(C/O)^2`. Columns: `Open`, `High`, `Low`, `Close`.

## rogers_satchell
Rogers–Satchell volatility estimator with drift: `ln(H/C) ln(H/O) + ln(L/C) ln(L/O)`. Columns: `Open`, `High`, `Low`, `Close`.

## corwin_schultz
Corwin–Schultz two-day spread estimator using consecutive daily high/low pairs. Columns: `High`, `Low`. Fills first-day value with zero.

## max_daily
Rolling maximum daily return (default 21 days). Uses daily close-to-close returns and broadcasts to minute bars. Columns: `Close`.

## Running Factors via CalFactorFramework
Use the registry to load data and emit each factor:

```bash
/home/mfin7037_best_students/miniconda3/envs/nlp/bin/python - <<'PY'
from src.utils.CalFactorFramework import FactorCalculator, FactorRegistry
from src.utils.factors import (
    register_rv_daily,
    register_tsmom,
    register_rev_short,
    register_dsv_daily,
    register_rsk_weekly,
    register_parkinson,
    register_garman_klass,
    register_rogers_satchell,
    register_corwin_schultz,
    register_max_daily,
)

calc = FactorCalculator("./data")
registry = FactorRegistry(calc, "./register/factor_registry.json")

for register in (
    register_rv_daily,
    register_tsmom,
    register_rev_short,
    register_dsv_daily,
    register_rsk_weekly,
    register_parkinson,
    register_garman_klass,
    register_rogers_satchell,
    register_corwin_schultz,
    register_max_daily,
):
    register(registry)

for name in (
    "rv_daily",
    "tsmom",
    "rev_short",
    "dsv_daily",
    "rsk_weekly",
    "parkinson",
    "garman_klass",
    "rogers_satchell",
    "corwin_schultz",
    "max_daily",
):
    df = registry.calculate(
        name,
        is_batch=True,
        dates=["20230101"],
        symbols=["BTCUSDT"],
        batch_size=1,
        n_jobs=1
    )
    print(name, df.shape)
PY
```

Each call returns a DataFrame aligned with the minute input and writes `factor_{name}.parquet` under `data/min_data/alpha/` (or `risk/`).
