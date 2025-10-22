"""Run momentum/reversal factor suite on hourly data."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable, Dict, Iterable

import pyarrow.parquet as pq

from config import TRADE_LIST
from src.utils.CalFactorFramework import FactorCalculator, FactorRegistry
from src.models import factors

LOGGER = logging.getLogger("run_hourly_factors")
DATA_ROOT = Path("./data")
REGISTRY_PATH = Path("./register/factor_registry.json")

FactorSpec = Dict[str, Any]


def _load_columns(parquet_path: Path) -> set[str]:
    return set(pq.read_schema(parquet_path).names)


FACTOR_SPECS: tuple[FactorSpec, ...] = (
    {
        "name": "mom_cross_sectional",
        "register": factors.register_mom_cross,
        "frequency": "hour",
        "kwargs": {"lookback": 720, "skip": 24},
        "description": "Cross-sectional momentum",
    },
    {
        "name": "tsmom_weighted",
        "register": factors.register_tsmom_weighted,
        "frequency": "hour",
        "kwargs": {"lookback": 720, "target_vol": 0.4},
        "description": "Time-series momentum with vol targeting",
    },
    {
        "name": "mom_vol_scaled",
        "register": factors.register_mom_vol_scaled,
        "frequency": "hour",
        "kwargs": {"lookback": 252, "target_vol": 0.2},
        "description": "Inverse-volatility momentum weights",
    },
    {
        "name": "mom_mu_over_sigma2",
        "register": factors.register_mom_mu_over_sigma2,
        "frequency": "hour",
        "kwargs": {"lookback_mu": 252, "lookback_sigma": 252},
        "description": "Dynamic μ/σ² weighting",
    },
    {
        "name": "mom_residual",
        "register": factors.register_mom_residual,
        "frequency": "hour",
        "kwargs": {"lookback": 720, "skip": 24},
        "description": "Residual momentum",
    },
    {
        "name": "mom_umd",
        "register": factors.register_mom_umd,
        "frequency": "hour",
        "kwargs": {"lookback": 720, "skip": 24, "quantile": 0.1},
        "description": "UMD style indicator",
    },
    {
        "name": "mom_group",
        "register": factors.register_mom_group,
        "frequency": "hour",
        "kwargs": {"lookback": 720, "skip": 24},
        "description": "Industry/group momentum",
    },
    {
        "name": "rev_short_term",
        "register": factors.register_rev_short_term,
        "frequency": "hour",
        "kwargs": {"lag": 5},
        "description": "Short-term reversal",
    },
    {
        "name": "rev_residual",
        "register": factors.register_rev_residual,
        "frequency": "hour",
        "kwargs": {"lag": 5},
        "description": "Residual reversal",
    },
    {
        "name": "rev_long_term",
        "register": factors.register_rev_long_term,
        "frequency": "hour",
        "kwargs": {"lookback": 24 * 365},
        "description": "Long-term reversal",
    },
)


def register_factor(registry: FactorRegistry, spec: FactorSpec) -> None:
    name = spec["name"]
    register_fn: Callable[[FactorRegistry], None] = spec["register"]

    if registry.exists(name):
        LOGGER.info("Removing existing registry entry for '%s'", name)
        registry.delete_factor(name, reason="Rebuild hourly factor")

    register_fn(registry)
    LOGGER.info("Registered factor '%s'", name)

    registry.update_factor(
        name,
        update_type=registry.UPDATE_TYPE_LOGIC,
        frequency=spec["frequency"],
        description=spec.get("description", ""),
        reason="Set to hourly frequency",
    )


def run_factor(
    registry: FactorRegistry,
    name: str,
    kwargs: dict[str, Any],
    dates: Iterable[str] | None,
    symbols: list[str] | None,
    n_jobs: int,
) -> None:
    registry.calculate(
        name,
        is_batch=False,
        is_parallel=True,
        dates=list(dates) if dates else None,
        symbols=symbols,
        n_jobs=n_jobs,
        **kwargs,
    )
    LOGGER.info("Finished factor '%s'", name)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    hour_path = DATA_ROOT / "hour_data" / "all_data.parquet"
    if not hour_path.exists():
        raise FileNotFoundError(f"Hourly parquet not found: {hour_path}")

    available_cols = _load_columns(hour_path)

    calc = FactorCalculator(str(DATA_ROOT))
    registry = FactorRegistry(calc, str(REGISTRY_PATH))

    for spec in FACTOR_SPECS:
        name = spec["name"]
        missing = set(spec.get("required", [])) - available_cols
        if missing:
            LOGGER.warning("Skipping '%s' due to missing columns %s", name, missing)
            continue

        register_factor(registry, spec)

        try:
            run_factor(
                registry,
                name,
                spec["kwargs"],
                dates=None,
                symbols=TRADE_LIST,
                n_jobs=5,
            )
        except Exception as exc:
            LOGGER.exception("Factor '%s' failed: %s", name, exc)


if __name__ == "__main__":
    main()

# Example usage:
# PYTHONPATH=/home/mfin7037_best_students/multi_branch_gru/aveniur-hku_comp \
# /home/mfin7037_best_students/miniconda3/envs/nlp/bin/python scripts/run_hourly_factors.py
