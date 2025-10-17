"""Batch runner for factor calculations on hourly data."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Callable, Any

import pyarrow.parquet as pq

from src.utils.CalFactorFramework import FactorCalculator, FactorRegistry
from src.utils import factors


LOGGER = logging.getLogger("run_hourly_factors")

DATA_ROOT = Path("./data")
REGISTRY_PATH = Path("./register/factor_registry.json")


FactorSpec = Dict[str, Any]


def _load_available_columns(parquet_path: Path) -> set[str]:
    schema = pq.read_schema(parquet_path)
    return set(schema.names)


FACTOR_SPECS: tuple[FactorSpec, ...] = (
    {
        "name": "mom_cross_sectional",
        "register": factors.register_mom_cross,
        "frequency": "hour",
        "compute_kwargs": {"lookback": 720, "skip": 24},
        "required_columns": {"Close"},
        "description": "Cross-sectional momentum (M1/M2/M10)",
    },
    {
        "name": "tsmom_weighted",
        "register": factors.register_tsmom_weighted,
        "frequency": "hour",
        "compute_kwargs": {"lookback": 720, "target_vol": 0.4},
        "required_columns": {"Close"},
        "description": "Time-series momentum with volatility scaling (M3)",
    },
    {
        "name": "mom_vol_scaled",
        "register": factors.register_mom_vol_scaled,
        "frequency": "hour",
        "compute_kwargs": {"lookback": 252, "target_vol": 0.2},
        "required_columns": {"Close"},
        "description": "Volatility-managed momentum (M4)",
    },
    {
        "name": "mom_umd",
        "register": factors.register_mom_umd,
        "frequency": "hour",
        "compute_kwargs": {"lookback": 720, "skip": 24, "quantile": 0.1},
        "required_columns": {"Close"},
        "description": "Carhart UMD indicator (M7)",
    },
    {
        "name": "mom_residual",
        "register": factors.register_mom_residual,
        "frequency": "hour",
        "compute_kwargs": {"lookback": 720, "skip": 24, "residual_col": "residual"},
        "required_columns": {"residual"},
        "description": "Residual momentum (M6)",
    },
    {
        "name": "mom_mu_over_sigma2",
        "register": factors.register_mom_mu_over_sigma2,
        "frequency": "hour",
        "compute_kwargs": {"mu_col": "mu_hat", "sigma_col": "sigma_hat"},
        "required_columns": {"mu_hat", "sigma_hat"},
        "description": "Dynamic μ/σ² weights (M5)",
    },
    {
        "name": "mom_group",
        "register": factors.register_mom_group,
        "frequency": "hour",
        "compute_kwargs": {"lookback": 720, "skip": 24, "group_col": "group"},
        "required_columns": {"group", "Close"},
        "description": "Group/industry momentum (M8/M9)",
    },
    {
        "name": "rev_short_term",
        "register": factors.register_rev_short_term,
        "frequency": "hour",
        "compute_kwargs": {"lag": 5},
        "required_columns": {"Close"},
        "description": "Short-term reversal (R1/R3)",
    },
    {
        "name": "rev_residual",
        "register": factors.register_rev_residual,
        "frequency": "hour",
        "compute_kwargs": {"lag": 5, "residual_col": "residual"},
        "required_columns": {"residual"},
        "description": "Residual-based reversal (R2)",
    },
    {
        "name": "rev_long_term",
        "register": factors.register_rev_long_term,
        "frequency": "hour",
        "compute_kwargs": {"lookback": 24 * 365},
        "required_columns": {"Close"},
        "description": "Long-term reversal (R4)",
    },
)


def register_factor(registry: FactorRegistry, spec: FactorSpec) -> None:
    register_fn: Callable[[FactorRegistry], None] = spec["register"]
    name = spec["name"]

    existing = registry._factors.get(name)  # type: ignore[attr-defined]
    needs_rebind = existing and not existing.get('func_module')

    if needs_rebind:
        LOGGER.info("Rebinding function for factor '%s'", name)
        registry._factors.pop(name, None)  # type: ignore[attr-defined]

    try:
        register_fn(registry)
        LOGGER.info("Registered factor '%s'", name)
    except Exception as exc:  # FactorRegistryError when already registered
        if "already registered" in str(exc):
            LOGGER.debug("Factor '%s' already registered; skipping new registration", name)
        else:
            raise

    registry.update_factor(
        name,
        update_type=registry.UPDATE_TYPE_LOGIC,
        frequency=spec["frequency"],
        description=spec.get("description", ""),
        reason="Set frequency to hourly",
    )


def run_factor(
    registry: FactorRegistry,
    name: str,
    compute_kwargs: dict[str, Any],
    dates: list[str] | None,
    symbols: list[str] | None,
    batch_size: int,
    n_jobs: int,
) -> None:
    registry.calculate(
        name,
        is_batch=True,
        dates=dates,
        symbols=symbols,
        batch_size=batch_size,
        n_jobs=n_jobs,
        **compute_kwargs,
    )
    LOGGER.info("Finished factor '%s'", name)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    hour_parquet = DATA_ROOT / "hour_data" / "all_data.parquet"
    if not hour_parquet.exists():
        raise FileNotFoundError(f"Hourly parquet not found at {hour_parquet}")

    available_columns = _load_available_columns(hour_parquet)

    calc = FactorCalculator(str(DATA_ROOT))
    registry = FactorRegistry(calc, str(REGISTRY_PATH))

    dates = None
    symbols = None
    batch_size = 365
    n_jobs = 8

    for spec in FACTOR_SPECS:
        missing = spec["required_columns"] - available_columns
        if missing:
            LOGGER.warning(
                "Skipping factor '%s': missing columns %s",
                spec["name"], missing,
            )
            continue

        register_factor(registry, spec)

        try:
            run_factor(
                registry,
                spec["name"],
                spec["compute_kwargs"],
                dates,
                symbols,
                batch_size,
                n_jobs,
            )
        except Exception as exc:
            LOGGER.exception("Factor '%s' failed: %s", spec["name"], exc)


if __name__ == "__main__":
    main()
