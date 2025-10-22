"""Factor library for CalFactorFramework integrations."""

from .rv import compute as rv_daily_compute
from .tsmom import compute as tsmom_compute
from .tsmom_weighted import compute as tsmom_weighted_compute
from .rev_short import compute as rev_short_compute
from .dsv_daily import compute as dsv_daily_compute
from .rsk import compute as rsk_weekly_compute
from .parkinson import compute as parkinson_compute
from .garman_klass import compute as garman_klass_compute
from .rogers_satchell import compute as rogers_satchell_compute
from .corwin_schultz import compute as corwin_schultz_compute
from .vmom import compute as vmom_hourly_compute
from .resmom_hourly import compute as resmom_hourly_compute, register as register_resmom_hourly
from .lar_hourly import compute as lar_hourly_compute, register as register_lar_hourly
from .vov_shock_hourly import compute as vov_shock_hourly_compute, register as register_vov_shock_hourly
from .bab import compute as bab_hourly_compute
from .mom_cross_sectional import compute as mom_cross_compute, register as register_mom_cross
from .mom_vol_scaled import compute as mom_vol_scaled_compute, register as register_mom_vol_scaled
from .mom_mu_over_sigma2 import compute as mom_mu_over_sigma2_compute, register as register_mom_mu_over_sigma2
from .mom_residual import compute as mom_residual_compute, register as register_mom_residual
from .mom_umd import compute as mom_umd_compute, register as register_mom_umd
from .mom_group import compute as mom_group_compute, register as register_mom_group
from .reversal_short_term import compute as rev_short_term_compute, register as register_rev_short_term
from .reversal_residual import compute as rev_residual_compute, register as register_rev_residual
from .reversal_long_term import compute as rev_long_term_compute, register as register_rev_long_term

__all__ = [
    "rv_daily_compute",
    "tsmom_compute",
    "tsmom_weighted_compute",
    "rev_short_compute",
    "dsv_daily_compute",
    "rsk_weekly_compute",
    "parkinson_compute",
    "garman_klass_compute",
    "rogers_satchell_compute",
    "corwin_schultz_compute",
    "vmom_hourly_compute",
    "resmom_hourly_compute",
    "register_resmom_hourly",
    "lar_hourly_compute",
    "register_lar_hourly",
    "vov_shock_hourly_compute",
    "register_vov_shock_hourly",
    "bab_hourly_compute",
    "mom_cross_compute",
    "register_mom_cross",
    "mom_vol_scaled_compute",
    "register_mom_vol_scaled",
    "mom_mu_over_sigma2_compute",
    "register_mom_mu_over_sigma2",
    "mom_residual_compute",
    "register_mom_residual",
    "mom_umd_compute",
    "register_mom_umd",
    "mom_group_compute",
    "register_mom_group",
    "rev_short_term_compute",
    "register_rev_short_term",
    "rev_residual_compute",
    "register_rev_residual",
    "rev_long_term_compute",
    "register_rev_long_term",
    "test_factor"
]
