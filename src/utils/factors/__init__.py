"""Factor library for CalFactorFramework integrations."""

from .rv_daily import compute as rv_daily_compute, register as register_rv_daily
from .tsmom import compute as tsmom_compute, register as register_tsmom
from .rev_short import compute as rev_short_compute, register as register_rev_short
from .dsv_daily import compute as dsv_daily_compute, register as register_dsv_daily
from .rsk_weekly import compute as rsk_weekly_compute, register as register_rsk_weekly
from .parkinson import compute as parkinson_compute, register as register_parkinson
from .garman_klass import compute as garman_klass_compute, register as register_garman_klass
from .rogers_satchell import compute as rogers_satchell_compute, register as register_rogers_satchell
from .corwin_schultz import compute as corwin_schultz_compute, register as register_corwin_schultz
from .max_daily import compute as max_daily_compute, register as register_max_daily
from .vmom_hourly import compute as vmom_hourly_compute, register as register_vmom_hourly
from .resmom_hourly import compute as resmom_hourly_compute, register as register_resmom_hourly
from .lar_hourly import compute as lar_hourly_compute, register as register_lar_hourly
from .vov_shock_hourly import compute as vov_shock_hourly_compute, register as register_vov_shock_hourly
from .bab_hourly import compute as bab_hourly_compute, register as register_bab_hourly

__all__ = [
    "rv_daily_compute",
    "register_rv_daily",
    "tsmom_compute",
    "register_tsmom",
    "rev_short_compute",
    "register_rev_short",
    "dsv_daily_compute",
    "register_dsv_daily",
    "rsk_weekly_compute",
    "register_rsk_weekly",
    "parkinson_compute",
    "register_parkinson",
    "garman_klass_compute",
    "register_garman_klass",
    "rogers_satchell_compute",
    "register_rogers_satchell",
    "corwin_schultz_compute",
    "register_corwin_schultz",
    "max_daily_compute",
    "register_max_daily",
    "vmom_hourly_compute",
    "register_vmom_hourly",
    "resmom_hourly_compute",
    "register_resmom_hourly",
    "lar_hourly_compute",
    "register_lar_hourly",
    "vov_shock_hourly_compute",
    "register_vov_shock_hourly",
    "bab_hourly_compute",
    "register_bab_hourly",
]
