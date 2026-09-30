from __future__ import annotations

BANNED_STRATEGY_TOKENS = ("martingale", "grid", "dca", "average_down", "averaging_down", "loss_recovery")
BANNED_FLAG_KEYS = {
    "martingale", "martingale_enabled", "grid", "grid_enabled", "grid_levels", "grid_step_pct",
    "dca", "dca_enabled", "average_down", "averaging_down", "loss_multiplier",
    "recovery_multiplier", "double_after_loss", "size_after_loss", "pyramid_loss",
}


def inspect_candidate(row: dict | None) -> tuple[bool, str]:
    row = row or {}
    for k in BANNED_FLAG_KEYS:
        if k in row and row.get(k) not in (None, False, 0, 0.0, "", "off", "OFF", "disabled", "DISABLED"):
            return False, f"BANNED_RISK_PATTERN:{k}"
    for k in ("strategy", "mode", "engine", "sizing_mode", "position_sizing", "name"):
        v = str(row.get(k) or "").lower().replace("-", "_").replace(" ", "_")
        if any(tok in v for tok in BANNED_STRATEGY_TOKENS):
            return False, f"BANNED_RISK_PATTERN:{k}={v[:80]}"
    return True, "OK"


def validate_research_config(cfg: dict | None) -> tuple[bool, str]:
    cfg = cfg or {}
    ok, reason = inspect_candidate(cfg)
    if not ok:
        return ok, reason
    for k, v in cfg.items():
        lk = str(k).lower()
        if any(x in lk for x in ("loss_mult", "martingale", "grid_", "dca_", "average_down", "recovery_mult")):
            return False, f"BANNED_CONFIG_FIELD:{k}"
    # Risk controls may only de-risk as stress rises.
    cut1 = cfg.get("cut1")
    cut2 = cfg.get("cut2")
    if cut1 is not None and not (0 < float(cut1) <= 1.0):
        return False, "INVALID_DERISK_CUT1"
    if cut2 is not None and not (0 < float(cut2) <= 1.0):
        return False, "INVALID_DERISK_CUT2"
    if cut1 is not None and cut2 is not None and float(cut2) > float(cut1):
        return False, "RISK_INCREASES_WITH_STRESS"
    for k in ("dd_scale", "dd_stop_scale"):
        if k in cfg and not (0 <= float(cfg[k]) <= 1.0):
            return False, f"INVALID_{k.upper()}"
    if "dd_scale" in cfg and "dd_stop_scale" in cfg and float(cfg["dd_stop_scale"]) > float(cfg["dd_scale"]):
        return False, "RISK_INCREASES_WITH_DRAWDOWN"
    return True, "OK"


def policy_status() -> dict:
    return {
        "grid_trading_allowed": False,
        "martingale_allowed": False,
        "dca_after_loss_allowed": False,
        "average_down_allowed": False,
        "loss_based_size_increase_allowed": False,
        "stress_based_risk_increase_allowed": False,
        "emergency_hedge_allowed": True,
        "amount_capacity_scan_allowed": True,
    }
