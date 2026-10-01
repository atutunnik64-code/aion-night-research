from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
DATA = ROOT / "data"
RELAY = DATA / "bybit_local_relay_v1.json"
RUNTIME = Path(os.getenv("AION_BYBIT_RUNTIME", str(ROOT.parent / "aion-bybit-runtime")))
RUNTIME.mkdir(parents=True, exist_ok=True)
FUNDING_REFRESH_SEC = 180.0
SHADOW_REFRESH_SEC = 60.0

# Keep local collector state outside the git checkout so pulling/pushing the
# compact relay file never conflicts with cloud-generated state.
import app.services.crossvenue_spot_arb_cloud_v1 as arbmod
import app.services.crossvenue_liquidation_asymmetry_collector as liqmod
import app.services.funding_oi_bybit_shadow_v1 as foimod
import app.services.bybit_price_shock_shadow_v1 as shockmod
import app.services.bybit_liquidation_regime_shadow_v1 as lregmod
import app.services.bybit_volatility_compression_shadow_v1 as volmod
from app.services.perp_funding_spread import PerpFundingSpreadScanner

arbmod.STATE = RUNTIME / "crossvenue_spot_arb_local_v1.json"
liqmod.BY_RAW = RUNTIME / "crossvenue_bybit_liquidations_local_v1.jsonl"
liqmod.BY_CTX = RUNTIME / "crossvenue_bybit_liq_context_local_v1.jsonl"
liqmod.STATE = RUNTIME / "crossvenue_liquidation_asymmetry_collector_local_v1.json"
foimod.SNAPS = liqmod.BY_CTX
foimod.STATE = RUNTIME / "funding_oi_bybit_shadow_local_v1.json"
shockmod.SNAPS = liqmod.BY_CTX
shockmod.STATE = RUNTIME / "bybit_price_shock_shadow_local_v1.json"
lregmod.RAW = liqmod.BY_RAW
lregmod.CTX = liqmod.BY_CTX
lregmod.STATE = RUNTIME / "bybit_liquidation_regime_shadow_local_v1.json"
volmod.SNAPS = liqmod.BY_CTX
volmod.STATE = RUNTIME / "bybit_volatility_compression_shadow_local_v1.json"

arb = arbmod.CrossVenueSpotArbCloudV1()
funding = PerpFundingSpreadScanner()
liq = liqmod.CrossVenueLiquidationAsymmetryCollector()
funding_oi = foimod.FundingOiBybitShadowV1()
price_shock = shockmod.BybitPriceShockShadowV1()
liq_regime = lregmod.BybitLiquidationRegimeShadowV1()
vol_compression = volmod.BybitVolatilityCompressionShadowV1()


def _atomic_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _bybit_events(limit: int = 100) -> list[dict]:
    rows = []
    for x in arb.state.get("events") or []:
        if x.get("buy_venue") == "Bybit" or x.get("sell_venue") == "Bybit":
            rows.append(x)
    return rows[-limit:]


def _bybit_signals(limit: int = 100) -> list[dict]:
    out = []
    for sig, row in (arb.state.get("signals") or {}).items():
        if ":Bybit>" in sig or ">Bybit" in sig:
            out.append({"signature": sig, **(row or {})})
    out.sort(key=lambda x: (float(x.get("last_seen") or 0), int(x.get("hits") or 0)), reverse=True)
    return out[:limit]


def _bybit_funding_rows(limit: int = 100) -> list[dict]:
    rows = [
        x for x in (funding.rows or [])
        if x.get("long_venue") == "Bybit" or x.get("short_venue") == "Bybit"
    ]
    rows.sort(
        key=lambda x: (
            x.get("status") == "PAPER_CANDIDATE",
            float(x.get("net_24h_conservative_pct") or -999.0),
        ),
        reverse=True,
    )
    return rows[:limit]


def _relay_payload() -> dict:
    counts = arb.state.get("venue_symbol_counts") or {}
    errs = arb.state.get("venue_errors") or {}
    market = arb.market or {}
    bybit_guarded = sum(1 for vm in market.values() if "Bybit" in vm)
    events = _bybit_events()
    signals = _bybit_signals()
    funding_rows = _bybit_funding_rows()
    funding_candidates = [x for x in funding_rows if x.get("status") == "PAPER_CANDIDATE"]
    liq_status = liq.status()
    foi_status = funding_oi.status()
    shock_status = price_shock.status()
    lreg_status = liq_regime.status()
    vol_status = vol_compression.status()
    return {
        "version": "BYBIT_LOCAL_RELAY_V3_FULL_PUBLIC_RESEARCH",
        "generated_at": time.time(),
        "paper_only": True,
        "live_enabled": False,
        "source": "LOCAL_WINDOWS_DIRECT_BYBIT_PUBLIC_API_AND_WS",
        "source_health": {
            "bybit_spot_symbol_count": int(counts.get("Bybit") or 0),
            "bybit_guarded_common_assets": bybit_guarded,
            "bybit_spot_error": errs.get("Bybit"),
            "all_spot_venue_symbol_counts": counts,
            "bybit_perp_symbol_count": int((funding.venue_asset_counts or {}).get("Bybit") or 0),
            "all_perp_venue_symbol_counts": funding.venue_asset_counts or {},
            "funding_last_error": funding.last_error,
            "bybit_ws_connected": bool(liq_status.get("bybit_connected")),
            "bybit_context_count": int(liq_status.get("context_count") or 0),
            "bybit_liquidation_events": int(liq_status.get("bybit_events") or 0),
            "bybit_context_last_error": liq_status.get("last_error"),
        },
        "spot_arb": {
            "scan_count": int(arb.state.get("scan_count") or 0),
            "last_raw_positive": int(arb.state.get("last_raw_positive") or 0),
            "last_after_fees_positive": int(arb.state.get("last_after_fees_positive") or 0),
            "last_depth_checks": int(arb.state.get("last_depth_checks") or 0),
            "last_depth_pass": int(arb.state.get("last_depth_pass") or 0),
            "execution_edge_quote_sum_all_venues": float(arb.state.get("execution_edge_quote") or 0),
            "recent_bybit_event_count": len(events),
            "recent_bybit_events": events,
            "bybit_signal_count": len(signals),
            "top_bybit_signals": signals,
        },
        "perp_funding": {
            "last_refresh": funding.last_refresh,
            "universe_bases": int(funding.universe_bases or 0),
            "multi_venue_bases": int(funding.multi_venue_bases or 0),
            "pairs_evaluated": int(funding.pairs_evaluated or 0),
            "bybit_route_count": len(funding_rows),
            "bybit_paper_candidate_count": len(funding_candidates),
            "top_bybit_routes": funding_rows,
        },
        "bybit_liquidation_context": {
            "connected": bool(liq_status.get("bybit_connected")),
            "context_count": int(liq_status.get("context_count") or 0),
            "liquidation_events": int(liq_status.get("bybit_events") or 0),
            "liquidation_notional_usdt": float(liq_status.get("bybit_notional_usdt") or 0),
            "last_event_at": liq_status.get("last_event_at"),
            "last_context_at": liq_status.get("last_context_at"),
            "last_error": liq_status.get("last_error"),
        },
        "funding_oi_shadow": {
            "pending_count": int(foi_status.get("pending_count") or 0),
            "resolved_count": int(foi_status.get("resolved_count") or 0),
            "last_new_events": int(foi_status.get("last_new_events") or 0),
            "follow": foi_status.get("follow") or {},
            "fade": foi_status.get("fade") or {},
            "future_gate": foi_status.get("future_gate") or {},
            "last_error": foi_status.get("last_error"),
        },
        "price_shock_shadow": {
            "pending_count": int(shock_status.get("pending_count") or 0),
            "resolved_count": int(shock_status.get("resolved_count") or 0),
            "last_new_events": int(shock_status.get("last_new_events") or 0),
            "continuation": shock_status.get("continuation") or {},
            "reversal": shock_status.get("reversal") or {},
            "future_gate": shock_status.get("future_gate") or {},
            "last_error": shock_status.get("last_error"),
        },
        "liquidation_regime_shadow": {
            "pending_count": int(lreg_status.get("pending_count") or 0),
            "resolved_count": int(lreg_status.get("resolved_count") or 0),
            "continuation": lreg_status.get("continuation") or {},
            "recovery": lreg_status.get("recovery") or {},
            "future_gate": lreg_status.get("future_gate") or {},
            "last_error": lreg_status.get("last_error"),
        },
        "volatility_compression_shadow": {
            "pending_count": int(vol_status.get("pending_count") or 0),
            "resolved_count": int(vol_status.get("resolved_count") or 0),
            "last_new_events": int(vol_status.get("last_new_events") or 0),
            "breakout": vol_status.get("breakout") or {},
            "fakeout": vol_status.get("fakeout") or {},
            "future_gate": vol_status.get("future_gate") or {},
            "last_error": vol_status.get("last_error"),
        },
        "policy": {
            "no_grid": True,
            "no_martingale": True,
            "no_dca": True,
            "no_live_orders": True,
            "compact_relay_only": True,
        },
    }


def _git(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=ROOT, text=True, capture_output=True, check=check)


def sync_relay() -> tuple[bool, str]:
    try:
        _git("fetch", "origin", "main")
        _git("pull", "--rebase", "--autostash", "origin", "main")
        _git("add", RELAY.relative_to(ROOT).as_posix())
        diff = _git("diff", "--cached", "--quiet", check=False)
        if diff.returncode == 0:
            return True, "NO_CHANGE"
        _git("-c", "user.name=aion-bybit-local", "-c", "user.email=aion-bybit-local@users.noreply.github.com", "commit", "-m", "research: Bybit local relay snapshot")
        _git("push", "origin", "HEAD:main")
        return True, "PUSHED"
    except Exception as exc:
        return False, f"{type(exc).__name__}:{exc}"


async def run(interval: float, sync_every: float, do_sync: bool, once: bool) -> None:
    last_sync = 0.0
    last_funding = 0.0
    last_shadow = 0.0
    await liq.start()
    if once:
        await asyncio.sleep(3.0)
    try:
        while True:
            await arb.refresh()
            now = time.time()
            if last_funding == 0.0 or now - last_funding >= FUNDING_REFRESH_SEC:
                await funding.refresh()
                last_funding = time.time()
            if last_shadow == 0.0 or now - last_shadow >= SHADOW_REFRESH_SEC:
                await funding_oi.refresh()
                await price_shock.refresh()
                await liq_regime.refresh()
                await vol_compression.refresh()
                last_shadow = time.time()
            payload = _relay_payload()
            _atomic_json(RELAY, payload)
            print(
                f"BYBIT_RELAY scan={payload['spot_arb']['scan_count']} "
                f"spot_symbols={payload['source_health']['bybit_spot_symbol_count']} "
                f"guarded={payload['source_health']['bybit_guarded_common_assets']} "
                f"spot_events={payload['spot_arb']['recent_bybit_event_count']} "
                f"perp_symbols={payload['source_health']['bybit_perp_symbol_count']} "
                f"funding_candidates={payload['perp_funding']['bybit_paper_candidate_count']} "
                f"ctx={payload['source_health']['bybit_context_count']} "
                f"liq_events={payload['source_health']['bybit_liquidation_events']} "
                f"ws={payload['source_health']['bybit_ws_connected']} "
                f"spot_err={payload['source_health']['bybit_spot_error']} "
                f"ctx_err={payload['source_health']['bybit_context_last_error']}",
                flush=True,
            )
            now = time.time()
            if do_sync and now - last_sync >= sync_every:
                ok, msg = sync_relay()
                print(f"BYBIT_RELAY_SYNC ok={ok} msg={msg}", flush=True)
                last_sync = now
            if once:
                return
            await asyncio.sleep(max(5.0, interval))
    finally:
        await liq.stop()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--interval", type=float, default=15.0)
    p.add_argument("--sync-every", type=float, default=300.0)
    p.add_argument("--sync", action="store_true")
    p.add_argument("--once", action="store_true")
    a = p.parse_args()
    asyncio.run(run(a.interval, a.sync_every, a.sync, a.once))


if __name__ == "__main__":
    main()
