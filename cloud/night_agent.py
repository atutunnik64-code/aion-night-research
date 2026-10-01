from __future__ import annotations
import asyncio, signal, time, os
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
(ROOT/"data").mkdir(parents=True,exist_ok=True)

# Public-data research only. No broker keys, no live execution.
from app.services.moex_futures_collector import moex_futures_collector
from app.services.moex_futures_shadow import moex_futures_shadow
from app.services.moex_feature_registry_v3 import moex_feature_registry
from app.services.moex_futures_universe import moex_futures_universe
from app.services.moex_universe_structure_v1 import moex_universe_structure_v1
from app.services.moex_universe_features_v1 import moex_universe_features_v1
from app.services.moex_spread_research_v1 import moex_spread_research_v1
from app.services.moex_spread_paper_v1 import moex_spread_paper_v1

from app.services.crossvenue_liquidation_asymmetry_collector import crossvenue_liquidation_asymmetry_collector
from app.services.funding_oi_bybit_shadow_v1 import funding_oi_bybit_shadow_v1
from app.services.bybit_liquidation_regime_shadow_v1 import bybit_liquidation_regime_shadow_v1
from app.services.funding_reset_bybit_shadow_v1 import funding_reset_bybit_shadow_v1
from app.services.bybit_price_shock_shadow_v1 import bybit_price_shock_shadow_v1
from app.services.liquidity_migration_perp_collector import liquidity_migration_perp_collector
from app.services.funding_dislocation_persistence_v3 import funding_dislocation_persistence_v3
from app.services.crossvenue_perp_taker_collector_v2 import crossvenue_perp_taker_collector_v2
from app.services.perp_funding_spread import perp_funding_spread_scanner
from app.services.perp_funding_spread_paper import perp_funding_spread_paper
from app.services.basis_funding import basis_funding_scanner
from app.services.crossvenue_spot_perp_shadow_v2 import crossvenue_spot_perp_shadow_v2
from app.services.crossvenue_spot_arb_cloud_v1 import crossvenue_spot_arb_cloud_v1

STOP = asyncio.Event()
SERVICES = [
    # MOEX / FORTS research
    moex_futures_collector, moex_futures_shadow, moex_feature_registry,
    moex_futures_universe, moex_universe_structure_v1, moex_universe_features_v1,
    moex_spread_research_v1,

    # Crypto public-data collectors + independent PAPER hypotheses
    crossvenue_liquidation_asymmetry_collector,
    funding_oi_bybit_shadow_v1,
    bybit_liquidation_regime_shadow_v1,
    funding_reset_bybit_shadow_v1,
    bybit_price_shock_shadow_v1,
    liquidity_migration_perp_collector,
    funding_dislocation_persistence_v3,
    crossvenue_perp_taker_collector_v2,
    perp_funding_spread_scanner,
    perp_funding_spread_paper,
    basis_funding_scanner,
    crossvenue_spot_perp_shadow_v2,
    crossvenue_spot_arb_cloud_v1,
]

async def _start_all():
    for svc in SERVICES:
        fn = getattr(svc, 'start', None)
        if fn:
            await fn()
    print(f'NIGHT_AGENT_STARTED services={len(SERVICES)} ts={time.time():.0f}', flush=True)

async def _stop_all():
    for svc in reversed(SERVICES):
        fn = getattr(svc, 'stop', None)
        if fn:
            try:
                await fn()
            except Exception as exc:
                print(f'STOP_WARN {type(svc).__name__}: {exc}', flush=True)

async def main():
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, STOP.set)
        except NotImplementedError:
            pass
    await _start_all()
    duration=max(0,int(os.getenv("AION_NIGHT_DURATION_SEC","0") or 0))
    deadline=time.monotonic()+duration if duration else None
    try:
        while not STOP.is_set():
            if deadline and time.monotonic()>=deadline:
                break
            await asyncio.sleep(5 if duration else 30)
    finally:
        await _stop_all()
        print('NIGHT_AGENT_STOPPED', flush=True)

if __name__ == '__main__':
    asyncio.run(main())
