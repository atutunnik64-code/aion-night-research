from __future__ import annotations
import asyncio,time
from app.services.crossvenue_spot_arb_cloud_v1 import crossvenue_spot_arb_cloud_v1,TAKER as SPOT_TAKER
from app.services.perp_funding_spread import perp_funding_spread_scanner,TAKER_FEE as PERP_TAKER

class BasisFundingScanner:
    def __init__(self):
        self.enabled=True; self.interval=120.0; self.safety_pct=0.10
        self.last_refresh=None; self.last_error=None; self.rows=[]; self.market={}; self.task=None
        self.shared_assets=0; self.venue_asset_counts={}

    async def refresh(self):
        if not self.enabled:return self.status()
        try:
            if not crossvenue_spot_arb_cloud_v1.market:
                await crossvenue_spot_arb_cloud_v1.refresh()
            if not perp_funding_spread_scanner.market:
                await perp_funding_spread_scanner.refresh()
            spot_market=crossvenue_spot_arb_cloud_v1.market
            perp_market=perp_funding_spread_scanner.market
            rows=[]; shared=set(); counts={}
            for base,spot_vm in spot_market.items():
                perp_vm=perp_market.get(base) or {}
                for venue,spot in spot_vm.items():
                    perp=perp_vm.get(venue)
                    if not perp or venue not in SPOT_TAKER or venue not in PERP_TAKER:continue
                    sask=float(spot.get('ask') or 0); sbid=float(spot.get('bid') or 0)
                    pbid=float(perp.get('bid') or 0); pask=float(perp.get('ask') or 0)
                    if min(sask,sbid,pbid,pask)<=0:continue
                    shared.add(base);counts[venue]=counts.get(venue,0)+1
                    sf=float(SPOT_TAKER[venue]);pf=float(PERP_TAKER[venue])
                    raw_basis=(pbid/sask-1.0)*100.0
                    round_trip=2.0*(sf+pf)*100.0+self.safety_pct
                    basis_net=raw_basis-round_trip
                    interval=max(1.0,float(perp.get('interval_hours') or 8.0))
                    funding_rate_pct=float(perp.get('rate') or 0)*100.0
                    funding_24h=funding_rate_pct*(24.0/interval)
                    need=max(0.0,-basis_net)
                    days=(need/funding_24h) if funding_24h>0 else None
                    status='BASIS_POSITIVE' if basis_net>0 else ('FUNDING_CARRY' if days is not None and days<=7.0 else 'NO_EDGE')
                    rows.append({'venue':venue,'base':base,'quote':'USDT','strategy':'spot_perp_cash_and_carry',
                        'spot_ask':sask,'spot_bid':sbid,'perp_bid':pbid,'perp_ask':pask,
                        'spot_fee_pct':sf*100.0,'perp_fee_pct':pf*100.0,'safety_pct':self.safety_pct,
                        'raw_basis_pct':round(raw_basis,6),'round_trip_cost_pct':round(round_trip,6),
                        'entry_basis_net_pct':round(basis_net,6),'funding_rate_pct':round(funding_rate_pct,6),
                        'funding_interval_hours':interval,'projected_24h_funding_pct':round(funding_24h,6),
                        'funding_break_even_days':round(days,3) if days is not None else None,
                        'next_funding_time':perp.get('next_funding_time'),'source':perp.get('source'),'status':status})
            self.market={f"{x['venue']}:{x['base']}:USDT":x for x in rows}
            self.rows=sorted([x for x in rows if x['status'] in {'BASIS_POSITIVE','FUNDING_CARRY'}],
                key=lambda x:(x['status']=='BASIS_POSITIVE',x['entry_basis_net_pct'],x['projected_24h_funding_pct']),reverse=True)[:500]
            self.shared_assets=len(shared);self.venue_asset_counts=counts
            self.last_refresh=time.time();self.last_error=None
        except Exception as exc:self.last_error=str(exc)[:500]
        return self.status()

    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name='basis-funding-scanner')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None
    async def _loop(self):
        await asyncio.sleep(25)
        while True:
            await self.refresh(); await asyncio.sleep(max(60.0,self.interval))
    def status(self):
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'ANALYSIS_ONLY','last_refresh':self.last_refresh,
                'last_error':self.last_error,'shared_assets':self.shared_assets,'venue_asset_counts':self.venue_asset_counts,
                'count':len(self.rows),'market_count':len(self.market),'positive_basis_count':sum(1 for x in self.rows if x['entry_basis_net_pct']>0),
                'positive_funding_count':sum(1 for x in self.rows if x['status']=='FUNDING_CARRY'),'opportunities':self.rows,
                'policy':{'dynamic_shared_spot_perp_universe':True,'no_core_assets_cap':True,'no_live_orders':True}}

basis_funding_scanner=BasisFundingScanner()
