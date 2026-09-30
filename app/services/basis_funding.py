from __future__ import annotations
import asyncio,time
from app.connectors.cex_public import LIVE_CONNECTOR_CLASSES, CORE_ASSETS
from app.services.synthetic_inventory import synthetic_inventory

SUPPORTED={"Binance","Bybit","OKX","Bitget","HTX"}
PERP_FEE={"Binance":0.0005,"Bybit":0.00055,"OKX":0.0005,"Bitget":0.0006,"HTX":0.0006}

class BasisFundingScanner:
    def __init__(self):
        self.enabled=True; self.interval=60.0; self.safety_pct=0.10
        self.last_refresh=None; self.last_error=None; self.rows=[]; self.market={}; self.task=None

    async def _spot(self,cls):
        name=getattr(cls,"name",cls.__name__)
        if name not in SUPPORTED:return name,{}
        try:
            edges=await asyncio.wait_for(cls().get_edges(100.0),timeout=18.0)
            out={}
            for e in edges:
                m=e.meta or {}
                if e.kind!="trade" or m.get("quote")!="USDT":continue
                base=str(m.get("base") or "").upper(); side=str(m.get("side") or "").lower()
                if not base:continue
                z=out.setdefault(base,{"fee":float(e.fee_rate or 0),"symbol":m.get("symbol")})
                if side=="buy":z["ask"]=float(m.get("best_ask") or 0)
                elif side=="sell":z["bid"]=float(m.get("best_bid") or 0)
            return name,{b:z for b,z in out.items() if float(z.get("ask") or 0)>0 and float(z.get("bid") or 0)>0}
        except Exception:return name,{}

    async def refresh(self):
        if not self.enabled:return self.status()
        try:
            classes=[c for c in LIVE_CONNECTOR_CLASSES if getattr(c,"name",c.__name__) in SUPPORTED]
            spots=dict(await asyncio.gather(*[self._spot(c) for c in classes]))
            probes=[]
            for venue,book in spots.items():
                ranked=[(b,i) for b,i in book.items() if b in CORE_ASSETS]
                for base,info in ranked:probes.append((venue,base,info))
            sem=asyncio.Semaphore(10)
            async def one(venue,base,info):
                async with sem: perp=await synthetic_inventory._perp(venue,base,"USDT")
                if not perp.get("ok"):return None
                spot=float(info["ask"]); sbid=float(info.get("bid") or 0); pbid=float(perp.get("bid") or 0); pask=float(perp.get("ask") or 0)
                if spot<=0 or sbid<=0 or pbid<=0 or pask<=0:return None
                sf=float(info.get("fee") or .001); pf=float(PERP_FEE.get(venue,.0006))
                raw_basis=(pbid/spot-1.0)*100.0
                round_trip_cost=2.0*(sf+pf)*100.0+self.safety_pct
                cycle_net=raw_basis-round_trip_cost
                fr=float(perp.get("funding_rate") or 0)*100.0
                need=max(0.0,-cycle_net); periods=(need/fr if fr>0 else 999999.0); days=periods/3.0
                status="BASIS_POSITIVE" if cycle_net>0 else ("FUNDING_CARRY" if fr>0 and days<=7.0 else "NO_EDGE")
                return {"venue":venue,"base":base,"quote":"USDT","spot_ask":spot,"spot_bid":sbid,"perp_bid":pbid,"perp_ask":pask,
                        "spot_fee_pct":sf*100,"perp_fee_pct":pf*100,"safety_pct":self.safety_pct,
                        "raw_basis_pct":round(raw_basis,6),"round_trip_cost_pct":round(round_trip_cost,6),
                        "entry_basis_net_pct":round(cycle_net,6),"funding_rate_pct":round(fr,6),
                        "funding_break_even_periods":round(periods,2) if periods<999999 else None,
                        "funding_break_even_days":round(days,2) if days<999999 else None,
                        "next_funding_time":perp.get("next_funding_time"),"source":perp.get("source"),"status":status}
            vals=await asyncio.gather(*[one(*x) for x in probes])
            all_rows=[x for x in vals if x]
            self.market={f"{x['venue']}:{x['base']}:USDT":x for x in all_rows}
            self.rows=sorted([x for x in all_rows if x["status"] in {"BASIS_POSITIVE","FUNDING_CARRY"}],
                             key=lambda x:(x["entry_basis_net_pct"],x["funding_rate_pct"]),reverse=True)[:100]
            self.last_refresh=time.time(); self.last_error=None
        except Exception as exc:self.last_error=str(exc)[:220]
        return self.status()

    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name="basis-funding-scanner")
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None
    async def _loop(self):
        await asyncio.sleep(8)
        while True:
            await self.refresh(); await asyncio.sleep(max(30.0,self.interval))
    def status(self):
        return {"ok":True,"enabled":self.enabled,"mode":"ANALYSIS_ONLY","last_refresh":self.last_refresh,
                "last_error":self.last_error,"count":len(self.rows),"market_count":len(self.market),"positive_basis_count":sum(1 for x in self.rows if x["entry_basis_net_pct"]>0),
                "positive_funding_count":sum(1 for x in self.rows if x["status"]=="FUNDING_CARRY"),"opportunities":self.rows}

basis_funding_scanner=BasisFundingScanner()
