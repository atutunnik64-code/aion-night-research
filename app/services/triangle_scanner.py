from __future__ import annotations
import asyncio,time
from app.connectors.cex_public import LIVE_CONNECTOR_CLASSES
from app.services.route_finder import RouteFinder

VENUES={"Binance","Bybit","OKX","Bitget","KuCoin","HTX"}
START={"USDT","USDC","FDUSD"}

class TriangleScanner:
    def __init__(self):
        self.enabled=True; self.amount=25.0; self.interval=45.0; self.discovery_pct=0.05; self.safety_pct=0.10
        self.rows=[]; self.last_refresh=None; self.last_error=None; self.task=None
    async def _edges(self,cls):
        name=getattr(cls,"name",cls.__name__)
        try:return name,await asyncio.wait_for(cls().get_edges(self.amount),timeout=18.0)
        except Exception:return name,[]
    def _cycles(self,edges):
        f=RouteFinder(max_hops=3,min_profit_pct=self.discovery_pct)
        return [x for x in f.find_cycles(edges,self.amount,START) if int(x.get("hops") or 0)==3]
    async def refresh(self):
        if not self.enabled:return self.status()
        try:
            classes=[c for c in LIVE_CONNECTOR_CLASSES if getattr(c,"name",c.__name__) in VENUES]
            first=dict(await asyncio.gather(*[self._edges(c) for c in classes])); await asyncio.sleep(.35)
            second=dict(await asyncio.gather(*[self._edges(c) for c in classes]))
            out=[]
            for venue in VENUES:
                a=self._cycles(first.get(venue,[])); b=self._cycles(second.get(venue,[])); bm={">".join(x.get("nodes") or []):x for x in b}
                for x in a:
                    sig=">".join(x.get("nodes") or []); y=bm.get(sig)
                    if not y:continue
                    raw=min(float(x.get("profit_pct") or -999),float(y.get("profit_pct") or -999)); net=raw-self.safety_pct
                    if net<=0:continue
                    z=dict(y); z.update({"venue":venue,"signature":sig,"first_net_pct":x.get("profit_pct"),"second_net_pct":y.get("profit_pct"),
                                       "safety_pct":self.safety_pct,"clean_net_pct":round(net,6),"clean_profit":round(self.amount*net/100.0,8),
                                       "status":"TRIANGLE_CONFIRMED"}); out.append(z)
            self.rows=sorted(out,key=lambda x:(x["clean_net_pct"],x["clean_profit"]),reverse=True)[:100]
            self.last_refresh=time.time(); self.last_error=None
        except Exception as exc:self.last_error=str(exc)[:220]
        return self.status()
    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name="triangle-scanner")
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None
    async def _loop(self):
        await asyncio.sleep(12)
        while True:
            await self.refresh(); await asyncio.sleep(max(30.0,self.interval))
    def status(self):
        return {"ok":True,"enabled":self.enabled,"mode":"ANALYSIS_ONLY","amount":self.amount,"discovery_pct":self.discovery_pct,
                "safety_pct":self.safety_pct,"last_refresh":self.last_refresh,"last_error":self.last_error,"count":len(self.rows),"opportunities":self.rows}

triangle_scanner=TriangleScanner()
