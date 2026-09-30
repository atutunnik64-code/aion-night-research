from __future__ import annotations
import asyncio,time
import httpx
from app.http_shared import SHARED_SSL_CONTEXT

PAIRS=(("USDC","USDT"),("FDUSD","USDT"),("FDUSD","USDC"))
FEE={"Binance":0.001,"Bybit":0.001,"OKX":0.001,"Bitget":0.001,"HTX":0.001,"KuCoin":0.001}

class StablecoinScanner:
    def __init__(self):
        self.enabled=True; self.amount=25.0; self.interval=30.0; self.safety_pct=0.05
        self.rows=[]; self.last_refresh=None; self.last_error=None; self.task=None

    async def _get_json(self,c,url,params=None):
        r=await c.get(url,params=params); r.raise_for_status(); return r.json()

    async def _venue(self,c,venue,base,quote):
        try:
            if venue=="Binance":
                d=await self._get_json(c,"https://data-api.binance.vision/api/v3/ticker/bookTicker",{"symbol":base+quote}); return float(d["bidPrice"]),float(d["askPrice"]),float(d.get("bidQty") or 0),float(d.get("askQty") or 0)
            if venue=="Bybit":
                d=await self._get_json(c,"https://api.bybit.com/v5/market/tickers",{"category":"spot","symbol":base+quote}); x=(d.get("result",{}).get("list") or [])[0]; return float(x["bid1Price"]),float(x["ask1Price"]),float(x.get("bid1Size") or 0),float(x.get("ask1Size") or 0)
            if venue=="OKX":
                d=await self._get_json(c,"https://www.okx.com/api/v5/market/ticker",{"instId":base+"-"+quote}); x=(d.get("data") or [])[0]; return float(x["bidPx"]),float(x["askPx"]),float(x.get("bidSz") or 0),float(x.get("askSz") or 0)
            if venue=="Bitget":
                d=await self._get_json(c,"https://api.bitget.com/api/v2/spot/market/tickers",{"symbol":base+quote}); x=(d.get("data") or [])[0]; return float(x["bidPr"]),float(x["askPr"]),float(x.get("bidSz") or 0),float(x.get("askSz") or 0)
            if venue=="HTX":
                d=await self._get_json(c,"https://api.huobi.pro/market/detail/merged",{"symbol":(base+quote).lower()}); x=d.get("tick") or {}; bid=(x.get("bid") or [0,0]); ask=(x.get("ask") or [0,0]); return float(bid[0]),float(ask[0]),float(bid[1]),float(ask[1])
            if venue=="KuCoin":
                d=await self._get_json(c,"https://api.kucoin.com/api/v1/market/orderbook/level1",{"symbol":base+"-"+quote}); x=d.get("data") or {}; return float(x["bestBid"]),float(x["bestAsk"]),float(x.get("bestBidSize") or 0),float(x.get("bestAskSize") or 0)
        except Exception:return None
        return None

    async def refresh(self):
        if not self.enabled:return self.status()
        venues=list(FEE)
        try:
            async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT,timeout=7,follow_redirects=True) as c:
                jobs=[self._venue(c,v,b,q) for b,q in PAIRS for v in venues]
                vals=await asyncio.gather(*jobs)
            books={}; i=0
            for b,q in PAIRS:
                for v in venues:
                    x=vals[i]; i+=1
                    if x and x[0]>0 and x[1]>0:books[(v,b,q)]=x
            out=[]
            for b,q in PAIRS:
                for bv in venues:
                    buy=books.get((bv,b,q))
                    if not buy:continue
                    for sv in venues:
                        if sv==bv:continue
                        sell=books.get((sv,b,q))
                        if not sell:continue
                        ask=buy[1]; bid=sell[0]; raw=(bid/ask-1)*100
                        fees=(FEE[bv]+FEE[sv])*100; net=raw-fees-self.safety_pct
                        cap=min(self.amount,ask*buy[3],bid*sell[2])
                        if net<=0 or cap<10:continue
                        out.append({"strategy":"STABLECOIN","base":b,"quote":q,"buy_venue":bv,"sell_venue":sv,"buy_ask":ask,"sell_bid":bid,"amount":round(cap,6),"raw_pct":round(raw,6),"fee_pct":round(fees,6),"safety_pct":self.safety_pct,"net_pct":round(net,6),"net_profit":round(cap*net/100,8),"status":"STABLE_EDGE"})
            self.rows=sorted(out,key=lambda x:(x["net_profit"],x["net_pct"]),reverse=True)[:100]; self.last_refresh=time.time(); self.last_error=None
        except Exception as exc:self.last_error=str(exc)[:220]
        return self.status()

    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name="stablecoin-scanner")
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None
    async def _loop(self):
        await asyncio.sleep(10)
        while True:
            await self.refresh(); await asyncio.sleep(max(15.0,self.interval))
    def status(self):
        return {"ok":True,"enabled":self.enabled,"mode":"ANALYSIS_ONLY","amount":self.amount,"last_refresh":self.last_refresh,"last_error":self.last_error,"count":len(self.rows),"opportunities":self.rows}

stablecoin_scanner=StablecoinScanner()
