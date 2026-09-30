from __future__ import annotations
import asyncio, json, time
from pathlib import Path
import httpx

ROOT=Path(__file__).parents[2]
OUT=ROOT/'data'/'positioning_history.jsonl'
SYMS=['BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','BNBUSDT','DOGEUSDT','ADAUSDT','LINKUSDT','AVAXUSDT','LTCUSDT']
BASE='https://fapi.binance.com/futures/data/'

class PositioningHistory:
    def __init__(self):
        self.enabled=True; self.interval=900.0; self.task=None
        self.last_write=None; self.last_error=None; self.samples=0; self.last_bucket=None

    async def _get(self,client,path,symbol):
        r=await client.get(BASE+path,params={'symbol':symbol,'period':'5m','limit':2})
        r.raise_for_status(); x=r.json(); return x[-1] if x else None

    async def _symbol(self,client,s):
        paths={'oi':'openInterestHist','global_ls':'globalLongShortAccountRatio','top_account_ls':'topLongShortAccountRatio','top_position_ls':'topLongShortPositionRatio','taker_ls':'takerlongshortRatio'}
        out={}
        for k,p in paths.items():
            try: out[k]=await self._get(client,p,s)
            except Exception as exc: out[k]={'error':str(exc)[:160]}
        try:
            r=await client.get('https://fapi.binance.com/fapi/v1/premiumIndex',params={'symbol':s}); r.raise_for_status(); m=r.json() or {}
            out['price']=float(m.get('markPrice') or 0); out['funding_rate']=float(m.get('lastFundingRate') or 0)
            out['next_funding_time']=m.get('nextFundingTime')
        except Exception as exc: out['market_error']=str(exc)[:160]
        return out
    async def capture(self):
        if not self.enabled:return self.status()
        now=time.time(); bucket=int(now//self.interval)
        if self.last_bucket==bucket:return self.status()
        try:
            async with httpx.AsyncClient(timeout=15,follow_redirects=True) as client:
                data={}
                for s in SYMS: data[s]=await self._symbol(client,s)
            row={'ts':now,'bucket_15m':bucket,'source':'BINANCE_PUBLIC_POSITIONING_5M','symbols':data}
            OUT.parent.mkdir(parents=True,exist_ok=True)
            with OUT.open('a',encoding='utf-8') as f:f.write(json.dumps(row,ensure_ascii=False,separators=(',',':'))+'\n')
            self.last_bucket=bucket; self.last_write=now; self.samples+=1; self.last_error=None
        except Exception as exc:self.last_error=str(exc)[:300]
        return self.status()

    def status(self):
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'DATA_COLLECTION_ONLY','interval_seconds':self.interval,'last_write':self.last_write,'samples':self.samples,'last_error':self.last_error,'path':str(OUT)}

    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name='positioning-history')

    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None

    async def _loop(self):
        await asyncio.sleep(35)
        while True:
            await self.capture(); await asyncio.sleep(max(300.0,self.interval))

positioning_history=PositioningHistory()
