from __future__ import annotations
from app.http_shared import SHARED_SSL_CONTEXT
import asyncio,json,time
import httpx,websockets

class BinanceBBOCache:
    def __init__(self):
        self.rows={}; self.task=None; self.ready=False; self.last_error=None; self.connected_at=0.0
    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._run())
    async def wait_ready(self,timeout=3.0):
        await self.start(); end=time.time()+timeout
        while time.time()<end:
            if self.rows:return True
            await asyncio.sleep(.05)
        return bool(self.rows)
    async def _symbols(self):
        async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=8,follow_redirects=True) as c:
            r=await c.get('https://data-api.binance.vision/api/v3/ticker/bookTicker'); r.raise_for_status(); rows=r.json()
        quotes=('USDT','USDC','FDUSD','BTC','ETH','BNB')
        symbols=[str(x.get('symbol','')).upper() for x in rows]
        symbols=[s for s in symbols if s and any(s.endswith(q) and len(s)>len(q) for q in quotes)]
        return symbols[:950]
    async def _run(self):
        while True:
            try:
                symbols=await self._symbols(); params=[s.lower()+'@bookTicker' for s in symbols]
                async with websockets.connect('wss://stream.binance.com:9443/ws',open_timeout=10,ping_interval=20,ping_timeout=20,max_size=12_000_000) as ws:
                    for i in range(0,len(params),200):
                        await ws.send(json.dumps({'method':'SUBSCRIBE','params':params[i:i+200],'id':i//200+1}))
                    self.ready=True; self.connected_at=time.time(); self.last_error=None
                    async for raw in ws:
                        d=json.loads(raw)
                        if 'result' in d and d.get('result') is None:continue
                        symbol=str(d.get('s','')).upper()
                        if not symbol:continue
                        self.rows[symbol]={'symbol':symbol,'bid':d.get('b'),'bid_qty':d.get('B'),'ask':d.get('a'),'ask_qty':d.get('A'),'exchange_ts_ms':int(time.time()*1000),'received_at':time.time()}
            except asyncio.CancelledError:raise
            except Exception as exc:
                self.ready=False; self.last_error=str(exc)[:300]; await asyncio.sleep(1.0)

    def snapshot(self,max_age=3.0):
        now=time.time()
        return {k:v.copy() for k,v in self.rows.items() if now-float(v.get('received_at',0))<=max_age}

    def status(self):
        return {'ready':self.ready,'markets':len(self.rows),'last_error':self.last_error,'connected_at':self.connected_at}

binance_bbo=BinanceBBOCache()
