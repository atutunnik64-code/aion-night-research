from __future__ import annotations
from app.http_shared import SHARED_SSL_CONTEXT
import asyncio,gzip,json,time
import httpx,websockets

class CoinExBBOCache:
    def __init__(self):
        self.rows={}; self.task=None; self.ready=False; self.last_error=None; self.connected_at=0.0
    async def start(self):
        if self.task and not self.task.done(): return
        self.task=asyncio.create_task(self._run())
    async def wait_ready(self,timeout=4.0):
        await self.start(); end=time.time()+timeout
        while time.time()<end:
            if self.rows:return True
            await asyncio.sleep(.1)
        return bool(self.rows)
    async def _markets(self):
        async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=20) as c:
            r=await c.get('https://api.coinex.com/v2/spot/ticker'); r.raise_for_status(); d=r.json()
        markets=[]
        for x in d.get('data',[]):
            m=str(x.get('market','')).upper()
            if m and not m.endswith('_INDEX'):markets.append(m)
        return markets
    async def _run(self):
        while True:
            try:
                markets=await self._markets()
                async with websockets.connect('wss://socket.coinex.com/v2/spot',open_timeout=15,ping_interval=20,ping_timeout=20,max_size=8_000_000) as ws:
                    await ws.send(json.dumps({'method':'bbo.subscribe','params':{'market_list':markets},'id':1}))
                    self.ready=True; self.connected_at=time.time(); self.last_error=None
                    async for raw in ws:
                        if isinstance(raw,(bytes,bytearray)):
                            raw=gzip.decompress(raw).decode('utf-8')
                        msg=json.loads(raw)
                        if msg.get('method')!='bbo.update':continue
                        d=msg.get('data') or {}; market=str(d.get('market','')).upper()
                        if market:
                            self.rows[market]={**d,'received_at':time.time()}
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.ready=False; self.last_error=str(exc)[:300]
                await asyncio.sleep(2.0)

    def snapshot(self,max_age=15.0):
        now=time.time(); return {k:v.copy() for k,v in self.rows.items() if now-float(v.get('received_at',0))<=max_age}
    def status(self):
        return {'ready':self.ready,'markets':len(self.rows),'last_error':self.last_error,'connected_at':self.connected_at}

coinex_bbo=CoinExBBOCache()
