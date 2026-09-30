from __future__ import annotations
import asyncio,json,time
from collections import defaultdict
from pathlib import Path
from websockets.legacy.client import connect

ROOT=Path(__file__).parents[2];DATA=ROOT/'data'
OUT=DATA/'crossvenue_perp_taker_buckets_v1.jsonl'
STATE=DATA/'crossvenue_perp_taker_collector_v1.json'
SYMS=['BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','BNBUSDT','DOGEUSDT','ADAUSDT','LINKUSDT','AVAXUSDT','LTCUSDT','SUIUSDT','AAVEUSDT']
BUCKET_SEC=60

class CrossVenuePerpTakerCollector:
    def __init__(self):
        self.enabled=True;self.live_enabled=False;self.tasks={};self.flush_task=None
        self.lock=asyncio.Lock();self.buckets=defaultdict(lambda:{'buy':0.0,'sell':0.0,'trades':0})
        self.stats={v:{'connected':False,'messages':0,'trades':0,'buckets':0,'errors':0,'last_error':None,'last_trade_at':None} for v in ('Binance','Bybit')}
    def _save(self):
        STATE.write_text(json.dumps({'version':'CROSSVENUE_PERP_TAKER_COLLECTOR_V1','stats':self.stats,'updated_at':time.time()},ensure_ascii=False,indent=2),encoding='utf-8')
    async def _store(self,venue,symbol,side,price,qty,ts_ms):
        try:p=float(price);q=float(qty);ts=float(ts_ms)/1000.0
        except Exception:return
        if p<=0 or q<=0 or symbol not in SYMS:return
        key=(venue,symbol,int(ts//BUCKET_SEC));quote=p*q
        async with self.lock:
            z=self.buckets[key];z['buy' if str(side).upper().startswith('B') else 'sell']+=quote;z['trades']+=1
        st=self.stats[venue];st['trades']+=1;st['last_trade_at']=time.time()

    async def _binance(self):
        streams='/'.join(f'{s.lower()}@aggTrade' for s in SYMS)
        url='wss://fstream.binance.com/stream?streams='+streams
        async with connect(url,open_timeout=10,close_timeout=3,ping_interval=20,ping_timeout=10,max_size=2**23) as ws:
            self.stats['Binance']['connected']=True
            async for raw in ws:
                self.stats['Binance']['messages']+=1;d=json.loads(raw);x=d.get('data') or {}
                if x.get('e')!='aggTrade':continue
                side='SELL' if bool(x.get('m')) else 'BUY'
                await self._store('Binance',str(x.get('s') or ''),side,x.get('p'),x.get('q'),x.get('T'))
    async def _bybit(self):
        url='wss://stream.bybit.com/v5/public/linear'
        async with connect(url,open_timeout=10,close_timeout=3,ping_interval=20,ping_timeout=10,max_size=2**23) as ws:
            await ws.send(json.dumps({'op':'subscribe','args':[f'publicTrade.{s}' for s in SYMS]}))
            self.stats['Bybit']['connected']=True
            async for raw in ws:
                self.stats['Bybit']['messages']+=1;d=json.loads(raw)
                if not str(d.get('topic') or '').startswith('publicTrade.'):continue
                for x in d.get('data') or []:
                    await self._store('Bybit',str(x.get('s') or ''),x.get('S'),x.get('p'),x.get('v'),x.get('T'))

    async def _runner(self,venue,fn):
        while True:
            try:await fn()
            except asyncio.CancelledError:raise
            except Exception as exc:
                st=self.stats[venue];st['errors']+=1;st['last_error']=str(exc)[:300]
                await asyncio.sleep(min(30,2+st['errors']))
            finally:self.stats[venue]['connected']=False
    async def _flush(self):
        while True:
            await asyncio.sleep(2);cut=int(time.time()//BUCKET_SEC)-1;rows=[]
            async with self.lock:
                keys=[k for k in self.buckets if k[2]<=cut]
                for k in keys:
                    venue,sym,bucket=k;z=self.buckets.pop(k);total=z['buy']+z['sell']
                    rows.append({'bucket':bucket,'ts':(bucket+1)*BUCKET_SEC,'venue':venue,'symbol':sym,
                        'buy_quote':z['buy'],'sell_quote':z['sell'],'quote_volume':total,
                        'imbalance':(z['buy']-z['sell'])/total if total>0 else 0.0,'trades':z['trades']})
            if rows:
                with OUT.open('a',encoding='utf-8') as f:
                    for row in rows:f.write(json.dumps(row,separators=(',',':'))+'\n')
                for row in rows:self.stats[row['venue']]['buckets']+=1
                self._save()

    async def start(self):
        if self.tasks:return
        self.tasks={'Binance':asyncio.create_task(self._runner('Binance',self._binance),name='perp-taker-binance'),
                    'Bybit':asyncio.create_task(self._runner('Bybit',self._bybit),name='perp-taker-bybit')}
        self.flush_task=asyncio.create_task(self._flush(),name='perp-taker-flush')
    async def stop(self):
        for t in list(self.tasks.values())+([self.flush_task] if self.flush_task else []):
            if t and not t.done():t.cancel()
        await asyncio.gather(*[t for t in list(self.tasks.values())+([self.flush_task] if self.flush_task else []) if t],return_exceptions=True)
        self.tasks={};self.flush_task=None
    def status(self):
        now=time.time();stats={}
        for v,src in self.stats.items():
            x=dict(src);last=x.get('last_trade_at');x['last_trade_age_ms']=round((now-last)*1000,1) if last else None;stats[v]=x
        return {'ok':all(x.get('last_error') is None for x in self.stats.values()),'enabled':self.enabled,
            'mode':'FUTURE_ONLY_COLLECT','strategy':'CROSSVENUE_TAKER_IMBALANCE_DIVERGENCE_V1','live_enabled':False,
            'paper_only':True,'symbols':SYMS,'bucket_seconds':BUCKET_SEC,'venues':stats,'path':str(OUT),
            'policy':{'perp_only':True,'no_grid':True,'no_martingale':True,'no_dca':True,'no_live_orders':True}}

crossvenue_perp_taker_collector=CrossVenuePerpTakerCollector()
