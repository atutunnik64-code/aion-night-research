from __future__ import annotations
import asyncio,json,time
from collections import defaultdict
from pathlib import Path
from websockets.legacy.client import connect

ROOT=Path(__file__).parents[2];DATA=ROOT/'data'
OUT=DATA/'crossvenue_perp_taker_buckets_v2.jsonl'
STATE=DATA/'crossvenue_perp_taker_collector_v2.json'
SYMS=['BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','BNBUSDT','DOGEUSDT','ADAUSDT','LINKUSDT','AVAXUSDT','LTCUSDT','SUIUSDT','AAVEUSDT']
BUCKET_SEC=300

class CrossVenuePerpTakerCollectorV2:
    def __init__(self):
        self.enabled=True;self.live_enabled=False;self.tasks={};self.flush_task=None
        self.lock=asyncio.Lock();self.buckets=defaultdict(lambda:{'buy_quote':0.0,'sell_quote':0.0,'trades':0})
        self.stats={v:{'connected':False,'messages':0,'trades':0,'buckets':0,'errors':0,'prior_errors':0,'last_error':None,'last_trade_at':None} for v in ('Binance','Bybit')}
        self._load_state()
    def _load_state(self):
        try:
            x=json.loads(STATE.read_text(encoding='utf-8'));saved=dict(x.get('stats') or {})
            for venue in ('Binance','Bybit'):
                if not isinstance(saved.get(venue),dict):continue
                for k in ('messages','trades','buckets','last_trade_at'):
                    if saved[venue].get(k) is not None:self.stats[venue][k]=saved[venue][k]
                self.stats[venue]['prior_errors']=int(saved[venue].get('prior_errors') or 0)+int(saved[venue].get('errors') or 0)
        except Exception:pass
    def _save(self):
        DATA.mkdir(parents=True,exist_ok=True)
        STATE.write_text(json.dumps({'version':'CROSSVENUE_PERP_TAKER_COLLECTOR_V3_WS','stats':self.stats,'updated_at':time.time()},ensure_ascii=False,indent=2),encoding='utf-8')
    @staticmethod
    def _append_rows(rows):
        if not rows:return
        DATA.mkdir(parents=True,exist_ok=True)
        with OUT.open('a',encoding='utf-8') as f:
            for row in rows:f.write(json.dumps(row,separators=(',',':'))+'\n')
    async def _store(self,venue,symbol,side,price,qty,ts_ms):
        try:p=float(price);q=float(qty);ts=float(ts_ms)/1000.
        except Exception:return
        if p<=0 or q<=0 or symbol not in SYMS:return
        key=(venue,symbol,int(ts//BUCKET_SEC));quote=p*q
        async with self.lock:
            z=self.buckets[key];z['buy_quote' if side=='BUY' else 'sell_quote']+=quote;z['trades']+=1
        st=self.stats[venue];st['trades']+=1;st['last_trade_at']=time.time()
    async def _binance(self):
        streams='/'.join(f'{s.lower()}@aggTrade' for s in SYMS)
        url='wss://fstream.binance.com/stream?streams='+streams
        async with connect(url,open_timeout=10,close_timeout=3,ping_interval=20,ping_timeout=10,max_size=2**23) as ws:
            st=self.stats['Binance'];st['connected']=True;st['last_error']=None
            async for raw in ws:
                st['messages']+=1;msg=json.loads(raw);x=msg.get('data') or msg
                if x.get('e')!='aggTrade':continue
                side='SELL' if bool(x.get('m')) else 'BUY'
                await self._store('Binance',str(x.get('s') or ''),side,x.get('p'),x.get('q'),x.get('T') or x.get('E'))
    async def _binance_loop(self):
        while True:
            try:await self._binance()
            except asyncio.CancelledError:raise
            except Exception as exc:
                st=self.stats['Binance'];st['errors']+=1;st['last_error']=str(exc)[:300];st['connected']=False;self._save()
                await asyncio.sleep(min(30,2+st['errors']))
            finally:self.stats['Binance']['connected']=False
    async def _bybit(self):
        url='wss://stream.bybit.com/v5/public/linear'
        async with connect(url,open_timeout=10,close_timeout=3,ping_interval=20,ping_timeout=10,max_size=2**23) as ws:
            await ws.send(json.dumps({'op':'subscribe','args':[f'publicTrade.{s}' for s in SYMS]}))
            st=self.stats['Bybit'];st['connected']=True;st['last_error']=None
            async for raw in ws:
                st['messages']+=1;d=json.loads(raw)
                if not str(d.get('topic') or '').startswith('publicTrade.'):continue
                for x in d.get('data') or []:
                    side='BUY' if str(x.get('S') or '').upper().startswith('B') else 'SELL'
                    await self._store('Bybit',str(x.get('s') or ''),side,x.get('p'),x.get('v'),x.get('T'))
    async def _bybit_loop(self):
        while True:
            try:await self._bybit()
            except asyncio.CancelledError:raise
            except Exception as exc:
                st=self.stats['Bybit'];st['errors']+=1;st['last_error']=str(exc)[:300];self._save()
                await asyncio.sleep(min(30,2+st['errors']))
            finally:self.stats['Bybit']['connected']=False
    async def _flush(self):
        while True:
            await asyncio.sleep(3);cut=int(time.time()//BUCKET_SEC)-1;rows=[]
            async with self.lock:
                keys=[k for k in self.buckets if k[2]<=cut]
                for venue,sym,bucket in keys:
                    z=self.buckets.pop((venue,sym,bucket));total=z['buy_quote']+z['sell_quote']
                    rows.append({'bucket':bucket,'ts':(bucket+1)*BUCKET_SEC,'venue':venue,'symbol':sym,
                        'buy_quote':z['buy_quote'],'sell_quote':z['sell_quote'],'quote_volume':total,
                        'imbalance':(z['buy_quote']-z['sell_quote'])/total if total else 0.0,'trades':z['trades'],
                        'source':'binance_futures_aggTrade' if venue=='Binance' else 'bybit_linear_publicTrade'})
            if rows:
                await asyncio.to_thread(self._append_rows,rows)
                for venue in ('Binance','Bybit'):self.stats[venue]['buckets']+=sum(1 for r in rows if r['venue']==venue)
                self._save()
    async def start(self):
        if self.tasks:return
        self.tasks={'Binance':asyncio.create_task(self._binance_loop(),name='perp-taker-binance-ws-v3'),
                    'Bybit':asyncio.create_task(self._bybit_loop(),name='perp-taker-bybit-v2')}
        self.flush_task=asyncio.create_task(self._flush(),name='perp-taker-flush-v3')
    async def stop(self):
        all_tasks=list(self.tasks.values())+([self.flush_task] if self.flush_task else [])
        for t in all_tasks:
            if t and not t.done():t.cancel()
        await asyncio.gather(*[t for t in all_tasks if t],return_exceptions=True);self.tasks={};self.flush_task=None;self._save()
    def status(self):
        now=time.time();stats={}
        for v,src in self.stats.items():
            x=dict(src);last=x.get('last_trade_at');x['last_trade_age_ms']=round((now-last)*1000,1) if last else None;stats[v]=x
        return {'ok':all(x.get('last_error') is None for x in self.stats.values()),'enabled':self.enabled,
            'mode':'FUTURE_ONLY_COLLECT','strategy':'CROSSVENUE_TAKER_IMBALANCE_DIVERGENCE_V3_WS','live_enabled':False,
            'paper_only':True,'symbols':SYMS,'bucket_seconds':BUCKET_SEC,'venues':stats,'path':str(OUT),
            'policy':{'public_market_data_only':True,'perp_only':True,'no_grid':True,'no_martingale':True,'no_dca':True,'no_live_orders':True}}

crossvenue_perp_taker_collector_v2=CrossVenuePerpTakerCollectorV2()
