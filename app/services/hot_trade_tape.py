from __future__ import annotations
import asyncio, json, time
from collections import defaultdict, deque
from websockets.legacy.client import connect

VENUES=('Binance','Bybit','OKX')
SYMBOLS=('BTCUSDT','ETHUSDT','SOLUSDT')
MAX_EVENTS=1200

class HotTradeTape:
    def __init__(self):
        self.events=defaultdict(lambda:deque(maxlen=MAX_EVENTS))
        self.tasks={}; self.started=False
        self.stats=defaultdict(lambda:{'connected':False,'reconnects':0,'errors':0,
            'messages':0,'trades':0,'last_error':None,'last_trade_ts':None})

    @staticmethod
    def _compact(symbol):
        return str(symbol or '').upper().replace('-','').replace('_','')

    @staticmethod
    def _dash(symbol):
        s=HotTradeTape._compact(symbol)
        return s[:-4]+'-USDT' if s.endswith('USDT') else s
    @staticmethod
    def _ts(value):
        try:
            x=float(value or 0)
            while x>1e12:x/=1000.0
            return x if x>1e9 else time.time()
        except Exception:
            return time.time()

    def _store(self,venue,symbol,side,price,qty,source_ts=None):
        p=float(price or 0); q=float(qty or 0)
        if p<=0 or q<=0:return
        s=self._compact(symbol)
        if s not in SYMBOLS:return
        direction=1.0 if str(side).upper().startswith('B') else -1.0
        ts=self._ts(source_ts); quote=p*q; sec=int(ts)
        dq=self.events[(venue,s)]
        if dq and int(dq[-1][0])==sec:
            old=dq[-1]; dq[-1]=(sec,old[1]+direction*quote,old[2]+quote,p,old[4]+1)
        else:dq.append((sec,direction*quote,quote,p,1))
        st=self.stats[venue]; st['trades']+=1; st['last_trade_ts']=ts

    def _venue_window(self,venue,symbol,seconds,now):
        rows=self.events.get((venue,symbol)) or ()
        cutoff=now-float(seconds); signed=0.0; total=0.0; count=0; last=None
        for ts,sq,aq,p,n in reversed(rows):
            if ts<cutoff:break
            signed+=sq; total+=aq; count+=int(n); last=ts if last is None else max(last,ts)
        return {'venue':venue,'count':count,'quote_volume':total,
                'imbalance':signed/total if total>0 else None,
                'age_ms':max(0.0,(now-last)*1000.0) if last else None}
    def pressure(self,symbol,seconds=60):
        symbol=self._compact(symbol); now=time.time(); rows=[]
        signed=0.0; total=0.0
        for venue in VENUES:
            x=self._venue_window(venue,symbol,seconds,now); rows.append(x)
            if x['imbalance'] is not None:
                signed+=float(x['imbalance'])*float(x['quote_volume'])
                total+=float(x['quote_volume'])
        fresh=[x for x in rows if x['count']>0 and x['age_ms'] is not None and x['age_ms']<=5000]
        return {'symbol':symbol,'window_seconds':int(seconds),
                'imbalance':signed/total if total>0 else None,
                'quote_volume':total,'venue_coverage':len(fresh),'venues':rows}

    async def _binance(self):
        streams='/'.join(f'{s.lower()}@aggTrade' for s in SYMBOLS)
        url='wss://stream.binance.com:9443/stream?streams='+streams
        async with connect(url,open_timeout=8,close_timeout=2,ping_interval=20,ping_timeout=10,max_size=2**22) as ws:
            self.stats['Binance']['connected']=True
            async for raw in ws:
                self.stats['Binance']['messages']+=1
                d=json.loads(raw); x=d.get('data') or {}
                if x.get('e')!='aggTrade':continue
                side='SELL' if bool(x.get('m')) else 'BUY'
                self._store('Binance',x.get('s'),side,x.get('p'),x.get('q'),x.get('T'))
    async def _bybit(self):
        url='wss://stream.bybit.com/v5/public/spot'
        async with connect(url,open_timeout=8,close_timeout=2,ping_interval=20,ping_timeout=10,max_size=2**22) as ws:
            await ws.send(json.dumps({'op':'subscribe','args':[f'publicTrade.{s}' for s in SYMBOLS]}))
            self.stats['Bybit']['connected']=True
            async for raw in ws:
                self.stats['Bybit']['messages']+=1
                d=json.loads(raw)
                if not str(d.get('topic') or '').startswith('publicTrade.'):continue
                for x in d.get('data') or []:
                    self._store('Bybit',x.get('s'),x.get('S'),x.get('p'),x.get('v'),x.get('T'))

    async def _okx(self):
        url='wss://ws.okx.com:8443/ws/v5/public'
        async with connect(url,open_timeout=8,close_timeout=2,ping_interval=20,ping_timeout=10,max_size=2**22) as ws:
            args=[{'channel':'trades','instId':self._dash(s)} for s in SYMBOLS]
            await ws.send(json.dumps({'op':'subscribe','args':args}))
            self.stats['OKX']['connected']=True
            async for raw in ws:
                self.stats['OKX']['messages']+=1
                d=json.loads(raw)
                if (d.get('arg') or {}).get('channel')!='trades':continue
                for x in d.get('data') or []:
                    self._store('OKX',x.get('instId'),x.get('side'),x.get('px'),x.get('sz'),x.get('ts'))
    async def _runner(self,venue,fn):
        while True:
            try:
                self.stats[venue]['reconnects']+=1
                await fn()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                st=self.stats[venue]; st['errors']+=1
                st['last_error']=str(exc)[:300]
                await asyncio.sleep(min(5.0,0.5+st['errors']*.15))
            finally:
                self.stats[venue]['connected']=False

    async def start(self):
        if self.started:return
        self.started=True
        self.tasks={
            'Binance':asyncio.create_task(self._runner('Binance',self._binance),name='trade-tape-binance'),
            'Bybit':asyncio.create_task(self._runner('Bybit',self._bybit),name='trade-tape-bybit'),
            'OKX':asyncio.create_task(self._runner('OKX',self._okx),name='trade-tape-okx')}

    async def stop(self):
        for t in self.tasks.values():t.cancel()
        if self.tasks:await asyncio.gather(*self.tasks.values(),return_exceptions=True)
        self.tasks={}; self.started=False
    def status(self):
        now=time.time(); venues={}
        for v in VENUES:
            st=dict(self.stats[v]); last=st.get('last_trade_ts')
            st['last_trade_age_ms']=round(max(0.0,now-float(last))*1000.0,1) if last else None
            venues[v]=st
        pressure={}
        for s in SYMBOLS:
            pressure[s]={str(w):self.pressure(s,w) for w in (30,60,300)}
        return {'started':self.started,'symbols':list(SYMBOLS),'venues':venues,
                'pressure':pressure,'aggregation':'1s_signed_quote_buckets'}

hot_trade_tape=HotTradeTape()
