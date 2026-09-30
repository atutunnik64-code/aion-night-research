from __future__ import annotations
from app.http_shared import SHARED_SSL_CONTEXT
import asyncio, gzip, json, time, uuid
from collections import defaultdict
import httpx
from websockets.legacy.client import connect

class HotBookCache:
    SUPPORTED={'Binance','Bybit','OKX','KuCoin','Bitget','HTX','CoinW'}
    def __init__(self):
        self.books={}; self.desired={}; self.tasks={}; self.started=False
        self.stale_after=1.5; self.execution_stale_after=0.8; self.quiet_book_grace=5.0; self.stream_stale_after=2.0; self.max_symbols_per_venue=60
        self.stats=defaultdict(lambda:{'messages':0,'updates':0,'reconnects':0,'errors':0,'last_error':None,'connected':False,'last_message_mono':0.0})
        self.bybit_state={}
        self.coinw_pair_codes={}; self.coinw_pair_symbols={}; self.coinw_pair_refresh_mono=0.0
        self.freshness_waits=0; self.freshness_recovered=0; self.freshness_timeouts=0
        self.refresh_requests=set(); self.refresh_last={}; self.refresh_sent=0; self.refresh_throttled=0
        self.control_last=defaultdict(float)

    @staticmethod
    def _compact(symbol): return str(symbol or '').upper().replace('-','').replace('_','')
    @staticmethod
    def _dash(symbol):
        s=str(symbol or '').upper().replace('_','-')
        if '-' in s:return s
        for q in ('USDT','USDC','FDUSD','BTC','ETH','EUR','USD'):
            if s.endswith(q) and len(s)>len(q): return s[:-len(q)]+'-'+q
        return s
    def _key(self,venue,symbol): return (str(venue),self._compact(symbol))
    @staticmethod
    def _source_wall(value):
        try:
            x=float(value or 0)
            if x<=0:return None
            while x>1e13:x/=1000.0
            if x>1e11:x/=1000.0
            return x if x>1e9 else None
        except Exception:return None
    def watch_legs(self,legs,priority=100,ttl=180.0):
        now=time.monotonic(); ttl=max(5.0,float(ttl)); priority=float(priority)
        for leg in legs or []:
            venue=str(leg.get('venue') or ''); symbol=self._compact(leg.get('symbol'))
            if venue not in self.SUPPORTED or not symbol:continue
            key=(venue,symbol); old=self.desired.get(key) or {}
            self.desired[key]={'ts':now,'until':max(float(old.get('until') or 0),now+ttl),'priority':max(priority,float(old.get('priority') or 0))}
    def request_refresh(self,legs,min_interval=.75):
        now=time.monotonic(); added=0; self.watch_legs(legs,priority=255,ttl=90)
        for leg in legs or []:
            venue=str(leg.get('venue') or ''); symbol=self._compact(leg.get('symbol'))
            if venue not in self.SUPPORTED or not symbol: continue
            key=(venue,symbol); last=float(self.refresh_last.get(key) or 0)
            if now-last < max(.1,float(min_interval)):
                self.refresh_throttled+=1; continue
            self.refresh_last[key]=now; self.refresh_requests.add(key); added+=1
        return added

    def wanted(self,venue):
        now=time.monotonic(); rows=[]
        for (v,sym),meta in list(self.desired.items()):
            if float(meta.get('until') or 0)<=now:
                self.desired.pop((v,sym),None); continue
            if v==venue:rows.append((sym,float(meta.get('priority') or 0),float(meta.get('ts') or 0)))
        rows.sort(key=lambda x:(x[1],x[2]),reverse=True)
        return [x[0] for x in rows[:self.max_symbols_per_venue]]
    def age_info(self,venue,symbol):
        row=self.books.get(self._key(venue,symbol))
        if not row:return None
        local_age=max(0.0,time.monotonic()-float(row.get('mono') or 0.0)); source_wall=row.get('source_wall')
        source_age=max(0.0,time.time()-float(source_wall)) if source_wall else None
        effective=max(local_age,source_age or 0.0)
        return {'venue':str(venue),'symbol':self._compact(symbol),'local_age_ms':round(local_age*1000,1),'source_age_ms':round(source_age*1000,1) if source_age is not None else None,'effective_age_ms':round(effective*1000,1),'has_source_ts':source_age is not None}
    def get(self,venue,symbol,max_age=None):
        row=self.books.get(self._key(venue,symbol))
        if not row:return None
        now=time.monotonic()
        info=self.age_info(venue,symbol); age=float(info['effective_age_ms'])/1000.0; lim=self.stale_after if max_age is None else float(max_age)
        if age>lim:
            # Explicit max_age is a hard execution safety bound. Quiet-book grace
            # is only allowed for background/non-execution reads where max_age is omitted.
            if max_age is not None:return None
            st=self.stats[str(venue)]; stream_age=now-float(st.get('last_message_mono') or 0)
            if not (st.get('connected') and age<=self.quiet_book_grace and stream_age<=self.stream_stale_after):return None
        return row['bids'],row['asks'],age*1000.0
    async def start(self):
        if self.started:return
        self.started=True
        for venue in self.SUPPORTED:self.tasks[venue]=asyncio.create_task(self._venue_loop(venue),name=f'hotbook-{venue}')
    async def stop(self):
        for t in self.tasks.values():t.cancel()
        if self.tasks:await asyncio.gather(*self.tasks.values(),return_exceptions=True)
        self.tasks={}; self.started=False
    def _store(self,venue,symbol,bids,asks,source_ts=None):
        b=[(float(x[0]),float(x[1])) for x in bids or [] if float(x[0])>0 and float(x[1])>0]
        a=[(float(x[0]),float(x[1])) for x in asks or [] if float(x[0])>0 and float(x[1])>0]
        if not b or not a:return
        b.sort(key=lambda x:x[0],reverse=True); a.sort(key=lambda x:x[0])
        now_wall=time.time(); source_wall=self._source_wall(source_ts)
        self.books[self._key(venue,symbol)]={'bids':b[:50],'asks':a[:50],'mono':time.monotonic(),'wall':now_wall,'source_wall':source_wall}
        self.stats[venue]['updates']+=1
    def revisions(self,legs):
        out=[]
        for leg in legs or []:
            row=self.books.get(self._key(leg.get('venue'),leg.get('symbol')))
            out.append(float((row or {}).get('mono') or 0.0))
        return out
    async def wait_for_newer(self,legs,revisions,timeout=.25,max_age=None):
        self.watch_legs(legs,priority=250,ttl=90)
        end=time.monotonic()+max(0.01,float(timeout)); lim=self.execution_stale_after if max_age is None else float(max_age)
        while time.monotonic()<end:
            rows=[self.books.get(self._key(x.get('venue'),x.get('symbol'))) for x in legs]
            if len(rows)==len(revisions) and all(row and float(row.get('mono') or 0)>float(revisions[i] or 0) and self.get(legs[i].get('venue'),legs[i].get('symbol'),lim) for i,row in enumerate(rows)):
                return True
            await asyncio.sleep(.015)
        return False

    async def wait_for_fresh_age(self,legs,max_age=.5,timeout=.45):
        self.freshness_waits+=1; self.request_refresh(legs,min_interval=.5)
        end=time.monotonic()+max(.01,float(timeout)); lim=max(.05,float(max_age))
        while time.monotonic()<end:
            if all(self.get(x.get('venue'),x.get('symbol'),lim) for x in legs):
                self.freshness_recovered+=1; return True
            await asyncio.sleep(.015)
        self.freshness_timeouts+=1; return False

    async def wait_for_legs(self,legs,timeout=.8,max_age=None):
        self.watch_legs(legs,priority=140,ttl=300); self.request_refresh(legs,min_interval=.5); end=time.monotonic()+timeout
        lim=self.execution_stale_after if max_age is None else float(max_age)
        while time.monotonic()<end:
            if all(self.get(x.get('venue'),x.get('symbol'),lim) for x in legs):return True
            await asyncio.sleep(.03)
        return False
    async def _kucoin_url(self):
        async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=5) as c:
            r=await c.post('https://api.kucoin.com/api/v1/bullet-public'); r.raise_for_status(); d=r.json().get('data') or {}
        server=(d.get('instanceServers') or [{}])[0]; ep=str(server.get('endpoint') or '').rstrip('/'); token=d.get('token')
        return f"{ep}?token={token}&connectId={uuid.uuid4().hex}",float(server.get('pingInterval') or 18000)/1000.0
    async def _coinw_pairs(self):
        if self.coinw_pair_codes and time.monotonic()-self.coinw_pair_refresh_mono<300:return
        d=None
        async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=12,follow_redirects=True) as c:
            for attempt in range(3):
                try:
                    r=await c.get('https://api.coinw.com/api/v1/public',params={'command':'returnTicker'}); r.raise_for_status(); d=r.json(); break
                except Exception:
                    if attempt==2: raise
                    await asyncio.sleep(.35*(attempt+1))
        codes={}; rev={}
        for sym,x in (d.get('data') or {}).items():
            code=str((x or {}).get('id') or '')
            key=self._compact(sym)
            if code and key: codes[key]=code; rev[code]=key
        if codes:
            self.coinw_pair_codes=codes; self.coinw_pair_symbols=rev; self.coinw_pair_refresh_mono=time.monotonic()

    async def _connection(self,venue):
        if venue=='Binance':return 'wss://stream.binance.com:9443/ws',20.0
        if venue=='Bybit':return 'wss://stream.bybit.com/v5/public/spot',20.0
        if venue=='OKX':return 'wss://ws.okx.com:8443/ws/v5/public',20.0
        if venue=='Bitget':return 'wss://ws.bitget.com/v2/ws/public',20.0
        if venue=='HTX':return 'wss://api.huobi.pro/ws',20.0
        if venue=='CoinW':return 'wss://ws.futurescw.com',20.0
        if venue=='KuCoin':return await self._kucoin_url()
        raise RuntimeError('UNSUPPORTED_WS')
    async def _control_gap(self,venue):
        if venue!='Binance':return
        now=time.monotonic(); wait=.23-(now-float(self.control_last[venue] or 0.0))
        if wait>0:await asyncio.sleep(wait)
        self.control_last[venue]=time.monotonic()

    async def _subscribe(self,ws,venue,symbols):
        if not symbols:return
        await self._control_gap(venue)
        rid=int(time.time()*1000)%1_000_000_000
        if venue=='Binance':
            await ws.send(json.dumps({'method':'SUBSCRIBE','params':[f'{s.lower()}@depth20@100ms' for s in symbols],'id':rid+1}))
        elif venue=='Bybit':
            await ws.send(json.dumps({'op':'subscribe','args':[f'orderbook.50.{s}' for s in symbols]}))
        elif venue=='OKX':
            await ws.send(json.dumps({'op':'subscribe','args':[{'channel':'books5','instId':self._dash(s)} for s in symbols]}))
        elif venue=='Bitget':
            await ws.send(json.dumps({'op':'subscribe','args':[{'instType':'SPOT','channel':'books5','instId':s} for s in symbols]}))
        elif venue=='HTX':
            for s in symbols:await ws.send(json.dumps({'sub':f'market.{s.lower()}.depth.step0','id':str(rid)}))
        elif venue=='KuCoin':
            for s in symbols:await ws.send(json.dumps({'id':str(rid),'type':'subscribe','topic':f'/spotMarket/level2Depth5:{self._dash(s)}','privateChannel':False,'response':True}))
        elif venue=='CoinW':
            await self._coinw_pairs()
            for s in symbols:
                code=self.coinw_pair_codes.get(self._compact(s))
                if code:await ws.send(json.dumps({'event':'sub','params':{'biz':'exchange','type':'depth_snapshot','pairCode':str(code)}}))
    def _bybit_apply(self,symbol,data,msg_type,source_ts=None):
        key=self._compact(symbol); state=self.bybit_state.setdefault(key,{'b':{},'a':{}})
        if msg_type=='snapshot':state={'b':{},'a':{}}; self.bybit_state[key]=state
        for side,field in (('b','b'),('a','a')):
            for p,q in data.get(field) or []:
                p=float(p); q=float(q)
                if q<=0:state[side].pop(p,None)
                else:state[side][p]=q
        bids=sorted(state['b'].items(),reverse=True)[:50]; asks=sorted(state['a'].items())[:50]
        self._store('Bybit',key,bids,asks,source_ts)
    def _handle(self,venue,raw):
        if isinstance(raw,(bytes,bytearray)):
            try:raw=gzip.decompress(raw).decode()
            except Exception:raw=raw.decode(errors='ignore')
        d=json.loads(raw) if isinstance(raw,str) else raw
        self.stats[venue]['messages']+=1; self.stats[venue]['last_message_mono']=time.monotonic()
        if venue=='Binance':
            if d.get('stream') and isinstance(d.get('data'),dict):
                x=d['data']; sym=str(d['stream']).split('@')[0]; self._store(venue,sym,x.get('bids') or x.get('b'),x.get('asks') or x.get('a'),x.get('E') or x.get('T'))
            elif d.get('s') and d.get('b') is not None:self._store(venue,d['s'],d.get('b'),d.get('a'),d.get('E') or d.get('T'))
        elif venue=='Bybit' and str(d.get('topic','')).startswith('orderbook.'):
            self._bybit_apply(str(d['topic']).split('.')[-1],d.get('data') or {},d.get('type'),d.get('ts') or (d.get('data') or {}).get('cts'))
        elif venue=='OKX' and d.get('arg',{}).get('channel')=='books5':
            x=(d.get('data') or [{}])[0]; self._store(venue,d['arg'].get('instId'),x.get('bids'),x.get('asks'),x.get('ts') or d.get('ts'))
        elif venue=='Bitget' and d.get('arg',{}).get('channel')=='books5':
            x=(d.get('data') or [{}])[0]; self._store(venue,d['arg'].get('instId'),x.get('bids'),x.get('asks'),x.get('ts') or d.get('ts'))
        elif venue=='HTX' and d.get('ch'):
            x=d.get('tick') or {}; sym=str(d['ch']).split('.')[1]; self._store(venue,sym,x.get('bids'),x.get('asks'),x.get('ts') or d.get('ts'))
        elif venue=='KuCoin' and d.get('type')=='message':
            x=d.get('data') or {}; sym=x.get('symbol') or str(d.get('topic','')).split(':')[-1]; self._store(venue,sym,x.get('bids'),x.get('asks'),x.get('timestamp') or x.get('ts') or d.get('ts'))
        elif venue=='CoinW' and d.get('type')=='depth_snapshot':
            x=d.get('data') or {}
            if isinstance(x,str):
                try:x=json.loads(x)
                except Exception:x={}
            code=str(d.get('pairCode') or ''); sym=self.coinw_pair_symbols.get(code)
            if sym:self._store(venue,sym,x.get('bids'),x.get('asks'),x.get('time'))
        if venue=='HTX' and d.get('ping'):return json.dumps({'pong':d['ping']})
        if venue=='KuCoin' and d.get('type')=='ping':return json.dumps({'id':d.get('id'),'type':'pong'})
        return None
    async def _unsubscribe(self,ws,venue,symbols):
        if not symbols:return
        await self._control_gap(venue)
        rid=int(time.time()*1000)%1_000_000_000
        if venue=='Binance':await ws.send(json.dumps({'method':'UNSUBSCRIBE','params':[f'{s.lower()}@depth20@100ms' for s in symbols],'id':rid}))
        elif venue=='Bybit':await ws.send(json.dumps({'op':'unsubscribe','args':[f'orderbook.50.{s}' for s in symbols]}))
        elif venue=='OKX':await ws.send(json.dumps({'op':'unsubscribe','args':[{'channel':'books5','instId':self._dash(s)} for s in symbols]}))
        elif venue=='Bitget':await ws.send(json.dumps({'op':'unsubscribe','args':[{'instType':'SPOT','channel':'books5','instId':s} for s in symbols]}))
        elif venue=='HTX':
            for s in symbols:await ws.send(json.dumps({'unsub':f'market.{s.lower()}.depth.step0','id':str(rid)}))
        elif venue=='KuCoin':
            for s in symbols:await ws.send(json.dumps({'id':str(rid),'type':'unsubscribe','topic':f'/spotMarket/level2Depth5:{self._dash(s)}','privateChannel':False,'response':True}))
        elif venue=='CoinW':
            await self._coinw_pairs()
            for s in symbols:
                code=self.coinw_pair_codes.get(self._compact(s))
                if code:await ws.send(json.dumps({'event':'unsub','params':{'biz':'exchange','type':'depth_snapshot','pairCode':str(code)}}))
    async def _venue_loop(self,venue):
        while True:
            try:
                wanted=self.wanted(venue)
                if not wanted:
                    self.stats[venue]['connected']=False; await asyncio.sleep(.25); continue
                url,ping_sec=await self._connection(venue); self.stats[venue]['reconnects']+=1
                async with connect(url,open_timeout=6,close_timeout=2,ping_interval=20,ping_timeout=10,max_size=2**22) as ws:
                    self.stats[venue]['connected']=True; subscribed=set(); last_app_ping=time.monotonic()
                    if venue=='Binance':
                        await self._control_gap(venue)
                        rid=int(time.time()*1000)%1_000_000_000
                        await ws.send(json.dumps({'method':'SET_PROPERTY','params':['combined',True],'id':rid}))
                    if venue=='KuCoin':
                        raw=await asyncio.wait_for(ws.recv(),timeout=3); self._handle(venue,raw)
                    while True:
                        current=set(self.wanted(venue)); missing=[s for s in current if s not in subscribed]; extra=[s for s in subscribed if s not in current]
                        if extra:
                            await self._unsubscribe(ws,venue,extra); subscribed.difference_update(extra)
                            for s in extra:self.refresh_requests.discard((venue,s))
                        if missing:
                            await self._subscribe(ws,venue,missing); subscribed.update(missing)
                            for s in missing:self.refresh_requests.discard((venue,s))
                        refresh=[s for s in subscribed if (venue,s) in self.refresh_requests]
                        if refresh:
                            await self._unsubscribe(ws,venue,refresh); subscribed.difference_update(refresh)
                            await asyncio.sleep(.02); await self._subscribe(ws,venue,refresh); subscribed.update(refresh)
                            self.refresh_sent+=len(refresh)
                            for s in refresh:self.refresh_requests.discard((venue,s))
                        try:
                            raw=await asyncio.wait_for(ws.recv(),timeout=.12); reply=self._handle(venue,raw)
                            if reply:await ws.send(reply)
                        except asyncio.TimeoutError:
                            if venue=='KuCoin' and time.monotonic()-last_app_ping>max(5.0,ping_sec*.7):
                                await ws.send(json.dumps({'id':str(int(time.time()*1000)),'type':'ping'})); last_app_ping=time.monotonic()
            except asyncio.CancelledError:raise
            except Exception as exc:
                st=self.stats[venue]; st['connected']=False; st['errors']+=1; st['last_error']=str(exc)[:240]
                await asyncio.sleep(min(5.0,0.5+st['errors']*.15))
    def status(self):
        now=time.monotonic(); fresh=[]; source_stale=[]
        for (v,s),x in self.books.items():
            info=self.age_info(v,s); age=float((info or {}).get('effective_age_ms') or 999999.0)
            if info and info.get('source_age_ms') is not None and float(info['source_age_ms'])>self.execution_stale_after*1000.0: source_stale.append(info)
            if age<=self.stale_after*1000:fresh.append({**info,'age_ms':age,'levels':min(len(x['bids']),len(x['asks']))})
        wanted_total=sum(len(self.wanted(v)) for v in self.SUPPORTED); coverage=(len(fresh)/wanted_total*100.0) if wanted_total else 0.0
        ready=[]
        for v in self.SUPPORTED:
            for sym in self.wanted(v):
                hit=self.get(v,sym,self.execution_stale_after)
                if hit:ready.append({'venue':v,'symbol':sym,'age_ms':round(hit[2],1)})
        coverage=(len(ready)/wanted_total*100.0) if wanted_total else 0.0
        venues={}
        for v in sorted(self.SUPPORTED):
            st=dict(self.stats[v]); lm=float(st.pop('last_message_mono',0) or 0); st['stream_age_ms']=round((now-lm)*1000,1) if lm else None; st['wanted']=len(self.wanted(v)); venues[v]=st
        return {'started':self.started,'stale_after_ms':self.stale_after*1000,'execution_stale_after_ms':self.execution_stale_after*1000,'strict_execution_age':True,'quiet_book_grace_ms':self.quiet_book_grace*1000,
                'fresh_count':len(fresh),'execution_ready_count':len(ready),'coverage_pct':round(coverage,1),
                'source_timestamp_guard':True,'source_stale_count':len(source_stale),'source_stale':sorted(source_stale,key=lambda x:x['source_age_ms'],reverse=True)[:50],
                'watched_count':len(self.desired),'fresh':sorted(fresh,key=lambda x:x['age_ms'])[:100],
                'execution_ready':sorted(ready,key=lambda x:x['age_ms'])[:100],'venues':venues,
                'freshness_recovery':{'waits':self.freshness_waits,'recovered':self.freshness_recovered,'timeouts':self.freshness_timeouts},
                'ws_snapshot_refresh':{'queued':len(self.refresh_requests),'sent':self.refresh_sent,'throttled':self.refresh_throttled}}

hot_book_cache=HotBookCache()

