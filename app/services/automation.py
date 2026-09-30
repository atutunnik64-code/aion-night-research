from __future__ import annotations
import asyncio,time
from app.services.paper_engine import paper_engine
from app.services.execution_engine import execution_engine
from app.services.autopilot import autopilot
from app.services.live_executor import live_executor
from app.services.hot_book_cache import hot_book_cache
from app.services.hot_signal_engine import hot_signal_engine
from app.services.order_rules import order_rules_registry

class AutomationController:
    def __init__(self,radar):
        self.radar=radar; self.tasks=[]; self.enabled=True
        self.amount=25.0; self.broad_interval=30.0; self.hot_interval=6.0; self.latency_interval=2.0; self.latency_hot_interval=1.0; self.classic_hot_interval=1.0; self.ws_signal_interval=0.25
        self.last_broad=0.0; self.last_hot=0.0; self.last_latency=0.0; self.last_latency_hot=0.0; self.last_classic_hot=0.0; self.last_ws_signal=0.0
        self.last_error=None; self.latest_broad=None; self.latest_hot=None; self.latest_latency=None
        self.market_lock=asyncio.Lock(); self.last_warm={'CLASSIC':0,'LATENCY':0,'DISCOVERY':0}; self.rule_warm_task=None; self.last_rule_warm=0

    def _warm_rows(self,rows,mode,limit=60,priority=80,ttl=240):
        legs=[]; amount=min(float(self.amount),1000.0)
        for row in list(rows or [])[:limit]:
            try: plan=live_executor.plan(row,mode,amount)
            except Exception: continue
            if len(plan)==2:legs.extend(plan)
        if legs:
            hot_book_cache.watch_legs(legs,priority=priority,ttl=ttl)
            if self.rule_warm_task is None or self.rule_warm_task.done():
                self.rule_warm_task=asyncio.create_task(order_rules_registry.prewarm(legs))
                self.last_rule_warm=time.time()
        self.last_warm[mode if mode in self.last_warm else 'DISCOVERY']=len(legs)//2
        return len(legs)//2

    async def start(self):
        if any(not t.done() for t in self.tasks): return
        self.tasks=[asyncio.create_task(self._broad_loop()),asyncio.create_task(self._hot_loop()),asyncio.create_task(self._latency_loop()),asyncio.create_task(self._latency_hot_loop()),asyncio.create_task(self._classic_hot_loop()),asyncio.create_task(self._ws_signal_loop())]

    async def _broad_loop(self):
        await asyncio.sleep(1.0)
        while True:
            if not self.enabled:
                await asyncio.sleep(1); continue
            try:
                async with self.market_lock:
                    self.latest_broad=await self.radar.scan(self.amount,'live'); self.last_broad=time.time()
                self._warm_rows(self.latest_broad.get('prefunded_candidates',[]),'CLASSIC',60,45,420)
                self._warm_rows(self.latest_broad.get('classic_watch_candidates',[]),'CLASSIC',60,100,420)
                self._warm_rows(self.latest_broad.get('classic_opportunities',[]),'CLASSIC',40,150,420)
                hot_signal_engine.refresh_classic(self.radar.classic_hot_candidates)
                self._warm_rows(self.latest_broad.get('full_loop_opportunities',[]),'FULL_NOW',30,255,420)
                self._warm_rows(self.latest_broad.get('series_ready_opportunities',[]),'BATCH_READY',30,255,420)
                if autopilot.allow_strategy('FULL_NOW'): await execution_engine.execute_rows_auto(self.latest_broad.get('full_loop_opportunities',[]),'FULL_NOW')
                if autopilot.allow_strategy('BATCH_READY'): await execution_engine.execute_rows_auto(self.latest_broad.get('series_ready_opportunities',[]),'BATCH_READY')
                self.last_error=None
            except Exception as exc:self.last_error='BROAD: '+str(exc)[:450]
            await asyncio.sleep(self.broad_interval)

    async def _hot_loop(self):
        await asyncio.sleep(3.0)
        while True:
            if not self.enabled or not self.radar.hot_candidates:
                await asyncio.sleep(1); continue
            if self.market_lock.locked():
                await asyncio.sleep(1); continue
            try:
                async with self.market_lock:
                    self.latest_hot=await self.radar.hot_scan(); self.last_hot=time.time()
                if self.latest_hot.get('ready'):
                    full_rows=self.latest_hot.get('full_loop_opportunities',[])
                    batch_rows=self.latest_hot.get('series_ready_opportunities',[])
                    self._warm_rows(full_rows,'FULL_NOW',30,245,90)
                    self._warm_rows(batch_rows,'BATCH_READY',30,245,90)
                    if full_rows or batch_rows: await asyncio.sleep(.06)
                    if autopilot.allow_strategy('FULL_NOW'): await execution_engine.execute_rows_auto(full_rows,'FULL_NOW')
                    if autopilot.allow_strategy('BATCH_READY'): await execution_engine.execute_rows_auto(batch_rows,'BATCH_READY')
                self.last_error=None
            except Exception as exc:self.last_error='HOT: '+str(exc)[:450]
            await asyncio.sleep(self.hot_interval)

    async def _latency_loop(self):
        await asyncio.sleep(1.5)
        while True:
            if not self.enabled:
                await asyncio.sleep(1); continue
            started=time.time()
            try:
                self.latest_latency=await self.radar.latency_scan(self.amount); self.last_latency=time.time()
                self._warm_rows(self.latest_latency.get('latency_candidates',[]),'LATENCY',50,90,300)
                self._warm_rows(self.latest_latency.get('latency_opportunities',[]),'LATENCY',50,160,300)
                latency_pool=list(self.latest_latency.get('latency_opportunities') or [])+[x for x in (self.latest_latency.get('latency_candidates') or []) if x.get('repeatable')]
                hot_signal_engine.refresh_latency(latency_pool)
                self.last_error=None
            except Exception as exc:self.last_error='LATENCY: '+str(exc)[:450]
            elapsed=time.time()-started
            await asyncio.sleep(max(.5,self.latency_interval-elapsed))

    async def _latency_hot_loop(self):
        await asyncio.sleep(6.0)
        while True:
            if not self.enabled or not self.latest_latency:
                await asyncio.sleep(.5); continue
            try:
                ready=list(self.latest_latency.get('latency_opportunities') or [])
                candidates=[x for x in (self.latest_latency.get('latency_candidates') or []) if x.get('repeatable') and x.get('latency_evidence')]
                pool={x.get('signature'):x for x in ready+candidates if x.get('signature')}
                rows=sorted(pool.values(),key=lambda x:(float(x.get('net_pct') or 0),float(x.get('aion_score') or 0)),reverse=True)[:20]
                self._warm_rows(rows,'LATENCY',20,170,300)
                if rows:self.last_latency_hot=time.time()
            except Exception as exc:self.last_error='LATENCY_HOT: '+str(exc)[:450]
            await asyncio.sleep(self.latency_hot_interval)

    async def _classic_hot_loop(self):
        await asyncio.sleep(8.0)
        while True:
            if not self.enabled or not self.radar.classic_hot_candidates:
                await asyncio.sleep(.5); continue
            try:
                rows=sorted(self.radar.classic_hot_candidates,key=lambda x:(float(x.get('net_pct') or 0),float(x.get('aion_score') or 0)),reverse=True)[:24]
                self._warm_rows(rows,'CLASSIC',24,170,300)
                if rows:self.last_classic_hot=time.time()
            except Exception as exc:self.last_error='CLASSIC_HOT: '+str(exc)[:450]
            await asyncio.sleep(self.classic_hot_interval)

    async def _ws_signal_loop(self):
        await asyncio.sleep(5.0)
        while True:
            if not self.enabled:
                await asyncio.sleep(.5); continue
            started=time.perf_counter()
            try:
                batch=hot_signal_engine.scan(limit=12)
                jobs=[]
                if batch['classic'] and autopilot.allow_strategy('CLASSIC'):jobs.append(execution_engine.probe_rows_auto(batch['classic'],'CLASSIC'))
                if batch['latency'] and autopilot.allow_strategy('LATENCY'):jobs.append(execution_engine.probe_rows_auto(batch['latency'],'LATENCY'))
                errors=[]
                if jobs:
                    results=await asyncio.gather(*jobs,return_exceptions=True)
                    hot_signal_engine.executions+=sum(len(x) for x in results if isinstance(x,list))
                    errors=[str(x)[:220] for x in results if isinstance(x,Exception)]
                self.last_ws_signal=time.time(); self.last_error=('WS_SIGNAL: '+errors[0]) if errors else None
            except Exception as exc:self.last_error='WS_SIGNAL: '+str(exc)[:450]
            elapsed=time.perf_counter()-started
            await asyncio.sleep(max(.05,self.ws_signal_interval-elapsed))

    def snapshot(self):
        return {'broad':self.latest_broad,'hot':self.latest_hot,'latency':self.latest_latency,'status':self.status()}

    def status(self):
        return {'enabled':self.enabled,'amount':self.amount,
                'broad_interval':self.broad_interval,'hot_interval':self.hot_interval,'latency_interval':self.latency_interval,'latency_hot_interval':self.latency_hot_interval,
                'last_broad':self.last_broad,'last_hot':self.last_hot,'last_latency':self.last_latency,'last_latency_hot':self.last_latency_hot,'last_classic_hot':self.last_classic_hot,'last_ws_signal':self.last_ws_signal,
                'last_error':self.last_error,'workers':{'broad':True,'hot':True,'latency':True,'latency_hot':True,'classic_hot':True,'ws_signal':True},
                'broad_counts':{'full':(self.latest_broad or {}).get('full_loop_count',0),'batch':(self.latest_broad or {}).get('series_ready_count',0)},
                'hot_counts':{'full':(self.latest_hot or {}).get('full_loop_count',0),'batch':(self.latest_hot or {}).get('series_ready_count',0)},
                'classic_counts':{'ready':(self.latest_broad or {}).get('classic_count',0),'watch':len(self.radar.classic_hot_candidates)},
                'latency_counts':{'ready':(self.latest_latency or {}).get('latency_ready_count',0),'repeatable':(self.latest_latency or {}).get('latency_repeatable_count',0)},
                'hot_book_warm':self.last_warm,'hot_books':hot_book_cache.status(),'order_rules':order_rules_registry.status(),'last_rule_warm':self.last_rule_warm,'ws_signal_interval':self.ws_signal_interval,'ws_signals':hot_signal_engine.status(),
                'paper':paper_engine.summary(25),'execution':execution_engine.summary(25),'autopilot':autopilot.status()}
