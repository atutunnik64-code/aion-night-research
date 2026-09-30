import asyncio,json,time
from collections import Counter
from pathlib import Path
from app.services.execution_engine import execution_engine
ROOT=Path(__file__).parents[2];OUT=ROOT/'data'/'arbitrage_venue_dependency_v1.json'

class ArbitrageVenueDependencyMonitor:
    def __init__(self):self.enabled=True;self.interval=900.;self.task=None;self.last_error=None;self.last_refresh=None;self.latest={}
    @staticmethod
    def _summary(z):
        c=Counter(x['venue_set'] for x in z);n=len(z);h=sum(1 for x in z if 'HTX' in x['venues'])
        return {'full_loops':n,'venue_sets':dict(c),'htx_loops':h,'htx_share':(h/n if n else 0.0),'no_htx_loops':n-h}
    def _refresh_sync(self):
        q="SELECT id,ts,payload FROM executions WHERE environment='DEMO' AND strategy='FULL_NOW' AND status='FILLED_BOTH' ORDER BY id"
        rows=execution_engine.db.execute(q).fetchall();items=[]
        for r in rows:
            try:p=json.loads(r['payload'] or '{}');row=p.get('row') or {}
            except Exception:continue
            if not bool(row.get('full_loop_confirmed')):continue
            v=set(row.get('execution_venues') or [])
            for z in row.get('steps') or []:
                if z.get('venue'):v.add(str(z['venue']))
            for k in ('buy_venue','sell_venue'):
                if row.get(k):v.add(str(row[k]))
            items.append({'id':int(r['id']),'venues':sorted(v),'venue_set':'|'.join(sorted(v))})
        mx=max([x['id'] for x in items],default=0);recent=[x for x in items if x['id']>max(0,mx-1500)]
        out={'version':'ARBITRAGE_VENUE_DEPENDENCY_V1','generated_at':time.time(),'all':self._summary(items),
             'recent_window_execution_ids':1500,'recent':self._summary(recent),
             'capital_policy':'HTX_RESEARCH_QUARANTINE_NOT_CAPITAL_READY','live_enabled':False}
        OUT.write_text(json.dumps(out,ensure_ascii=False,indent=2),encoding='utf-8');self.latest=out;return out
    async def refresh(self):
        try:self.latest=await asyncio.to_thread(self._refresh_sync);self.last_error=None
        except Exception as exc:self.last_error=str(exc)[:500]
        self.last_refresh=time.time();return self.status()
    def status(self):
        x=self.latest
        if not x:
            try:x=json.loads(OUT.read_text(encoding='utf-8'))
            except Exception:x={}
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'DEMO_AUDIT_ONLY','live_enabled':False,
                'interval_seconds':self.interval,'all':x.get('all') or {},'recent':x.get('recent') or {},
                'capital_policy':x.get('capital_policy'),'last_refresh':self.last_refresh,'last_error':self.last_error}
    async def start(self):
        if self.task and not self.task.done():return
        await self.refresh();self.task=asyncio.create_task(self._loop(),name='arbitrage-venue-dependency-monitor')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None
    async def _loop(self):
        while True:
            await asyncio.sleep(max(300.,self.interval))
            if self.enabled:await self.refresh()
arbitrage_venue_dependency_monitor=ArbitrageVenueDependencyMonitor()