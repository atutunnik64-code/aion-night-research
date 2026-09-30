import asyncio,hashlib,json,time
from pathlib import Path
from app.services.research_frontier import research_frontier
ROOT=Path(__file__).parents[2];DATA=ROOT/'data';LOG=DATA/'research_frontier_history.jsonl';STATE=DATA/'research_frontier_history_state.json'

class ResearchFrontierHistory:
    def __init__(self):
        self.enabled=True;self.interval=900.;self.task=None;self.last_error=None;self.last_refresh=None
        try:self.state=json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:self.state={'last_signature':None,'snapshots':0,'created_at':time.time()}
    @staticmethod
    def _compact(x):
        rows=[]
        for r in x.get('rows') or []:
            f=r.get('forward') or {};g=f.get('gate') or {}
            rows.append({'cluster':r.get('cluster'),'representative':r.get('representative'),'kind':r.get('kind'),
                         'observations':f.get('observations'),'events':f.get('events'),'resolved':f.get('resolved'),
                         'return_pct':f.get('return_pct'),'max_dd_pct':f.get('max_dd_pct'),'ready':f.get('ready'),
                         'blocker':r.get('blocker'),'gate':g,'progress':f.get('progress') or []})
        s=x.get('summary') or {}
        return {'portfolio_eligible_count':s.get('portfolio_eligible_count'),'portfolio_weights':s.get('portfolio_weights'),
                'portfolio_equity':s.get('portfolio_equity'),'live_enabled':False,'rows':rows,
                'overlays':x.get('overlays') or [],'diagnostics':x.get('diagnostics') or [],
                'data_integrity':x.get('data_integrity') or {}}
    @staticmethod
    def _sig(x):return hashlib.sha1(json.dumps(x,sort_keys=True,default=str).encode()).hexdigest()
    def _refresh_sync(self):
        snap=self._compact(research_frontier.status());sig=self._sig(snap);added=False
        if sig!=self.state.get('last_signature'):
            row={'ts':time.time(),'signature':sig,'snapshot':snap}
            with LOG.open('a',encoding='utf-8') as f:f.write(json.dumps(row,ensure_ascii=False,default=str)+'\n')
            self.state['last_signature']=sig;self.state['snapshots']=int(self.state.get('snapshots') or 0)+1;added=True
        self.state['last_refresh']=time.time();STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
        return added
    async def refresh(self):
        try:added=await asyncio.to_thread(self._refresh_sync);self.last_error=None
        except Exception as exc:added=False;self.last_error=str(exc)[:500]
        self.last_refresh=time.time();return {**self.status(),'added':added}
    def status(self):
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'APPEND_ONLY_FRONTIER_HISTORY',
                'live_enabled':False,'interval_seconds':self.interval,'snapshots':int(self.state.get('snapshots') or 0),
                'last_refresh':self.last_refresh or self.state.get('last_refresh'),'last_error':self.last_error}
    async def start(self):
        if self.task and not self.task.done():return
        await self.refresh();self.task=asyncio.create_task(self._loop(),name='research-frontier-history')
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
research_frontier_history=ResearchFrontierHistory()