from __future__ import annotations
import asyncio,json,time
from pathlib import Path

ROOT=Path(__file__).parents[2]
V47=ROOT/'data'/'raven_v47_shadow_state.json'
V55=ROOT/'data'/'raven_positioning_v55_shadow.json'
STATE=ROOT/'data'/'raven_blend_v58_shadow.json'

class RavenBlendV58Shadow:
    def __init__(self):
        self.enabled=True; self.interval=300.0; self.task=None
        self.last_error=None; self.last_refresh=None
        self.state=self._load(); self.latest={}
    def _load(self):
        try:return json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:return {'mode':'PAPER_SHADOW','equity':100.0,'peak':100.0,'max_dd_pct':0.0,'observations':0,'history':[]}
    def _save(self):
        STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    @staticmethod
    def _json(path):
        return json.loads(path.read_text(encoding='utf-8'))
    async def refresh(self):
        try:
            a=self._json(V47); b=self._json(V55)
            ah={x['bar_ts']:float(x['equity']) for x in (a.get('equity_history') or [])}
            bh={x['ts']:float(x['equity']) for x in (b.get('history') or [])}
            common=sorted(set(ah).intersection(bh))
            if not common: raise RuntimeError('NO_COMMON_V47_V55_BARS')
            latest=common[-1]
            if not self.state.get('started_bar_ts'):
                self.state.update({'started_bar_ts':latest,'last_bar_ts':latest,
                    'base_v47_equity':ah[latest],'base_v55_equity':bh[latest],
                    'equity':100.0,'peak':100.0,'max_dd_pct':0.0,'observations':0,'history':[],'started_at':time.time()})
                self._save()
            base47=float(self.state['base_v47_equity']); base55=float(self.state['base_v55_equity'])
            last=str(self.state.get('last_bar_ts') or self.state['started_bar_ts'])
            for ts in [x for x in common if x>last]:
                eq=50.0*(ah[ts]/base47)+50.0*(bh[ts]/base55)
                peak=max(float(self.state.get('peak') or 100.0),eq)
                dd=(eq/peak-1)*100.0
                self.state['equity']=eq; self.state['peak']=peak
                self.state['max_dd_pct']=min(float(self.state.get('max_dd_pct') or 0.0),dd)
                self.state['observations']=int(self.state.get('observations') or 0)+1
                hist=self.state.get('history') or []
                hist.append({'ts':ts,'equity':eq,'v47_equity':ah[ts],'v55_equity':bh[ts],'drawdown_pct':dd})
                self.state['history']=hist[-300:]; self.state['last_bar_ts']=ts; last=ts
            self._save()
            eq=float(self.state.get('equity') or 100.0)
            self.latest={'mode':'PAPER_SHADOW','version':'v58','allocation':{'v47':0.5,'v55':0.5},
                'equity':round(eq,6),'return_pct':round(eq-100.0,4),'max_dd_pct':round(float(self.state.get('max_dd_pct') or 0),4),
                'observations':int(self.state.get('observations') or 0),'last_bar_ts':self.state.get('last_bar_ts'),
                'future_only':True,'promotion_eligible':False,'live_enabled':False,'grid':False,'martingale':False,
                'component_latest':{'v47':ah[latest],'v55':bh[latest]}}
            self.last_refresh=time.time(); self.last_error=None
        except Exception as exc:self.last_error=str(exc)[:300]
        return self.status()
    def status(self):
        return {'ok':self.last_error is None,'enabled':self.enabled,'last_refresh':self.last_refresh,'last_error':self.last_error,'latest':self.latest}
    async def start(self):
        if self.task and not self.task.done():return
        await self.refresh(); self.task=asyncio.create_task(self._loop(),name='raven-blend-v58-shadow')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None
    async def _loop(self):
        await asyncio.sleep(70)
        while True:
            await self.refresh(); await asyncio.sleep(max(300.0,self.interval))

raven_blend_v58_shadow=RavenBlendV58Shadow()
