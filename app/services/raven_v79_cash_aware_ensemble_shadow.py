from __future__ import annotations
import asyncio,json,time
from pathlib import Path
from app.services.raven_v70_regime_shadow import raven_v70_regime_shadow
from app.services.raven_smart_positioning_v61_shadow import raven_smart_positioning_v61_shadow

ROOT=Path(__file__).parents[2]
STATE=ROOT/'data'/'raven_v79_cash_aware_ensemble_shadow.json'
ALLOC_COST=.0012

class RavenV79CashAwareEnsembleShadow:
    def __init__(self):
        self.enabled=True;self.interval=300.0;self.task=None
        self.last_error=None;self.last_refresh=None;self.latest={};self.state=self._load()
    @staticmethod
    def _blank():
        return {'equity':100.0,'peak':100.0,'max_dd_pct':0.0,'last_bar_ts':None,
                'last_v70_equity':None,'last_smart_equity':None,'smart_weight':.5,'v70_weight':.5,
                'observation_count':0,'allocator_cost_sum':0.0,'history':[],'started_at':time.time()}
    def _load(self):
        x=self._blank()
        try:x.update(json.loads(STATE.read_text(encoding='utf-8')))
        except Exception:pass
        return x
    def _save(self):
        STATE.parent.mkdir(parents=True,exist_ok=True)
        STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    @staticmethod
    def _component_snapshot():
        a=raven_v70_regime_shadow.status();b=raven_smart_positioning_v61_shadow.status()
        la=a.get('latest') or {};lb=b.get('latest') or {}
        ba=str(la.get('bar_ts') or '');bb=str(lb.get('bar_ts') or '')
        if not ba or not bb or ba!=bb:return None
        target=la.get('target_weights') or {}
        active=sum(abs(float(v)) for v in target.values())>1e-9
        return {'bar_ts':ba,'v70_equity':float(a.get('equity') or 100.0),
                'smart_equity':float(b.get('equity') or 100.0),'v70_active':active,
                'v70_obs':int(a.get('observation_count') or 0),'smart_obs':int(b.get('observation_count') or 0)}

    def _initialize(self,x):
        sw,vw=(.5,.5) if x['v70_active'] else (1.0,0.0)
        self.state.update({'last_bar_ts':x['bar_ts'],'last_v70_equity':x['v70_equity'],
                           'last_smart_equity':x['smart_equity'],'smart_weight':sw,'v70_weight':vw})
        self._save()

    def _apply_bar(self,x):
        p70=max(float(self.state.get('last_v70_equity') or x['v70_equity']),1e-9)
        psp=max(float(self.state.get('last_smart_equity') or x['smart_equity']),1e-9)
        r70=x['v70_equity']/p70-1.0;rsp=x['smart_equity']/psp-1.0
        sw=float(self.state.get('smart_weight') or 0.0);vw=float(self.state.get('v70_weight') or 0.0)
        raw=sw*rsp+vw*r70;nsw,nvw=((.5,.5) if x['v70_active'] else (1.0,0.0))
        meta_cost=ALLOC_COST*(abs(nsw-sw)+abs(nvw-vw))
        eq=float(self.state.get('equity') or 100.0)*max(.001,1.0+raw-meta_cost)
        peak=max(float(self.state.get('peak') or eq),eq);dd=(eq/peak-1.0)*100.0
        self.state.update({'equity':eq,'peak':peak,'max_dd_pct':min(float(self.state.get('max_dd_pct') or 0.0),dd),
                           'last_bar_ts':x['bar_ts'],'last_v70_equity':x['v70_equity'],'last_smart_equity':x['smart_equity'],
                           'smart_weight':nsw,'v70_weight':nvw,
                           'observation_count':int(self.state.get('observation_count') or 0)+1,
                           'allocator_cost_sum':float(self.state.get('allocator_cost_sum') or 0.0)+meta_cost})
        row={'bar_ts':x['bar_ts'],'equity':round(eq,6),'smart_ret_pct':round(rsp*100,6),
             'v70_ret_pct':round(r70*100,6),'raw_portfolio_pct':round(raw*100,6),
             'allocator_cost_pct':round(meta_cost*100,6),'next_smart_weight':nsw,'next_v70_weight':nvw}
        h=list(self.state.get('history') or []);h.append(row);self.state['history']=h[-300:]
        self._save();return row

    async def refresh(self):
        try:
            x=self._component_snapshot()
            if not x:
                self.latest={'strategy':'v79_cash_aware_dual','waiting_for':'SAME_CLOSED_8H_BAR'}
                self.last_error=None;self.last_refresh=time.time();return self.status()
            if not self.state.get('last_bar_ts'):
                self._initialize(x);row=None
            elif x['bar_ts']!=str(self.state.get('last_bar_ts')):
                row=self._apply_bar(x)
            else:row=None
            self.last_error=None;self.last_refresh=time.time()
            eq=float(self.state.get('equity') or 100.0)
            self.latest={'strategy':'v79_cash_aware_dual','bar_ts':x['bar_ts'],'paper_equity':eq,
                         'paper_return_pct':(eq/100.0-1.0)*100.0,'smart_weight':self.state.get('smart_weight'),
                         'v70_weight':self.state.get('v70_weight'),'v70_active':x['v70_active'],
                         'component_observations':{'smart':x['smart_obs'],'v70':x['v70_obs']},'last_bar':row}
        except Exception as exc:
            self.last_error=str(exc)[:300];self.last_refresh=time.time()
        return self.status()

    def status(self):
        eq=float(self.state.get('equity') or 100.0);obs=int(self.state.get('observation_count') or 0)
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'PAPER_SHADOW',
                'strategy':'v79_cash_aware_dual','future_only':True,'promotion_eligible':False,
                'live_enabled':False,'grid':False,'martingale':False,'dca':False,
                'locked_rule':'50_50_WHEN_V70_ACTIVE__100_SMART_WHEN_V70_CASH',
                'allocator_cost_per_turnover':ALLOC_COST,'equity':round(eq,6),
                'return_pct':round((eq/100.0-1.0)*100.0,4),'max_dd_pct':round(float(self.state.get('max_dd_pct') or 0.0),4),
                'observation_count':obs,'phase':'WARMUP' if obs<24 else 'FUTURE_VALIDATION',
                'latest':self.latest,'last_refresh':self.last_refresh,'last_error':self.last_error}
    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name='raven-v79-cash-aware-ensemble-shadow')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None;self._save()
    async def _loop(self):
        while True:
            await self.refresh()
            await asyncio.sleep(max(300.0,self.interval))

raven_v79_cash_aware_ensemble_shadow=RavenV79CashAwareEnsembleShadow()
