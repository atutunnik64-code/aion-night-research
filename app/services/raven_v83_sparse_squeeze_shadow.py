from __future__ import annotations
import asyncio,json,time
from pathlib import Path
from app.services.raven_v70_regime_shadow import raven_v70_regime_shadow
from app.services.raven_v47_shadow import raven_v47_shadow
ROOT=Path(__file__).parents[2]
STATE=ROOT/'data'/'raven_v83_sparse_squeeze_shadow.json'
ALPHA=.25; THRESHOLD=.23328; COST70=.0012; COSTSQ=.0011; FUND=.00005

class RavenV83SparseSqueezeShadow:
    def __init__(self):
        self.enabled=True;self.interval=900.;self.task=None;self.last_error=None;self.last_refresh=None;self.latest={}
        self.state=self._load()
    @staticmethod
    def _blank():
        return {'equity':100.,'peak':100.,'max_dd_pct':0.,'weights':{},'last_prices':{},'last_bar_ts':None,
                'strong_history':[],'observations':0,'active_observations':0,'costs':0.,'funding':0.,'history':[]}
    def _load(self):
        x=self._blank()
        try:x.update(json.loads(STATE.read_text(encoding='utf-8')))
        except Exception:pass
        return x
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    @staticmethod
    def _turn(a,b):
        keys=set(a)|set(b);return sum(abs(float(b.get(k,0))-float(a.get(k,0))) for k in keys)
    def _mark(self,prices):
        old=self.state.get('last_prices') or {};w=self.state.get('weights') or {};eq=float(self.state.get('equity') or 100.)
        if old:
            pnl=0.;gross_short=0.
            for s,x in w.items():
                a=float(old.get(s) or 0);b=float(prices.get(s) or 0)
                if a>0 and b>0:pnl+=float(x)*(b/a-1.)
                if float(x)<0:gross_short+=abs(float(x))
            eq*=max(.001,1+pnl);fund=eq*gross_short*FUND;eq-=fund;self.state['funding']+=fund
        self.state['equity']=eq;self.state['last_prices']={k:float(v) for k,v in prices.items() if float(v)>0}
        self.state['peak']=max(float(self.state.get('peak') or eq),eq)
        self.state['max_dd_pct']=min(float(self.state.get('max_dd_pct') or 0.),(eq/self.state['peak']-1)*100)
    def _rebalance(self,target):
        old=self.state.get('weights') or {};turn=self._turn(old,target)
        filler_syms=set((raven_v47_shadow.latest or {}).get('squeeze_raw') or {})
        trsq=sum(abs(float(target.get(k,0))-float(old.get(k,0))) for k in set(old)|set(target) if k in filler_syms)
        tr70=max(0.,turn-trsq);eq=float(self.state.get('equity') or 100.)
        cost=eq*(tr70*COST70+trsq*COSTSQ);self.state['equity']=eq-cost;self.state['costs']+=cost
        self.state['weights']={k:float(v) for k,v in target.items() if abs(float(v))>1e-12}
        return turn,cost
    async def refresh(self):
        try:
            v70=(raven_v70_regime_shadow.latest or {});v47=(raven_v47_shadow.latest or {})
            bar70=str(v70.get('bar_ts') or '');bar47=str(v47.get('bar_ts') or '')
            if not bar70 or not bar47 or bar70!=bar47:
                self.latest={'waiting_for':'same_closed_bar','v70_bar':bar70,'v47_bar':bar47};self.last_error=None;self.last_refresh=time.time();return self.status()
            prices=dict(raven_v47_shadow.state.get('last_closes') or {})
            if not prices:raise RuntimeError('V83_NO_PRICES')
            self._mark(prices)
            if bar70!=str(self.state.get('last_bar_ts') or ''):
                base=dict(v70.get('target_weights') or {});sq=dict(v47.get('squeeze_raw') or {})
                strong=sum(abs(float(x)) for x in sq.values())>=THRESHOLD
                h=list(self.state.get('strong_history') or []);h.append({'bar_ts':bar70,'strong':strong});h=h[-3:];self.state['strong_history']=h
                confirmed=len(h)>=2 and bool(h[-1]['strong']) and bool(h[-2]['strong'])
                filler={} if base or not confirmed else {k:ALPHA*float(x) for k,x in sq.items() if abs(float(x))>1e-12}
                target=dict(base)
                for k,x in filler.items():target[k]=target.get(k,0.)+x
                turn,cost=self._rebalance(target);self.state['last_bar_ts']=bar70
                self.state['observations']=int(self.state.get('observations') or 0)+1
                self.state['active_observations']=int(self.state.get('active_observations') or 0)+int(bool(filler))
                row={'bar_ts':bar70,'equity':self.state['equity'],'strong':strong,'confirmed':confirmed,'base':base,'filler':filler,'turnover':turn,'cost':cost}
                hh=list(self.state.get('history') or []);hh.append(row);self.state['history']=hh[-300:];self._save()
            self.last_error=None;self.last_refresh=time.time();eq=float(self.state.get('equity') or 100.)
            self.latest={'strategy':'v83_sparse_squeeze','bar_ts':bar70,'paper_equity':eq,'paper_return_pct':eq-100.,
                         'weights':self.state.get('weights') or {},'strong_history':self.state.get('strong_history') or []}
        except Exception as exc:self.last_error=str(exc)[:300];self.last_refresh=time.time()
        return self.status()
    def status(self):
        eq=float(self.state.get('equity') or 100.);obs=int(self.state.get('observations') or 0)
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'PAPER_SHADOW','strategy':'v83_sparse_squeeze',
                'future_only':True,'promotion_eligible':False,'live_enabled':False,'grid':False,'martingale':False,'dca':False,
                'locked_config':{'alpha':ALPHA,'strength_threshold':THRESHOLD,'persistence_bars':2},
                'equity':round(eq,6),'return_pct':round(eq-100.,4),'max_dd_pct':round(float(self.state.get('max_dd_pct') or 0.),4),
                'observation_count':obs,'active_observation_count':int(self.state.get('active_observations') or 0),
                'phase':'WARMUP' if obs<24 else 'FUTURE_VALIDATION','latest':self.latest,
                'last_refresh':self.last_refresh,'last_error':self.last_error}
    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name='raven-v83-sparse-squeeze-shadow')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None;self._save()
    async def _loop(self):
        while True:
            await self.refresh();await asyncio.sleep(max(300.,self.interval))

raven_v83_sparse_squeeze_shadow=RavenV83SparseSqueezeShadow()
