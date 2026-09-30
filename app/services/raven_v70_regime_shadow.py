from __future__ import annotations
import asyncio,json,time
from pathlib import Path
from app.services.raven_turbo_shadow import raven_turbo_shadow,COST,FUND,SYMBOLS

ROOT=Path(__file__).parents[2]
STATE=ROOT/'data'/'raven_v70_regime_shadow.json'
CONFIG={'breadth_gate':0.45,'momentum_gate':0.0,'slots':3,'ratio_gate':0.75,'sleeves':3}

class RavenV70RegimeShadow:
    def __init__(self):
        self.enabled=True;self.interval=900.0;self.task=None;self.last_error=None;self.last_refresh=None
        self.latest={};self.state=self._load()
    @staticmethod
    def _blank():
        return {'equity':100.0,'peak':100.0,'max_dd_pct':0.0,'weights':{},'last_prices':{},
                'last_mark_ts':None,'last_bar_ts':None,'costs':0.0,'funding':0.0,
                'observation_count':0,'diversified_count':0,'sleeves':[],'history':[],'started_at':time.time()}
    def _load(self):
        x=self._blank()
        try:x.update(json.loads(STATE.read_text(encoding='utf-8')))
        except Exception:pass
        return x
    def _save(self):
        STATE.parent.mkdir(parents=True,exist_ok=True)
        STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    def _mark(self,prices,now):
        old=self.state.get('last_prices') or {};w=self.state.get('weights') or {}
        eq=float(self.state.get('equity') or 100.0)
        if old:
            pnl=0.0
            for s,x in w.items():
                a=float(old.get(s) or 0);b=float(prices.get(s) or 0)
                if a>0 and b>0:pnl+=float(x)*(b/a-1.0)
            eq*=max(.01,1.0+pnl)
            hours=max(0.0,(now-float(self.state.get('last_mark_ts') or now))/3600.0)
            gross=sum(abs(float(x)) for x in w.values())
            fund=eq*gross*FUND*(hours/8.0);eq-=fund
            self.state['funding']=float(self.state.get('funding') or 0)+fund
        self.state['equity']=eq
        self.state['last_prices']={k:float(v) for k,v in prices.items()}
        self.state['last_mark_ts']=now
        self.state['peak']=max(float(self.state.get('peak') or eq),eq)
        self.state['max_dd_pct']=min(float(self.state.get('max_dd_pct') or 0.0),(eq/self.state['peak']-1.0)*100.0)

    @staticmethod
    def _average_targets(rows):
        if not rows:return {}
        keys=set().union(*(set((r.get('target') or {}).keys()) for r in rows));n=float(len(rows));out={}
        for k in keys:
            v=sum(float((r.get('target') or {}).get(k,0.0)) for r in rows)/n
            if abs(v)>1e-9:out[k]=v
        return out
    def _rebalance(self,target,bar_ts):
        old={k:float(v) for k,v in (self.state.get('weights') or {}).items()}
        keys=set(old)|set(target)
        turn=sum(abs(float(target.get(k,0))-float(old.get(k,0))) for k in keys)
        eq=float(self.state.get('equity') or 100.0)
        cost=eq*turn*COST;eq-=cost
        self.state['equity']=eq
        self.state['costs']=float(self.state.get('costs') or 0)+cost
        self.state['weights']={k:float(v) for k,v in target.items() if abs(float(v))>1e-9}
        self.state['last_bar_ts']=str(bar_ts)
        row={'ts':time.time(),'bar_ts':str(bar_ts),'equity':round(eq,6),
             'turnover':round(turn,6),'cost':round(cost,8),'weights':self.state['weights']}
        h=list(self.state.get('history') or []);h.append(row);self.state['history']=h[-300:]
        return row

    async def refresh(self):
        try:
            x=dict(raven_turbo_shadow.latest or {})
            prices=dict(raven_turbo_shadow.state.get('last_prices') or {})
            if not x.get('bar_ts') or not prices or 'v70_diversified_target_weights' not in x:
                self.last_error=None;self.last_refresh=time.time()
                self.latest={'strategy':'v70_regime_staggered','waiting_for':'v24_enriched_snapshot'}
                return self.status()
            now=time.time();self._mark(prices,now);bar_ts=str(x['bar_ts'])
            new_bar=bar_ts!=str(self.state.get('last_bar_ts') or '')
            reb=None;use_div=False
            if new_bar:
                breadth=float(x.get('breadth_bull') or 0)/max(len(SYMBOLS),1)
                mom=float(x.get('btc_momentum_72h_pct') or 0)/100.0
                use_div=breadth>=CONFIG['breadth_gate'] and mom>=CONFIG['momentum_gate']
                raw=dict(x.get('v70_diversified_target_weights') or {}) if use_div else dict(x.get('target_weights') or {})
                sleeves=list(self.state.get('sleeves') or [])
                sleeves.append({'bar_ts':bar_ts,'target':{k:float(v) for k,v in raw.items()},'diversified':use_div})
                self.state['sleeves']=sleeves[-CONFIG['sleeves']:]
                target=self._average_targets(self.state['sleeves'])
                reb=self._rebalance(target,bar_ts)
                self.state['observation_count']=int(self.state.get('observation_count') or 0)+1
                self.state['diversified_count']=int(self.state.get('diversified_count') or 0)+int(use_div)
            else:
                target=self._average_targets(self.state.get('sleeves') or [])
            self._save();self.last_error=None;self.last_refresh=now
            bench_now=float(raven_turbo_shadow.state.get('equity') or 100.0)
            if not self.state.get('benchmark_start_equity'):
                self.state['benchmark_start_equity']=bench_now;self._save()
            bench=100.0*bench_now/max(float(self.state['benchmark_start_equity']),1e-9)
            eq=float(self.state.get('equity') or 100.0)
            self.latest={'strategy':'v70_regime_staggered','bar_ts':bar_ts,
                         'source_regime':x.get('regime'),'diversified_gate':use_div,
                         'sleeve_count':len(self.state.get('sleeves') or []),
                         'target_weights':target,'paper_equity':eq,
                         'paper_return_pct':(eq/100.0-1.0)*100.0,
                         'benchmark_v24_equity':bench,'benchmark_v24_return_pct':bench-100.0,
                         'relative_edge_pct':eq-bench,'rebalance':reb}
        except Exception as exc:
            self.last_error=str(exc)[:300];self.last_refresh=time.time()
        return self.status()

    def status(self):
        eq=float(self.state.get('equity') or 100.0);obs=int(self.state.get('observation_count') or 0)
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'PAPER_SHADOW',
                'strategy':'v70_regime_staggered','future_only':True,'promotion_eligible':False,
                'live_enabled':False,'grid':False,'martingale':False,'dca':False,
                'locked_config':CONFIG,'equity':round(eq,6),'return_pct':round((eq/100.0-1.0)*100.0,4),
                'max_dd_pct':round(float(self.state.get('max_dd_pct') or 0.0),4),
                'observation_count':obs,'diversified_count':int(self.state.get('diversified_count') or 0),
                'phase':'WARMUP' if obs<24 else 'FUTURE_VALIDATION','latest':self.latest,
                'last_refresh':self.last_refresh,'last_error':self.last_error}
    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name='raven-v70-regime-shadow')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None;self._save()
    async def _loop(self):
        while True:
            if not (raven_turbo_shadow.latest or {}).get('bar_ts'):
                await asyncio.sleep(5.0);continue
            await self.refresh()
            await asyncio.sleep(max(300.0,self.interval))

raven_v70_regime_shadow=RavenV70RegimeShadow()
