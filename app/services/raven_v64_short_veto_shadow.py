from __future__ import annotations
import asyncio,json,time
from pathlib import Path
from app.services.raven_turbo_shadow import raven_turbo_shadow,COST,FUND,SYMBOLS

ROOT=Path(__file__).parents[2]
STATE=ROOT/'data'/'raven_v64_short_veto_shadow.json'
FILTER={'bear':0.50,'m9':-0.03,'m21':-0.03,'vrmax':1.25}

class RavenV64ShortVetoShadow:
    def __init__(self):
        self.enabled=True;self.interval=900.0;self.task=None;self.last_error=None;self.last_refresh=None
        self.latest={};self.state=self._load()
    @staticmethod
    def _blank():
        return {'equity':100.0,'peak':100.0,'max_dd_pct':0.0,'weights':{},'last_prices':{},
                'last_mark_ts':None,'last_bar_ts':None,'costs':0.0,'funding':0.0,
                'observation_count':0,'active_observation_count':0,'veto_count':0,'history':[],'started_at':time.time()}
    def _load(self):
        x=self._blank()
        try:x.update(json.loads(STATE.read_text(encoding='utf-8')))
        except Exception:pass
        return x
    def _save(self):
        STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')

    def _mark(self,prices,now):
        old=self.state.get('last_prices') or {};w=self.state.get('weights') or {};eq=float(self.state.get('equity') or 100.0)
        if old:
            pnl=0.0
            for s,x in w.items():
                a=float(old.get(s) or 0);b=float(prices.get(s) or 0)
                if a>0 and b>0:pnl+=float(x)*(b/a-1.0)
            eq*=max(.01,1.0+pnl)
            hours=max(0.0,(now-float(self.state.get('last_mark_ts') or now))/3600.0)
            short=sum(abs(float(x)) for x in w.values() if float(x)<0)
            fund=eq*short*FUND*(hours/8.0);eq-=fund;self.state['funding']+=fund
        self.state['equity']=eq;self.state['last_prices']={k:float(v) for k,v in prices.items()};self.state['last_mark_ts']=now
        self.state['peak']=max(float(self.state.get('peak') or eq),eq)
        self.state['max_dd_pct']=min(float(self.state.get('max_dd_pct') or 0.0),(eq/self.state['peak']-1.0)*100.0)
    @staticmethod
    def _short_allowed(x):
        bear=float(x.get('breadth_bear') or 0)/len(SYMBOLS)
        m9=float(x.get('btc_momentum_72h_pct') or 0)/100.0
        m21=float(x.get('btc_momentum_168h_pct') or 0)/100.0
        vr=float(x.get('btc_vol_ratio') or 99.0)
        return bear>=FILTER['bear'] and m9<=FILTER['m9'] and m21<=FILTER['m21'] and vr<=FILTER['vrmax']

    def _rebalance(self,target,bar_ts):
        old={k:float(v) for k,v in (self.state.get('weights') or {}).items()};keys=set(old)|set(target)
        turn=sum(abs(float(target.get(k,0))-float(old.get(k,0))) for k in keys)
        eq=float(self.state.get('equity') or 100.0);cost=eq*turn*COST;eq-=cost
        self.state['equity']=eq;self.state['costs']=float(self.state.get('costs') or 0)+cost
        self.state['weights']={k:float(v) for k,v in target.items() if abs(float(v))>1e-9}
        self.state['last_bar_ts']=str(bar_ts);self.state['last_target_key']=json.dumps(self.state['weights'],sort_keys=True)
        return {'turnover':turn,'cost':cost}

    async def refresh(self):
        try:
            x=dict(raven_turbo_shadow.latest or {});prices=dict(raven_turbo_shadow.state.get('last_prices') or {})
            if not x.get('bar_ts') or not prices:
                self.last_error=None;self.last_refresh=time.time();self.latest={'strategy':'v64_short_veto','waiting_for':'v24_snapshot'}
                return self.status()
            now=time.time();self._mark(prices,now);target=dict(x.get('target_weights') or {});veto=False
            if x.get('regime')=='BEAR_TURBO' and not self._short_allowed(x):target={};veto=True
            target_key=json.dumps({k:float(v) for k,v in target.items()},sort_keys=True)
            new_obs=str(x['bar_ts'])!=str(self.state.get('last_bar_ts'))
            changed=target_key!=str(self.state.get('last_target_key') or '')
            reb=self._rebalance(target,x['bar_ts']) if (new_obs or changed) else None
            if new_obs:
                self.state['observation_count']=int(self.state.get('observation_count') or 0)+1
                self.state['active_observation_count']+=int(bool(target));self.state['veto_count']+=int(veto)
            self._save();self.last_error=None;self.last_refresh=now
            bench_now=float(raven_turbo_shadow.state.get('equity') or 100.0)
            if not self.state.get('benchmark_start_equity'):self.state['benchmark_start_equity']=bench_now
            bench=100.0*bench_now/max(float(self.state['benchmark_start_equity']),1e-9)
            eq=float(self.state.get('equity') or 100.0)
            self.latest={'strategy':'v64_short_veto','bar_ts':str(x['bar_ts']),'source_regime':x.get('regime'),
                         'veto_applied':veto,'target_weights':target,'paper_equity':eq,
                         'paper_return_pct':(eq/100.0-1.0)*100.0,'benchmark_v24_equity':bench,
                         'benchmark_v24_return_pct':bench-100.0,'relative_edge_pct':eq-bench,'rebalance':reb}
        except Exception as exc:self.last_error=str(exc)[:300];self.last_refresh=time.time()
        return self.status()

    def status(self):
        eq=float(self.state.get('equity') or 100.0);obs=int(self.state.get('observation_count') or 0)
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'PAPER_SHADOW','strategy':'v64_short_veto',
                'future_only':True,'promotion_eligible':False,'live_enabled':False,
                'grid':False,'martingale':False,'dca':False,'locked_filter':FILTER,
                'equity':round(eq,6),'return_pct':round(eq-100.0,4),
                'max_dd_pct':round(float(self.state.get('max_dd_pct') or 0.0),4),'observation_count':obs,
                'active_observation_count':int(self.state.get('active_observation_count') or 0),
                'veto_count':int(self.state.get('veto_count') or 0),'phase':'WARMUP' if obs<24 else 'FUTURE_VALIDATION',
                'latest':self.latest,'last_refresh':self.last_refresh,'last_error':self.last_error}

    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name='raven-v64-short-veto-shadow')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None;self._save()
    async def _loop(self):
        while True:
            if not (raven_turbo_shadow.latest or {}).get('bar_ts'):
                await asyncio.sleep(5.0)
                continue
            await self.refresh()
            await asyncio.sleep(max(300.0,self.interval))

raven_v64_short_veto_shadow=RavenV64ShortVetoShadow()
