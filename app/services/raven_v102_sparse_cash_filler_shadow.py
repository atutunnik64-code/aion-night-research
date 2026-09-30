from __future__ import annotations
import asyncio,json,time
from pathlib import Path
from app.services.raven_turbo_shadow import COST,FUND
from app.services.raven_v70_regime_shadow import raven_v70_regime_shadow
from app.services.raven_v47_shadow import raven_v47_shadow,COSTSQ

ROOT=Path(__file__).parents[2]
STATE=ROOT/'data'/'raven_v102_sparse_cash_filler_shadow.json'
ALPHA=0.5
STRENGTH_THRESHOLD=0.23328

class RavenV102SparseCashFillerShadow:
    def __init__(self):
        self.enabled=True;self.interval=300.0;self.task=None
        self.last_error=None;self.last_refresh=None;self.latest={}
        self.refresh_lock=asyncio.Lock();self.state=self._load()
    @staticmethod
    def _blank():
        return {'equity':100.0,'benchmark_equity':100.0,'peak':100.0,
                'max_dd_pct':0.0,'v70_weights':{},'filler_weights':{},
                'benchmark_weights':{},'last_prices':{},'last_bar_ts':None,
                'costs':0.0,'benchmark_costs':0.0,'funding':0.0,
                'benchmark_funding':0.0,'observation_count':0,
                'active_filler_bars':0,'history':[],'started_at':time.time()}
    def _load(self):
        x=self._blank()
        try:x.update(json.loads(STATE.read_text(encoding='utf-8')))
        except Exception:pass
        return x
    def _save(self):
        STATE.parent.mkdir(parents=True,exist_ok=True)
        STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    @staticmethod
    def _turn(old,new):
        keys=set(old)|set(new)
        return sum(abs(float(new.get(k,0))-float(old.get(k,0))) for k in keys)
    @staticmethod
    def _leg_return(weights,old_px,new_px,fund_all):
        r=0.0
        for s,w in weights.items():
            a=float(old_px.get(s) or 0);b=float(new_px.get(s) or 0)
            if a>0 and b>0:r+=float(w)*(b/a-1.0)
        if fund_all:gross=sum(abs(float(w)) for w in weights.values())
        else:gross=sum(abs(float(w)) for w in weights.values() if float(w)<0)
        return r-gross*FUND,gross*FUND
    @staticmethod
    def _filler_target(v70_target,squeeze_raw):
        cash=not any(abs(float(v))>1e-12 for v in (v70_target or {}).values())
        strength=sum(abs(float(v)) for v in (squeeze_raw or {}).values())
        active=cash and strength>=STRENGTH_THRESHOLD
        filler={k:ALPHA*float(v) for k,v in (squeeze_raw or {}).items()} if active else {}
        filler={k:v for k,v in filler.items() if abs(v)>1e-12}
        return filler,cash,strength,active
    def _mark_new_bar(self,prices):
        old_px=self.state.get('last_prices') or {}
        if not old_px:return
        v70=self.state.get('v70_weights') or {};fil=self.state.get('filler_weights') or {}
        bench=self.state.get('benchmark_weights') or {}
        r70,f70=self._leg_return(v70,old_px,prices,True)
        rf,ff=self._leg_return(fil,old_px,prices,False)
        rb,fb=self._leg_return(bench,old_px,prices,True)
        eq=float(self.state.get('equity') or 100.0)*max(.01,1+r70+rf)
        be=float(self.state.get('benchmark_equity') or 100.0)*max(.01,1+rb)
        self.state['equity']=eq;self.state['benchmark_equity']=be
        self.state['funding']=float(self.state.get('funding') or 0)+f70+ff
        self.state['benchmark_funding']=float(self.state.get('benchmark_funding') or 0)+fb
        self.state['peak']=max(float(self.state.get('peak') or eq),eq)
        self.state['max_dd_pct']=min(float(self.state.get('max_dd_pct') or 0.0),(eq/self.state['peak']-1)*100.0)
    def _rebalance(self,v70_target,filler_target,bar_ts):
        old70=self.state.get('v70_weights') or {};oldf=self.state.get('filler_weights') or {}
        oldb=self.state.get('benchmark_weights') or {}
        t70=self._turn(old70,v70_target);tf=self._turn(oldf,filler_target);tb=self._turn(oldb,v70_target)
        eq=float(self.state.get('equity') or 100.0);be=float(self.state.get('benchmark_equity') or 100.0)
        cost=eq*(t70*COST+tf*COSTSQ);bcost=be*tb*COST
        self.state['equity']=eq*max(.01,1-(t70*COST+tf*COSTSQ))
        self.state['benchmark_equity']=be*max(.01,1-tb*COST)
        self.state['costs']=float(self.state.get('costs') or 0)+cost
        self.state['benchmark_costs']=float(self.state.get('benchmark_costs') or 0)+bcost
        self.state['v70_weights']={k:float(v) for k,v in v70_target.items() if abs(float(v))>1e-12}
        self.state['filler_weights']={k:float(v) for k,v in filler_target.items() if abs(float(v))>1e-12}
        self.state['benchmark_weights']=dict(self.state['v70_weights']);self.state['last_bar_ts']=str(bar_ts)
        return {'turn_v70':t70,'turn_filler':tf,'turn_benchmark':tb,'cost':cost,'benchmark_cost':bcost}

    async def refresh(self):
        async with self.refresh_lock:
            try:
                v70=raven_v70_regime_shadow.status();vx=v70.get('latest') or {}
                v47=raven_v47_shadow.status();sx=v47.get('latest') or {}
                vb=str(vx.get('bar_ts') or '');sb=str(sx.get('bar_ts') or '')
                if not vb or not sb or vb!=sb:
                    self.latest={'strategy':'v102_sparse_cash_filler','waiting_for':'ALIGNED_V70_V47_SNAPSHOT',
                                 'v70_bar':vb or None,'squeeze_bar':sb or None}
                    self.last_error=None;self.last_refresh=time.time();return self.status()
                prices=dict(raven_v47_shadow.state.get('last_closes') or {})
                if not prices:
                    self.latest={'strategy':'v102_sparse_cash_filler','waiting_for':'SOURCE_PRICES'}
                    self.last_error=None;self.last_refresh=time.time();return self.status()
                new_bar=vb!=str(self.state.get('last_bar_ts') or '')
                v70_target={k:float(v) for k,v in (vx.get('target_weights') or {}).items()}
                squeeze_raw={k:float(v) for k,v in (sx.get('squeeze_raw') or {}).items()}
                filler,cash,strength,active=self._filler_target(v70_target,squeeze_raw)
                reb=None
                if not self.state.get('last_bar_ts'):
                    reb=self._rebalance(v70_target,filler,vb)
                    self.state['last_prices']={k:float(v) for k,v in prices.items()}
                elif new_bar:
                    self._mark_new_bar(prices);reb=self._rebalance(v70_target,filler,vb)
                    self.state['last_prices']={k:float(v) for k,v in prices.items()}
                    self.state['observation_count']=int(self.state.get('observation_count') or 0)+1
                    self.state['active_filler_bars']=int(self.state.get('active_filler_bars') or 0)+int(active)
                    h=list(self.state.get('history') or [])
                    h.append({'bar_ts':vb,'equity':round(float(self.state['equity']),6),
                              'benchmark_equity':round(float(self.state['benchmark_equity']),6),
                              'cash':cash,'strength':round(strength,6),'filler_active':active})
                    self.state['history']=h[-300:]
                self._save();self.last_error=None;self.last_refresh=time.time()
                eq=float(self.state.get('equity') or 100.0);be=float(self.state.get('benchmark_equity') or 100.0)
                self.latest={'strategy':'v102_sparse_cash_filler','bar_ts':vb,'source_v70_equity':v70.get('equity'),
                             'cash':cash,'squeeze_strength':strength,'threshold':STRENGTH_THRESHOLD,
                             'filler_active':active,'v70_target':v70_target,'filler_target':filler,
                             'paper_equity':eq,'paper_return_pct':eq-100.0,
                             'benchmark_equity':be,'benchmark_return_pct':be-100.0,
                             'relative_edge_pct':eq-be,'rebalance':reb}
            except Exception as exc:
                self.last_error=str(exc)[:400];self.last_refresh=time.time()
            return self.status()
    def status(self):
        eq=float(self.state.get('equity') or 100.0);be=float(self.state.get('benchmark_equity') or 100.0)
        obs=int(self.state.get('observation_count') or 0);active=int(self.state.get('active_filler_bars') or 0)
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'PAPER_SHADOW',
                'strategy':'v102_sparse_cash_filler','source_research':'v82_robust_sparse_cash_filler',
                'future_only':True,'promotion_eligible':False,'live_enabled':False,
                'grid':False,'martingale':False,'dca':False,
                'locked_config':{'alpha':ALPHA,'strength_threshold':STRENGTH_THRESHOLD},
                'equity':round(eq,6),'return_pct':round(eq-100.0,4),
                'benchmark_equity':round(be,6),'benchmark_return_pct':round(be-100.0,4),
                'relative_edge_pct':round(eq-be,4),'max_dd_pct':round(float(self.state.get('max_dd_pct') or 0.0),4),
                'observation_count':obs,'active_filler_bars':active,
                'phase':'WARMUP' if obs<24 else 'FUTURE_VALIDATION',
                'latest':self.latest,'last_refresh':self.last_refresh,'last_error':self.last_error}
    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name='raven-v102-sparse-cash-filler-shadow')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None;self._save()
    async def _loop(self):
        while True:
            await self.refresh()
            waiting=bool((self.latest or {}).get('waiting_for'))
            await asyncio.sleep(5.0 if waiting else max(300.0,self.interval))

raven_v102_sparse_cash_filler_shadow=RavenV102SparseCashFillerShadow()
