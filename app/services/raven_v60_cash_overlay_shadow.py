from __future__ import annotations
import asyncio,json,math,time
from pathlib import Path
import numpy as np,pandas as pd
from app.services.raven_challenger_shadow import raven_challenger_shadow,RavenChallengerShadow

ROOT=Path(__file__).parents[2]
STATE=ROOT/'data'/'raven_v60_cash_overlay_shadow.json'
V24=ROOT/'data'/'raven_turbo_highbeta_v24_results.json'
SYMS=['BTCUSDT','ETHUSDT','SOLUSDT','SUIUSDT','NEARUSDT','APTUSDT','INJUSDT','TIAUSDT','SEIUSDT','WIFUSDT','PEPEUSDT','FLOKIUSDT','ARBUSDT','OPUSDT','AAVEUSDT','RUNEUSDT','JUPUSDT']
COST=.0012; FUND8=.00005
OVERLAY={'spread':2.0,'gross':1.0,'neutral_btc':.03}

class RavenV60CashOverlayShadow:
    def __init__(self):
        self.enabled=True;self.interval=1800.;self.task=None;self.last_error=None;self.last_refresh=None;self.latest={}
        self.v24_cfg=json.loads(V24.read_text(encoding='utf-8'))['chosen_cfg'];self.state=self._load()
    def _blank(self):
        return {'strategy':'v60','started_at':time.time(),'equity':100.,'benchmark_equity':100.,'weights':{},'benchmark_weights':{},
                'last_bar_ts':None,'last_closes':{},'observations':0,'overlay_bars':0,'costs':0.,'benchmark_costs':0.,'history':[]}
    def _load(self):
        try:return json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:return self._blank()
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    @staticmethod
    def _turn(a,b):return sum(abs(float(b.get(k,0))-float(a.get(k,0))) for k in set(a)|set(b))
    @staticmethod
    def _dd(hist):
        peak=100.;worst=0.
        for x in hist:
            v=float(x.get('equity') or 100.);peak=max(peak,v);worst=min(worst,v/peak-1)
        return worst*100.
    def _overlay(self,idx,c,score,vol):
        i=len(idx)-1;btc_m=float(c.BTCUSDT.pct_change(9).iloc[i])
        if not np.isfinite(btc_m) or abs(btc_m)>OVERLAY['neutral_btc']:return {},None
        pairs=[]
        for j,s in enumerate(SYMS):
            if s in ('BTCUSDT','ETHUSDT'):continue
            q=float(score[s].iloc[i]);vv=float(vol[s].iloc[i])
            if np.isfinite(q) and np.isfinite(vv):pairs.append((q,j,s))
        if len(pairs)<4:return {},None
        pairs.sort();lo=pairs[0];hi=pairs[-1];spread=hi[0]-lo[0]
        if spread<OVERLAY['spread']:return {},spread
        leg=OVERLAY['gross']/2.;return {hi[2]:leg,lo[2]:-leg},spread
    def _snapshot(self,frames,bar):
        vf={s:frames[s][frames[s].index<=bar] for s in SYMS}
        idx,c,bull,bear,score,vol=RavenChallengerShadow._features(vf)
        v24,regime,up,dn,bt,rs,vr,btc_m,_=RavenChallengerShadow._target(idx,c,bull,bear,score,vol,self.v24_cfg,None)
        if pd.Timestamp(bt)!=pd.Timestamp(bar):raise RuntimeError(f'V60_BAR_MISMATCH:{bar}:{bt}')
        overlay,spread=({},None) if v24 else self._overlay(idx,c,score,vol)
        target=v24 if v24 else overlay
        closes={s:float(c[s].iloc[-1]) for s in SYMS}
        meta={'regime':regime if v24 else ('OVERLAY' if overlay else 'CASH'),'breadth_up':up,'breadth_down':dn,
              'btc_momentum_72h_pct':btc_m*100.,'vol_ratio':vr,'overlay_spread':spread,'v24_target':v24,'overlay_target':overlay}
        return target,v24,closes,meta
    @staticmethod
    def _bar_return(w,old,new):
        gross=0.
        for s,x in w.items():
            a=float(old.get(s) or 0);b=float(new.get(s) or 0)
            if a>0 and b>0:gross+=float(x)*(b/a-1.)
        return gross-sum(abs(float(x)) for x in w.values())*FUND8
    def _charge(self,key,turn,cost_key):
        eq=float(self.state[key]);frac=max(0.,turn*COST);paid=eq*frac
        self.state[key]=eq*max(.001,1.-frac);self.state[cost_key]=float(self.state.get(cost_key) or 0)+paid
    def _record(self,bar,meta):
        h=self.state.get('history') or []
        h.append({'bar_ts':str(bar),'equity':float(self.state['equity']),'benchmark_equity':float(self.state['benchmark_equity']),
                  'mode':meta['regime'],'overlay_spread':meta.get('overlay_spread')})
        self.state['history']=h[-1000:]
    def _initialise(self,bar,target,bench,closes,meta):
        self._charge('equity',self._turn({},target),'costs');self._charge('benchmark_equity',self._turn({},bench),'benchmark_costs')
        self.state.update({'weights':target,'benchmark_weights':bench,'last_bar_ts':str(bar),'last_closes':closes,'started_at':time.time(),'observations':0})
        if meta['regime']=='OVERLAY':self.state['overlay_bars']=1
        self._record(bar,meta);self._save()
    def _advance(self,bar,target,bench,closes,meta):
        old=self.state.get('last_closes') or {};w=self.state.get('weights') or {};bw=self.state.get('benchmark_weights') or {}
        self.state['equity']=float(self.state['equity'])*max(.001,1+self._bar_return(w,old,closes))
        self.state['benchmark_equity']=float(self.state['benchmark_equity'])*max(.001,1+self._bar_return(bw,old,closes))
        self._charge('equity',self._turn(w,target),'costs');self._charge('benchmark_equity',self._turn(bw,bench),'benchmark_costs')
        self.state.update({'weights':target,'benchmark_weights':bench,'last_bar_ts':str(bar),'last_closes':closes,
                           'observations':int(self.state.get('observations') or 0)+1})
        if meta['regime']=='OVERLAY':self.state['overlay_bars']=int(self.state.get('overlay_bars') or 0)+1
        self._record(bar,meta)
    async def refresh(self):
        try:
            frames,_=await raven_challenger_shadow._frames();common=frames['BTCUSDT'].index
            for s in SYMS:common=common.intersection(frames[s].index)
            common=common.sort_values();latest=common[-1];prev=self.state.get('last_bar_ts')
            pending=[] if prev is None else list(common[common>pd.Timestamp(prev)])
            if len(pending)>90:raise RuntimeError(f'V60_REPLAY_TOO_LARGE:{len(pending)}')
            meta=None;target={};bench={};closes={}
            if prev is None:
                target,bench,closes,meta=self._snapshot(frames,latest);self._initialise(latest,target,bench,closes,meta)
            else:
                for bt in pending:
                    target,bench,closes,meta=self._snapshot(frames,bt);self._advance(bt,target,bench,closes,meta)
                if pending:self._save()
                else:target,bench,closes,meta=self._snapshot(frames,latest)
            self.last_error=None;self.last_refresh=time.time()
            self.latest={'bar_ts':str(latest),'mode':meta['regime'],'breadth_up':meta['breadth_up'],'breadth_down':meta['breadth_down'],
                         'btc_momentum_72h_pct':round(meta['btc_momentum_72h_pct'],4),'vol_ratio':round(float(meta['vol_ratio']),4),
                         'overlay_spread':round(float(meta['overlay_spread']),4) if meta.get('overlay_spread') is not None else None,
                         'v24_target':meta['v24_target'],'overlay_target':meta['overlay_target'],'replayed_bars':len(pending)}
        except Exception as exc:self.last_error=str(exc)[:500];self.last_refresh=time.time()
        return self.status()
    def status(self):
        eq=float(self.state.get('equity') or 100.);be=float(self.state.get('benchmark_equity') or 100.);obs=int(self.state.get('observations') or 0)
        age=max(0.,(time.time()-float(self.state.get('started_at') or time.time()))/3600.)
        return {'ok':self.last_error is None,'mode':'PAPER_SHADOW','strategy':'v60_v24_plus_v26_cash_overlay','enabled':self.enabled,
                'equity':round(eq,6),'return_pct':round(eq-100.,4),'max_dd_pct':round(self._dd(self.state.get('history') or []),4),
                'benchmark':'v24_same_clock','benchmark_equity':round(be,6),'benchmark_return_pct':round(be-100.,4),
                'relative_edge_pct':round(eq-be,4),'observations':obs,'overlay_bars':int(self.state.get('overlay_bars') or 0),
                'comparison_age_hours':round(age,2),'comparison_ready':bool(obs>=6 and age>=48),'weights':self.state.get('weights') or {},
                'last_bar_ts':self.state.get('last_bar_ts'),'last_refresh':self.last_refresh,'last_error':self.last_error,'latest':self.latest,
                'overlay_cfg':OVERLAY,'future_only':True,'promotion_eligible':False,'live_enabled':False,'grid':False,'martingale':False}
    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name='raven-v60-cash-overlay-shadow')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None;self._save()
    async def _loop(self):
        while True:
            if self.enabled:await self.refresh()
            await asyncio.sleep(max(300.,self.interval))

raven_v60_cash_overlay_shadow=RavenV60CashOverlayShadow()
