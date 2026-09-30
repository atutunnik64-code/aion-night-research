from __future__ import annotations
import asyncio,json,time
from pathlib import Path
from app.services.raven_turbo_shadow import raven_turbo_shadow,SYMBOLS,COST,FUND
from app.services.raven_funding_feature import raven_funding_feature

ROOT=Path(__file__).parents[2]
STATE=ROOT/'data'/'raven_v42_funding_shadow.json'
PRICE_CFG={'long_mult':2.5,'short_mult':2.0,'bull_frac':.30,'bear_frac':.65,
           'btc_long_m':.025,'btc_short_m':-.01,'slots':1,'vol1':1.25,'vol2':1.75,
           'cut1':.4,'cut2':.3,'gross_cap':3.0}
FUND_CFG={'lag_bars':2,'fund_long_q':.50,'fund_short_q':.50,
          'fund_mismatch_scale':.25,'fund_disp_gate':.0012}

class RavenV42FundingShadow:
    def __init__(self):
        self.enabled=True;self.interval=900.;self.task=None;self.last_error=None;self.last_refresh=None;self.latest={}
        self.state=self._load()
    @staticmethod
    def _blank():
        return {'equity':100.,'baseline_equity':100.,'peak':100.,'baseline_peak':100.,
                'max_dd_pct':0.,'baseline_max_dd_pct':0.,'weights':{},'baseline_weights':{},
                'last_prices':{},'last_bar_ts':None,'observations':0,'affected_events':0,'history':[],'started_at':time.time()}
    def _load(self):
        x=self._blank()
        try:x.update(json.loads(STATE.read_text(encoding='utf-8')))
        except Exception:pass
        return x
    def _save(self):
        STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    @staticmethod
    def _mark(eq,weights,old_prices,new_prices):
        pnl=0.
        for s,w in (weights or {}).items():
            a=float(old_prices.get(s) or 0);b=float(new_prices.get(s) or 0)
            if a>0 and b>0:pnl+=float(w)*(b/a-1.)
        gross=sum(abs(float(x)) for x in (weights or {}).values())
        return float(eq)*max(.001,1.+pnl-gross*FUND)
    @staticmethod
    def _cost(eq,old,new):
        keys=set(old)|set(new);turn=sum(abs(float(new.get(k,0))-float(old.get(k,0))) for k in keys)
        return float(eq)*max(.001,1.-turn*COST),turn

    async def _snapshot(self):
        raw=[]
        for s in SYMBOLS:
            raw.append(await raven_turbo_shadow._frame4h(s));await asyncio.sleep(.05)
        fresh={s:x for s,x in zip(SYMBOLS,raw)};frames={s:raven_turbo_shadow._resample8(x) for s,x in fresh.items()}
        if any(len(frames[s])<370 for s in SYMBOLS):raise RuntimeError('V42_HISTORY_WARMUP_INCOMPLETE')
        idx,c,r,bull,bear,score,vol=raven_turbo_shadow._features(frames)
        base,regime,up,dn,bar_ts,rs,vr,btc_m=raven_turbo_shadow._target(idx,c,bull,bear,score,vol,PRICE_CFG)
        base={k:float(v) for k,v in base.items() if abs(float(v))>1e-12}
        fs=await raven_funding_feature.snapshot(bar_ts,lag_bars=FUND_CFG['lag_bars'])
        filt=dict(base);affected=False
        if fs.get('ok') and float(fs.get('dispersion') or 0)>=FUND_CFG['fund_disp_gate']:
            ranks=fs.get('ranks') or {}
            for s,w in list(filt.items()):
                q=ranks.get(s)
                if q is None:continue
                aligned=(w>0 and float(q)>=FUND_CFG['fund_long_q']) or (w<0 and float(q)<=FUND_CFG['fund_short_q'])
                if not aligned:filt[s]=w*FUND_CFG['fund_mismatch_scale'];affected=True
        prices={s:float(fresh[s].iloc[-1].close) for s in SYMBOLS}
        return {'bar_ts':str(bar_ts),'prices':prices,'base':base,'filtered':filt,'affected':affected,
                'funding':fs,'regime':regime,'breadth_bull':up,'breadth_bear':dn,'vol_ratio':vr,'btc_m':btc_m}

    async def refresh(self):
        try:
            x=await self._snapshot();bar=x['bar_ts'];oldbar=self.state.get('last_bar_ts')
            if not oldbar:
                self.state['last_bar_ts']=bar;self.state['last_prices']=x['prices'];self.state['last_signal']=x;self._save()
            elif bar!=oldbar:
                oldp=self.state.get('last_prices') or {}
                ce=self._mark(self.state['equity'],self.state.get('weights') or {},oldp,x['prices'])
                be=self._mark(self.state['baseline_equity'],self.state.get('baseline_weights') or {},oldp,x['prices'])
                ce,ct=self._cost(ce,self.state.get('weights') or {},x['filtered'])
                be,bt=self._cost(be,self.state.get('baseline_weights') or {},x['base'])
                self.state['equity']=ce;self.state['baseline_equity']=be
                self.state['weights']=x['filtered'];self.state['baseline_weights']=x['base']
                self.state['last_prices']=x['prices'];self.state['last_bar_ts']=bar;self.state['last_signal']=x
                self.state['observations']=int(self.state.get('observations') or 0)+1
                if x['affected'] and x['base']:self.state['affected_events']=int(self.state.get('affected_events') or 0)+1
                self.state['peak']=max(float(self.state.get('peak') or 100.),ce)
                self.state['baseline_peak']=max(float(self.state.get('baseline_peak') or 100.),be)
                self.state['max_dd_pct']=min(float(self.state.get('max_dd_pct') or 0.),(ce/self.state['peak']-1.)*100.)
                self.state['baseline_max_dd_pct']=min(float(self.state.get('baseline_max_dd_pct') or 0.),(be/self.state['baseline_peak']-1.)*100.)
                h=list(self.state.get('history') or []);h.append({'bar_ts':bar,'equity':round(ce,6),'baseline_equity':round(be,6),
                    'alpha_pp':round(ce-be,6),'funding_filter_applied':bool(x['affected']),'candidate_turnover':round(ct,6),'baseline_turnover':round(bt,6)})
                self.state['history']=h[-300:];self._save()
            self.last_error=None;self.last_refresh=time.time();self.latest=x
        except Exception as exc:self.last_error=str(exc)[:400];self.last_refresh=time.time()
        return self.status()

    def _gate(self):
        obs=int(self.state.get('observations') or 0);aff=int(self.state.get('affected_events') or 0)
        alpha=float(self.state.get('equity') or 100.)-float(self.state.get('baseline_equity') or 100.)
        dd=float(self.state.get('max_dd_pct') or 0.);bdd=float(self.state.get('baseline_max_dd_pct') or 0.)
        return {'required_observations':24,'required_affected_events':10,'observations':obs,'affected_events':aff,
                'relative_alpha_pp':round(alpha,6),'candidate_dd_pct':round(dd,6),'baseline_dd_pct':round(bdd,6),
                'ready':bool(obs>=24 and aff>=10 and alpha>0 and dd>=bdd-.5)}
    def status(self):
        eq=float(self.state.get('equity') or 100.);be=float(self.state.get('baseline_equity') or 100.)
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'PAPER_SHADOW','strategy':'v42_funding_confirmation',
                'future_only':True,'promotion_eligible':False,'live_enabled':False,'grid':False,'martingale':False,'dca':False,
                'locked_price_config':PRICE_CFG,'locked_funding_config':FUND_CFG,'equity':round(eq,6),
                'return_pct':round(eq-100.,4),'baseline_equity':round(be,6),'baseline_return_pct':round(be-100.,4),
                'max_dd_pct':round(float(self.state.get('max_dd_pct') or 0.),4),'observation_count':int(self.state.get('observations') or 0),
                'future_gate':self._gate(),'latest':self.latest,'last_refresh':self.last_refresh,'last_error':self.last_error}
    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name='raven-v42-funding-shadow')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None;self._save()
    async def _loop(self):
        while True:
            await self.refresh();await asyncio.sleep(max(300.,self.interval))

raven_v42_funding_shadow=RavenV42FundingShadow()
