from __future__ import annotations
import asyncio,json,time,statistics
from pathlib import Path
ROOT=Path(__file__).parents[2]; DATA=ROOT/'data'
FEATURES=DATA/'moex_universe_features_v1.json'; STATE=DATA/'moex_spread_research_v1.json'
INTERVAL=300.0

def _load(p,default):
    try:return json.loads(p.read_text(encoding='utf-8'))
    except Exception:return default

def _diag():
    src=_load(FEATURES,{})
    amap=src.get('assets') or {}
    assets=[]
    for name,a in amap.items():
        vals=[float(x) for x in (a.get('curve_pct') or []) if x is not None]
        if not vals: continue
        n=len(vals); last=vals[-1]; mean=sum(vals)/n
        sd=statistics.pstdev(vals) if n>1 else 0.0
        z=(last-mean)/sd if n>=10 and sd>1e-12 else None
        assets.append({'asset':name,'samples':int(a.get('samples') or n),'front':a.get('front'),'next':a.get('next'),
                       'curve_pct':last,'mean_curve_pct':mean,'stdev_curve_pct':sd,
                       'zscore':None if z is None else round(z,4),'front_tier':a.get('front_tier'),'next_tier':a.get('next_tier')})
    ranked=sorted([x for x in assets if x['zscore'] is not None],key=lambda x:abs(x['zscore']),reverse=True)
    now=time.time(); started=float(src.get('started_at') or now); samples=int(src.get('samples') or 0)
    hours=max(0.0,(now-started)/3600.0); gate=bool(hours>=24.0 and samples>=288)
    return {'ts':now,'mode':'MOEX_SPREAD_RESEARCH_ONLY','paper_only':True,'live_enabled':False,
            'source_samples':samples,'collection_hours':hours,'gate_ready':gate,'assets':assets,'top_deviations':ranked[:25]}

class MoexSpreadResearchV1:
    def __init__(self):
        self.enabled=True; self.live_enabled=False; self.task=None; self.last_error=None
        self.state=_load(STATE,{'version':'MOEX_SPREAD_RESEARCH_V1','started_at':time.time(),'refresh_count':0,'last':None})
    def _save(self): STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    async def refresh(self):
        try:
            row=await asyncio.to_thread(_diag)
            self.state['refresh_count']=int(self.state.get('refresh_count') or 0)+1
            self.state['last']=row; self.state['last_refresh']=row['ts']; self.last_error=None; self._save()
        except Exception as exc:self.last_error=str(exc)[:500]
        return self.status()
    def status(self):
        last=self.state.get('last') or {}
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'MOEX_SPREAD_RESEARCH_ONLY','paper_only':True,'live_enabled':False,
                'refresh_count':int(self.state.get('refresh_count') or 0),'source_samples':last.get('source_samples'),
                'collection_hours':last.get('collection_hours'),'gate_ready':last.get('gate_ready',False),
                'top_deviations':last.get('top_deviations') or [],'last_error':self.last_error,
                'predeclared_hypotheses':['SPREAD_MEAN_REVERSION_PER_ASSET','SPREAD_CONTINUATION_PER_ASSET'],
                'execution_model':['LONG_FRONT_SHORT_NEXT','SHORT_FRONT_LONG_NEXT'],
                'policy':{'rule_selection_locked_until_baseline':True,'cost_model_required_before_paper':True,'no_live_orders':True,'no_grid':True,'no_martingale':True,'no_dca':True}}
    async def start(self):
        if not self.task or self.task.done(): self.task=asyncio.create_task(self._loop(),name='moex-spread-research-v1')
    async def stop(self):
        if self.task and not self.task.done(): self.task.cancel()
        self.task=None
    async def _loop(self):
        while True:
            if self.enabled: await self.refresh()
            await asyncio.sleep(INTERVAL)

moex_spread_research_v1=MoexSpreadResearchV1()
