from __future__ import annotations
import asyncio,json,time,statistics
from pathlib import Path
from app.services.moex_futures_shadow import moex_futures_shadow
ROOT=Path(__file__).parents[2];DATA=ROOT/'data';STATE=DATA/'moex_feature_registry_v1.json';RAW=DATA/'moex_feature_registry_v1.jsonl';INTERVAL=300.0
FEATURES=('gold_curve_pct','silver_curve_pct','gold_silver_ratio','sber_curve_pct','sber_perp_front_pct','gold_perp_oi','silver_perp_oi','sber_perp_oi')
def _n(x):
    try:return float(x) if x is not None else None
    except:return None
def _row(x):return {'ts':x.get('source_ts'),'gold_curve_pct':_n((x.get('gold')or{}).get('curve_pct')),'silver_curve_pct':_n((x.get('silver')or{}).get('curve_pct')),'gold_silver_ratio':_n(x.get('gold_silver_ratio')),'sber_curve_pct':_n((x.get('sber')or{}).get('curve_pct')),'sber_perp_front_pct':_n(x.get('sber_perp_vs_front_pct')),'gold_perp_oi':_n((x.get('gldrubl_perp')or{}).get('oi')),'silver_perp_oi':_n((x.get('slvrubl_perp')or{}).get('oi')),'sber_perp_oi':_n((x.get('sber_perp')or{}).get('oi'))}
def _stats(h):
    out={}
    for k in FEATURES:
        v=[_n(x.get(k)) for x in h if _n(x.get(k)) is not None];out[k]={'n':len(v)} if not v else {'n':len(v),'last':v[-1],'mean':statistics.fmean(v),'min':min(v),'max':max(v),'delta':(v[-1]-v[-2] if len(v)>1 else 0.0)}
    return out
class MoexFeatureRegistry:
    def __init__(self):
        self.enabled=True;self.task=None;self.last_error=None
        try:self.state=json.loads(STATE.read_text(encoding='utf-8'))
        except:self.state={'history':[],'last_source_ts':None}
    async def refresh(self):
        try:
            x=moex_futures_shadow.status().get('latest') or {};ts=x.get('source_ts')
            if ts and ts!=self.state.get('last_source_ts'):
                h=(self.state.get('history')or[])+[_row(x)];h=h[-1000:];self.state.update(history=h,last_source_ts=ts,sample_count=len(h),stats=_stats(h),last_refresh=time.time());STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8');open(RAW,'a',encoding='utf-8').write(json.dumps(h[-1],ensure_ascii=False)+'\n')
            self.last_error=None
        except Exception as e:self.last_error=str(e)[:500]
        return self.status()
    def status(self):
        g=moex_futures_shadow.status().get('future_gate') or {};return {'ok':self.last_error is None,'mode':'FEATURE_COLLECTION_ONLY','live_enabled':False,'sample_count':int(self.state.get('sample_count') or 0),'features':list(FEATURES),'stats':self.state.get('stats') or {},'future_gate':g,'rule_selection_locked':not bool(g.get('ready_for_rule_design')),'predeclared_hypotheses':['GOLD_CURVE_MEAN_REVERSION','GOLD_CURVE_CONTINUATION','SILVER_CURVE_MEAN_REVERSION','SILVER_CURVE_CONTINUATION','GOLD_SILVER_RATIO_REVERSION','SBER_CURVE_MEAN_REVERSION','SBER_CURVE_CONTINUATION','SBER_PERP_QUARTER_CONVERGENCE'],'policy':{'no_threshold_tuning_before_gate':True,'no_live_orders':True,'no_grid':True,'no_martingale':True,'no_dca':True}}
    async def start(self):
        if not self.task or self.task.done():self.task=asyncio.create_task(self._loop())
    async def stop(self):self.task.cancel() if self.task and not self.task.done() else None;self.task=None
    async def _loop(self):
        while True: await self.refresh();await asyncio.sleep(INTERVAL)
moex_feature_registry=MoexFeatureRegistry()
