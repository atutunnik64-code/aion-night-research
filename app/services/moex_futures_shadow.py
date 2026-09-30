from __future__ import annotations
import asyncio,json,time
from datetime import date
from pathlib import Path
from app.services.moex_futures_collector import moex_futures_collector

ROOT=Path(__file__).parents[2]; DATA=ROOT/'data'
RAW=DATA/'moex_futures_research_v1.jsonl'
STATE=DATA/'moex_futures_shadow_v1.json'
INTERVAL=300.0

def _append(row):
    with RAW.open('a',encoding='utf-8') as f:
        f.write(json.dumps(row,ensure_ascii=False,separators=(',',':'))+'\n')

def _px(row):
    try:return float(row.get('LAST') if row.get('LAST') is not None else row.get('SETTLEPRICE'))
    except Exception:return None

def _series(rows,asset):
    today=date.today().isoformat()
    x=[r for r in rows if str(r.get('ASSETCODE') or '').upper()==asset and str(r.get('LASTTRADEDATE') or '')>=today and _px(r)]
    return sorted(x,key=lambda r:str(r.get('LASTTRADEDATE') or ''))
def _pair(rows,asset):
    s=_series(rows,asset)
    if len(s)<2:return None
    a,b=s[0],s[1]; pa,pb=_px(a),_px(b)
    return {'front':a.get('SECID'),'front_name':a.get('SHORTNAME'),'front_px':pa,
            'next':b.get('SECID'),'next_name':b.get('SHORTNAME'),'next_px':pb,
            'curve_pct':(pb/pa-1.0)*100.0 if pa else None}

def _perp(rows,secid):
    for r in rows:
        if str(r.get('SECID') or '').upper()==secid:
            return {'secid':secid,'px':_px(r),'oi':r.get('OPENPOSITION'),'volume':r.get('VOLTODAY')}
    return None

def _diagnostics(last_good):
    rows=list(last_good.get('rows') or [])
    gold=_pair(rows,'GOLD'); silver=_pair(rows,'SILV'); sber=_pair(rows,'SBRF')
    out={'source_ts':last_good.get('ts'),'gold':gold,'silver':silver,'sber':sber,
         'gldrubl_perp':_perp(rows,'GLDRUBF'),'slvrubl_perp':_perp(rows,'SLVRUBF'),'sber_perp':_perp(rows,'SBERF')}
    if gold and silver and gold.get('front_px') and silver.get('front_px'):
        out['gold_silver_ratio']=gold['front_px']/silver['front_px']
    if sber and out['sber_perp']:
        f=next((r for r in rows if str(r.get('SECID') or '').upper()==str(sber.get('front') or '').upper()),None)
        p=next((r for r in rows if str(r.get('SECID') or '').upper()=='SBERF'),None)
        try:
            fr=float((f or {}).get('LAST_RUB')); pr=float((p or {}).get('LAST_RUB'))
            out['sber_perp_vs_front_pct']=(pr/fr-1.0)*100.0 if fr else None
            out['sber_perp_normalization']='MOEX_LAST_RUB'
        except Exception:
            out['sber_perp_normalization_required']=True
    return out
class MoexFuturesShadow:
    def __init__(self):
        self.enabled=True; self.live_enabled=False; self.task=None; self.last_error=None
        try:self.state=json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:self.state={'version':'MOEX_FUTURES_SHADOW_V1','started_at':time.time(),'sample_count':0,'last_source_ts':None,'latest':None}
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    async def refresh(self):
        try:
            lg=(moex_futures_collector.status().get('last_good') or {})
            src=lg.get('ts')
            if src and src!=self.state.get('last_source_ts'):
                d=_diagnostics(lg); _append(d)
                self.state['sample_count']=int(self.state.get('sample_count') or 0)+1
                self.state['last_source_ts']=src; self.state['latest']=d
                self.state.setdefault('first_source_ts',src); self._save()
            self.last_error=None
        except Exception as exc:self.last_error=str(exc)[:500]
        return self.status()
    def status(self):
        n=int(self.state.get('sample_count') or 0); first=float(self.state.get('first_source_ts') or time.time())
        hours=max(0.0,(time.time()-first)/3600.0) if n else 0.0
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'FUTURE_ONLY_RESEARCH',
                'strategy':'MOEX_METALS_SBER_STRUCTURE_V1','live_enabled':False,'paper_only':True,
                'sample_count':n,'latest':self.state.get('latest'),'last_error':self.last_error,
                'future_gate':{'collection_hours':hours,'required_hours':24.0,
                               'baseline_samples':n,'required_baseline_samples':288,
                               'ready_for_rule_design':bool(hours>=24.0 and n>=288)},
                'policy':{'no_live_orders':True,'no_historical_selection':True,
                          'no_grid':True,'no_martingale':True,'no_dca':True,
                          'rule_design_only_after_baseline':True}}
    async def start(self):
        if not self.task or self.task.done():self.task=asyncio.create_task(self._loop(),name='moex-futures-shadow')
    async def stop(self):
        if self.task and not self.task.done():self.task.cancel()
        self.task=None
    async def _loop(self):
        while True:
            if self.enabled:await self.refresh()
            await asyncio.sleep(INTERVAL)

moex_futures_shadow=MoexFuturesShadow()


