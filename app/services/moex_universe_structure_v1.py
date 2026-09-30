from __future__ import annotations
import asyncio,json,time,datetime as dt
from pathlib import Path
ROOT=Path(__file__).parents[2]; DATA=ROOT/'data'
UNIVERSE=DATA/'moex_futures_universe_v1.json'; STATE=DATA/'moex_universe_structure_v1.json'; HIST=DATA/'moex_universe_structure_v1.jsonl'
INTERVAL=300.0

def _load(p,default):
    try:return json.loads(p.read_text(encoding='utf-8'))
    except Exception:return default

def _px(r):
    for k in ('LAST','SETTLEPRICE'):
        try:
            v=float(r.get(k) or 0)
            if v>0:return v
        except Exception:pass
    return None

def _dated(r):
    s=str(r.get('LASTTRADEDATE') or '')
    try:d=dt.date.fromisoformat(s)
    except Exception:return None
    if d.year>=2099:return None
    return d
def _build():
    u=_load(UNIVERSE,{}); last=u.get('last') or {}; rows=last.get('contracts') or []
    groups={}
    for r in rows:
        asset=str(r.get('ASSETCODE') or 'UNKNOWN')
        d=_dated(r); p=_px(r)
        if not d or not p:continue
        groups.setdefault(asset,[]).append((d,p,r))
    curves=[]
    for asset,items in groups.items():
        items=sorted(items,key=lambda x:x[0])
        if len(items)<2:continue
        f,n=items[0],items[1]
        fp,np=f[1],n[1]
        curve=(np/fp-1.0)*100.0 if fp else None
        curves.append({'asset':asset,'front':f[2].get('SECID'),'front_expiry':str(f[0]),'front_px':fp,
                       'next':n[2].get('SECID'),'next_expiry':str(n[0]),'next_px':np,'curve_pct':curve,
                       'front_tier':f[2].get('tier'),'next_tier':n[2].get('tier'),
                       'front_value_today':float(f[2].get('VALTODAY') or 0),'next_value_today':float(n[2].get('VALTODAY') or 0)})
    curves.sort(key=lambda x:float(x.get('front_value_today') or 0)+float(x.get('next_value_today') or 0),reverse=True)
    liquid=[x for x in curves if x['front_tier'] in ('CORE','LIQUID') and x['next_tier'] in ('CORE','LIQUID','WATCH')]
    return {'ts':time.time(),'mode':'FUTURE_ONLY_STRUCTURE','source':'MOEX_UNIVERSE_V1','curve_assets':len(curves),
            'liquid_curve_assets':len(liquid),'curves':curves,'liquid_curves':liquid}

class MoexUniverseStructureV1:
    def __init__(self):
        self.enabled=True;self.live_enabled=False;self.task=None;self.last_error=None
        self.state=_load(STATE,{'version':'MOEX_UNIVERSE_STRUCTURE_V1','started_at':time.time(),'samples':0,'last':None})
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    async def refresh(self):
        try:
            row=await asyncio.to_thread(_build)
            self.state['samples']=int(self.state.get('samples') or 0)+1;self.state['last']=row;self.state['last_refresh']=row['ts']
            self.last_error=None;self._save()
            with HIST.open('a',encoding='utf-8') as f:f.write(json.dumps(row,ensure_ascii=False,separators=(',',':'))+'\n')
        except Exception as exc:self.last_error=str(exc)[:500]
        return self.status()
    def status(self):
        last=self.state.get('last') or {}
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'FUTURE_ONLY_STRUCTURE','live_enabled':False,'paper_only':True,
                'samples':int(self.state.get('samples') or 0),'last_refresh':self.state.get('last_refresh'),
                'curve_assets':last.get('curve_assets'),'liquid_curve_assets':last.get('liquid_curve_assets'),
                'top_liquid_curves':(last.get('liquid_curves') or [])[:50],'last_error':self.last_error,
                'policy':{'all_dated_futures_groups':True,'no_live_orders':True,'no_grid':True,'no_martingale':True,'no_dca':True,'no_rule_selection_yet':True}}
    async def start(self):
        if not self.task or self.task.done():self.task=asyncio.create_task(self._loop(),name='moex-universe-structure-v1')
    async def stop(self):
        if self.task and not self.task.done():self.task.cancel()
        self.task=None
    async def _loop(self):
        while True:
            if self.enabled:await self.refresh()
            await asyncio.sleep(INTERVAL)

moex_universe_structure_v1=MoexUniverseStructureV1()
