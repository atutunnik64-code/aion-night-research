from __future__ import annotations
import asyncio,json,time,statistics
from pathlib import Path
from app.services.options_surface_shadow import options_surface_shadow
from app.services.options_ivrv_package_shadow import choose_exp,leg_entry,liquidate
ROOT=Path(__file__).parents[2];STATE=ROOT/'data'/'options_term_structure_shadow.json'
NEAR_DAYS=7;FAR_DAYS=30;SLOPE_MIN=3.0;HOLD=24*3600

def component(asset,exp,kind):
 legs=[]
 if kind=='LONG_STRADDLE':
  for k in ('atm_call_instrument','atm_put_instrument'):
   if not exp.get(k):return None
   legs.append(leg_entry(exp[k],1))
 else:
  spec=[('atm_call_instrument',-1),('atm_put_instrument',-1),('put90_instrument',1),('call110_instrument',1)]
  for k,side in spec:
   if not exp.get(k):return None
   legs.append(leg_entry(exp[k],side))
 return {'asset':asset,'kind':kind,'expiry':exp.get('expiry'),'legs':legs}

class OptionsTermStructureShadow:
 def __init__(self):
  self.enabled=True;self.interval=900.;self.task=None;self.last_error=None;self.last_refresh=None;self.state=self._load()
 def _load(self):
  base={'last_surface_ts':None,'open':[],'resolved':[],'started_at':time.time()}
  try:base.update(json.loads(STATE.read_text(encoding='utf-8')))
  except Exception:pass
  return base
 def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
 def _resolve_due(self,now):
  keep=[];done=list(self.state.get('resolved') or [])
  for p in self.state.get('open') or []:
   if now<float(p['resolve_at']):keep.append(p);continue
   comps=[];rets=[];err=None
   for c in p['components']:
    try:pnl,gross,marks=liquidate(c['legs']);ret=pnl/max(gross,1e-12)*100;rets.append(ret);comps.append({**c,'pnl_units':pnl,'gross_entry_units':gross,'package_return_pct':ret,'close_marks':marks})
    except Exception as exc:err=str(exc)[:200];break
   if err:p['resolve_error']=err;keep.append(p);continue
   q=dict(p);q.update({'closed_at':now,'components':comps,'calendar_package_return_pct':statistics.mean(rets),'win':statistics.mean(rets)>0});done.append(q)
  self.state['open']=keep;self.state['resolved']=done[-500:]
 def _candidate(self,st,asset,now):
  edge=st.get('edge_map') or {};a=(edge.get('assets') or {}).get(asset) or {};term=a.get('term_structure') or {};slope=float(term.get('slope_30m7') or 0)
  if abs(slope)<SLOPE_MIN:return None
  hs={int(x.get('days') or 0):x for x in a.get('horizons') or []};cur=(st.get('currencies') or {}).get(asset)
  if NEAR_DAYS not in hs or FAR_DAYS not in hs:return None
  near=choose_exp(cur,hs[NEAR_DAYS].get('source_expiries'));far=choose_exp(cur,hs[FAR_DAYS].get('source_expiries'))
  if not near or not far:return None
  cheap_exp,rich_exp=(near,far) if slope>0 else (far,near)
  x=component(asset,cheap_exp,'LONG_STRADDLE');y=component(asset,rich_exp,'DEFINED_RISK_IRON_FLY')
  if not x or not y:return None
  return {'id':f'TERM-{asset}-{int(now)}','asset':asset,'opened_at':now,'resolve_at':now+HOLD,'slope_30m7':slope,'components':[x,y]}
 def _refresh_sync(self):
  st=options_surface_shadow.status();raw=options_surface_shadow.latest or {};snap=raw.get('last_snapshot') or {};surface_ts=float(snap.get('ts') or st.get('last_refresh') or 0);now=time.time();self._resolve_due(now)
  if not surface_ts:return
  if self.state.get('last_surface_ts') is None:self.state['last_surface_ts']=surface_ts;self._save();return
  if surface_ts<=float(self.state.get('last_surface_ts') or 0):self._save();return
  self.state['last_surface_ts']=surface_ts;opened={x['asset'] for x in self.state.get('open') or []}
  for asset in ('BTC','ETH'):
   if asset in opened:continue
   try:p=self._candidate(st,asset,now)
   except Exception:p=None
   if p:self.state.setdefault('open',[]).append(p);opened.add(asset)
  self._save()
 async def refresh(self):
  try:await asyncio.to_thread(self._refresh_sync);self.last_error=None
  except Exception as exc:self.last_error=str(exc)[:500]
  self.last_refresh=time.time();return self.status()
 def status(self):
  r=list(self.state.get('resolved') or []);vals=[float(x.get('calendar_package_return_pct') or 0) for x in r];n=len(vals);wins=sum(x>0 for x in vals)
  return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'ANALYSIS_SHADOW','strategy':'options_term_structure_exact_calendar','future_only':True,
          'promotion_eligible':False,'live_enabled':False,'naked_options_enabled':False,'grid':False,'martingale':False,'dca':False,
          'locked_config':{'near_days':NEAR_DAYS,'far_days':FAR_DAYS,'slope_min_vol_points':SLOPE_MIN,'hold_hours':24},
          'open_count':len(self.state.get('open') or []),'resolved_count':n,'wins':wins,'win_rate':wins/n if n else None,
          'mean_calendar_return_pct':statistics.mean(vals) if vals else None,'median_calendar_return_pct':statistics.median(vals) if vals else None,
          'recent_resolved':r[-20:],'last_refresh':self.last_refresh,'last_error':self.last_error}
 async def start(self):
  if self.task and not self.task.done():return
  asyncio.create_task(self.refresh(),name='initial-refresh-'+self.__class__.__name__);self.task=asyncio.create_task(self._loop(),name='options-term-structure-shadow')
 async def stop(self):
  if self.task and not self.task.done():
   self.task.cancel()
   try:await self.task
   except BaseException:pass
  self.task=None;self._save()
 async def _loop(self):
  while True:
   await asyncio.sleep(max(300.,self.interval))
   if self.enabled:await self.refresh()

options_term_structure_shadow=OptionsTermStructureShadow()
