from __future__ import annotations
import asyncio,json,time,statistics
from pathlib import Path
from app.services.options_surface_shadow import options_surface_shadow
from app.services.options_ivrv_package_shadow import choose_exp,leg_entry,liquidate
ROOT=Path(__file__).parents[2];STATE=ROOT/'data'/'options_skew_shadow.json'
HORIZON=7;SKEW_MIN=2.0;HOLD=24*3600

def skew_package(asset,exp,skew):
 if skew>0:
  spec=[('put90_instrument',-1),('put80_instrument',1),('call110_instrument',1),('call120_instrument',-1)];kind='SELL_PUT_SKEW_BUY_CALL_SKEW'
 else:
  spec=[('put90_instrument',1),('put80_instrument',-1),('call110_instrument',-1),('call120_instrument',1)];kind='BUY_PUT_SKEW_SELL_CALL_SKEW'
 legs=[]
 for k,side in spec:
  if not exp.get(k):return None
  legs.append(leg_entry(exp[k],side))
 return {'asset':asset,'kind':kind,'expiry':exp.get('expiry'),'skew':skew,'legs':legs}

class OptionsSkewShadow:
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
   try:pnl,gross,marks=liquidate(p['legs']);ret=pnl/max(gross,1e-12)*100
   except Exception as exc:p['resolve_error']=str(exc)[:200];keep.append(p);continue
   q=dict(p);q.update({'closed_at':now,'pnl_units':pnl,'gross_entry_units':gross,'skew_package_return_pct':ret,'win':ret>0,'close_marks':marks});done.append(q)
  self.state['open']=keep;self.state['resolved']=done[-500:]
 def _candidate(self,st,asset,now):
  edge=st.get('edge_map') or {};a=(edge.get('assets') or {}).get(asset) or {};h=next((x for x in a.get('horizons') or [] if int(x.get('days') or 0)==HORIZON),None)
  if not h:return None
  exp=choose_exp((st.get('currencies') or {}).get(asset),h.get('source_expiries'))
  if not exp:return None
  skew=float(exp.get('skew_down_minus_up') or 0)
  if abs(skew)<SKEW_MIN:return None
  pkg=skew_package(asset,exp,skew)
  if not pkg:return None
  return {'id':f'SKEW-{asset}-{int(now)}','opened_at':now,'resolve_at':now+HOLD,**pkg}
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
  r=list(self.state.get('resolved') or []);vals=[float(x.get('skew_package_return_pct') or 0) for x in r];n=len(vals);wins=sum(x>0 for x in vals)
  return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'ANALYSIS_SHADOW','strategy':'options_skew_defined_risk','future_only':True,
          'promotion_eligible':False,'live_enabled':False,'naked_options_enabled':False,'grid':False,'martingale':False,'dca':False,
          'locked_config':{'horizon_days':HORIZON,'skew_min_vol_points':SKEW_MIN,'hold_hours':24},
          'open_count':len(self.state.get('open') or []),'resolved_count':n,'wins':wins,'win_rate':wins/n if n else None,
          'mean_skew_return_pct':statistics.mean(vals) if vals else None,'median_skew_return_pct':statistics.median(vals) if vals else None,
          'recent_resolved':r[-20:],'last_refresh':self.last_refresh,'last_error':self.last_error}
 async def start(self):
  if self.task and not self.task.done():return
  asyncio.create_task(self.refresh(),name='initial-refresh-'+self.__class__.__name__);self.task=asyncio.create_task(self._loop(),name='options-skew-shadow')
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

options_skew_shadow=OptionsSkewShadow()
