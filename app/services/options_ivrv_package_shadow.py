from __future__ import annotations
import asyncio,json,time,urllib.parse,urllib.request,statistics
from pathlib import Path
from app.services.options_surface_shadow import options_surface_shadow
ROOT=Path(__file__).parents[2];STATE=ROOT/'data'/'options_ivrv_package_shadow.json'
TICK='https://www.deribit.com/api/v2/public/ticker';HOLD=24*3600;EDGE_MIN=4.0;HORIZON=7

def ticker(name):
 url=TICK+'?'+urllib.parse.urlencode({'instrument_name':name})
 with urllib.request.urlopen(url,timeout=15) as r:x=json.loads(r.read().decode())['result']
 return {'mark':float(x.get('mark_price') or 0),'bid':float(x.get('best_bid_price') or 0),'ask':float(x.get('best_ask_price') or 0)}

def expiry_map(cur):return {x.get('expiry'):x for x in (cur or {}).get('expiries') or []}

def choose_exp(cur,names):
 m=expiry_map(cur)
 for n in reversed(names or []):
  if n in m:return m[n]
 return None

def leg_entry(name,side):
 q=ticker(name);px=q['ask'] if side>0 else q['bid']
 if px<=0:raise ValueError(f'NO_EXECUTABLE_ENTRY_QUOTE:{name}:{side}')
 return {'instrument':name,'side':side,'entry':px}

def liquidate(legs):
 pnl=0.;gross=0.;marks=[]
 for l in legs:
  q=ticker(l['instrument']);side=int(l['side']);entry=float(l['entry'])
  close=q['bid'] if side>0 else q['ask']
  if close<=0:raise ValueError(f'NO_EXECUTABLE_CLOSE_QUOTE:{l["instrument"]}:{side}')
  p=(close-entry)*side
  pnl+=p;gross+=abs(entry);marks.append({'instrument':l['instrument'],'close':close,'pnl':p})
 return pnl,gross,marks
class OptionsIVRVPackageShadow:
 def __init__(self):
  self.enabled=True;self.interval=900.;self.task=None;self.last_error=None;self.last_refresh=None;self.state=self._load()
 def _load(self):
  base={'last_surface_ts':None,'open':[],'resolved':[],'started_at':time.time()}
  try:base.update(json.loads(STATE.read_text(encoding='utf-8')))
  except Exception:pass
  return base
 def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
 def _signals(self,st):
  edge=st.get('edge_map') or {};cur=st.get('currencies') or {};out=[]
  for asset in ('BTC','ETH'):
   a=(edge.get('assets') or {}).get(asset) or {};h=next((x for x in a.get('horizons') or [] if int(x.get('days') or 0)==HORIZON),None)
   if not h or abs(float(h.get('iv_minus_forecast_rv') or 0))<EDGE_MIN:continue
   exp=choose_exp(cur.get(asset),h.get('source_expiries'))
   if not exp:continue
   out.append((asset,h,exp))
  return out
 def _open_one(self,asset,h,e,now):
  state=h.get('state');legs=[]
  if state=='IV_CHEAP':
   for k in ('atm_call_instrument','atm_put_instrument'):
    if not e.get(k):return None
    legs.append(leg_entry(e[k],1))
   kind='LONG_STRADDLE'
  elif state=='IV_RICH':
   spec=[('atm_call_instrument',-1),('atm_put_instrument',-1),('put90_instrument',1),('call110_instrument',1)]
   for k,side in spec:
    if not e.get(k):return None
    legs.append(leg_entry(e[k],side))
   kind='DEFINED_RISK_IRON_FLY'
  else:return None
  return {'id':f'{asset}-{int(now)}','asset':asset,'kind':kind,'opened_at':now,'resolve_at':now+HOLD,'edge_state':state,
          'iv_minus_rv':float(h.get('iv_minus_forecast_rv') or 0),'expiry':e.get('expiry'),'underlying':float(e.get('underlying') or 0),'legs':legs}
 def _resolve_due(self,now):
  keep=[];done=list(self.state.get('resolved') or [])
  for p in self.state.get('open') or []:
   if now<float(p['resolve_at']):keep.append(p);continue
   try:pnl,gross,marks=liquidate(p['legs']);ret=pnl/max(gross,1e-12)*100
   except Exception as exc:p['resolve_error']=str(exc)[:200];keep.append(p);continue
   q=dict(p);q.update({'closed_at':now,'pnl_underlying_units':pnl,'gross_entry_premium_units':gross,
                       'package_premium_return_pct':ret,'win':ret>0,'close_marks':marks});done.append(q)
  self.state['open']=keep;self.state['resolved']=done[-500:]
 def _refresh_sync(self):
  st=options_surface_shadow.status();raw=options_surface_shadow.latest or {};snap=raw.get('last_snapshot') or {}
  surface_ts=float(snap.get('ts') or st.get('last_refresh') or 0);now=time.time();self._resolve_due(now)
  if not surface_ts:return
  if self.state.get('last_surface_ts') is None:
   self.state['last_surface_ts']=surface_ts;self._save();return
  if surface_ts<=float(self.state.get('last_surface_ts') or 0):self._save();return
  self.state['last_surface_ts']=surface_ts;opened={x['asset'] for x in self.state.get('open') or []}
  for asset,h,e in self._signals(st):
   if asset in opened:continue
   try:p=self._open_one(asset,h,e,now)
   except Exception:continue
   if p:self.state.setdefault('open',[]).append(p);opened.add(asset)
  self._save()
 async def refresh(self):
  try:await asyncio.to_thread(self._refresh_sync);self.last_error=None
  except Exception as exc:self.last_error=str(exc)[:500]
  self.last_refresh=time.time();return self.status()
 def status(self):
  r=list(self.state.get('resolved') or []);vals=[float(x.get('package_premium_return_pct') or 0) for x in r]
  wins=sum(x>0 for x in vals);n=len(vals)
  return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'ANALYSIS_SHADOW','strategy':'options_ivrv_exact_package',
          'future_only':True,'promotion_eligible':False,'live_enabled':False,'naked_options_enabled':False,
          'grid':False,'martingale':False,'dca':False,'locked_config':{'horizon_days':HORIZON,'edge_min_vol_points':EDGE_MIN,'hold_hours':24},
          'open_count':len(self.state.get('open') or []),'resolved_count':n,'wins':wins,'win_rate':wins/n if n else None,
          'mean_package_return_pct':statistics.mean(vals) if vals else None,'median_package_return_pct':statistics.median(vals) if vals else None,
          'recent_resolved':r[-20:],'last_refresh':self.last_refresh,'last_error':self.last_error}
 async def start(self):
  if self.task and not self.task.done():return
  asyncio.create_task(self.refresh(),name='initial-refresh-'+self.__class__.__name__);self.task=asyncio.create_task(self._loop(),name='options-ivrv-package-shadow')
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

options_ivrv_package_shadow=OptionsIVRVPackageShadow()
