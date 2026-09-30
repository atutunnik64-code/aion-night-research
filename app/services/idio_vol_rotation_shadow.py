from __future__ import annotations
import asyncio,json,time
from pathlib import Path
import numpy as np,pandas as pd
from app.services.raven_smart_positioning_v61_shadow import raven_smart_positioning_v61_shadow,SYMS,COST

ROOT=Path(__file__).parents[2];STATE=ROOT/'data'/'idio_vol_rotation_shadow.json'
ALTS=[s for s in SYMS if s!='BTCUSDT'];LOOKBACK=63;K=2;REBALANCE_HOUR_UTC=8

class IdioVolRotationShadow:
 def __init__(self):
  self.enabled=True;self.interval=900.;self.task=None;self.last_error=None;self.last_refresh=None;self.latest={};self.lock=asyncio.Lock();self.state=self._load()
 @staticmethod
 def _blank():
  return {'equity':100.,'equity_2x':100.,'equity_3x':100.,'peak':100.,'max_dd_pct':0.,'weights':{},'observations':0,'rebalances':0,'last_bar_ts':None,'history':[],'started_at':time.time()}
 def _load(self):
  x=self._blank()
  try:x.update(json.loads(STATE.read_text(encoding='utf-8')))
  except Exception:pass
  return x
 def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
 def _feature_tables(self,frames,common):
  c=pd.DataFrame({s:frames[s].reindex(common).close for s in SYMS});r=c.pct_change().fillna(0);b=r.BTCUSDT;var=b.rolling(LOOKBACK).var().replace(0,np.nan)
  bet={};iv={}
  for s in ALTS:
   beta=r[s].rolling(LOOKBACK).cov(b)/var;res=r[s]-beta*b;bet[s]=beta;iv[s]=res.rolling(LOOKBACK).std()
  return pd.DataFrame(bet),pd.DataFrame(iv)
 def _target(self,frames,common,t):
  B,IV=self._feature_tables(frames,common);row=IV.loc[t].replace([np.inf,-np.inf],np.nan).dropna()
  if len(row)<6:return None
  lo=list(row.sort_values().index[:K]);hi=list(row.sort_values(ascending=False).index[:K]);w={s:0. for s in SYMS}
  for s in lo:w[s]=.5/len(lo)
  for s in hi:w[s]=-.5/len(hi)
  beta=sum(w[s]*float(B.at[t,s]) for s in ALTS if np.isfinite(B.at[t,s]));w['BTCUSDT']=-beta;gross=sum(abs(x) for x in w.values())
  if gross>1.:w={s:x/gross for s,x in w.items()}
  return {'weights':w,'longs':lo,'shorts':hi,'btc_hedge':w['BTCUSDT'],'gross':sum(abs(x) for x in w.values())}
 def _mark_factor(self,frames,t,prev):
  w=self.state.get('weights') or {};gross=fund=0.
  for s,z in frames.items():
   if t not in z.index or prev not in z.index:continue
   x=float(w.get(s,0));gross+=x*float(z.at[t,'close']/z.at[prev,'close']-1);fv=float(z.at[t,'fund']) if np.isfinite(z.at[t,'fund']) else 0.;fund+=x*fv
  return max(.001,1+gross-fund),gross,fund
 def _refresh_sync(self):
  try:
   frames,common=raven_smart_positioning_v61_shadow._dataset();common=common.sort_values()
   if len(common)<LOOKBACK*2:raise RuntimeError('IDIO_VOL_HISTORY_WARMUP')
   latest=common[-1];last_raw=self.state.get('last_bar_ts')
   if not last_raw:
    self.state['last_bar_ts']=str(latest);self._save();self.latest={'bar_ts':str(latest),'seed_only':True,'weights':{}}
   else:
    last=pd.Timestamp(last_raw);bars=[t for t in common if t>last]
    for t in bars:
     fac,gross,fund=self._mark_factor(frames,t,last)
     for key in ('equity','equity_2x','equity_3x'):self.state[key]=float(self.state.get(key) or 100.)*fac
     reb=False;turn=0.;sig=None
     if int(t.hour)==REBALANCE_HOUR_UTC:
      sig=self._target(frames,common,t)
      if sig:
       old=self.state.get('weights') or {};new=sig['weights'];turn=sum(abs(float(new.get(s,0))-float(old.get(s,0))) for s in SYMS)
       for mult,key in ((1.,'equity'),(2.,'equity_2x'),(3.,'equity_3x')):self.state[key]*=max(.001,1-COST*mult*turn)
       self.state['weights']=new;self.state['rebalances']=int(self.state.get('rebalances') or 0)+1;reb=True
     eq=float(self.state['equity']);self.state['peak']=max(float(self.state.get('peak') or 100.),eq);self.state['max_dd_pct']=min(float(self.state.get('max_dd_pct') or 0.),(eq/self.state['peak']-1)*100)
     self.state['observations']=int(self.state.get('observations') or 0)+1;self.state['last_bar_ts']=str(t)
     h=list(self.state.get('history') or []);h.append({'bar_ts':str(t),'equity':round(eq,6),'equity_2x':round(float(self.state['equity_2x']),6),'equity_3x':round(float(self.state['equity_3x']),6),'gross_pct':round(gross*100,6),'funding_pct':round(fund*100,6),'turnover':round(turn,6),'rebalanced':reb,'signal':sig});self.state['history']=h[-400:];last=t
    self._save();self.latest={'bar_ts':self.state.get('last_bar_ts'),'weights':self.state.get('weights') or {},'rebalances':self.state.get('rebalances',0)}
   self.last_error=None;self.last_refresh=time.time()
  except Exception as exc:self.last_error=str(exc)[:400];self.last_refresh=time.time()
  return self.status()
 async def refresh(self):
  async with self.lock:return await asyncio.to_thread(self._refresh_sync)
 def _gate(self):
  obs=int(self.state.get('observations') or 0);reb=int(self.state.get('rebalances') or 0);r1=float(self.state.get('equity') or 100.)-100.;r2=float(self.state.get('equity_2x') or 100.)-100.;r3=float(self.state.get('equity_3x') or 100.)-100.;dd=float(self.state.get('max_dd_pct') or 0.)
  return {'required_observations':90,'required_rebalances':20,'observations':obs,'rebalances':reb,'return_1x_pct':round(r1,6),'return_2x_pct':round(r2,6),'return_3x_pct':round(r3,6),'max_dd_pct':round(dd,6),'ready':bool(obs>=90 and reb>=20 and r1>0 and r2>0 and dd>=-10)}
 def status(self):
  e=float(self.state.get('equity') or 100.);e2=float(self.state.get('equity_2x') or 100.);e3=float(self.state.get('equity_3x') or 100.)
  return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'PAPER_SHADOW','strategy':'idio_vol_rotation_v1','future_only':True,'promotion_eligible':False,'live_enabled':False,'grid':False,'martingale':False,'dca':False,'cost_fragile_historical':True,'locked_config':{'lookback':LOOKBACK,'k':K,'rebalance_hour_utc':REBALANCE_HOUR_UTC,'cost_per_turnover':COST},'equity':round(e,6),'return_pct':round(e-100.,4),'equity_2x':round(e2,6),'return_2x_pct':round(e2-100.,4),'equity_3x':round(e3,6),'return_3x_pct':round(e3-100.,4),'max_dd_pct':round(float(self.state.get('max_dd_pct') or 0.),4),'observation_count':int(self.state.get('observations') or 0),'rebalance_count':int(self.state.get('rebalances') or 0),'future_gate':self._gate(),'latest':self.latest,'last_refresh':self.last_refresh,'last_error':self.last_error}
 async def start(self):
  if self.task and not self.task.done():return
  self.task=asyncio.create_task(self._loop(),name='idio-vol-rotation-shadow')
 async def stop(self):
  if self.task and not self.task.done():
   self.task.cancel()
   try:await self.task
   except BaseException:pass
  self.task=None;self._save()
 async def _loop(self):
  while True:
   await self.refresh();await asyncio.sleep(max(300.,self.interval))

idio_vol_rotation_shadow=IdioVolRotationShadow()
