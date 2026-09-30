from __future__ import annotations
import asyncio,json,time,statistics
from pathlib import Path
from app.services.raven_turbo_shadow import raven_turbo_shadow,COST,FUND
from app.services.raven_v70_regime_shadow import raven_v70_regime_shadow
ROOT=Path(__file__).parents[2];STATE=ROOT/'data'/'raven_v76_robust_short_veto_shadow.json'
M21_MAX=-0.01;VR_MAX=1.6

class RavenV76RobustShortVetoShadow:
 def __init__(self):
  self.enabled=True;self.interval=900.;self.task=None;self.last_error=None;self.last_refresh=None;self.latest={};self.state=self._load()
 def _load(self):
  base={'equity':100.,'peak':100.,'max_dd_pct':0.,'weights':{},'last_prices':{},'last_mark_ts':None,'last_bar_ts':None,
        'costs':0.,'funding':0.,'observation_count':0,'veto_count':0,'history':[],'started_at':time.time(),
        'in_short_episode':False,'episode_affected':False,'short_episodes':[]}
  try:base.update(json.loads(STATE.read_text(encoding='utf-8')))
  except Exception:pass
  return base
 def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
 def _mark(self,prices,now):
  old=self.state.get('last_prices') or {};w=self.state.get('weights') or {};eq=float(self.state.get('equity') or 100.)
  if old:
   pnl=sum(float(x)*(float(prices.get(s) or 0)/float(old.get(s) or 1)-1) for s,x in w.items() if float(old.get(s) or 0)>0 and float(prices.get(s) or 0)>0)
   eq*=max(.01,1+pnl);hours=max(0.,(now-float(self.state.get('last_mark_ts') or now))/3600);short=sum(abs(float(x)) for x in w.values() if float(x)<0)
   fc=eq*short*FUND*(hours/8);eq-=fc;self.state['funding']=float(self.state.get('funding') or 0)+fc
  self.state['equity']=eq;self.state['last_prices']={k:float(v) for k,v in prices.items()};self.state['last_mark_ts']=now
  self.state['peak']=max(float(self.state.get('peak') or eq),eq);self.state['max_dd_pct']=min(float(self.state.get('max_dd_pct') or 0),(eq/self.state['peak']-1)*100)
 def _rebalance(self,target,bar_ts):
  old={k:float(v) for k,v in (self.state.get('weights') or {}).items()};keys=set(old)|set(target)
  turn=sum(abs(float(target.get(k,0))-float(old.get(k,0))) for k in keys);eq=float(self.state.get('equity') or 100.)
  cost=eq*turn*COST;self.state['equity']=eq-cost;self.state['costs']=float(self.state.get('costs') or 0)+cost
  self.state['weights']={k:float(v) for k,v in target.items() if abs(float(v))>1e-12};self.state['last_bar_ts']=str(bar_ts)
  h=list(self.state.get('history') or []);h.append({'bar_ts':str(bar_ts),'equity':self.state['equity'],'turnover':turn,'cost':cost,'weights':self.state['weights']});self.state['history']=h[-300:]
 def _episode(self,raw_short,affected,bar_ts,v70eq):
  active=bool(self.state.get('in_short_episode'))
  if raw_short and not active:
   self.state['in_short_episode']=True;self.state['episode_affected']=bool(affected);self.state['short_start_bar']=str(bar_ts)
   self.state['short_start_equity']=float(self.state.get('equity') or 100.);self.state['short_start_v70']=float(v70eq)
  elif raw_short and active:self.state['episode_affected']=bool(self.state.get('episode_affected')) or bool(affected)
  elif (not raw_short) and active:
   a=float(self.state.get('short_start_equity') or 100.);b=float(self.state.get('short_start_v70') or 100.);eq=float(self.state.get('equity') or 100.);v=float(v70eq)
   row={'start':self.state.get('short_start_bar'),'end':str(bar_ts),'candidate_pct':(eq/a-1)*100,'v70_pct':(v/b-1)*100,
        'alpha_pp':(eq/a-v/b)*100,'affected':bool(self.state.get('episode_affected'))}
   h=list(self.state.get('short_episodes') or []);h.append(row);self.state['short_episodes']=h[-100:]
   self.state['in_short_episode']=False;self.state['episode_affected']=False
 @staticmethod
 def _filter(raw,x):
  raw_short=sum(float(v) for v in raw.values())<-1e-12
  if not raw_short:return raw,False,False
  m21=float(x.get('btc_momentum_168h_pct') or 0)/100.;vr=float(x.get('btc_vol_ratio') or 99.)
  allowed=m21<=M21_MAX and vr<=VR_MAX
  return (raw if allowed else {}),True,(not allowed)
 async def refresh(self):
  try:
   v70=raven_v70_regime_shadow.status();x=dict(raven_turbo_shadow.latest or {});vx=v70.get('latest') or {};prices=dict(raven_turbo_shadow.state.get('last_prices') or {})
   if not vx.get('bar_ts') or not prices or not x.get('bar_ts') or str(vx.get('bar_ts'))!=str(x.get('bar_ts')):
    self.latest={'waiting_for':'V70_TURBO_SYNC'};self.last_error=None;self.last_refresh=time.time();return self.status()
   bar=str(vx['bar_ts']);now=time.time();self._mark(prices,now);raw=dict(vx.get('target_weights') or {});target,raw_short,veto=self._filter(raw,x)
   new=bar!=str(self.state.get('last_bar_ts') or '')
   if not self.state.get('last_bar_ts'):
    self.state['last_bar_ts']=bar;self.state['last_prices']={k:float(v) for k,v in prices.items()};self.latest={'bar_ts':bar,'waiting_for':'first_future_closed_bar','observed_veto':veto,'raw_short':raw_short};self._save();return self.status()
   if new:
    self._rebalance(target,bar);self.state['observation_count']=int(self.state.get('observation_count') or 0)+1;self.state['veto_count']=int(self.state.get('veto_count') or 0)+int(veto)
    self._episode(raw_short,veto,bar,float(v70.get('equity') or 100.))
   self._save();self.last_error=None;self.last_refresh=now;eq=float(self.state.get('equity') or 100.)
   self.latest={'bar_ts':bar,'paper_equity':eq,'paper_return_pct':eq-100,'raw_short':raw_short,'veto_applied':veto,
                'btc_momentum_168h_pct':x.get('btc_momentum_168h_pct'),'btc_vol_ratio':x.get('btc_vol_ratio'),'target_weights':target}
  except Exception as exc:self.last_error=str(exc)[:400];self.last_refresh=time.time()
  return self.status()
 def status(self):
  eq=float(self.state.get('equity') or 100.);eps=[e for e in (self.state.get('short_episodes') or []) if e.get('affected')]
  a=[float(e.get('alpha_pp') or 0) for e in eps];pos=sum(x>0 for x in a)/len(a) if a else 0.;med=float(statistics.median(a)) if a else 0.
  gate={'required_affected_episodes':10,'completed':len(eps),'positive_alpha_ratio':pos,'median_alpha_pp':med,'ready':len(eps)>=10 and pos>=.6 and med>0}
  return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'PAPER_SHADOW','strategy':'v76_robust_short_veto','future_only':True,
          'promotion_eligible':False,'live_enabled':False,'grid':False,'martingale':False,'dca':False,
          'locked_rule':{'btc_momentum_168h_max':M21_MAX,'btc_vol_ratio_max':VR_MAX},'equity':round(eq,6),'return_pct':round(eq-100,4),
          'max_dd_pct':round(float(self.state.get('max_dd_pct') or 0),4),'observation_count':int(self.state.get('observation_count') or 0),
          'veto_count':int(self.state.get('veto_count') or 0),'future_gate':gate,'latest':self.latest,'last_refresh':self.last_refresh,'last_error':self.last_error}
 async def start(self):
  if self.task and not self.task.done():return
  asyncio.create_task(self.refresh(),name='initial-refresh-'+self.__class__.__name__);self.task=asyncio.create_task(self._loop(),name='raven-v76-robust-short-veto-shadow')
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

raven_v76_robust_short_veto_shadow=RavenV76RobustShortVetoShadow()
