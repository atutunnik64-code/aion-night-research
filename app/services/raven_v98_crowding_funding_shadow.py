from __future__ import annotations
import asyncio,json,time,urllib.request,urllib.parse
from pathlib import Path
import pandas as pd
from app.services.raven_turbo_shadow import raven_turbo_shadow,COST,FUND
from app.services.raven_v70_regime_shadow import raven_v70_regime_shadow
from app.services.raven_v94_bybit_short_sizing_shadow import raven_v94_bybit_short_sizing_shadow
ROOT=Path(__file__).parents[2];STATE=ROOT/'data'/'raven_v98_crowding_funding_shadow.json';HIST=ROOT/'data'/'derivatives_history'/'binance_funding_BTCUSDT.csv'
TRAIL=180;URL='https://fapi.binance.com/fapi/v1/fundingRate'

def hist_funding():
 d=pd.read_csv(HIST);d['ts']=pd.to_datetime(d.fundingTime,unit='ms',utc=True);d['fund']=pd.to_numeric(d.fundingRate,errors='coerce')
 return d[['ts','fund']].dropna().sort_values('ts')
HIST_F=hist_funding()

def fetch_funding():
 q=urllib.parse.urlencode({'symbol':'BTCUSDT','limit':1000})
 with urllib.request.urlopen(URL+'?'+q,timeout=10) as r:x=json.loads(r.read().decode())
 return [{'timestamp':int(z['fundingTime']),'fund':float(z['fundingRate'])} for z in x]

class RavenV98CrowdingFundingShadow:
 def __init__(self):
  self.enabled=True;self.interval=300.;self.task=None;self.last_error=None;self.last_refresh=None;self.latest={};self.state=self._load()
 def _load(self):
  base={'equity':100.,'peak':100.,'max_dd_pct':0.,'weights':{},'last_prices':{},'last_mark_ts':None,'last_bar_ts':None,'costs':0.,'funding':0.,'observation_count':0,'fund_rows':[],'history':[],'short_episodes':[],'in_short_episode':False}
  try:base.update(json.loads(STATE.read_text(encoding='utf-8')))
  except Exception:pass
  return base
 def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
 def _merge_funding(self,rows):
  old={int(x['timestamp']):float(x['fund']) for x in self.state.get('fund_rows') or []}
  for x in rows:old[int(x['timestamp'])]=float(x['fund'])
  ks=sorted(old)[-1200:];self.state['fund_rows']=[{'timestamp':k,'fund':old[k]} for k in ks]
 def _fund_pct(self,bar_ts):
  live=pd.DataFrame(self.state.get('fund_rows') or [])
  if len(live):
   live['ts']=pd.to_datetime(live.timestamp,unit='ms',utc=True);live=live[['ts','fund']]
   d=pd.concat([HIST_F,live],ignore_index=True).drop_duplicates('ts',keep='last').sort_values('ts')
  else:d=HIST_F
  s=d.set_index('ts').fund.sort_index().resample('8h').last();bar=pd.Timestamp(bar_ts)
  if bar not in s.index or not pd.notna(s.loc[bar]):return None
  pos=s.index.get_loc(bar);h=s.iloc[max(0,pos-TRAIL):pos+1].dropna()
  if len(h)<120:return None
  cur=float(s.loc[bar]);return {'funding':cur,'percentile':float((h<=cur).mean()),'samples':int(len(h))}
 def _mark(self,prices,now):
  old=self.state.get('last_prices') or {};w=self.state.get('weights') or {};eq=float(self.state.get('equity') or 100.)
  if old:
   pnl=sum(float(x)*(float(prices.get(s) or 0)/float(old.get(s) or 1)-1) for s,x in w.items() if float(old.get(s) or 0)>0 and float(prices.get(s) or 0)>0)
   eq*=max(.01,1+pnl);hours=max(0.,(now-float(self.state.get('last_mark_ts') or now))/3600);gross=sum(abs(float(x)) for x in w.values());fc=eq*gross*FUND*(hours/8);eq-=fc;self.state['funding']=float(self.state.get('funding') or 0)+fc
  self.state['equity']=eq;self.state['last_prices']={k:float(v) for k,v in prices.items()};self.state['last_mark_ts']=now;self.state['peak']=max(float(self.state.get('peak') or eq),eq);self.state['max_dd_pct']=min(float(self.state.get('max_dd_pct') or 0),(eq/self.state['peak']-1)*100)
 def _rebalance(self,target,bar_ts):
  old={k:float(v) for k,v in (self.state.get('weights') or {}).items()};keys=set(old)|set(target);turn=sum(abs(float(target.get(k,0))-float(old.get(k,0))) for k in keys)
  eq=float(self.state.get('equity') or 100.);cost=eq*turn*COST;self.state['equity']=eq-cost;self.state['costs']=float(self.state.get('costs') or 0)+cost;self.state['weights']={k:float(v) for k,v in target.items() if abs(float(v))>1e-12};self.state['last_bar_ts']=str(bar_ts)
  h=list(self.state.get('history') or []);h.append({'bar_ts':str(bar_ts),'equity':self.state['equity'],'turnover':turn,'cost':cost,'weights':self.state['weights']});self.state['history']=h[-300:]
 def _episode(self,is_short,bar_ts,v70eq):
  active=bool(self.state.get('in_short_episode'))
  if is_short and not active:
   self.state['in_short_episode']=True;self.state['short_start_bar']=str(bar_ts);self.state['short_start_equity']=float(self.state.get('equity') or 100.);self.state['short_start_v70']=float(v70eq)
  elif (not is_short) and active:
   a=float(self.state.get('short_start_equity') or 100.);b=float(self.state.get('short_start_v70') or 100.);eq=float(self.state.get('equity') or 100.);v=float(v70eq)
   row={'start':self.state.get('short_start_bar'),'end':str(bar_ts),'candidate_pct':(eq/a-1)*100,'v70_pct':(v/b-1)*100,'alpha_pp':(eq/a-v/b)*100}
   h=list(self.state.get('short_episodes') or []);h.append(row);self.state['short_episodes']=h[-100:];self.state['in_short_episode']=False
 def _target(self,base,bar_ts):
  target={k:float(v) for k,v in (base or {}).items()};is_short=sum(target.values())<-1e-12
  if not is_short:return target,False,1.0,None
  ri=raven_v94_bybit_short_sizing_shadow._ratio_scale(bar_ts);fi=self._fund_pct(bar_ts)
  if ri is None or fi is None:return None,True,None,{'ratio':ri,'funding':fi}
  score=.5*(float(ri['percentile'])+float(fi['percentile']));sc=.875+.25*score
  return {k:v*sc for k,v in target.items()},True,sc,{'ratio':ri,'funding':fi,'score':score}
 async def refresh(self):
  try:
   v70=raven_v70_regime_shadow.status();x=v70.get('latest') or {};prices=dict(raven_turbo_shadow.state.get('last_prices') or {})
   if not x.get('bar_ts') or not prices:self.latest={'waiting_for':'V70_SNAPSHOT'};return self.status()
   bar=str(x['bar_ts']);new=bar!=str(self.state.get('last_bar_ts') or '')
   if new or not self.state.get('fund_rows'):self._merge_funding(await asyncio.to_thread(fetch_funding))
   now=time.time();self._mark(prices,now);target,is_short,sc,info=self._target(x.get('target_weights') or {},bar)
   if target is None:self.latest={'bar_ts':bar,'waiting_for':'CLOSED_CROWDING_DATA','is_short':True};self._save();return self.status()
   if not self.state.get('last_bar_ts') or new:
    self._rebalance(target,bar);self.state['observation_count']=int(self.state.get('observation_count') or 0)+int(bool(self.state.get('last_bar_ts')));self._episode(is_short,bar,float(v70.get('equity') or 100.))
   self._save();self.last_error=None;self.last_refresh=now;eq=float(self.state.get('equity') or 100.)
   self.latest={'bar_ts':bar,'paper_equity':eq,'paper_return_pct':eq-100,'is_short':is_short,'short_scale':sc,'crowding':info,'target_weights':target}
  except Exception as exc:self.last_error=str(exc)[:400];self.last_refresh=time.time()
  return self.status()
 def status(self):
  eq=float(self.state.get('equity') or 100.);eps=list(self.state.get('short_episodes') or []);a=[float(x.get('alpha_pp') or 0) for x in eps];pos=sum(x>0 for x in a)/len(a) if a else 0.;med=float(pd.Series(a).median()) if a else 0.;gate={'required_episodes':10,'completed':len(eps),'positive_alpha_ratio':pos,'median_alpha_pp':med,'ready':len(eps)>=10 and pos>=.6 and med>0}
  return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'PAPER_SHADOW','strategy':'v98_crowding_funding_short_sizing','future_only':True,'promotion_eligible':False,'live_enabled':False,'grid':False,'martingale':False,'dca':False,'fragile_historical_alpha':True,'locked_rule':'short_scale=.875+.25*mean(BTC_Bybit_LS_pct,BTC_funding_pct)','equity':round(eq,6),'return_pct':round(eq-100,4),'max_dd_pct':round(float(self.state.get('max_dd_pct') or 0),4),'observation_count':int(self.state.get('observation_count') or 0),'future_short_gate':gate,'latest':self.latest,'last_refresh':self.last_refresh,'last_error':self.last_error}
 async def start(self):
  if self.task and not self.task.done():return
  asyncio.create_task(self.refresh(),name='initial-refresh-'+self.__class__.__name__);self.task=asyncio.create_task(self._loop(),name='raven-v98-crowding-funding-shadow')
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

raven_v98_crowding_funding_shadow=RavenV98CrowdingFundingShadow()
