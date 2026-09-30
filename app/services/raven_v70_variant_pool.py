from __future__ import annotations
import asyncio,json,time
from pathlib import Path
from app.services.raven_turbo_shadow import raven_turbo_shadow,COST,FUND,SYMBOLS
ROOT=Path(__file__).parents[2];V=ROOT/'data'/'profit_branch_variants.json';STATE=ROOT/'data'/'raven_v70_variant_pool.json'

def blank(cfg,bar_ts=None,prices=None):
 return {'config':cfg,'equity':100.,'peak':100.,'max_dd_pct':0.,'weights':{},'last_prices':prices or {},'last_mark_ts':time.time(),
         'last_bar_ts':bar_ts,'costs':0.,'funding':0.,'observation_count':0,'diversified_count':0,'sleeves':[],'history':[],'started_at':time.time()}
def avg_targets(rows):
 if not rows:return {}
 keys=set().union(*(set((r.get('target') or {}).keys()) for r in rows));n=float(len(rows));out={}
 for k in keys:
  v=sum(float((r.get('target') or {}).get(k,0)) for r in rows)/n
  if abs(v)>1e-9:out[k]=v
 return out
class RavenV70VariantPool:
 def __init__(self):self.enabled=True;self.interval=900.;self.task=None;self.last_error=None;self.last_refresh=None;self.state=self._load()
 def _load(self):
  try:return json.loads(STATE.read_text(encoding='utf-8'))
  except Exception:return {'children':{}}
 def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
 def _active_variants(self):
  try:v=json.loads(V.read_text(encoding='utf-8'))
  except Exception:return []
  out=[]
  for x in v:
   if (x.get('root_parent') or x.get('parent'))!='v70' or x.get('status') not in {'RESEARCH_CANDIDATE','FUTURE_SHADOW'}:continue
   cfg=dict(x.get('config') or {})
   supported=int(cfg.get('slots',3))==3 and abs(float(cfg.get('ratio_gate',.75))-.75)<1e-9
   if supported:out.append(x)
  return out
 def _mark(self,st,prices,now):
  old=st.get('last_prices') or {};w=st.get('weights') or {};eq=float(st.get('equity') or 100.)
  if old:
   pnl=sum(float(x)*(float(prices.get(s) or 0)/float(old.get(s) or 1)-1) for s,x in w.items() if float(old.get(s) or 0)>0 and float(prices.get(s) or 0)>0)
   eq*=max(.01,1+pnl);hours=max(0.,(now-float(st.get('last_mark_ts') or now))/3600);gross=sum(abs(float(x)) for x in w.values());fund=eq*gross*FUND*(hours/8)
   eq-=fund;st['funding']=float(st.get('funding') or 0)+fund
  st['equity']=eq;st['last_prices']={k:float(v) for k,v in prices.items()};st['last_mark_ts']=now;st['peak']=max(float(st.get('peak') or eq),eq);st['max_dd_pct']=min(float(st.get('max_dd_pct') or 0),(eq/st['peak']-1)*100)
 def _rebalance(self,st,target,bar_ts):
  old={k:float(v) for k,v in (st.get('weights') or {}).items()};keys=set(old)|set(target);turn=sum(abs(float(target.get(k,0))-old.get(k,0)) for k in keys)
  eq=float(st.get('equity') or 100.);cost=eq*turn*COST;eq-=cost;st['equity']=eq;st['costs']=float(st.get('costs') or 0)+cost
  st['weights']={k:float(v) for k,v in target.items() if abs(float(v))>1e-9};st['last_bar_ts']=str(bar_ts)
  h=list(st.get('history') or []);h.append({'ts':time.time(),'bar_ts':str(bar_ts),'equity':eq,'turnover':turn,'cost':cost,'weights':st['weights']});st['history']=h[-300:]
 def _activate_status(self,vid):
  try:v=json.loads(V.read_text(encoding='utf-8'))
  except Exception:return
  changed=False
  for x in v:
   if x.get('id')==vid and x.get('status')=='RESEARCH_CANDIDATE':x['status']='FUTURE_SHADOW';x['shadow_started_at']=time.time();changed=True
  if changed:V.write_text(json.dumps(v,ensure_ascii=False,indent=2),encoding='utf-8')
 def _refresh_sync(self):
  x=dict(raven_turbo_shadow.latest or {});prices=dict(raven_turbo_shadow.state.get('last_prices') or {})
  if not x.get('bar_ts') or not prices:return
  now=time.time();bar_ts=str(x['bar_ts']);children=self.state.setdefault('children',{})
  for v in self._active_variants():
   vid=v['id'];cfg=dict(v.get('config') or {});self._activate_status(vid)
   if vid not in children:
    children[vid]=blank(cfg,bar_ts,{k:float(z) for k,z in prices.items()});continue
   st=children[vid];self._mark(st,prices,now)
   if bar_ts==str(st.get('last_bar_ts') or ''):continue
   breadth=float(x.get('breadth_bull') or 0)/max(len(SYMBOLS),1);mom=float(x.get('btc_momentum_72h_pct') or 0)/100
   use_div=breadth>=float(cfg.get('breadth_gate',.45)) and mom>=float(cfg.get('momentum_gate',0))
   raw=dict(x.get('v70_diversified_target_weights') or {}) if use_div else dict(x.get('target_weights') or {})
   sleeves=list(st.get('sleeves') or []);sleeves.append({'bar_ts':bar_ts,'target':{k:float(z) for k,z in raw.items()},'diversified':use_div})
   st['sleeves']=sleeves[-int(cfg.get('sleeves',3)):];target=avg_targets(st['sleeves']);self._rebalance(st,target,bar_ts)
   st['observation_count']=int(st.get('observation_count') or 0)+1;st['diversified_count']=int(st.get('diversified_count') or 0)+int(use_div)
  self._save()
 async def refresh(self):
  try:await asyncio.to_thread(self._refresh_sync);self.last_error=None
  except Exception as exc:self.last_error=str(exc)[:500]
  self.last_refresh=time.time();return self.status()
 def status(self):
  out={}
  for vid,st in (self.state.get('children') or {}).items():
   eq=float(st.get('equity') or 100.);obs=int(st.get('observation_count') or 0)
   out[vid]={'ok':True,'mode':'PAPER_SHADOW','strategy':'v70_variant','future_only':True,'promotion_eligible':False,'live_enabled':False,
    'grid':False,'martingale':False,'dca':False,'locked_config':st.get('config') or {},'equity':round(eq,6),'return_pct':round(eq-100,4),
    'max_dd_pct':round(float(st.get('max_dd_pct') or 0),4),'observation_count':obs,'diversified_count':int(st.get('diversified_count') or 0),
    'phase':'WARMUP' if obs<24 else 'FUTURE_VALIDATION','started_at':st.get('started_at')}
  return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'PAPER_CHILD_POOL','future_only':True,'live_enabled':False,'children':out,'last_refresh':self.last_refresh,'last_error':self.last_error}
 async def start(self):
  if self.task and not self.task.done():return
  asyncio.create_task(self.refresh(),name='initial-refresh-'+self.__class__.__name__);self.task=asyncio.create_task(self._loop(),name='raven-v70-variant-pool')
 async def stop(self):
  if self.task and not self.task.done():
   self.task.cancel()
   try:await self.task
   except BaseException:pass
  self.task=None;self._save()
 async def _loop(self):
  while True:
   if not (raven_turbo_shadow.latest or {}).get('bar_ts'):
    await asyncio.sleep(5.0);continue
   await self.refresh();await asyncio.sleep(max(300.,self.interval))
raven_v70_variant_pool=RavenV70VariantPool()
