from __future__ import annotations
import asyncio,json,time
from pathlib import Path
from app.services.raven_v70_regime_shadow import raven_v70_regime_shadow
from app.services.raven_v47_shadow import raven_v47_shadow
ROOT=Path(__file__).parents[2];V=ROOT/'data'/'profit_branch_variants.json';STATE=ROOT/'data'/'raven_v83_variant_pool.json'
COST70=.0012;COSTSQ=.0011;FUND=.00005;THRESH=.23328
def blank(cfg,bar=None,prices=None):
 return {'config':cfg,'equity':100.,'peak':100.,'max_dd_pct':0.,'weights':{},'last_prices':prices or {},'last_bar_ts':bar,'strong_history':[],
         'observations':0,'active_observations':0,'costs':0.,'funding':0.,'history':[],'started_at':time.time()}
class RavenV83VariantPool:
 def __init__(self):self.enabled=True;self.interval=900.;self.task=None;self.last_error=None;self.last_refresh=None;self.state=self._load()
 def _load(self):
  try:return json.loads(STATE.read_text(encoding='utf-8'))
  except Exception:return {'children':{}}
 def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
 def _active(self):
  try:v=json.loads(V.read_text(encoding='utf-8'))
  except Exception:return []
  return [x for x in v if (x.get('root_parent') or x.get('parent'))=='v83' and x.get('status') in {'RESEARCH_CANDIDATE','FUTURE_SHADOW'} and abs(float((x.get('config') or {}).get('strength_threshold',THRESH))-THRESH)<1e-9]
 def _activate(self,vid):
  try:v=json.loads(V.read_text(encoding='utf-8'))
  except Exception:return
  ch=False
  for x in v:
   if x.get('id')==vid and x.get('status')=='RESEARCH_CANDIDATE':x['status']='FUTURE_SHADOW';x['shadow_started_at']=time.time();ch=True
  if ch:V.write_text(json.dumps(v,ensure_ascii=False,indent=2),encoding='utf-8')
 def _mark(self,st,prices):
  old=st.get('last_prices') or {};w=st.get('weights') or {};eq=float(st.get('equity') or 100.);pnl=0.;gs=0.
  if old:
   for s,x in w.items():
    a=float(old.get(s) or 0);b=float(prices.get(s) or 0)
    if a>0 and b>0:pnl+=float(x)*(b/a-1)
    if float(x)<0:gs+=abs(float(x))
   eq*=max(.001,1+pnl);fund=eq*gs*FUND;eq-=fund;st['funding']=float(st.get('funding') or 0)+fund
  st['equity']=eq;st['last_prices']={k:float(v) for k,v in prices.items() if float(v)>0};st['peak']=max(float(st.get('peak') or eq),eq);st['max_dd_pct']=min(float(st.get('max_dd_pct') or 0),(eq/st['peak']-1)*100)
 def _rebalance(self,st,target,filler_syms):
  old=st.get('weights') or {};keys=set(old)|set(target);turn=sum(abs(float(target.get(k,0))-float(old.get(k,0))) for k in keys)
  trsq=sum(abs(float(target.get(k,0))-float(old.get(k,0))) for k in keys if k in filler_syms);tr70=max(0.,turn-trsq);eq=float(st.get('equity') or 100.)
  cost=eq*(tr70*COST70+trsq*COSTSQ);st['equity']=eq-cost;st['costs']=float(st.get('costs') or 0)+cost;st['weights']={k:float(v) for k,v in target.items() if abs(float(v))>1e-12}
  return turn,cost
 def _refresh_sync(self):
  v70=dict(raven_v70_regime_shadow.latest or {});v47=dict(raven_v47_shadow.latest or {});bar70=str(v70.get('bar_ts') or '');bar47=str(v47.get('bar_ts') or '')
  if not bar70 or bar70!=bar47:return
  prices=dict(raven_v47_shadow.state.get('last_closes') or {});base=dict(v70.get('target_weights') or {});sq=dict(v47.get('squeeze_raw') or {});children=self.state.setdefault('children',{})
  for v in self._active():
   vid=v['id'];cfg=dict(v.get('config') or {});p=int(cfg.get('persistence_bars',2));alpha=float(cfg.get('alpha',.25));self._activate(vid)
   if vid not in children:
    children[vid]=blank(cfg,bar70,{k:float(z) for k,z in prices.items()});continue
   st=children[vid];self._mark(st,prices)
   if bar70==str(st.get('last_bar_ts') or ''):continue
   strong=sum(abs(float(x)) for x in sq.values())>=float(cfg.get('strength_threshold',THRESH));h=list(st.get('strong_history') or []);h.append({'bar_ts':bar70,'strong':strong});h=h[-max(3,p):];st['strong_history']=h
   confirmed=len(h)>=p and all(bool(x['strong']) for x in h[-p:]);filler={} if base or not confirmed else {k:alpha*float(x) for k,x in sq.items() if abs(float(x))>1e-12}
   target=dict(base)
   for k,x in filler.items():target[k]=target.get(k,0.)+x
   turn,cost=self._rebalance(st,target,set(sq));st['last_bar_ts']=bar70;st['observations']=int(st.get('observations') or 0)+1;st['active_observations']=int(st.get('active_observations') or 0)+int(bool(filler))
   hist=list(st.get('history') or []);hist.append({'bar_ts':bar70,'equity':st['equity'],'strong':strong,'confirmed':confirmed,'filler':filler,'turnover':turn,'cost':cost});st['history']=hist[-300:]
  self._save()
 async def refresh(self):
  try:await asyncio.to_thread(self._refresh_sync);self.last_error=None
  except Exception as exc:self.last_error=str(exc)[:500]
  self.last_refresh=time.time();return self.status()
 def status(self):
  out={}
  for vid,st in (self.state.get('children') or {}).items():
   eq=float(st.get('equity') or 100.);obs=int(st.get('observations') or 0)
   out[vid]={'ok':True,'mode':'PAPER_SHADOW','strategy':'v83_variant','future_only':True,'promotion_eligible':False,'live_enabled':False,'grid':False,'martingale':False,'dca':False,
    'locked_config':st.get('config') or {},'equity':round(eq,6),'return_pct':round(eq-100,4),'max_dd_pct':round(float(st.get('max_dd_pct') or 0),4),
    'observation_count':obs,'active_observation_count':int(st.get('active_observations') or 0),'phase':'WARMUP' if obs<24 else 'FUTURE_VALIDATION','started_at':st.get('started_at')}
  return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'PAPER_CHILD_POOL','future_only':True,'live_enabled':False,'children':out,'last_refresh':self.last_refresh,'last_error':self.last_error}
 async def start(self):
  if self.task and not self.task.done():return
  asyncio.create_task(self.refresh(),name='initial-refresh-'+self.__class__.__name__);self.task=asyncio.create_task(self._loop(),name='raven-v83-variant-pool')
 async def stop(self):
  if self.task and not self.task.done():
   self.task.cancel()
   try:await self.task
   except BaseException:pass
  self.task=None;self._save()
 async def _loop(self):
  while True:
   v70=raven_v70_regime_shadow.latest or {};v47=raven_v47_shadow.latest or {}
   if not v70.get('bar_ts') or str(v70.get('bar_ts'))!=str(v47.get('bar_ts')):await asyncio.sleep(5.0);continue
   await self.refresh();await asyncio.sleep(max(300.,self.interval))
raven_v83_variant_pool=RavenV83VariantPool()
