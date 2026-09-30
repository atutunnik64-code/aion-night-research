from __future__ import annotations
import asyncio,json,time,math
from pathlib import Path
ROOT=Path(__file__).parents[2];DATA=ROOT/'data'
EVENTS=DATA/'profit_branch_events.jsonl';NETWORK=DATA/'profit_branch_network.json';OUT=DATA/'trade_postmortems.json';MUT=DATA/'postmortem_mutations.json'

class TradePostmortemAnalyzer:
 def __init__(self):self.enabled=True;self.interval=900.;self.task=None;self.last_error=None;self.last_run=None;self.latest={}
 @staticmethod
 def _num(d,*keys):
  for k in keys:
   try:
    v=d.get(k)
    if v is not None and math.isfinite(float(v)):return float(v)
   except Exception:pass
  return None
 @classmethod
 def _value(cls,row):
  p=row.get('payload') or {};kind=row.get('kind')
  if kind=='EQUITY_STEP':return cls._num(p,'delta')
  if kind=='CLOSED_REBALANCE_CYCLE':return cls._num(p,'cycle_return_pct')
  if kind=='RESOLVED_VOL_SIGNAL':
   v=cls._num(p,'package_return_pct','return_pct','edge_pct');return v if v is not None else (1.0 if p.get('correct') is True else (-1.0 if p.get('correct') is False else None))
  return cls._num(p,'net_pct','pnl_pct','return_pct','package_return_pct','pnl','profit')
 @staticmethod
 def _reason(row,val,tail_cut=None):
  p=row.get('payload') or {};reasons=[]
  if val is not None and val<0:
   turn=float(p.get('turnover') or p.get('closing_turnover') or 0);cost=float(p.get('cost') or 0);fund=float(p.get('funding_pct') or 0)
   gross=float(p.get('gross_pct') or 0)
   if turn>=1.0 or cost>0:reasons.append('COST_SENSITIVE')
   if abs(fund)>0 and abs(fund)>=abs(gross)*.25:reasons.append('FUNDING_DRAG')
   if tail_cut is not None and abs(val)>=tail_cut:reasons.append('TAIL_LOSS')
   if not reasons:reasons.append('SIGNAL_LOSS')
  elif val is not None and val>0:reasons.append('WIN')
  else:reasons.append('UNRESOLVED')
  return reasons
 def _network(self):
  try:return json.loads(NETWORK.read_text(encoding='utf-8')).get('branches') or {}
  except Exception:return {}
 def _events(self):
  rows=[]
  if not EVENTS.exists():return rows
  for line in EVENTS.read_text(encoding='utf-8').splitlines():
   try:rows.append(json.loads(line))
   except Exception:pass
  return rows
 def _scan_sync(self):
  rows=self._events();net=self._network();by={}
  for r in rows:by.setdefault(str(r.get('branch') or 'unknown'),[]).append(r)
  summaries={};reviews=[];mutations=[]
  for branch,evs in by.items():
   vals=[self._value(x) for x in evs];known=[x for x in vals if x is not None]
   absvals=sorted(abs(x) for x in known);tail_cut=(absvals[int(.8*(len(absvals)-1))]*2 if len(absvals)>=5 else None)
   wins=[x for x in known if x>0];losses=[x for x in known if x<0];netv=sum(known);pf=(sum(wins)/abs(sum(losses))) if losses else (999.0 if wins else None)
   streak=mx=0
   for v in known:
    if v<0:streak+=1;mx=max(mx,streak)
    else:streak=0
   tail_losses=sum(1 for v in losses if tail_cut is not None and abs(v)>=tail_cut)
   wr=(len(wins)/len(known)) if known else None;stage=(net.get(branch) or {}).get('stage')
   if len(known)<5:diag='SPARSE'
   elif netv<0 and wr is not None and wr<.35:diag='NEGATIVE_EXPECTANCY'
   elif pf is not None and pf<.8:diag='LOSS_DOMINATED'
   elif tail_losses>=2:diag='TAIL_RISK'
   elif netv>0 and pf is not None and pf>=1.2:diag='PROMISING'
   else:diag='MIXED'
   summaries[branch]={'events':len(evs),'resolved':len(known),'wins':len(wins),'losses':len(losses),'win_rate':wr,'net_value':netv,'profit_factor':pf,'max_loss_streak':mx,'tail_losses':tail_losses,'diagnosis':diag,'network_stage':stage}
   for r,v in zip(evs,vals):reviews.append({'id':r.get('id'),'ts':r.get('ts'),'branch':branch,'kind':r.get('kind'),'value':v,'outcome':'WIN' if v is not None and v>0 else ('LOSS' if v is not None and v<0 else 'UNKNOWN'),'reasons':self._reason(r,v,tail_cut)})
   if stage in {'PROFIT_BRANCH','PORTFOLIO_ELIGIBLE'} and len(known)>=5:
    recipe='PRESERVE_EDGE'
    if diag=='TAIL_RISK':recipe='TAIL_RISK_CAP'
    elif diag in {'NEGATIVE_EXPECTANCY','LOSS_DOMINATED'}:recipe='TIGHTEN_ENTRY_GATE'
    elif mx>=3:recipe='LOSS_STREAK_COOLDOWN'
    mutations.append({'id':f'{branch}:{recipe}','branch':branch,'recipe':recipe,'status':'PENDING_RESEARCH','source_events':len(known),'created_at':time.time(),'live_enabled':False})
  payload={'generated_at':time.time(),'event_count':len(rows),'branch_count':len(summaries),'summaries':summaries,'recent_reviews':reviews[-200:],'mutation_queue':mutations}
  OUT.write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding='utf-8');MUT.write_text(json.dumps(mutations,ensure_ascii=False,indent=2),encoding='utf-8');return payload
 async def refresh(self):
  try:self.latest=await asyncio.to_thread(self._scan_sync);self.last_error=None
  except Exception as exc:self.last_error=str(exc)[:500]
  self.last_run=time.time();return self.status()
 def status(self):
  x=self.latest
  if not x:
   try:x=json.loads(OUT.read_text(encoding='utf-8'))
   except Exception:x={}
  return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'RESEARCH_CONTROL','live_enabled':False,'interval_seconds':self.interval,'event_count':x.get('event_count',0),'branch_count':x.get('branch_count',0),'mutation_count':len(x.get('mutation_queue') or []),'summaries':x.get('summaries') or {},'recent_reviews':(x.get('recent_reviews') or [])[-30:],'last_run':self.last_run,'last_error':self.last_error}
 async def start(self):
  if self.task and not self.task.done():return
  asyncio.create_task(self.refresh(),name='trade-postmortem-initial');self.task=asyncio.create_task(self._loop(),name='trade-postmortem-analyzer')
 async def stop(self):
  if self.task and not self.task.done():
   self.task.cancel()
   try:await self.task
   except BaseException:pass
  self.task=None
 async def _loop(self):
  while True:
   await asyncio.sleep(max(300.,self.interval));await self.refresh()

trade_postmortem_analyzer=TradePostmortemAnalyzer()
