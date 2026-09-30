from __future__ import annotations
import asyncio,json,time,re
from pathlib import Path
ROOT=Path(__file__).parents[2];DATA=ROOT/'data';OUT=DATA/'research_archive_candidates.json';DECISIONS=DATA/'research_archive_decisions.json'

class ResearchArchiveMiner:
 def __init__(self):self.enabled=True;self.interval=1800.;self.task=None;self.last_error=None;self.last_run=None;self.latest={}
 @staticmethod
 def _ret(x,key):
  q=x.get(key);return float(q.get('return_pct')) if isinstance(q,dict) and q.get('return_pct') is not None else None
 @staticmethod
 def _dd(x):
  q=x.get('holdout_180d') or {};return float(q.get('max_dd_pct') or 0)
 def _scan_sync(self):
  rows=[];rejected=0
  try: decisions=json.loads(DECISIONS.read_text(encoding='utf-8'))
  except Exception: decisions={}
  for p in DATA.rglob('*.json'):
   try:x=json.loads(p.read_text(encoding='utf-8'))
   except Exception:continue
   if not isinstance(x,dict) or not isinstance(x.get('holdout_180d'),dict):continue
   strategy=str(x.get('strategy') or x.get('name') or '')
   if 'audit' in p.stem.lower() or strategy.upper().endswith('_AUDIT') or x.get('source_strategy'):continue
   ret=self._ret(x,'holdout_180d');dd=self._dd(x)
   if ret is None or ret<=0:continue
   if x.get('selection_uses_holdout') is True or x.get('grid') is True or x.get('grid_trading') is True or x.get('martingale') is True or x.get('dca') is True:rejected+=1;continue
   r2=self._ret(x,'holdout_double_cost');r3=self._ret(x,'holdout_triple_cost');seg=x.get('holdout_segments_60d') or x.get('holdout_segments') or []
   segv=[float(z.get('return_pct') or z.get('candidate',{}).get('return_pct') or 0) for z in seg if isinstance(z,dict)]
   if r2 is not None and r2<=0:rejected+=1;continue
   if r3 is not None and r3<=0:rejected+=1;continue
   if dd<-15:rejected+=1;continue
   stress_tier=2 if r2 is not None and r3 is not None else (1 if r2 is not None or r3 is not None else 0)
   h=x.get('holdout_180d') or {}; evidence=h.get('events',h.get('trade_count',h.get('resolved'))); evidence=int(evidence) if evidence is not None else None
   score=ret-.75*abs(dd)+(r2 or 0)*.25+(r3 or 0)*.25
   if stress_tier==0:score-=20
   elif stress_tier==1:score-=7
   seg_ratio=(sum(v>=0 for v in segv)/len(segv)) if segv else None
   if segv:score+=5*seg_ratio-.5*abs(min(segv))
   sparse=bool(evidence is not None and evidence<10)
   episode_count=None; episode_positive_ratio=None; episode_median_alpha=None; episode_fragile=False
   m=re.search(r'v(\d+)',p.name,re.I)
   if m:
    af=list(DATA.glob(f'*v{m.group(1)}*episode*attribution*.json'))
    if af:
     try:
      aa=json.loads(af[0].read_text(encoding='utf-8')); ss=aa.get('summary') or {}
      episode_count=int(ss.get('count',ss.get('episode_count',0)) or 0)
      pos=int(ss.get('positive',ss.get('positive_alpha_episodes',0)) or 0)
      episode_positive_ratio=(pos/episode_count) if episode_count else 0.0
      episode_median_alpha=float(ss.get('median_alpha_pp') or 0.0)
      episode_fragile=bool(episode_count<5 or episode_positive_ratio<.60 or episode_median_alpha<=0)
     except Exception:pass
   if episode_fragile:score-=18
   decision=decisions.get(str(x.get('strategy') or '')) or {}; review_status=decision.get('status'); review_reason=decision.get('reason')
   if sparse: score-=15
   priority='FRAGILE' if episode_fragile else ('HIGH' if (not sparse) and stress_tier==2 and dd>=-12 and (seg_ratio is None or seg_ratio>=.66) else ('MEDIUM' if (not sparse) and stress_tier>=1 and dd>=-15 else ('SPARSE' if sparse else 'LOW')))
   if review_status in {'REJECTED','ACTIVE_SHADOW','ACTIVE'}: priority=review_status
   rows.append({'file':str(p.relative_to(ROOT)),'strategy':x.get('strategy') or x.get('name'),'holdout_return_pct':ret,'max_dd_pct':dd,'double_cost_pct':r2,'triple_cost_pct':r3,'segments':segv,'stress_tier':stress_tier,'evidence_count':evidence,'episode_count':episode_count,'episode_positive_ratio':episode_positive_ratio,'episode_median_alpha_pp':episode_median_alpha,'episode_fragile':episode_fragile,'audit_priority':priority,'review_status':review_status,'review_reason':review_reason,'score':score})
  pr={'HIGH':6,'MEDIUM':5,'FRAGILE':4,'LOW':3,'SPARSE':2,'ACTIVE_SHADOW':1,'ACTIVE':1,'REJECTED':0}
  best={}
  for z in rows:
   k=str(z.get('strategy') or z.get('file'))
   cur=best.get(k)
   if cur is None or (pr.get(z['audit_priority'],0),z['stress_tier'],z['score'])>(pr.get(cur['audit_priority'],0),cur['stress_tier'],cur['score']):best[k]=z
  rows=list(best.values());rows.sort(key=lambda z:(pr.get(z['audit_priority'],0),z['stress_tier'],z['score']),reverse=True);queue=[z for z in rows if z['audit_priority']=='HIGH'][:30];payload={'generated_at':time.time(),'candidate_count':len(rows),'rejected_count':rejected,'audit_queue':queue,'top':rows[:100]};OUT.write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding='utf-8');return payload
 async def refresh(self):
  try:self.latest=await asyncio.to_thread(self._scan_sync);self.last_error=None
  except Exception as exc:self.last_error=str(exc)[:500]
  self.last_run=time.time();return self.status()
 def status(self):
  x=self.latest
  if not x:
   try:x=json.loads(OUT.read_text(encoding='utf-8'))
   except Exception:x={}
  return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'RESEARCH_ONLY','live_enabled':False,'interval_seconds':self.interval,'candidate_count':x.get('candidate_count',0),'rejected_count':x.get('rejected_count',0),'top_candidates':(x.get('top') or [])[:20],'last_run':self.last_run,'last_error':self.last_error}
 async def start(self):
  if self.task and not self.task.done():return
  self.task=asyncio.create_task(self._loop(initial=True),name='research-archive-miner')
 async def stop(self):
  if self.task and not self.task.done():
   self.task.cancel()
   try:await self.task
   except BaseException:pass
  self.task=None
 async def _loop(self,initial=False):
  if initial:
   await asyncio.sleep(2.0)
   if self.enabled: await self.refresh()
  while True:
   await asyncio.sleep(max(900.,self.interval))
   if self.enabled:await self.refresh()

research_archive_miner=ResearchArchiveMiner()



