from __future__ import annotations
import asyncio,json,time,subprocess,sys
from pathlib import Path
ROOT=Path(__file__).parents[2];V=ROOT/'data'/'profit_branch_variants.json';LOG=ROOT/'data'/'profit_variant_worker_runs.jsonl'
SCRIPTS={'aftershock_v2':'evaluate_aftershock_variants_exact.py','v70':'evaluate_v70_variants.py','v83':'evaluate_v83_variants.py','smartpos':'evaluate_smartpos_variants.py'}
class ProfitVariantResearchWorker:
 def __init__(self):self.enabled=True;self.interval=1800.;self.task=None;self.last_error=None;self.last_run=None;self.last_actions=[]
 def _load(self):
  try:return json.loads(V.read_text(encoding='utf-8'))
  except Exception:return []
 def _save(self,v):V.write_text(json.dumps(v,ensure_ascii=False,indent=2),encoding='utf-8')
 def _run_script(self,parent,script):
  p=subprocess.run([sys.executable,str(ROOT/'research'/script)],cwd=str(ROOT),capture_output=True,text=True,timeout=180)
  row={'ts':time.time(),'parent':parent,'script':script,'returncode':p.returncode,'stdout_tail':p.stdout[-3000:],'stderr_tail':p.stderr[-1500:]}
  with LOG.open('a',encoding='utf-8') as f:f.write(json.dumps(row,ensure_ascii=False)+'\n')
  if p.returncode!=0:raise RuntimeError(f'{parent}_EVAL_FAILED:{p.stderr[-300:]}')
  return row
 def _classify_waiting(self,v):
  changed=0
  for x in v:
   if x.get('status')!='PENDING_RESEARCH':continue
   p=x.get('parent')
   if p=='options_v2':x['status']='FUTURE_SIGNAL_SEED';x['wait_reason']='HISTORICAL_IV_SURFACE_UNAVAILABLE';changed+=1
   elif p=='funding_spread':x['status']='WAIT_MARKET_EDGE';x['wait_reason']='NO_CURRENT_NET_POSITIVE_CARRY';changed+=1
   elif p=='v63_2':x['status']='WAIT_PARENT_EDGE';x['wait_reason']='PARENT_CAPITAL_GATE_CLOSED';changed+=1
  return changed
 async def refresh(self):
  actions=[]
  try:
   v=self._load();pending={(x.get('root_parent') or x.get('parent')) for x in v if x.get('status')=='PENDING_RESEARCH'}
   for parent,script in SCRIPTS.items():
    if parent in pending:
     row=await asyncio.to_thread(self._run_script,parent,script);actions.append({'parent':parent,'returncode':row['returncode']})
   v=self._load();classified=self._classify_waiting(v)
   if classified:self._save(v);actions.append({'classified_waiting':classified})
   self.last_error=None;self.last_run=time.time();self.last_actions=actions
  except Exception as exc:self.last_error=str(exc)[:500];self.last_run=time.time()
  return self.status()
 def status(self):
  v=self._load();counts={}
  for x in v:counts[x.get('status')]=counts.get(x.get('status'),0)+1
  return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'RESEARCH_ONLY','interval_seconds':self.interval,'live_enabled':False,
          'last_run':self.last_run,'last_error':self.last_error,'last_actions':self.last_actions,'variant_status_counts':counts}
 async def start(self):
  if self.task and not self.task.done():return
  self.task=asyncio.create_task(self._loop(initial=True),name='profit-variant-research-worker')
 async def stop(self):
  if self.task and not self.task.done():self.task.cancel()
  self.task=None
 async def _loop(self,initial=False):
  if initial:
   await asyncio.sleep(2.0); await self.refresh()
  while True:await asyncio.sleep(max(900.,self.interval));await self.refresh()
profit_variant_research_worker=ProfitVariantResearchWorker()
