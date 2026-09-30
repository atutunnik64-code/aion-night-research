from __future__ import annotations
import asyncio,json,time
from pathlib import Path
ROOT=Path(__file__).parents[2];DATA=ROOT/'data'
DEC=DATA/'research_archive_decisions.json';OUT=DATA/'research_opportunity_radar.json'
HYPOTHESES=[
 {'id':'liquidation_sweep_recovery','family':'Liquidations','stage':'FUTURE_ONLY_COLLECT','needs':'public liquidation flow + OI + mark/index','terms':['LIQUIDATION_SWEEP_RECOVERY']},
 {'id':'cross_exchange_mark_index_dislocation','family':'Mark / Index','stage':'RESEARCH_DESIGN','needs':'mark-index divergence across Binance/Bybit/OKX','terms':['MARK_INDEX_DISLOCATION']},
 {'id':'venue_fragmentation_liquidity_shift','family':'Liquidity Migration','stage':'RESEARCH_DESIGN','needs':'cross-venue depth share + spread migration','terms':['VENUE_FRAGMENTATION_LIQUIDITY_SHIFT']},
 {'id':'funding_oi_state_transition','family':'Funding + OI','stage':'RESEARCH_DESIGN','needs':'funding sign/state + OI transition, market-neutral test','terms':['FUNDING_OI_STATE_TRANSITION']},
 {'id':'options_perp_gamma_rebalance','family':'Options + Perp','stage':'FUTURE_ONLY_DESIGN','needs':'delta/gamma proxy + perp hedge rebalance costs','terms':['OPTIONS_PERP_GAMMA_REBALANCE']},
 {'id':'cross_asset_dispersion_entropy','family':'Dispersion','stage':'RESEARCH_DESIGN','needs':'cross-sectional dispersion concentration/entropy','terms':['CROSS_ASSET_DISPERSION_ENTROPY']},
 {'id':'dated_futures_curve_dislocation','family':'Futures Curve','stage':'RESEARCH_DESIGN','needs':'perp vs dated-futures calendar basis across Binance/OKX','terms':['DATED_FUTURES_CURVE_DISLOCATION']},
 {'id':'options_funding_regime_mismatch','family':'Vol Г— Funding','stage':'FUTURE_ONLY_DESIGN','needs':'Deribit IV/skew vs perp funding + OI regime mismatch','terms':['OPTIONS_FUNDING_REGIME_MISMATCH']},
 {'id':'crossvenue_liquidation_asymmetry','family':'Liquidation Cross-Venue','stage':'FUTURE_ONLY_COLLECT','needs':'Binance + Bybit liquidation-flow asymmetry with OI context','terms':['CROSSVENUE_LIQUIDATION_ASYMMETRY']},
 {'id':'crossvenue_oi_migration','family':'OI Migration Cross-Venue','stage':'FUTURE_ONLY_COLLECT','needs':'Binance/Bybit OI share migration + total OI growth + funding','terms':['CROSSVENUE_OI_MIGRATION']},
 {'id':'major_alt_liquidation_contagion','family':'Liquidation Contagion','stage':'RESEARCH_DESIGN','needs':'BTC/ETH/SOL liquidation shock to lagged alt response','terms':['LIQUIDATION_CONTAGION_MAJOR_ALT']},
 {'id':'liquidation_absorption','family':'Liquidation Absorption','stage':'FUTURE_ONLY_COLLECT','needs':'liquidation pressure + opposite book absorption across perp venues','terms':['LIQUIDATION_ABSORPTION']},
 {'id':'funding_reset_drift','family':'Funding Reset Drift','stage':'FUTURE_ONLY_COLLECT','needs':'exact funding settlement transition + next-hour perp drift','terms':['FUNDING_RESET_DRIFT']},
 {'id':'crossvenue_taker_imbalance_divergence','family':'Orderflow Cross-Venue','stage':'RESEARCH_DESIGN','needs':'Binance/Bybit taker imbalance divergence with OI confirmation','terms':['CROSSVENUE_TAKER_IMBALANCE_DIVERGENCE']},
]
class ResearchOpportunityRadar:
 def __init__(self):self.interval=1800.;self.task=None;self.last_error=None;self.latest={}
 def _scan(self):
  try:d=json.loads(DEC.read_text(encoding='utf-8'))
  except Exception:d={}
  rows=[]
  for h in HYPOTHESES:
   matches=[]
   for k,v in d.items():
    text=(str(k)+' '+str(v.get('reason',''))).upper()
    if any(t.upper() in text for t in h['terms']):matches.append(v)
   x=dict(h)
   if not matches:x['status']='QUEUED'
   elif any(v.get('status')=='ACTIVE_SHADOW' for v in matches):x['status']='ACTIVE_SHADOW'
   elif all(v.get('status')=='REJECTED' for v in matches):x['status']='RELATED_REJECTED'
   else:x['status']='RELATED_HISTORY'
   x['live_enabled']=False;rows.append(x)
  rejected=sum(1 for v in d.values() if v.get('status')=='REJECTED');queued=[x for x in rows if x['status']=='QUEUED']
  p={'generated_at':time.time(),'mode':'RESEARCH_ONLY','live_enabled':False,'queue_count':len(queued),'related_count':len(rows)-len(queued),'archive_rejected_count':rejected,'queue':queued,'all':rows}
  OUT.write_text(json.dumps(p,ensure_ascii=False,indent=2),encoding='utf-8');return p
 async def refresh(self):
  try:self.latest=await asyncio.to_thread(self._scan);self.last_error=None
  except Exception as e:self.last_error=str(e)[:500]
  return self.status()
 def status(self):
  if not self.latest:
   try:self.latest=json.loads(OUT.read_text(encoding='utf-8'))
   except Exception:self.latest=self._scan()
  return {'ok':self.last_error is None,**self.latest,'interval_seconds':self.interval,'last_error':self.last_error}
 async def _loop(self):
  await asyncio.sleep(3)
  while True:
   await self.refresh();await asyncio.sleep(self.interval)
 async def start(self):
  if self.task and not self.task.done():return
  self.task=asyncio.create_task(self._loop(),name='research-opportunity-radar')
 async def stop(self):
  if self.task and not self.task.done():
   self.task.cancel()
   try:await self.task
   except BaseException:pass
  self.task=None
research_opportunity_radar=ResearchOpportunityRadar()

