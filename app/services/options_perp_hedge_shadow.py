from __future__ import annotations
import asyncio,json,time,urllib.parse,urllib.request
from pathlib import Path
ROOT=Path(__file__).parents[2];DATA=ROOT/'data'
STATE=DATA/'options_perp_hedge_shadow.json';HIST=DATA/'options_perp_hedge_history.jsonl'
TICK='https://www.deribit.com/api/v2/public/ticker'
PREMIUM='https://fapi.binance.com/fapi/v1/premiumIndex'
SOURCES={
 'options_ivrv':'options_ivrv_package_shadow.json',
 'options_relvol':'options_relative_vol_shadow.json',
 'options_term':'options_term_structure_shadow.json',
 'options_skew':'options_skew_shadow.json',
}
PERP_FEE_RATE=.0005;SLIPPAGE_RATE=.0002;DELTA_BAND=.03

def _json(path):
 try:return json.loads(path.read_text(encoding='utf-8'))
 except Exception:return {}
def _get(url,params):
 q=url+'?'+urllib.parse.urlencode(params)
 with urllib.request.urlopen(q,timeout=15) as r:return json.loads(r.read().decode())
def _ticker(name):
 x=_get(TICK,{'instrument_name':name})['result'];g=x.get('greeks') or {}
 return {'delta':float(g.get('delta') or 0),'gamma':float(g.get('gamma') or 0),'underlying':float(x.get('underlying_price') or 0),'mark_iv':float(x.get('mark_iv') or 0)}
def _perp(asset):
 x=_get(PREMIUM,{'symbol':asset+'USDT'})
 return {'mark':float(x.get('markPrice') or 0),'index':float(x.get('indexPrice') or 0),'funding_rate':float(x.get('lastFundingRate') or 0),'next_funding_time':x.get('nextFundingTime')}
def _legs(x):
 out=[]
 if isinstance(x,dict):
  if x.get('instrument') and 'side' in x:out.append(x)
  for k,v in x.items():
   if k in {'close_marks'}:continue
   if isinstance(v,(dict,list)):out.extend(_legs(v))
 elif isinstance(x,list):
  for v in x:out.extend(_legs(v))
 return out

def _positions():
 out=[]
 for source,file in SOURCES.items():
  st=_json(DATA/file)
  for p in st.get('open') or []:
   out.append({'source':source,'id':str(p.get('id') or f'{source}-{len(out)}'),'raw':p})
 return out

class OptionsPerpHedgeShadow:
 def __init__(self):
  self.enabled=True;self.interval=900.;self.task=None;self.last_error=None;self.last_refresh=None;self.state=self._load()
 def _load(self):
  base={'hedges':{},'snapshots':0,'started_at':time.time(),'live_enabled':False}
  try:base.update(_json(STATE))
  except Exception:pass
  return base
 def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
 def _position_delta(self,p):
  by_asset={};gamma_by_asset={};detail=[]
  for leg in _legs(p['raw']):
   name=str(leg.get('instrument') or '');asset=name.split('-',1)[0].upper();side=float(leg.get('side') or 0)
   if asset not in {'BTC','ETH','SOL'}:continue
   q=_ticker(name);d=side*q['delta'];g=side*q['gamma'];by_asset[asset]=by_asset.get(asset,0.0)+d;gamma_by_asset[asset]=gamma_by_asset.get(asset,0.0)+g
   detail.append({'instrument':name,'side':side,'option_delta':q['delta'],'signed_delta':d,'option_gamma':q['gamma'],'signed_gamma':g,'underlying':q['underlying'],'mark_iv':q['mark_iv']})
  return by_asset,gamma_by_asset,detail
 def _refresh_sync(self):
  now=time.time();positions=_positions();old=dict(self.state.get('hedges') or {});hedges={};rows=[]
  coverage={'positions':len(positions),'legs':0,'hedged_positions':0,'errors':0}
  for p in positions:
   try:by_asset,gamma_by_asset,detail=self._position_delta(p)
   except Exception as exc:
    coverage['errors']+=1;rows.append({'source':p['source'],'position_id':p['id'],'error':str(exc)[:180]});continue
   coverage['legs']+=len(detail);targets={};cost=0.0
   for asset,delta in by_asset.items():
    q=_perp(asset);target=-delta;key=f"{p['source']}|{p['id']}|{asset}";prev=float((old.get(key) or {}).get('target_base_units') or 0)
    change=target-prev;notional=abs(change)*q['mark'];rebalance=abs(change)>DELTA_BAND
    est_cost=notional*(PERP_FEE_RATE+SLIPPAGE_RATE) if rebalance else 0.0;cost+=est_cost
    targets[asset]={'option_net_delta':delta,'option_net_gamma':float(gamma_by_asset.get(asset) or 0.0),
                    'target_base_units':target,'previous_target_base_units':prev,
                    'rebalance_required':rebalance,'delta_change_base_units':change,'perp':q,
                    'rebalance_notional_usdt':notional if rebalance else 0.0,'estimated_execution_cost_usdt':est_cost}
    hedges[key]={'source':p['source'],'position_id':p['id'],'asset':asset,**targets[asset],'updated_at':now}
   coverage['hedged_positions']+=int(bool(targets));rows.append({'source':p['source'],'position_id':p['id'],'legs':detail,'targets':targets,'estimated_rebalance_cost_usdt':cost})
  snap={'ts':now,'mode':'PAPER_DELTA_HEDGE','live_enabled':False,'delta_band_base_units':DELTA_BAND,
        'fee_assumption':PERP_FEE_RATE,'slippage_assumption':SLIPPAGE_RATE,'coverage':coverage,'positions':rows}
  with HIST.open('a',encoding='utf-8') as f:f.write(json.dumps(snap,ensure_ascii=False)+'\n')
  self.state={'hedges':hedges,'snapshots':int(self.state.get('snapshots') or 0)+1,'last_snapshot':snap,'last_refresh':now,'live_enabled':False};self._save();return snap
 async def refresh(self):
  try:await asyncio.to_thread(self._refresh_sync);self.last_error=None
  except Exception as exc:self.last_error=str(exc)[:500]
  self.last_refresh=time.time();return self.status()
 def status(self):
  snap=self.state.get('last_snapshot') or {}
  return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'PAPER_DELTA_HEDGE','strategy':'options_perp_delta_hedge_v1',
          'future_only':True,'promotion_eligible':False,'live_enabled':False,'paper_only':True,
          'perp_market':'BINANCE_USDT_PERPETUAL','delta_source':'DERIBIT_LIVE_GREEKS',
          'delta_band_base_units':DELTA_BAND,'fee_assumption':PERP_FEE_RATE,'slippage_assumption':SLIPPAGE_RATE,
          'snapshots':int(self.state.get('snapshots') or 0),'coverage':snap.get('coverage') or {},
          'positions':snap.get('positions') or [],'last_refresh':self.last_refresh or self.state.get('last_refresh'),'last_error':self.last_error}
 async def start(self):
  if self.task and not self.task.done():return
  asyncio.create_task(self.refresh(),name='initial-options-perp-hedge-refresh');self.task=asyncio.create_task(self._loop(),name='options-perp-hedge-shadow')
 async def stop(self):
  if self.task and not self.task.done():
   self.task.cancel()
   try:await self.task
   except BaseException:pass
  self.task=None
 async def _loop(self):
  while True:
   await asyncio.sleep(max(300.,self.interval))
   if self.enabled:await self.refresh()

options_perp_hedge_shadow=OptionsPerpHedgeShadow()
