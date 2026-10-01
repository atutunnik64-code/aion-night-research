from __future__ import annotations
import asyncio,json,time
from pathlib import Path
from app.services.basis_funding import basis_funding_scanner
from app.services.crossvenue_spot_arb_cloud_v1 import CANONICAL_MAJOR

ROOT=Path(__file__).parents[2]
STATE=ROOT/'data'/'crossvenue_spot_perp_shadow_v2.json'
EVENTS=ROOT/'data'/'crossvenue_spot_perp_shadow_v2_events.jsonl'
SAFE={'Bybit','OKX','Bitget','Gate','KuCoin'}
THRESHOLD=.05; SAFETY=.10; HORIZON=8*3600; NOTIONAL=25.0; MAX_PENDING=24
REVIEW_RAW_BASIS_PCT=5.0

class CrossVenueSpotPerpShadowV2:
 def __init__(self):
  self.enabled=True;self.live_enabled=False;self.interval=65.0;self.task=None;self.last_error=None;self.last_refresh=None
  self.state=self._load();self.latest={};self._quarantine_legacy_implausible_pending();self._save()
 def _load(self):
  base={'version':'CROSSVENUE_SPOT_PERP_SHADOW_V4_PROFIT_PRESERVING','created_at':time.time(),'confirm':{},'pending':{},'resolved':[],'quarantined_pending':[],'verification_watch':{},'scan_count':0,'last_market_refresh':None}
  try:base.update(json.loads(STATE.read_text(encoding='utf-8-sig')))
  except Exception:pass
  base['version']='CROSSVENUE_SPOT_PERP_SHADOW_V4_PROFIT_PRESERVING';base.setdefault('verification_watch',{})
  return base
 def _save(self):
  tmp=STATE.with_suffix(STATE.suffix+'.tmp');tmp.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8');tmp.replace(STATE)
 def _quarantine_legacy_implausible_pending(self):
  pending=self.state.get('pending') or {};keep={};q=list(self.state.get('quarantined_pending') or [])
  for sig,p in pending.items():
   raw=abs(float(p.get('entry_raw_basis_pct') or 0))
   if raw>REVIEW_RAW_BASIS_PCT and p.get('identity_confidence') not in {'CANONICAL_MAJOR'}:
    q.append({**p,'quarantined_at':time.time(),'quarantine_reason':'LEGACY_HIGH_BASIS_NEEDS_IDENTITY_REVIEW','review_raw_basis_pct':REVIEW_RAW_BASIS_PCT})
   else:keep[sig]=p
  self.state['pending']=keep;self.state['quarantined_pending']=q[-1000:]
 def _append(self,row):
  with EVENTS.open('a',encoding='utf-8') as f:f.write(json.dumps(row,ensure_ascii=False,separators=(',',':'))+'\n')
 def _routes(self):
  rows=list((basis_funding_scanner.market or {}).values());out=[]
  for s in rows:
   sv=str(s.get('venue') or '');base=str(s.get('base') or '')
   if sv not in SAFE:continue
   ask=float(s.get('spot_ask') or 0);bid=float(s.get('spot_bid') or 0);sf=float(s.get('spot_fee_pct') or 0)
   if ask<=0 or bid<=0 or (ask/bid-1.0)*100.0>2.0:continue
   for p in rows:
    pv=str(p.get('venue') or '')
    if pv not in SAFE or pv==sv or str(p.get('base') or '')!=base:continue
    pb=float(p.get('perp_bid') or 0);pa=float(p.get('perp_ask') or 0);pf=float(p.get('perp_fee_pct') or 0)
    if pb<=0 or pa<=0 or (pa/pb-1.0)*100.0>2.0:continue
    raw=(pb/ask-1)*100;fee_only=2*(sf+pf);execution_net=raw-fee_only;conservative_net=execution_net-SAFETY
    out.append({'signature':f'{base}:{sv}>{pv}','base':base,'spot_venue':sv,'perp_venue':pv,
     'spot_ask':ask,'spot_bid':bid,'perp_bid':pb,'perp_ask':pa,'spot_fee_pct':sf,'perp_fee_pct':pf,
     'funding_rate_pct':float(p.get('funding_rate_pct') or 0),'funding_interval_hours':max(1.0,float(p.get('funding_interval_hours') or 8.0)),
     'raw_basis_pct':raw,'fee_only_cost_pct':fee_only,'execution_net_basis_pct':execution_net,'conservative_net_basis_pct':conservative_net,
     'verification_required':abs(raw)>REVIEW_RAW_BASIS_PCT and base not in CANONICAL_MAJOR,
     'identity_confidence':'CANONICAL_MAJOR' if base in CANONICAL_MAJOR else ('NORMAL_RANGE' if abs(raw)<=REVIEW_RAW_BASIS_PCT else 'REVIEW_REQUIRED')})
  return sorted(out,key=lambda x:x['execution_net_basis_pct'],reverse=True)
 def _resolve(self,sig,pos,row,now):
  qty=float(pos['qty']);sbid=float(row['spot_bid']);pask=float(row['perp_ask'])
  sf=float(pos['spot_fee_pct'])/100;pf=float(pos['perp_fee_pct'])/100
  gross=qty*(sbid-float(pos['spot_entry']))+qty*(float(pos['perp_entry'])-pask)
  exit_fees=qty*sbid*sf+qty*pask*pf;fees=float(pos['entry_fees'])+exit_fees
  pnl=gross-fees+float(pos.get('funding_quote') or 0);capital=2*float(pos['notional'])
  ret=pnl/capital*100;ret2=(pnl-fees)/capital*100;ret3=(pnl-2*fees)/capital*100
  rec={'resolved_at':now,'opened_at':pos['opened_at'],'signature':sig,'base':pos['base'],'spot_venue':pos['spot_venue'],
   'perp_venue':pos['perp_venue'],'age_hours':(now-float(pos['opened_at']))/3600,'entry_execution_net_basis_pct':pos['entry_execution_net_basis_pct'],
   'entry_conservative_net_basis_pct':pos['entry_conservative_net_basis_pct'],'exit_basis_pct':(pask/max(sbid,1e-12)-1)*100,'gross_quote':gross,'fees_quote':fees,
   'funding_quote':float(pos.get('funding_quote') or 0),'pnl_quote':pnl,'return_on_total_capital_pct':ret,'stress_2x_return_pct':ret2,'stress_3x_return_pct':ret3,
   'identity_confidence':pos.get('identity_confidence')}
  z=list(self.state.get('resolved') or []);z.append(rec);self.state['resolved']=z[-1500:];self.state['pending'].pop(sig,None);self._append(rec)
 def _gate(self):
  z=list(self.state.get('resolved') or []);n=len(z);rets=[float(x['return_on_total_capital_pct']) for x in z];r2=[float(x['stress_2x_return_pct']) for x in z];r3=[float(x['stress_3x_return_pct']) for x in z]
  med=lambda a: sorted(a)[len(a)//2] if a else 0.0;uniq=len({x['signature'] for x in z});pos=sum(x>0 for x in rets)
  ready=bool(n>=20 and uniq>=5 and pos/n>=.6 and med(rets)>0 and med(r2)>0 and med(r3)>0)
  return {'required_resolved':20,'required_unique_routes':5,'resolved':n,'unique_routes':uniq,'positive_ratio':(pos/n if n else 0),'median_return_pct':med(rets),'median_2x_pct':med(r2),'median_3x_pct':med(r3),'ready':ready}
 async def refresh(self):
  try:
   if not basis_funding_scanner.market:await basis_funding_scanner.refresh()
   now=time.time();routes=self._routes();by={x['signature']:x for x in routes};market_ts=basis_funding_scanner.last_refresh
   pending=self.state.get('pending') or {};confirm=self.state.get('confirm') or {};watch=self.state.get('verification_watch') or {}
   for sig,pos in list(pending.items()):
    row=by.get(sig)
    if not row:continue
    elapsed=max(0.0,now-float(pos.get('last_mark') or now));mid=(float(row['perp_bid'])+float(row['perp_ask']))/2
    interval=max(1.0,float(row.get('funding_interval_hours') or pos.get('funding_interval_hours') or 8.0))
    pos['funding_quote']=float(pos.get('funding_quote') or 0)+float(pos['qty'])*mid*(float(row['funding_rate_pct'])/100)*(elapsed/(interval*3600.0));pos['last_mark']=now;pos['funding_interval_hours']=interval
    if now-float(pos['opened_at'])>=HORIZON:self._resolve(sig,pos,row,now)
   if market_ts and market_ts!=self.state.get('last_market_refresh'):
    execution_positive={x['signature']:x for x in routes if float(x['execution_net_basis_pct'])>0}
    strict={k:x for k,x in execution_positive.items() if float(x['conservative_net_basis_pct'])>=THRESHOLD}
    for sig in list(confirm):
     if sig not in execution_positive:confirm[sig]={'hits':0,'last_execution_net_pct':None,'last_conservative_net_pct':None}
    active_bases={p.get('base') for p in pending.values()}
    for sig,row in execution_positive.items():
     c=confirm.get(sig) or {'hits':0};c['hits']=int(c.get('hits') or 0)+1;c['last_execution_net_pct']=row['execution_net_basis_pct'];c['last_conservative_net_pct']=row['conservative_net_basis_pct'];confirm[sig]=c
     if row.get('verification_required'):
      watch[sig]={'last_seen':now,'hits':c['hits'],'base':row['base'],'spot_venue':row['spot_venue'],'perp_venue':row['perp_venue'],'raw_basis_pct':row['raw_basis_pct'],'execution_net_basis_pct':row['execution_net_basis_pct'],'conservative_net_basis_pct':row['conservative_net_basis_pct'],'reason':'PRESERVED_HIGH_BASIS_REQUIRES_IDENTITY_VERIFICATION'}
      continue
     if sig not in strict:continue
     if c['hits']>=3 and sig not in pending and len(pending)<MAX_PENDING and row['base'] not in active_bases:
      sf=float(row['spot_fee_pct'])/100;pf=float(row['perp_fee_pct'])/100;qty=NOTIONAL/float(row['spot_ask'])
      pending[sig]={'opened_at':now,'last_mark':now,'signature':sig,'base':row['base'],'spot_venue':row['spot_venue'],'perp_venue':row['perp_venue'],'notional':NOTIONAL,'qty':qty,
       'spot_entry':row['spot_ask'],'perp_entry':row['perp_bid'],'spot_fee_pct':row['spot_fee_pct'],'perp_fee_pct':row['perp_fee_pct'],'entry_fees':NOTIONAL*sf+qty*float(row['perp_bid'])*pf,
       'funding_quote':0.0,'funding_interval_hours':row['funding_interval_hours'],'entry_execution_net_basis_pct':row['execution_net_basis_pct'],'entry_conservative_net_basis_pct':row['conservative_net_basis_pct'],
       'entry_raw_basis_pct':row['raw_basis_pct'],'identity_confidence':row['identity_confidence']};active_bases.add(row['base']);c['hits']=0
    self.state['last_market_refresh']=market_ts;self.state['scan_count']=int(self.state.get('scan_count') or 0)+1
   self.state['confirm']=confirm;self.state['pending']=pending;self.state['verification_watch']=dict(list(watch.items())[-1000:]);self._save();g=self._gate()
   self.latest={'mode':'FUTURE_ONLY_FIXED_8H','strategy':'crossvenue_spot_perp_basis_v4_profit_preserving','live_enabled':False,'paper_only':True,
    'threshold_conservative_net_pct':THRESHOLD,'review_raw_basis_pct':REVIEW_RAW_BASIS_PCT,'confirmation_scans':3,'horizon_hours':8,'notional_per_leg':NOTIONAL,'safe_venues':sorted(SAFE),'max_pending':MAX_PENDING,
    'route_count':len(routes),'execution_positive_now':sum(float(x['execution_net_basis_pct'])>0 for x in routes),'conservative_positive_now':sum(float(x['conservative_net_basis_pct'])>=THRESHOLD for x in routes),
    'top_routes':routes[:40],'pending_count':len(self.state.get('pending') or {}),'verification_watch_count':len(self.state.get('verification_watch') or {}),'quarantined_legacy_count':len(self.state.get('quarantined_pending') or []),
    'pending':list((self.state.get('pending') or {}).values()),'verification_watch':list((self.state.get('verification_watch') or {}).values())[-30:],'gate':g}
   self.last_error=None;self.last_refresh=now
  except Exception as exc:self.last_error=str(exc)[:500]
  return self.status()
 def status(self):
  return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'FUTURE_ONLY_FIXED_8H','last_refresh':self.last_refresh,'last_error':self.last_error,'future_only_start':self.state.get('created_at'),'scan_count':int(self.state.get('scan_count') or 0),'latest':self.latest,
   'policy':{'five_venue_dynamic_universe':True,'profit_preserving_watch':True,'high_basis_not_discarded':True,'one_position_per_base':True,'no_grid':True,'no_martingale':True,'no_dca':True,'no_live_orders':True}}
 async def start(self):
  if self.task and not self.task.done():return
  self.task=asyncio.create_task(self._loop(),name='crossvenue-spot-perp-shadow-v4')
 async def stop(self):
  if self.task and not self.task.done():
   self.task.cancel()
   try:await self.task
   except BaseException:pass
  self.task=None
 async def _loop(self):
  await asyncio.sleep(20)
  while True:
   if self.enabled:await self.refresh()
   await asyncio.sleep(self.interval)

crossvenue_spot_perp_shadow_v2=CrossVenueSpotPerpShadowV2()
