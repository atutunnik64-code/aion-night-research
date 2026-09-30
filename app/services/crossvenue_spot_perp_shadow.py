from __future__ import annotations
import asyncio,json,time
from pathlib import Path
from app.services.basis_funding import basis_funding_scanner

ROOT=Path(__file__).parents[2]
STATE=ROOT/'data'/'crossvenue_spot_perp_shadow_v1.json'
EVENTS=ROOT/'data'/'crossvenue_spot_perp_shadow_v1_events.jsonl'
SAFE={'Binance','Bybit','OKX','Bitget'}
THRESHOLD=.05; SAFETY=.10; HORIZON=8*3600; NOTIONAL=25.0

class CrossVenueSpotPerpShadow:
 def __init__(self):
  self.enabled=True;self.live_enabled=False;self.interval=65.0;self.task=None;self.last_error=None;self.last_refresh=None
  self.state=self._load();self.latest={}
 def _load(self):
  base={'version':'CROSSVENUE_SPOT_PERP_SHADOW_V1','created_at':time.time(),'confirm':{},'pending':{},'resolved':[],'scan_count':0,'last_market_refresh':None}
  try:base.update(json.loads(STATE.read_text(encoding='utf-8')))
  except Exception:pass
  return base
 def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
 def _append(self,row):
  with EVENTS.open('a',encoding='utf-8') as f:f.write(json.dumps(row,ensure_ascii=False,separators=(',',':'))+'\n')
 def _routes(self):
  rows=list((basis_funding_scanner.market or {}).values());out=[]
  for s in rows:
   sv=str(s.get('venue') or '');base=str(s.get('base') or '')
   if sv not in SAFE:continue
   ask=float(s.get('spot_ask') or 0);bid=float(s.get('spot_bid') or 0);sf=float(s.get('spot_fee_pct') or 0)
   if ask<=0 or bid<=0:continue
   for p in rows:
    pv=str(p.get('venue') or '')
    if pv not in SAFE or pv==sv or str(p.get('base') or '')!=base:continue
    pb=float(p.get('perp_bid') or 0);pa=float(p.get('perp_ask') or 0);pf=float(p.get('perp_fee_pct') or 0)
    if pb<=0 or pa<=0:continue
    raw=(pb/ask-1)*100;cost=2*(sf+pf)+SAFETY;net=raw-cost
    out.append({'signature':f'{base}:{sv}>{pv}','base':base,'spot_venue':sv,'perp_venue':pv,
     'spot_ask':ask,'spot_bid':bid,'perp_bid':pb,'perp_ask':pa,'spot_fee_pct':sf,'perp_fee_pct':pf,
     'funding_8h_pct':float(p.get('funding_rate_pct') or 0),'raw_basis_pct':raw,'roundtrip_cost_pct':cost,'net_basis_pct':net})
  return sorted(out,key=lambda x:x['net_basis_pct'],reverse=True)

 def _resolve(self,sig,pos,row,now):
  qty=float(pos['qty']);sbid=float(row['spot_bid']);pask=float(row['perp_ask'])
  sf=float(pos['spot_fee_pct'])/100;pf=float(pos['perp_fee_pct'])/100
  gross=qty*(sbid-float(pos['spot_entry']))+qty*(float(pos['perp_entry'])-pask)
  exit_fees=qty*sbid*sf+qty*pask*pf;fees=float(pos['entry_fees'])+exit_fees
  pnl=gross-fees+float(pos.get('funding_quote') or 0);capital=2*float(pos['notional'])
  ret=pnl/capital*100;ret2=(pnl-fees)/capital*100;ret3=(pnl-2*fees)/capital*100
  rec={'resolved_at':now,'opened_at':pos['opened_at'],'signature':sig,'base':pos['base'],'spot_venue':pos['spot_venue'],
   'perp_venue':pos['perp_venue'],'age_hours':(now-float(pos['opened_at']))/3600,'entry_net_basis_pct':pos['entry_net_basis_pct'],
   'exit_basis_pct':(pask/max(sbid,1e-12)-1)*100,'gross_quote':gross,'fees_quote':fees,'funding_quote':float(pos.get('funding_quote') or 0),
   'pnl_quote':pnl,'return_on_total_capital_pct':ret,'stress_2x_return_pct':ret2,'stress_3x_return_pct':ret3}
  z=list(self.state.get('resolved') or []);z.append(rec);self.state['resolved']=z[-500:];self.state['pending'].pop(sig,None);self._append(rec)

 def _gate(self):
  z=list(self.state.get('resolved') or []);n=len(z);rets=[float(x['return_on_total_capital_pct']) for x in z]
  r2=[float(x['stress_2x_return_pct']) for x in z];r3=[float(x['stress_3x_return_pct']) for x in z]
  med=lambda a: sorted(a)[len(a)//2] if a else 0.0
  uniq=len({x['signature'] for x in z});pos=sum(x>0 for x in rets)
  ready=bool(n>=10 and uniq>=3 and pos/n>=.6 and med(rets)>0 and med(r2)>0 and med(r3)>0)
  return {'required_resolved':10,'required_unique_routes':3,'resolved':n,'unique_routes':uniq,
   'positive_ratio':(pos/n if n else 0),'median_return_pct':med(rets),'median_2x_pct':med(r2),'median_3x_pct':med(r3),'ready':ready}

 async def refresh(self):
  try:
   if not basis_funding_scanner.market:await basis_funding_scanner.refresh()
   now=time.time();routes=self._routes();by={x['signature']:x for x in routes};market_ts=basis_funding_scanner.last_refresh
   pending=self.state.get('pending') or {};confirm=self.state.get('confirm') or {}
   for sig,pos in list(pending.items()):
    row=by.get(sig)
    if not row:continue
    elapsed=max(0.0,now-float(pos.get('last_mark') or now));mid=(float(row['perp_bid'])+float(row['perp_ask']))/2
    pos['funding_quote']=float(pos.get('funding_quote') or 0)+float(pos['qty'])*mid*(float(row['funding_8h_pct'])/100)*(elapsed/28800)
    pos['last_mark']=now
    if now-float(pos['opened_at'])>=HORIZON:self._resolve(sig,pos,row,now)
   if market_ts and market_ts!=self.state.get('last_market_refresh'):
    strict={x['signature']:x for x in routes if float(x['net_basis_pct'])>=THRESHOLD}
    for sig in list(confirm):
     if sig not in strict:confirm[sig]={'hits':0,'last_net_pct':None}
    for sig,row in strict.items():
     c=confirm.get(sig) or {'hits':0};c['hits']=int(c.get('hits') or 0)+1;c['last_net_pct']=row['net_basis_pct'];confirm[sig]=c
     if c['hits']>=2 and sig not in pending:
      sf=float(row['spot_fee_pct'])/100;pf=float(row['perp_fee_pct'])/100;qty=NOTIONAL/float(row['spot_ask'])
      pending[sig]={'opened_at':now,'last_mark':now,'signature':sig,'base':row['base'],'spot_venue':row['spot_venue'],'perp_venue':row['perp_venue'],
       'notional':NOTIONAL,'qty':qty,'spot_entry':row['spot_ask'],'perp_entry':row['perp_bid'],'spot_fee_pct':row['spot_fee_pct'],
       'perp_fee_pct':row['perp_fee_pct'],'entry_fees':NOTIONAL*sf+qty*float(row['perp_bid'])*pf,'funding_quote':0.0,
       'entry_net_basis_pct':row['net_basis_pct'],'entry_raw_basis_pct':row['raw_basis_pct']};c['hits']=0
    self.state['last_market_refresh']=market_ts;self.state['scan_count']=int(self.state.get('scan_count') or 0)+1
   self.state['confirm']=confirm;self.state['pending']=pending;self._save();g=self._gate()
   self.latest={'mode':'FUTURE_ONLY_FIXED_8H','strategy':'crossvenue_spot_perp_basis_v1','live_enabled':False,'paper_only':True,
    'threshold_net_pct':THRESHOLD,'confirmation_scans':2,'horizon_hours':8,'notional_per_leg':NOTIONAL,'safe_venues':sorted(SAFE),'htx_excluded':True,
    'route_count':len(routes),'strict_positive_now':sum(float(x['net_basis_pct'])>=THRESHOLD for x in routes),'top_routes':routes[:12],
    'pending_count':len(self.state.get('pending') or {}),'pending':list((self.state.get('pending') or {}).values()),'gate':g}
   self.last_error=None;self.last_refresh=now
  except Exception as exc:self.last_error=str(exc)[:500]
  return self.status()
 def status(self):
  return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'FUTURE_ONLY_FIXED_8H','last_refresh':self.last_refresh,
   'last_error':self.last_error,'future_only_start':self.state.get('created_at'),'scan_count':int(self.state.get('scan_count') or 0),'latest':self.latest}
 async def start(self):
  if self.task and not self.task.done():return
  self.task=asyncio.create_task(self._loop(),name='crossvenue-spot-perp-shadow')
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

crossvenue_spot_perp_shadow=CrossVenueSpotPerpShadow()
