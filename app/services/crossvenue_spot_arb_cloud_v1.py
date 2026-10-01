from __future__ import annotations
import asyncio,json,time
from pathlib import Path
import httpx
from app.http_shared import SHARED_SSL_CONTEXT

ROOT=Path(__file__).parents[2]; DATA=ROOT/'data'; STATE=DATA/'crossvenue_spot_arb_cloud_v1.json'
ASSETS=('BTC','ETH','SOL','XRP','DOGE','ADA','LINK','AVAX','LTC','SUI')
VENUES=('Bybit','OKX','Bitget')
TAKER={'Bybit':0.0010,'OKX':0.0010,'Bitget':0.0010}
NOTIONAL=25.0; SAFETY_PCT=0.05; REBALANCE_RESERVE_PCT=0.10
MIN_ALL_IN_NET_PCT=0.05; COOLDOWN_SEC=300; MIN_HITS=2
DEPTH_LEVELS=20

class CrossVenueSpotArbCloudV1:
    def __init__(self):
        self.enabled=True; self.live_enabled=False; self.task=None; self.last_error=None; self.last_refresh=None; self.market={}
        try:self.state=json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:self.state={'version':'CROSSVENUE_SPOT_ARB_CLOUD_V1','started_at':time.time(),'scan_count':0,'signals':{},'events':[],'pnl_quote':0.0,'loss_reasons':{}}
    def _save(self): STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    async def _quote(self,c,venue,base):
        try:
            if venue=='Bybit':
                r=await c.get('https://api.bybit.com/v5/market/tickers',params={'category':'spot','symbol':base+'USDT'})
                d=r.json(); x=((d.get('result') or {}).get('list') or [{}])[0]
                bid=float(x.get('bid1Price') or 0); ask=float(x.get('ask1Price') or 0)
            elif venue=='OKX':
                r=await c.get('https://www.okx.com/api/v5/market/ticker',params={'instId':base+'-USDT'})
                d=r.json(); x=(d.get('data') or [{}])[0]; bid=float(x.get('bidPx') or 0); ask=float(x.get('askPx') or 0)
            else:
                r=await c.get('https://api.bitget.com/api/v2/spot/market/tickers',params={'symbol':base+'USDT'})
                d=r.json(); x=((d.get('data') or [{}])[0]); bid=float(x.get('bidPr') or 0); ask=float(x.get('askPr') or 0)
            if not r.is_success or bid<=0 or ask<=0:return None
            return {'venue':venue,'base':base,'bid':bid,'ask':ask,'ts':time.time()}
        except Exception:return None
    async def _book(self,c,venue,base):
        try:
            if venue=='Bybit':
                r=await c.get('https://api.bybit.com/v5/market/orderbook',params={'category':'spot','symbol':base+'USDT','limit':DEPTH_LEVELS})
                d=r.json(); x=d.get('result') or {}; bids=x.get('b') or []; asks=x.get('a') or []
            elif venue=='OKX':
                r=await c.get('https://www.okx.com/api/v5/market/books',params={'instId':base+'-USDT','sz':DEPTH_LEVELS})
                d=r.json(); x=(d.get('data') or [{}])[0]; bids=x.get('bids') or []; asks=x.get('asks') or []
            else:
                r=await c.get('https://api.bitget.com/api/v2/spot/market/orderbook',params={'symbol':base+'USDT','type':'step0','limit':DEPTH_LEVELS})
                d=r.json(); x=d.get('data') or {}; bids=x.get('bids') or []; asks=x.get('asks') or []
            if not r.is_success:return None
            def norm(rows):
                out=[]
                for z in rows:
                    try:
                        p=float(z[0]); q=float(z[1])
                        if p>0 and q>0:out.append((p,q))
                    except Exception:pass
                return out
            b=norm(bids); a=norm(asks)
            if not b or not a:return None
            return {'bids':b,'asks':a}
        except Exception:return None
    @staticmethod
    def _buy_vwap(asks,quote_amount):
        remain=float(quote_amount); base=0.0; spent=0.0
        for px,qty in asks:
            level_quote=px*qty; take_quote=min(remain,level_quote); take_base=take_quote/px
            base+=take_base; spent+=take_quote; remain-=take_quote
            if remain<=1e-9:break
        return {'ok':remain<=1e-6,'base_qty':base,'quote_spent':spent,'vwap':spent/base if base>0 else 0.0,'unfilled_quote':max(0.0,remain)}
    @staticmethod
    def _sell_vwap(bids,base_qty):
        remain=float(base_qty); sold=0.0; received=0.0
        for px,qty in bids:
            take=min(remain,qty); sold+=take; received+=take*px; remain-=take
            if remain<=1e-12:break
        return {'ok':remain<=1e-9,'base_sold':sold,'quote_received':received,'vwap':received/sold if sold>0 else 0.0,'unfilled_base':max(0.0,remain)}
    def _reason(self,gross,fees,all_in):
        if gross<=0:return 'NO_RAW_SPREAD'
        if gross-fees<=0:return 'TRADING_FEES'
        if all_in<=0:return 'REBALANCE_AND_SAFETY'
        if all_in<MIN_ALL_IN_NET_PCT:return 'EDGE_TOO_SMALL'
        return 'PASS'
    async def refresh(self):
        try:
            timeout=httpx.Timeout(7.0)
            async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT,timeout=timeout,follow_redirects=True) as c:
                vals=await asyncio.gather(*[self._quote(c,v,a) for a in ASSETS for v in VENUES])
                market={}
                for q in vals:
                    if q:market.setdefault(q['base'],{})[q['venue']]=q
                self.market=market; now=time.time(); events=[]; reasons={}; depth_checks=0; depth_pass=0
                for base,vm in market.items():
                    qs=list(vm.values())
                    for buy in qs:
                        for sell in qs:
                            if buy['venue']==sell['venue']:continue
                            gross=(sell['bid']/buy['ask']-1.0)*100.0
                            fees=(TAKER[buy['venue']]+TAKER[sell['venue']])*100.0
                            top_all_in=gross-fees-SAFETY_PCT-REBALANCE_RESERVE_PCT
                            reason=self._reason(gross,fees,top_all_in)
                            if reason!='PASS': reasons[reason]=reasons.get(reason,0)+1; continue
                            depth_checks+=1
                            bb,sb=await asyncio.gather(self._book(c,buy['venue'],base),self._book(c,sell['venue'],base))
                            if not bb or not sb:
                                reasons['DEPTH_UNAVAILABLE']=reasons.get('DEPTH_UNAVAILABLE',0)+1; continue
                            bfill=self._buy_vwap(bb['asks'],NOTIONAL)
                            if not bfill['ok']:
                                reasons['BUY_DEPTH_INSUFFICIENT']=reasons.get('BUY_DEPTH_INSUFFICIENT',0)+1; continue
                            sfill=self._sell_vwap(sb['bids'],bfill['base_qty'])
                            if not sfill['ok']:
                                reasons['SELL_DEPTH_INSUFFICIENT']=reasons.get('SELL_DEPTH_INSUFFICIENT',0)+1; continue
                            buy_fee=NOTIONAL*TAKER[buy['venue']]; sell_fee=sfill['quote_received']*TAKER[sell['venue']]
                            net_quote=sfill['quote_received']-NOTIONAL-buy_fee-sell_fee
                            reserve_quote=NOTIONAL*(SAFETY_PCT+REBALANCE_RESERVE_PCT)/100.0
                            all_in_quote=net_quote-reserve_quote
                            all_in=all_in_quote/NOTIONAL*100.0
                            depth_reason=self._reason((sfill['quote_received']/NOTIONAL-1.0)*100.0,(buy_fee+sell_fee)/NOTIONAL*100.0,all_in)
                            if depth_reason!='PASS': reasons['DEPTH_'+depth_reason]=reasons.get('DEPTH_'+depth_reason,0)+1; continue
                            depth_pass+=1
                            sig=f"{base}:{buy['venue']}>{sell['venue']}"
                            st=self.state.setdefault('signals',{}).get(sig) or {'hits':0,'last_seen':0,'last_event':0}
                            st['hits']=int(st.get('hits') or 0)+1; st['last_seen']=now; st['last_all_in_net_pct']=all_in; self.state['signals'][sig]=st
                            if st['hits']<MIN_HITS or now-float(st.get('last_event') or 0)<COOLDOWN_SEC:continue
                            pnl=all_in_quote; st['last_event']=now
                            ev={'ts':now,'signature':sig,'base':base,'buy_venue':buy['venue'],'sell_venue':sell['venue'],
                                'top_buy_ask':buy['ask'],'top_sell_bid':sell['bid'],'buy_vwap':bfill['vwap'],'sell_vwap':sfill['vwap'],
                                'filled_base_qty':bfill['base_qty'],'gross_depth_pct':(sfill['quote_received']/NOTIONAL-1.0)*100.0,
                                'trading_fee_quote':buy_fee+sell_fee,'safety_pct':SAFETY_PCT,'rebalance_reserve_pct':REBALANCE_RESERVE_PCT,
                                'all_in_net_pct':all_in,'notional_quote':NOTIONAL,'paper_pnl_quote':pnl,
                                'assumption':'PREFUNDED_INVENTORY_BATCHED_REBALANCE_RESERVE_DEPTH_VALIDATED'}
                            events.append(ev); self.state.setdefault('events',[]).append(ev); self.state['events']=self.state['events'][-1000:]
                            self.state['pnl_quote']=float(self.state.get('pnl_quote') or 0)+pnl
                self.state['scan_count']=int(self.state.get('scan_count') or 0)+1; self.state['last_scan_ts']=now; self.state['loss_reasons']=reasons
                self.state['last_new_events']=len(events); self.state['last_depth_checks']=depth_checks; self.state['last_depth_pass']=depth_pass; self._save()
            self.last_error=None; self.last_refresh=now
        except Exception as exc:self.last_error=str(exc)[:500]
        return self.status()
    def status(self):
        ev=self.state.get('events') or []; pnl=float(self.state.get('pnl_quote') or 0); capital=NOTIONAL*2
        return {'ok':self.last_error is None,'strategy':'CROSSVENUE_SPOT_ARB_CLOUD_V1','mode':'FUTURE_ONLY_PAPER','paper_only':True,'live_enabled':False,
            'venues':VENUES,'assets':ASSETS,'scan_count':self.state.get('scan_count',0),'event_count':len(ev),'last_new_events':self.state.get('last_new_events',0),
            'paper_pnl_quote':round(pnl,6),'return_on_two_leg_capital_pct':round(pnl/capital*100,4) if capital else 0.0,
            'depth_validation':{'levels':DEPTH_LEVELS,'last_checks':self.state.get('last_depth_checks',0),'last_pass':self.state.get('last_depth_pass',0)},
            'cost_model':{'taker_fee_each_venue':TAKER,'safety_pct':SAFETY_PCT,'rebalance_reserve_pct':REBALANCE_RESERVE_PCT,'notional_quote':NOTIONAL},
            'loss_reasons':self.state.get('loss_reasons') or {},'recent_events':ev[-20:],'last_error':self.last_error,
            'policy':{'prefunded_inventory':True,'batched_rebalance_assumption':True,'depth_validated':True,'no_grid':True,'no_martingale':True,'no_dca':True,'no_live_orders':True}}
    async def start(self):
        if self.task and not self.task.done():return
        await self.refresh(); self.task=asyncio.create_task(self._loop(),name='crossvenue-spot-arb-cloud-v1')
    async def stop(self):
        if self.task and not self.task.done():self.task.cancel(); await asyncio.gather(self.task,return_exceptions=True)
        self.task=None
    async def _loop(self):
        while True:
            await asyncio.sleep(30)
            if self.enabled:await self.refresh()

crossvenue_spot_arb_cloud_v1=CrossVenueSpotArbCloudV1()
