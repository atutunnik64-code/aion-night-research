from __future__ import annotations
import asyncio,json,time,re
from pathlib import Path
import httpx
from app.http_shared import SHARED_SSL_CONTEXT

ROOT=Path(__file__).parents[2]; DATA=ROOT/'data'; STATE=DATA/'crossvenue_spot_arb_cloud_v1.json'
VENUES=('Bybit','OKX','Bitget','Gate','KuCoin')
TAKER={v:0.0010 for v in VENUES}
NOTIONAL=25.0; SAFETY_PCT=0.05; REBALANCE_RESERVE_PCT=0.10
MIN_ALL_IN_NET_PCT=0.05; COOLDOWN_SEC=300; MIN_HITS=2
DEPTH_LEVELS=20; MAX_DEPTH_CHECKS_PER_SCAN=40
LEVERAGED=re.compile(r'(?:3L|3S|5L|5S|BULL|BEAR|UP|DOWN)$',re.I)

class CrossVenueSpotArbCloudV1:
    def __init__(self):
        self.enabled=True; self.live_enabled=False; self.task=None; self.last_error=None; self.last_refresh=None; self.market={}
        try:self.state=json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:self.state={'version':'CROSSVENUE_SPOT_ARB_CLOUD_V1','started_at':time.time(),'scan_count':0,'signals':{},'events':[],'pnl_quote':0.0,'loss_reasons':{}}
        self.state['version']='CROSSVENUE_SPOT_ARB_CLOUD_V2_DYNAMIC_UNIVERSE'
    def _save(self): STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    @staticmethod
    def _base_ok(base):
        b=str(base or '').upper().strip()
        return bool(b and len(b)<=24 and not LEVERAGED.search(b) and b not in {'USDT','USDC','USD','DAI','FDUSD'})
    async def _bulk_tickers(self,c,venue):
        out={}
        try:
            if venue=='Bybit':
                r=await c.get('https://api.bybit.com/v5/market/tickers',params={'category':'spot'})
                rows=((r.json().get('result') or {}).get('list') or [])
                for x in rows:
                    s=str(x.get('symbol') or '')
                    if not s.endswith('USDT'):continue
                    base=s[:-4]; bid=float(x.get('bid1Price') or 0); ask=float(x.get('ask1Price') or 0)
                    if self._base_ok(base) and bid>0 and ask>0:out[base]={'venue':venue,'base':base,'bid':bid,'ask':ask}
            elif venue=='OKX':
                r=await c.get('https://www.okx.com/api/v5/market/tickers',params={'instType':'SPOT'})
                rows=r.json().get('data') or []
                for x in rows:
                    s=str(x.get('instId') or '')
                    if not s.endswith('-USDT'):continue
                    base=s[:-5]; bid=float(x.get('bidPx') or 0); ask=float(x.get('askPx') or 0)
                    if self._base_ok(base) and bid>0 and ask>0:out[base]={'venue':venue,'base':base,'bid':bid,'ask':ask}
            elif venue=='Bitget':
                r=await c.get('https://api.bitget.com/api/v2/spot/market/tickers')
                rows=r.json().get('data') or []
                for x in rows:
                    s=str(x.get('symbol') or '')
                    if not s.endswith('USDT'):continue
                    base=s[:-4]; bid=float(x.get('bidPr') or 0); ask=float(x.get('askPr') or 0)
                    if self._base_ok(base) and bid>0 and ask>0:out[base]={'venue':venue,'base':base,'bid':bid,'ask':ask}
            elif venue=='Gate':
                r=await c.get('https://api.gateio.ws/api/v4/spot/tickers')
                rows=r.json() if r.is_success else []
                for x in rows if isinstance(rows,list) else []:
                    s=str(x.get('currency_pair') or '')
                    if not s.endswith('_USDT'):continue
                    base=s[:-5]; bid=float(x.get('highest_bid') or 0); ask=float(x.get('lowest_ask') or 0)
                    if self._base_ok(base) and bid>0 and ask>0:out[base]={'venue':venue,'base':base,'bid':bid,'ask':ask}
            elif venue=='KuCoin':
                r=await c.get('https://api.kucoin.com/api/v1/market/allTickers')
                rows=((r.json().get('data') or {}).get('ticker') or [])
                for x in rows:
                    s=str(x.get('symbol') or '')
                    if not s.endswith('-USDT'):continue
                    base=s[:-5]; bid=float(x.get('buy') or 0); ask=float(x.get('sell') or 0)
                    if self._base_ok(base) and bid>0 and ask>0:out[base]={'venue':venue,'base':base,'bid':bid,'ask':ask}
        except Exception:
            return {}
        return out
    async def _book(self,c,venue,base):
        try:
            if venue=='Bybit':
                r=await c.get('https://api.bybit.com/v5/market/orderbook',params={'category':'spot','symbol':base+'USDT','limit':DEPTH_LEVELS})
                x=r.json().get('result') or {}; bids=x.get('b') or []; asks=x.get('a') or []
            elif venue=='OKX':
                r=await c.get('https://www.okx.com/api/v5/market/books',params={'instId':base+'-USDT','sz':DEPTH_LEVELS})
                x=(r.json().get('data') or [{}])[0]; bids=x.get('bids') or []; asks=x.get('asks') or []
            elif venue=='Bitget':
                r=await c.get('https://api.bitget.com/api/v2/spot/market/orderbook',params={'symbol':base+'USDT','type':'step0','limit':DEPTH_LEVELS})
                x=r.json().get('data') or {}; bids=x.get('bids') or []; asks=x.get('asks') or []
            elif venue=='Gate':
                r=await c.get('https://api.gateio.ws/api/v4/spot/order_book',params={'currency_pair':base+'_USDT','limit':DEPTH_LEVELS})
                x=r.json() if r.is_success else {}; bids=x.get('bids') or []; asks=x.get('asks') or []
            else:
                r=await c.get('https://api.kucoin.com/api/v1/market/orderbook/level2_20',params={'symbol':base+'-USDT'})
                x=(r.json().get('data') or {}); bids=x.get('bids') or []; asks=x.get('asks') or []
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
            return {'bids':b,'asks':a} if b and a else None
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
            timeout=httpx.Timeout(10.0)
            async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT,timeout=timeout,follow_redirects=True) as c:
                venue_maps=await asyncio.gather(*[self._bulk_tickers(c,v) for v in VENUES])
                market={}
                venue_status={}
                for venue,mp in zip(VENUES,venue_maps):
                    venue_status[venue]=len(mp)
                    for base,q in mp.items():market.setdefault(base,{})[venue]=q
                market={b:vm for b,vm in market.items() if len(vm)>=2}
                self.market=market; now=time.time(); events=[]; reasons={}; raw_positive=0; after_fees=0
                candidates=[]
                for base,vm in market.items():
                    qs=list(vm.values())
                    for buy in qs:
                        for sell in qs:
                            if buy['venue']==sell['venue']:continue
                            gross=(sell['bid']/buy['ask']-1.0)*100.0
                            fees=(TAKER[buy['venue']]+TAKER[sell['venue']])*100.0
                            top_all_in=gross-fees-SAFETY_PCT-REBALANCE_RESERVE_PCT
                            if gross>0:raw_positive+=1
                            if gross-fees>0:after_fees+=1
                            reason=self._reason(gross,fees,top_all_in)
                            if reason!='PASS':reasons[reason]=reasons.get(reason,0)+1;continue
                            candidates.append((top_all_in,base,buy,sell,gross,fees))
                candidates.sort(key=lambda x:x[0],reverse=True)
                depth_checks=0; depth_pass=0
                for _,base,buy,sell,gross,fees in candidates[:MAX_DEPTH_CHECKS_PER_SCAN]:
                    depth_checks+=1
                    bb,sb=await asyncio.gather(self._book(c,buy['venue'],base),self._book(c,sell['venue'],base))
                    if not bb or not sb:
                        reasons['DEPTH_UNAVAILABLE']=reasons.get('DEPTH_UNAVAILABLE',0)+1;continue
                    bfill=self._buy_vwap(bb['asks'],NOTIONAL)
                    if not bfill['ok']:
                        reasons['BUY_DEPTH_INSUFFICIENT']=reasons.get('BUY_DEPTH_INSUFFICIENT',0)+1;continue
                    sfill=self._sell_vwap(sb['bids'],bfill['base_qty'])
                    if not sfill['ok']:
                        reasons['SELL_DEPTH_INSUFFICIENT']=reasons.get('SELL_DEPTH_INSUFFICIENT',0)+1;continue
                    buy_fee=NOTIONAL*TAKER[buy['venue']]; sell_fee=sfill['quote_received']*TAKER[sell['venue']]
                    net_quote=sfill['quote_received']-NOTIONAL-buy_fee-sell_fee
                    reserve_quote=NOTIONAL*(SAFETY_PCT+REBALANCE_RESERVE_PCT)/100.0
                    all_in_quote=net_quote-reserve_quote; all_in=all_in_quote/NOTIONAL*100.0
                    depth_reason=self._reason((sfill['quote_received']/NOTIONAL-1.0)*100.0,(buy_fee+sell_fee)/NOTIONAL*100.0,all_in)
                    if depth_reason!='PASS':reasons['DEPTH_'+depth_reason]=reasons.get('DEPTH_'+depth_reason,0)+1;continue
                    depth_pass+=1
                    sig=f"{base}:{buy['venue']}>{sell['venue']}"
                    st=self.state.setdefault('signals',{}).get(sig) or {'hits':0,'last_seen':0,'last_event':0}
                    st['hits']=int(st.get('hits') or 0)+1;st['last_seen']=now;st['last_all_in_net_pct']=all_in;self.state['signals'][sig]=st
                    if st['hits']<MIN_HITS or now-float(st.get('last_event') or 0)<COOLDOWN_SEC:continue
                    pnl=all_in_quote;st['last_event']=now
                    ev={'ts':now,'signature':sig,'base':base,'buy_venue':buy['venue'],'sell_venue':sell['venue'],
                        'top_buy_ask':buy['ask'],'top_sell_bid':sell['bid'],'buy_vwap':bfill['vwap'],'sell_vwap':sfill['vwap'],
                        'filled_base_qty':bfill['base_qty'],'gross_depth_pct':(sfill['quote_received']/NOTIONAL-1.0)*100.0,
                        'trading_fee_quote':buy_fee+sell_fee,'safety_pct':SAFETY_PCT,'rebalance_reserve_pct':REBALANCE_RESERVE_PCT,
                        'all_in_net_pct':all_in,'notional_quote':NOTIONAL,'paper_pnl_quote':pnl,
                        'assumption':'PREFUNDED_INVENTORY_BATCHED_REBALANCE_RESERVE_DEPTH_VALIDATED'}
                    events.append(ev);self.state.setdefault('events',[]).append(ev);self.state['events']=self.state['events'][-2000:]
                    self.state['pnl_quote']=float(self.state.get('pnl_quote') or 0)+pnl
                self.state['scan_count']=int(self.state.get('scan_count') or 0)+1;self.state['last_scan_ts']=now
                self.state['loss_reasons']=reasons;self.state['last_new_events']=len(events);self.state['last_depth_checks']=depth_checks;self.state['last_depth_pass']=depth_pass
                self.state['last_raw_positive']=raw_positive;self.state['last_after_fees_positive']=after_fees;self.state['last_top_candidates']=len(candidates)
                self.state['venue_symbol_counts']=venue_status;self.state['common_assets']=len(market);self._save()
            self.last_error=None;self.last_refresh=now
        except Exception as exc:self.last_error=str(exc)[:500]
        return self.status()
    def status(self):
        ev=self.state.get('events') or [];pnl=float(self.state.get('pnl_quote') or 0);capital=NOTIONAL*2
        return {'ok':self.last_error is None,'strategy':'CROSSVENUE_SPOT_ARB_CLOUD_V2_DYNAMIC_UNIVERSE','mode':'FUTURE_ONLY_PAPER','paper_only':True,'live_enabled':False,
            'venues':VENUES,'common_assets':self.state.get('common_assets',0),'venue_symbol_counts':self.state.get('venue_symbol_counts') or {},
            'scan_count':self.state.get('scan_count',0),'event_count':len(ev),'last_new_events':self.state.get('last_new_events',0),
            'raw_positive_routes':self.state.get('last_raw_positive',0),'after_fees_positive_routes':self.state.get('last_after_fees_positive',0),'top_all_in_candidates':self.state.get('last_top_candidates',0),
            'paper_pnl_quote':round(pnl,6),'return_on_two_leg_capital_pct':round(pnl/capital*100,4) if capital else 0.0,
            'depth_validation':{'levels':DEPTH_LEVELS,'max_checks_per_scan':MAX_DEPTH_CHECKS_PER_SCAN,'last_checks':self.state.get('last_depth_checks',0),'last_pass':self.state.get('last_depth_pass',0)},
            'cost_model':{'taker_fee_each_venue':TAKER,'safety_pct':SAFETY_PCT,'rebalance_reserve_pct':REBALANCE_RESERVE_PCT,'notional_quote':NOTIONAL},
            'loss_reasons':self.state.get('loss_reasons') or {},'recent_events':ev[-20:],'last_error':self.last_error,
            'policy':{'dynamic_universe':True,'prefunded_inventory':True,'batched_rebalance_assumption':True,'depth_validated':True,'no_grid':True,'no_martingale':True,'no_dca':True,'no_live_orders':True}}
    async def start(self):
        if self.task and not self.task.done():return
        await self.refresh();self.task=asyncio.create_task(self._loop(),name='crossvenue-spot-arb-cloud-v2')
    async def stop(self):
        if self.task and not self.task.done():self.task.cancel();await asyncio.gather(self.task,return_exceptions=True)
        self.task=None
    async def _loop(self):
        while True:
            await asyncio.sleep(30)
            if self.enabled:await self.refresh()

crossvenue_spot_arb_cloud_v1=CrossVenueSpotArbCloudV1()
