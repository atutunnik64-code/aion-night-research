from __future__ import annotations
import asyncio,json,time,re,statistics
from pathlib import Path
import httpx
from app.http_shared import SHARED_SSL_CONTEXT

ROOT=Path(__file__).parents[2]; DATA=ROOT/'data'; STATE=DATA/'crossvenue_spot_arb_cloud_v1.json'
VENUES=('Bybit','OKX','Bitget','Gate','KuCoin')
TAKER={v:0.0010 for v in VENUES}
NOTIONAL=25.0; SAFETY_PCT=0.05; REBALANCE_RESERVE_PCT=0.10
MIN_CONSERVATIVE_NET_PCT=0.05; COOLDOWN_SEC=300; MIN_HITS=2
DEPTH_LEVELS=20; MAX_DEPTH_CHECKS_PER_SCAN=60
MIN_OBSERVE_QUOTE_VOLUME=10_000.0; TRUSTED_QUOTE_VOLUME=100_000.0
MAX_LOCAL_SPREAD_PCT=1.5; MAX_PRICE_DEVIATION_PCT=3.0; REVIEW_RAW_SPREAD_PCT=2.5
CANONICAL_MAJOR={'BTC','ETH','SOL','XRP','DOGE','ADA','LINK','AVAX','LTC','SUI','TRX','TON','BNB','BCH','DOT','ATOM','NEAR','APT','ARB','OP'}
LEVERAGED=re.compile(r'(?:3L|3S|5L|5S|BULL|BEAR|UP|DOWN)$',re.I)

class CrossVenueSpotArbCloudV1:
    def __init__(self):
        self.enabled=True; self.live_enabled=False; self.task=None; self.last_error=None; self.last_refresh=None; self.market={}
        try:self.state=json.loads(STATE.read_text(encoding='utf-8-sig'))
        except Exception:self.state={'started_at':time.time(),'scan_count':0,'signals':{},'events':[],'loss_reasons':{}}
        if int(self.state.get('quality_version') or 0)<3:
            self.state['legacy_unverified_event_count']=len(self.state.get('events') or [])
            self.state['legacy_unverified_pnl_quote']=float(self.state.get('pnl_quote') or 0)
            self.state['legacy_signal_count']=len(self.state.get('signals') or {})
            self.state['signals']={};self.state['research_pnl_quote']=0.0
        self.state.setdefault('verification_queue',[])
        self.state.setdefault('execution_edge_quote',float(self.state.get('research_pnl_quote') or 0))
        self.state['quality_version']=4
        self.state['version']='CROSSVENUE_SPOT_ARB_CLOUD_V4_PROFIT_PRESERVING'
    def _save(self):
        tmp=STATE.with_suffix(STATE.suffix+'.tmp');tmp.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8');tmp.replace(STATE)
    @staticmethod
    def _base_ok(base):
        b=str(base or '').upper().strip()
        return bool(b and len(b)<=24 and not LEVERAGED.search(b) and b not in {'USDT','USDC','USD','DAI','FDUSD'})
    @staticmethod
    def _q(v):
        try:return float(v or 0)
        except Exception:return 0.0
    async def _bulk_tickers(self,c,venue):
        out={}
        try:
            if venue=='Bybit':
                r=await c.get('https://api.bybit.com/v5/market/tickers',params={'category':'spot'});rows=((r.json().get('result') or {}).get('list') or [])
                for x in rows:
                    s=str(x.get('symbol') or '');base=s[:-4] if s.endswith('USDT') else '';bid=self._q(x.get('bid1Price'));ask=self._q(x.get('ask1Price'));qv=self._q(x.get('turnover24h'))
                    if self._base_ok(base) and bid>0 and ask>0:out[base]={'venue':venue,'base':base,'bid':bid,'ask':ask,'quote_volume':qv}
            elif venue=='OKX':
                r=await c.get('https://www.okx.com/api/v5/market/tickers',params={'instType':'SPOT'});rows=r.json().get('data') or []
                for x in rows:
                    s=str(x.get('instId') or '');base=s[:-5] if s.endswith('-USDT') else '';bid=self._q(x.get('bidPx'));ask=self._q(x.get('askPx'));qv=self._q(x.get('volCcy24h'))
                    if self._base_ok(base) and bid>0 and ask>0:out[base]={'venue':venue,'base':base,'bid':bid,'ask':ask,'quote_volume':qv}
            elif venue=='Bitget':
                r=await c.get('https://api.bitget.com/api/v2/spot/market/tickers');rows=r.json().get('data') or []
                for x in rows:
                    s=str(x.get('symbol') or '');base=s[:-4] if s.endswith('USDT') else '';bid=self._q(x.get('bidPr'));ask=self._q(x.get('askPr'));qv=self._q(x.get('usdtVolume') or x.get('quoteVolume'))
                    if self._base_ok(base) and bid>0 and ask>0:out[base]={'venue':venue,'base':base,'bid':bid,'ask':ask,'quote_volume':qv}
            elif venue=='Gate':
                r=await c.get('https://api.gateio.ws/api/v4/spot/tickers');rows=r.json() if r.is_success else []
                for x in rows if isinstance(rows,list) else []:
                    s=str(x.get('currency_pair') or '');base=s[:-5] if s.endswith('_USDT') else '';bid=self._q(x.get('highest_bid'));ask=self._q(x.get('lowest_ask'));qv=self._q(x.get('quote_volume'))
                    if self._base_ok(base) and bid>0 and ask>0:out[base]={'venue':venue,'base':base,'bid':bid,'ask':ask,'quote_volume':qv}
            else:
                r=await c.get('https://api.kucoin.com/api/v1/market/allTickers');rows=((r.json().get('data') or {}).get('ticker') or [])
                for x in rows:
                    s=str(x.get('symbol') or '');base=s[:-5] if s.endswith('-USDT') else '';bid=self._q(x.get('buy'));ask=self._q(x.get('sell'));qv=self._q(x.get('volValue'))
                    if self._base_ok(base) and bid>0 and ask>0:out[base]={'venue':venue,'base':base,'bid':bid,'ask':ask,'quote_volume':qv}
        except Exception:return {}
        return out
    def _guard_market(self,raw):
        guarded={};reject={'TOO_LOW_TO_OBSERVE':0,'LOCAL_SPREAD_WIDE':0,'PRICE_IDENTITY_OUTLIER':0,'INSUFFICIENT_IDENTITY_VENUES':0}
        for base,vm in raw.items():
            if len(vm)<2:continue
            mids=[(q['bid']+q['ask'])/2 for q in vm.values() if q['bid']>0 and q['ask']>0]
            if len(mids)<2:continue
            med=statistics.median(mids);good={}
            for venue,q in vm.items():
                mid=(q['bid']+q['ask'])/2;local=(q['ask']/q['bid']-1)*100 if q['bid']>0 else 999
                if q.get('quote_volume',0)<MIN_OBSERVE_QUOTE_VOLUME:reject['TOO_LOW_TO_OBSERVE']+=1;continue
                if local>MAX_LOCAL_SPREAD_PCT:reject['LOCAL_SPREAD_WIDE']+=1;continue
                if med<=0 or abs(mid/med-1)*100>MAX_PRICE_DEVIATION_PCT:reject['PRICE_IDENTITY_OUTLIER']+=1;continue
                q=dict(q);q['liquidity_tier']='TRUSTED' if q.get('quote_volume',0)>=TRUSTED_QUOTE_VOLUME else 'RESEARCH'
                good[venue]=q
            need=2 if base in CANONICAL_MAJOR else 3
            if len(good)<need:reject['INSUFFICIENT_IDENTITY_VENUES']+=1;continue
            guarded[base]=good
        return guarded,reject
    async def _book(self,c,venue,base):
        try:
            if venue=='Bybit':r=await c.get('https://api.bybit.com/v5/market/orderbook',params={'category':'spot','symbol':base+'USDT','limit':DEPTH_LEVELS});x=r.json().get('result') or {};bids=x.get('b') or [];asks=x.get('a') or []
            elif venue=='OKX':r=await c.get('https://www.okx.com/api/v5/market/books',params={'instId':base+'-USDT','sz':DEPTH_LEVELS});x=(r.json().get('data') or [{}])[0];bids=x.get('bids') or [];asks=x.get('asks') or []
            elif venue=='Bitget':r=await c.get('https://api.bitget.com/api/v2/spot/market/orderbook',params={'symbol':base+'USDT','type':'step0','limit':DEPTH_LEVELS});x=r.json().get('data') or {};bids=x.get('bids') or [];asks=x.get('asks') or []
            elif venue=='Gate':r=await c.get('https://api.gateio.ws/api/v4/spot/order_book',params={'currency_pair':base+'_USDT','limit':DEPTH_LEVELS});x=r.json() if r.is_success else {};bids=x.get('bids') or [];asks=x.get('asks') or []
            else:r=await c.get('https://api.kucoin.com/api/v1/market/orderbook/level2_20',params={'symbol':base+'-USDT'});x=r.json().get('data') or {};bids=x.get('bids') or [];asks=x.get('asks') or []
            if not r.is_success:return None
            def norm(rows):
                z=[]
                for a in rows:
                    try:
                        p=float(a[0]);q=float(a[1])
                        if p>0 and q>0:z.append((p,q))
                    except Exception:pass
                return z
            b=norm(bids);a=norm(asks);return {'bids':b,'asks':a} if b and a else None
        except Exception:return None
    @staticmethod
    def _buy_vwap(asks,quote_amount):
        remain=float(quote_amount);base=spent=0.0
        for px,qty in asks:
            tq=min(remain,px*qty);base+=tq/px;spent+=tq;remain-=tq
            if remain<=1e-9:break
        return {'ok':remain<=1e-6,'base_qty':base,'quote_spent':spent,'vwap':spent/base if base>0 else 0.0}
    @staticmethod
    def _sell_vwap(bids,base_qty):
        remain=float(base_qty);sold=received=0.0
        for px,qty in bids:
            t=min(remain,qty);sold+=t;received+=t*px;remain-=t
            if remain<=1e-12:break
        return {'ok':remain<=1e-9,'base_sold':sold,'quote_received':received,'vwap':received/sold if sold>0 else 0.0}
    def _identity_confidence(self,base,vm,buy,sell,gross):
        trusted_liq=buy.get('liquidity_tier')=='TRUSTED' and sell.get('liquidity_tier')=='TRUSTED'
        if base in CANONICAL_MAJOR:return 'HIGH_MAJOR' if trusted_liq else 'MEDIUM_MAJOR'
        if len(vm)>=4 and trusted_liq:return 'HIGH_4VENUE'
        if gross<=REVIEW_RAW_SPREAD_PCT and trusted_liq:return 'MEDIUM_3VENUE'
        return 'REVIEW_REQUIRED'
    async def refresh(self):
        try:
            async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT,timeout=httpx.Timeout(10.0),follow_redirects=True) as c:
                maps=await asyncio.gather(*[self._bulk_tickers(c,v) for v in VENUES]);raw={};venue_status={}
                for venue,mp in zip(VENUES,maps):
                    venue_status[venue]=len(mp)
                    for base,q in mp.items():raw.setdefault(base,{})[venue]=q
                raw_common={b:vm for b,vm in raw.items() if len(vm)>=2};market,guard_reject=self._guard_market(raw_common);self.market=market
                now=time.time();events=[];reasons=dict(guard_reject);raw_positive=after_fees=0;candidates=[];verification=[]
                for base,vm in market.items():
                    qs=list(vm.values())
                    for buy in qs:
                        for sell in qs:
                            if buy['venue']==sell['venue']:continue
                            gross=(sell['bid']/buy['ask']-1)*100;fees=(TAKER[buy['venue']]+TAKER[sell['venue']])*100;execution=gross-fees;conservative=execution-SAFETY_PCT-REBALANCE_RESERVE_PCT
                            if gross>0:raw_positive+=1
                            if execution>0:after_fees+=1
                            if gross<=0:reasons['NO_RAW_SPREAD']=reasons.get('NO_RAW_SPREAD',0)+1;continue
                            if execution<=0:reasons['TRADING_FEES']=reasons.get('TRADING_FEES',0)+1;continue
                            conf=self._identity_confidence(base,vm,buy,sell,gross)
                            candidates.append((execution,conservative,base,buy,sell,conf,len(vm)))
                candidates.sort(reverse=True,key=lambda x:(x[1]>=MIN_CONSERVATIVE_NET_PCT,x[0]));depth_checks=depth_pass=0
                for _,_,base,buy,sell,conf,venue_count in candidates[:MAX_DEPTH_CHECKS_PER_SCAN]:
                    depth_checks+=1;bb,sb=await asyncio.gather(self._book(c,buy['venue'],base),self._book(c,sell['venue'],base))
                    if not bb or not sb:reasons['DEPTH_UNAVAILABLE']=reasons.get('DEPTH_UNAVAILABLE',0)+1;continue
                    bf=self._buy_vwap(bb['asks'],NOTIONAL)
                    if not bf['ok']:reasons['BUY_DEPTH_INSUFFICIENT']=reasons.get('BUY_DEPTH_INSUFFICIENT',0)+1;continue
                    sf=self._sell_vwap(sb['bids'],bf['base_qty'])
                    if not sf['ok']:reasons['SELL_DEPTH_INSUFFICIENT']=reasons.get('SELL_DEPTH_INSUFFICIENT',0)+1;continue
                    buy_fee=NOTIONAL*TAKER[buy['venue']];sell_fee=sf['quote_received']*TAKER[sell['venue']]
                    execution_q=sf['quote_received']-NOTIONAL-buy_fee-sell_fee;execution_pct=execution_q/NOTIONAL*100
                    reserve=NOTIONAL*(SAFETY_PCT+REBALANCE_RESERVE_PCT)/100;conservative_q=execution_q-reserve;conservative_pct=conservative_q/NOTIONAL*100
                    gross_depth=(sf['quote_received']/NOTIONAL-1)*100
                    if execution_pct<=0:reasons['DEPTH_TRADING_FEES']=reasons.get('DEPTH_TRADING_FEES',0)+1;continue
                    depth_pass+=1;high_spread=gross_depth>REVIEW_RAW_SPREAD_PCT
                    if high_spread and conf=='REVIEW_REQUIRED':
                        verification.append({'ts':now,'base':base,'buy_venue':buy['venue'],'sell_venue':sell['venue'],'gross_depth_pct':gross_depth,'execution_net_pct':execution_pct,'conservative_net_pct':conservative_pct,'venue_consensus_count':venue_count,'buy_quote_volume':buy.get('quote_volume',0),'sell_quote_volume':sell.get('quote_volume',0),'reason':'HIGH_EDGE_REQUIRES_STRONGER_IDENTITY_CHECK'})
                        continue
                    sig=f"{base}:{buy['venue']}>{sell['venue']}";st=self.state.setdefault('signals',{}).get(sig) or {'hits':0,'last_event':0}
                    st['hits']=int(st.get('hits') or 0)+1;st['last_seen']=now;st['last_execution_net_pct']=execution_pct;st['last_conservative_net_pct']=conservative_pct;self.state['signals'][sig]=st
                    if st['hits']<MIN_HITS or now-float(st.get('last_event') or 0)<COOLDOWN_SEC:continue
                    st['last_event']=now;tier='CONSERVATIVE_POSITIVE' if conservative_pct>=MIN_CONSERVATIVE_NET_PCT else 'EXECUTION_POSITIVE_REBALANCE_SENSITIVE'
                    ev={'ts':now,'signature':sig,'base':base,'buy_venue':buy['venue'],'sell_venue':sell['venue'],'buy_vwap':bf['vwap'],'sell_vwap':sf['vwap'],'gross_depth_pct':gross_depth,'execution_net_pct':execution_pct,'conservative_net_pct':conservative_pct,'execution_edge_quote':execution_q,'conservative_pnl_quote':conservative_q,'notional_quote':NOTIONAL,'quality_guard':'PRICE_VOLUME_CONSENSUS_V2','quality_tier':tier,'identity_confidence':conf,'high_spread_review':high_spread,'venue_consensus_count':venue_count,'identity_verified_contract_address':False}
                    events.append(ev);self.state.setdefault('events',[]).append(ev);self.state['events']=self.state['events'][-3000:]
                    self.state['execution_edge_quote']=float(self.state.get('execution_edge_quote') or 0)+execution_q
                    if tier=='CONSERVATIVE_POSITIVE':self.state['research_pnl_quote']=float(self.state.get('research_pnl_quote') or 0)+conservative_q
                if verification:
                    q=list(self.state.get('verification_queue') or []);q.extend(verification);self.state['verification_queue']=q[-1000:]
                self.state.update({'scan_count':int(self.state.get('scan_count') or 0)+1,'last_scan_ts':now,'loss_reasons':reasons,'last_new_events':len(events),'last_verification_candidates':len(verification),'last_depth_checks':depth_checks,'last_depth_pass':depth_pass,'last_raw_positive':raw_positive,'last_after_fees_positive':after_fees,'last_top_candidates':len(candidates),'venue_symbol_counts':venue_status,'raw_common_assets':len(raw_common),'common_assets':len(market)});self._save()
            self.last_error=None;self.last_refresh=now
        except Exception as exc:self.last_error=str(exc)[:500]
        return self.status()
    def status(self):
        ev=[x for x in (self.state.get('events') or []) if str(x.get('quality_guard') or '').startswith('PRICE_VOLUME_CONSENSUS')];pnl=float(self.state.get('research_pnl_quote') or 0);execution=float(self.state.get('execution_edge_quote') or 0)
        return {'ok':self.last_error is None,'strategy':'CROSSVENUE_SPOT_ARB_CLOUD_V4_PROFIT_PRESERVING','mode':'FUTURE_ONLY_PAPER_RESEARCH','paper_only':True,'live_enabled':False,'venues':VENUES,'raw_common_assets':self.state.get('raw_common_assets',0),'guarded_common_assets':self.state.get('common_assets',0),'venue_symbol_counts':self.state.get('venue_symbol_counts') or {},'scan_count':self.state.get('scan_count',0),'research_event_count':len(ev),'legacy_unverified_event_count':self.state.get('legacy_unverified_event_count',0),'legacy_unverified_pnl_quote':self.state.get('legacy_unverified_pnl_quote',0),'conservative_research_pnl_quote':round(pnl,6),'execution_edge_quote':round(execution,6),'raw_positive_routes':self.state.get('last_raw_positive',0),'after_fees_positive_routes':self.state.get('last_after_fees_positive',0),'all_execution_candidates':self.state.get('last_top_candidates',0),'verification_queue_count':len(self.state.get('verification_queue') or []),'last_verification_candidates':self.state.get('last_verification_candidates',0),'depth_validation':{'last_checks':self.state.get('last_depth_checks',0),'last_pass':self.state.get('last_depth_pass',0)},'identity_guard':{'min_observe_quote_volume':MIN_OBSERVE_QUOTE_VOLUME,'trusted_quote_volume':TRUSTED_QUOTE_VOLUME,'max_local_spread_pct':MAX_LOCAL_SPREAD_PCT,'max_price_deviation_pct':MAX_PRICE_DEVIATION_PCT,'review_raw_spread_pct':REVIEW_RAW_SPREAD_PCT,'non_major_min_venues':3,'high_spread_not_discarded':True,'contract_address_verified':False},'loss_reasons':self.state.get('loss_reasons') or {},'recent_events':ev[-20:],'verification_queue':(self.state.get('verification_queue') or [])[-20:],'promotion_blocked':True,'blocking_reason':'CONTRACT_ADDRESS_OR_NETWORK_IDENTITY_NOT_YET_VERIFIED_FOR_REVIEW_QUEUE','last_error':self.last_error,'policy':{'dynamic_universe':True,'prefunded_inventory':True,'depth_validated':True,'profit_preserving_tiers':True,'legacy_false_edges_quarantined':True,'no_grid':True,'no_martingale':True,'no_dca':True,'no_live_orders':True}}
    async def start(self):
        if self.task and not self.task.done():return
        await self.refresh();self.task=asyncio.create_task(self._loop(),name='crossvenue-spot-arb-cloud-v4')
    async def stop(self):
        if self.task and not self.task.done():self.task.cancel();await asyncio.gather(self.task,return_exceptions=True)
        self.task=None
    async def _loop(self):
        while True:
            await asyncio.sleep(30)
            if self.enabled:await self.refresh()

crossvenue_spot_arb_cloud_v1=CrossVenueSpotArbCloudV1()
