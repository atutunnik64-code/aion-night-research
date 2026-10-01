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
            self.market=market; now=time.time(); events=[]; reasons={}
            for base,vm in market.items():
                qs=list(vm.values())
                for buy in qs:
                    for sell in qs:
                        if buy['venue']==sell['venue']:continue
                        gross=(sell['bid']/buy['ask']-1.0)*100.0
                        fees=(TAKER[buy['venue']]+TAKER[sell['venue']])*100.0
                        all_in=gross-fees-SAFETY_PCT-REBALANCE_RESERVE_PCT
                        reason=self._reason(gross,fees,all_in); reasons[reason]=reasons.get(reason,0)+1
                        if reason!='PASS':continue
                        sig=f"{base}:{buy['venue']}>{sell['venue']}"
                        st=self.state.setdefault('signals',{}).get(sig) or {'hits':0,'last_seen':0,'last_event':0}
                        st['hits']=int(st.get('hits') or 0)+1; st['last_seen']=now; st['last_all_in_net_pct']=all_in; self.state['signals'][sig]=st
                        if st['hits']<MIN_HITS or now-float(st.get('last_event') or 0)<COOLDOWN_SEC:continue
                        pnl=NOTIONAL*all_in/100.0; st['last_event']=now
                        ev={'ts':now,'signature':sig,'base':base,'buy_venue':buy['venue'],'sell_venue':sell['venue'],'buy_ask':buy['ask'],'sell_bid':sell['bid'],
                            'gross_pct':gross,'trading_fee_pct':fees,'safety_pct':SAFETY_PCT,'rebalance_reserve_pct':REBALANCE_RESERVE_PCT,
                            'all_in_net_pct':all_in,'notional_quote':NOTIONAL,'paper_pnl_quote':pnl,'assumption':'PREFUNDED_INVENTORY_BATCHED_REBALANCE_RESERVE'}
                        events.append(ev); self.state.setdefault('events',[]).append(ev); self.state['events']=self.state['events'][-1000:]
                        self.state['pnl_quote']=float(self.state.get('pnl_quote') or 0)+pnl
            self.state['scan_count']=int(self.state.get('scan_count') or 0)+1; self.state['last_scan_ts']=now; self.state['loss_reasons']=reasons; self.state['last_new_events']=len(events); self._save()
            self.last_error=None; self.last_refresh=now
        except Exception as exc:self.last_error=str(exc)[:500]
        return self.status()
    def status(self):
        ev=self.state.get('events') or []; pnl=float(self.state.get('pnl_quote') or 0); capital=NOTIONAL*2
        return {'ok':self.last_error is None,'strategy':'CROSSVENUE_SPOT_ARB_CLOUD_V1','mode':'FUTURE_ONLY_PAPER','paper_only':True,'live_enabled':False,
            'venues':VENUES,'assets':ASSETS,'scan_count':self.state.get('scan_count',0),'event_count':len(ev),'last_new_events':self.state.get('last_new_events',0),
            'paper_pnl_quote':round(pnl,6),'return_on_two_leg_capital_pct':round(pnl/capital*100,4) if capital else 0.0,
            'cost_model':{'taker_fee_each_venue':TAKER,'safety_pct':SAFETY_PCT,'rebalance_reserve_pct':REBALANCE_RESERVE_PCT,'notional_quote':NOTIONAL},
            'loss_reasons':self.state.get('loss_reasons') or {},'recent_events':ev[-20:],'last_error':self.last_error,
            'policy':{'prefunded_inventory':True,'batched_rebalance_assumption':True,'no_grid':True,'no_martingale':True,'no_dca':True,'no_live_orders':True,'top_of_book_only_requires_future_depth_validation':True}}
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
