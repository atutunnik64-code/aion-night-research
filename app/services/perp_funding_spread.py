from __future__ import annotations
import asyncio,time
import httpx
from app.http_shared import SHARED_SSL_CONTEXT
from app.services.synthetic_inventory import synthetic_inventory

VENUES=('Bitget','Binance','Bybit','OKX')
ASSETS=('BTC','ETH','SOL','XRP','BNB','DOGE','ADA','LINK','AVAX','LTC')
TAKER_FEE={'Bitget':0.0006,'Binance':0.0005,'Bybit':0.00055,'OKX':0.0005}
MAKER_FEE={'Bitget':0.0002,'Binance':0.0002,'Bybit':0.0002}

class PerpFundingSpreadScanner:
    def __init__(self):
        self.enabled=True; self.interval=120.0; self.task=None
        self.last_refresh=None; self.last_error=None; self.rows=[]; self.market={}
        self.interval_cache={}; self.interval_cache_ts=0.0
        self.safety_pct=0.05; self.max_basis_abs_pct=0.60

    async def _interval(self,venue,base,perp):
        key=(venue,base); now=time.time()
        if now-self.interval_cache_ts>21600:
            self.interval_cache={}; self.interval_cache_ts=now
        if key in self.interval_cache:return self.interval_cache[key]
        out={'hours':8.0,'source':'DEFAULT_8H','next_funding_time':perp.get('next_funding_time')}
        try:
            async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT,timeout=5,follow_redirects=True) as c:
                if venue=='Bitget':
                    r=await c.get('https://api.bitget.com/api/v2/mix/market/funding-time',params={'symbol':base+'USDT','productType':'USDT-FUTURES'})
                    d=r.json(); x=((d.get('data') or [{}])[0])
                    if r.is_success and d.get('code')=='00000':
                        out={'hours':max(1.0,float(x.get('ratePeriod') or 8)),'source':'BITGET_FUNDING_TIME','next_funding_time':x.get('nextFundingTime')}
                elif venue=='Bybit':
                    r=await c.get('https://api.bybit.com/v5/market/instruments-info',params={'category':'linear','symbol':base+'USDT'})
                    d=r.json(); x=(((d.get('result') or {}).get('list') or [{}])[0])
                    if r.is_success and str(d.get('retCode'))=='0':
                        out={'hours':max(1.0,float(x.get('fundingInterval') or 480)/60.0),'source':'BYBIT_INSTRUMENT','next_funding_time':perp.get('next_funding_time')}
                elif venue=='OKX':
                    f=perp.get('funding_time'); n=perp.get('next_funding_time')
                    if f and n and float(n)>float(f):
                        out={'hours':max(1.0,(float(n)-float(f))/3600000.0),'source':'OKX_FUNDING_TIMES','next_funding_time':n}
                elif venue=='Binance':
                    r=await c.get('https://fapi.binance.com/fapi/v1/fundingInfo')
                    if r.is_success:
                        row=next((x for x in r.json() if str(x.get('symbol'))==base+'USDT'),None)
                        if row:out={'hours':max(1.0,float(row.get('fundingIntervalHours') or 8)),'source':'BINANCE_FUNDING_INFO','next_funding_time':perp.get('next_funding_time')}
                        else:out={'hours':8.0,'source':'BINANCE_DEFAULT_8H','next_funding_time':perp.get('next_funding_time')}
        except Exception:pass
        self.interval_cache[key]=out; return out

    async def _one(self,base,venue,sem):
        async with sem:
            p=await synthetic_inventory._perp(venue,base,'USDT')
            if not p.get('ok') or p.get('funding_rate') is None:return base,venue,None
            iv=await self._interval(venue,base,p)
            bid=float(p.get('bid') or 0); ask=float(p.get('ask') or 0); rate=float(p.get('funding_rate') or 0)
            if bid<=0 or ask<=0:return base,venue,None
            return base,venue,{'venue':venue,'base':base,'bid':bid,'ask':ask,'rate':rate,'interval_hours':float(iv['hours']),
                               'next_funding_time':iv.get('next_funding_time'),'interval_source':iv.get('source'),'source':p.get('source')}

    @staticmethod
    def _pair_row(base,longm,shortm,safety,max_basis):
        if longm['venue']==shortm['venue']:return None
        long_h=max(1e-9,float(longm['interval_hours'])); short_h=max(1e-9,float(shortm['interval_hours']))
        long_hour=float(longm['rate'])*100.0/long_h; short_hour=float(shortm['rate'])*100.0/short_h
        carry_hour=short_hour-long_hour; carry24=carry_hour*24.0
        basis=(float(shortm['bid'])/float(longm['ask'])-1.0)*100.0
        if abs(basis)>max_basis:return None
        lf=float(TAKER_FEE[longm['venue']]); sf=float(TAKER_FEE[shortm['venue']])
        round_trip=2.0*(lf+sf)*100.0
        conservative=carry24-round_trip-abs(basis)-safety
        break_even=(round_trip+abs(basis)+safety)/carry_hour if carry_hour>0 else None
        ml=MAKER_FEE.get(longm['venue']); ms=MAKER_FEE.get(shortm['venue'])
        maker_round_trip=(2.0*(ml+ms)*100.0) if ml is not None and ms is not None else None
        maker_net=(carry24-maker_round_trip-abs(basis)-safety) if maker_round_trip is not None else None
        maker_be=((maker_round_trip+abs(basis)+safety)/carry_hour) if maker_round_trip is not None and carry_hour>0 else None
        sig=f"{base}:{longm['venue']}:LONG>{shortm['venue']}:SHORT"
        return {'signature':sig,'strategy':'perp_perp_funding','base':base,'quote':'USDT',
                'long_venue':longm['venue'],'short_venue':shortm['venue'],'long_ask':longm['ask'],'long_bid':longm['bid'],
                'short_bid':shortm['bid'],'short_ask':shortm['ask'],'long_funding_rate_pct':round(longm['rate']*100,6),
                'short_funding_rate_pct':round(shortm['rate']*100,6),'long_interval_hours':long_h,'short_interval_hours':short_h,
                'carry_per_hour_pct':round(carry_hour,7),'projected_24h_carry_pct':round(carry24,6),'entry_basis_pct':round(basis,6),
                'round_trip_fee_pct':round(round_trip,6),'safety_pct':safety,'net_24h_conservative_pct':round(conservative,6),
                'break_even_hours':round(break_even,2) if break_even is not None else None,
                'maker_round_trip_fee_pct':round(maker_round_trip,6) if maker_round_trip is not None else None,
                'maker_net_24h_pct':round(maker_net,6) if maker_net is not None else None,
                'maker_break_even_hours':round(maker_be,2) if maker_be is not None else None,
                'maker_status':'RESEARCH_CANDIDATE' if maker_net is not None and maker_be is not None and maker_be<=168 else 'WATCH',
                'capital_required_usdt':50.0,
                'notional_per_leg_usdt':25.0,'long_next_funding_time':longm.get('next_funding_time'),'short_next_funding_time':shortm.get('next_funding_time'),
                'interval_sources':[longm.get('interval_source'),shortm.get('interval_source')],
                'status':'PAPER_CANDIDATE' if conservative>0 and break_even is not None and break_even<=72 else 'WATCH'}
    async def refresh(self):
        if not self.enabled:return self.status()
        try:
            sem=asyncio.Semaphore(6)
            vals=await asyncio.gather(*[self._one(base,venue,sem) for base in ASSETS for venue in VENUES],return_exceptions=True)
            market={}
            for x in vals:
                if isinstance(x,Exception) or not x or x[2] is None:continue
                base,venue,row=x; market.setdefault(base,{})[venue]=row
            rows=[]
            for base,venues in market.items():
                ms=list(venues.values())
                for a in ms:
                    for b in ms:
                        r=self._pair_row(base,a,b,self.safety_pct,self.max_basis_abs_pct)
                        if r:rows.append(r)
            self.market=market; self.rows=sorted(rows,key=lambda x:(x['status']=='PAPER_CANDIDATE',x['net_24h_conservative_pct']),reverse=True)[:100]
            self.last_refresh=time.time(); self.last_error=None
        except Exception as exc:self.last_error=str(exc)[:300]
        return self.status()

    def status(self):
        candidates=[x for x in self.rows if x.get('status')=='PAPER_CANDIDATE']
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'ANALYSIS_ONLY','interval_seconds':self.interval,
                'last_refresh':self.last_refresh,'last_error':self.last_error,'market_assets':len(self.market),
                'count':len(self.rows),'candidate_count':len(candidates),'opportunities':self.rows}

    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name='perp-funding-spread-scanner')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None
    async def _loop(self):
        await asyncio.sleep(20)
        while True:
            await self.refresh(); await asyncio.sleep(max(60.0,self.interval))

perp_funding_spread_scanner=PerpFundingSpreadScanner()

