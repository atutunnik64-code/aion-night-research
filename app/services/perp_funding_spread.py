from __future__ import annotations
import asyncio,time
import httpx
from app.http_shared import SHARED_SSL_CONTEXT

VENUES=('Bybit','Bitget','Gate','KuCoin','OKX')
TAKER_FEE={'Bybit':0.00055,'Bitget':0.0006,'Gate':0.0006,'KuCoin':0.0006,'OKX':0.0005}
MAKER_FEE={'Bybit':0.0002,'Bitget':0.0002,'Gate':0.0002,'KuCoin':0.0002,'OKX':0.0002}
OKX_FUNDING_LIMIT=60


def _f(v,default=0.0):
    try:return float(v)
    except Exception:return float(default)


def _base_from_symbol(symbol,suffix):
    s=str(symbol or '').upper()
    return s[:-len(suffix)] if suffix and s.endswith(suffix) else ''


class PerpFundingSpreadScanner:
    def __init__(self):
        self.enabled=True; self.interval=180.0; self.task=None
        self.last_refresh=None; self.last_error=None; self.rows=[]; self.market={}
        self.safety_pct=0.05; self.max_basis_abs_pct=0.60
        self.venue_asset_counts={}; self.universe_bases=0; self.multi_venue_bases=0; self.pairs_evaluated=0

    async def _bybit(self,c):
        out={}
        try:
            tr,ir=await asyncio.gather(
                c.get('https://api.bybit.com/v5/market/tickers',params={'category':'linear'}),
                c.get('https://api.bybit.com/v5/market/instruments-info',params={'category':'linear','limit':1000})
            )
            td=tr.json(); idd=ir.json(); im={}
            for x in (((idd.get('result') or {}).get('list')) or []):
                sym=str(x.get('symbol') or '').upper()
                if sym.endswith('USDT'):
                    im[sym]=max(1.0,_f(x.get('fundingInterval'),480.0)/60.0)
            for x in (((td.get('result') or {}).get('list')) or []):
                sym=str(x.get('symbol') or '').upper(); base=_base_from_symbol(sym,'USDT')
                if not base:continue
                bid=_f(x.get('bid1Price'));ask=_f(x.get('ask1Price'));rate=_f(x.get('fundingRate'))
                if bid<=0 or ask<=0:continue
                out[base]={'venue':'Bybit','base':base,'bid':bid,'ask':ask,'rate':rate,
                    'interval_hours':im.get(sym,8.0),'next_funding_time':x.get('nextFundingTime'),
                    'interval_source':'BYBIT_INSTRUMENT_BULK','source':'BYBIT_LINEAR_BULK','quote_volume':_f(x.get('turnover24h'))}
        except Exception:pass
        return out

    async def _bitget(self,c):
        out={}
        try:
            r=await c.get('https://api.bitget.com/api/v2/mix/market/tickers',params={'productType':'USDT-FUTURES'})
            d=r.json()
            for x in (d.get('data') or []):
                sym=str(x.get('symbol') or '').upper();base=_base_from_symbol(sym,'USDT')
                if not base:continue
                bid=_f(x.get('bidPr'));ask=_f(x.get('askPr'));rate=_f(x.get('fundingRate'))
                if bid<=0 or ask<=0:continue
                out[base]={'venue':'Bitget','base':base,'bid':bid,'ask':ask,'rate':rate,'interval_hours':8.0,
                    'next_funding_time':x.get('nextFundingTime'),'interval_source':'BITGET_DEFAULT_8H_BULK',
                    'source':'BITGET_USDT_FUTURES_BULK','quote_volume':_f(x.get('usdtVolume') or x.get('quoteVolume'))}
        except Exception:pass
        return out

    async def _gate(self,c):
        out={}
        try:
            tr,cr=await asyncio.gather(
                c.get('https://api.gateio.ws/api/v4/futures/usdt/tickers'),
                c.get('https://api.gateio.ws/api/v4/futures/usdt/contracts')
            )
            contracts={str(x.get('name') or '').upper():x for x in (cr.json() if cr.is_success else [])}
            for x in (tr.json() if tr.is_success else []):
                sym=str(x.get('contract') or '').upper()
                if not sym.endswith('_USDT'):continue
                base=sym[:-5]
                bid=_f(x.get('highest_bid'));ask=_f(x.get('lowest_ask'));rate=_f(x.get('funding_rate'))
                if bid<=0 or ask<=0:continue
                cx=contracts.get(sym) or {}; interval_s=_f(cx.get('funding_interval'),28800.0)
                out[base]={'venue':'Gate','base':base,'bid':bid,'ask':ask,'rate':rate,'interval_hours':max(1.0,interval_s/3600.0),
                    'next_funding_time':None,'interval_source':'GATE_CONTRACTS_BULK','source':'GATE_USDT_FUTURES_BULK',
                    'quote_volume':_f(x.get('volume_24h_quote') or x.get('volume_24h'))}
        except Exception:pass
        return out

    async def _kucoin(self,c):
        out={}
        try:
            r=await c.get('https://api-futures.kucoin.com/api/v1/contracts/active')
            d=r.json(); rows=d.get('data') or []
            for x in rows:
                if str(x.get('quoteCurrency') or '').upper()!='USDT':continue
                sym=str(x.get('symbol') or '').upper();root=str(x.get('rootSymbol') or '').upper()
                base=('BTC' if root=='XBT' else root) or _base_from_symbol(sym,'USDTM')
                bid=_f(x.get('bestBidPrice'));ask=_f(x.get('bestAskPrice'));rate=_f(x.get('fundingFeeRate'))
                if not base or bid<=0 or ask<=0:continue
                gran=_f(x.get('fundingRateGranularity'),28800000.0)
                out[base]={'venue':'KuCoin','base':base,'bid':bid,'ask':ask,'rate':rate,'interval_hours':max(1.0,gran/3600000.0),
                    'next_funding_time':x.get('nextFundingRateTime'),'interval_source':'KUCOIN_ACTIVE_CONTRACTS',
                    'source':'KUCOIN_USDT_FUTURES_BULK','quote_volume':_f(x.get('turnoverOf24h'))}
        except Exception:pass
        return out

    async def _okx(self,c):
        out={}
        try:
            r=await c.get('https://www.okx.com/api/v5/market/tickers',params={'instType':'SWAP'})
            d=r.json(); candidates=[]
            for x in (d.get('data') or []):
                inst=str(x.get('instId') or '').upper()
                if not inst.endswith('-USDT-SWAP'):continue
                base=inst[:-10];bid=_f(x.get('bidPx'));ask=_f(x.get('askPx'))
                if not base or bid<=0 or ask<=0:continue
                qv=_f(x.get('volCcy24h'))*((bid+ask)/2.0)
                candidates.append((qv,base,inst,bid,ask))
            candidates.sort(reverse=True)
            sem=asyncio.Semaphore(12)
            async def one(row):
                qv,base,inst,bid,ask=row
                async with sem:
                    try:
                        fr=await c.get('https://www.okx.com/api/v5/public/funding-rate',params={'instId':inst})
                        fd=fr.json();f=(fd.get('data') or [{}])[0]
                        if not fr.is_success or fd.get('code')!='0':return None
                        ft=_f(f.get('fundingTime'));nft=_f(f.get('nextFundingTime'))
                        hours=max(1.0,(nft-ft)/3600000.0) if nft>ft>0 else 8.0
                        return base,{'venue':'OKX','base':base,'bid':bid,'ask':ask,'rate':_f(f.get('fundingRate')),
                            'interval_hours':hours,'next_funding_time':f.get('nextFundingTime'),'interval_source':'OKX_FUNDING_TIMES',
                            'source':'OKX_SWAP_TOP_VOLUME','quote_volume':qv}
                    except Exception:return None
            vals=await asyncio.gather(*[one(x) for x in candidates[:OKX_FUNDING_LIMIT]])
            for z in vals:
                if z:out[z[0]]=z[1]
        except Exception:pass
        return out

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
                'capital_required_usdt':50.0,'notional_per_leg_usdt':25.0,
                'long_next_funding_time':longm.get('next_funding_time'),'short_next_funding_time':shortm.get('next_funding_time'),
                'interval_sources':[longm.get('interval_source'),shortm.get('interval_source')],
                'status':'PAPER_CANDIDATE' if conservative>0 and break_even is not None and break_even<=72 else 'WATCH'}

    async def refresh(self):
        if not self.enabled:return self.status()
        try:
            timeout=httpx.Timeout(12.0)
            async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT,timeout=timeout,follow_redirects=True,headers={'User-Agent':'AION-Research/1.0'}) as c:
                bundles=await asyncio.gather(self._bybit(c),self._bitget(c),self._gate(c),self._kucoin(c),self._okx(c))
            byvenue={v:m for v,m in zip(VENUES,bundles)}
            market={}
            for venue,rows in byvenue.items():
                for base,row in rows.items():market.setdefault(base,{})[venue]=row
            rows=[];pairs=0
            for base,venues in market.items():
                if len(venues)<2:continue
                ms=list(venues.values())
                for a in ms:
                    for b in ms:
                        if a['venue']==b['venue']:continue
                        pairs+=1
                        rr=self._pair_row(base,a,b,self.safety_pct,self.max_basis_abs_pct)
                        if rr:rows.append(rr)
            self.market=market
            self.rows=sorted(rows,key=lambda x:(x['status']=='PAPER_CANDIDATE',x['net_24h_conservative_pct']),reverse=True)[:500]
            self.venue_asset_counts={v:len(x) for v,x in byvenue.items()};self.universe_bases=len(market)
            self.multi_venue_bases=sum(1 for x in market.values() if len(x)>=2);self.pairs_evaluated=pairs
            self.last_refresh=time.time(); self.last_error=None
        except Exception as exc:self.last_error=str(exc)[:500]
        return self.status()

    def status(self):
        candidates=[x for x in self.rows if x.get('status')=='PAPER_CANDIDATE']
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'ANALYSIS_ONLY','interval_seconds':self.interval,
                'last_refresh':self.last_refresh,'last_error':self.last_error,'venues':VENUES,
                'venue_asset_counts':self.venue_asset_counts,'universe_bases':self.universe_bases,'multi_venue_bases':self.multi_venue_bases,
                'pairs_evaluated':self.pairs_evaluated,'market_assets':len(self.market),'count':len(self.rows),
                'candidate_count':len(candidates),'opportunities':self.rows,
                'policy':{'dynamic_perp_universe':True,'binance_cloud_dependency_removed':True,'no_live_orders':True}}

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
            await self.refresh(); await asyncio.sleep(max(120.0,self.interval))

perp_funding_spread_scanner=PerpFundingSpreadScanner()
