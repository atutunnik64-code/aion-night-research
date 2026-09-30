from __future__ import annotations
import asyncio,time
import httpx
from app.http_shared import SHARED_SSL_CONTEXT
from app.services.private_trading import ADAPTERS,readiness

class SyntheticInventory:
    def __init__(self):
        self.enabled=True; self.live_enabled=False; self.hold_hours=1.0
        self.pending=[]; self.borrow_rows=[]; self.perp_rows=[]
        self.last_refresh=None; self.last_error=None; self.task=None

    def ingest(self,wide):
        top={x.get('signature'):x for x in (wide.get('top_market') or [])}
        rows=[]
        for a in wide.get('audits') or []:
            if not a.get('passed'):continue
            m=top.get(a.get('signature')) or {}
            z={**m,**a}
            if float(z.get('market_net_pct') or z.get('net_pct') or 0)>=0.20:rows.append(z)
        self.pending=rows[:12]; self.schedule_refresh(); return self.status()

    def schedule_refresh(self):
        try:
            if self.task is None or self.task.done():self.task=asyncio.create_task(self.refresh(),name='synthetic-inventory-refresh')
        except RuntimeError:pass

    async def _bitget_borrow(self,coin):
        url='https://api.bitget.com/api/v3/market/margin-loans'
        async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT,timeout=6) as c:r=await c.get(url,params={'coin':coin})
        d=r.json(); x=d.get('data') or {}
        if not r.is_success or d.get('code')!='00000':return {'ok':False,'reason':'BITGET_MARGIN_INFO_FAILED'}
        daily=float(x.get('dailyInterest') or 0); limit=float(x.get('limit') or 0)
        return {'ok':limit>0,'daily_interest_pct':daily,'loan_limit_raw':limit,'source':'BITGET_PUBLIC_MARGIN_LOANS'}
    async def _htx_borrow(self,coin):
        a=ADAPTERS.get('HTX'); creds=readiness().get('HTX') or {}
        if not a or not creds.get('verified'):return {'ok':False,'reason':'HTX_AUTH_NOT_VERIFIED'}
        path='/v1/cross-margin/loan-info'
        try:
            async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT,timeout=7) as c:
                r=await c.get(a.base_url+path+'?'+a._auth_query('GET',path))
            d=r.json(); rows=d.get('data') or []; x=next((z for z in rows if str(z.get('currency') or '').upper()==coin.upper()),None)
            if not r.is_success or d.get('status')!='ok' or not x:return {'ok':False,'reason':'HTX_MARGIN_INFO_UNAVAILABLE'}
            rate=float(x.get('interest-rate') or 0); loanable=float(x.get('loanable-amt') or 0)
            return {'ok':loanable>0,'daily_interest_pct':rate*100.0,'loanable_qty':loanable,
                    'min_loan_qty':float(x.get('min-loan-amt') or 0),'max_loan_qty':float(x.get('max-loan-amt') or 0),
                    'source':'HTX_CROSS_MARGIN_LOAN_INFO'}
        except Exception as exc:return {'ok':False,'reason':'HTX_MARGIN_INFO_ERROR','error':str(exc)[:160]}

    async def _perp(self,venue,base,quote):
        if quote!='USDT':return {'ok':False,'reason':'PERP_QUOTE_UNSUPPORTED'}
        try:
            async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT,timeout=6,follow_redirects=True) as c:
                if venue=='Bitget':
                    r=await c.get('https://api.bitget.com/api/v2/mix/market/ticker',params={'symbol':base+'USDT','productType':'USDT-FUTURES'})
                    d=r.json(); x=((d.get('data') or [{}])[0]); bid=float(x.get('bidPr') or 0)
                    return {'ok':r.is_success and d.get('code')=='00000' and bid>0,'bid':bid,'ask':float(x.get('askPr') or 0),
                            'funding_rate':float(x.get('fundingRate') or 0),'source':'BITGET_USDT_PERP'}
                if venue=='HTX':
                    r=await c.get('https://api.hbdm.com/linear-swap-ex/market/detail/batch_merged',params={'contract_code':base+'-USDT'})
                    d=r.json(); x=((d.get('ticks') or [{}])[0]); bid=float((x.get('bid') or [0])[0] or 0); ask=float((x.get('ask') or [0])[0] or 0)
                    return {'ok':r.is_success and d.get('status')=='ok' and bid>0,'bid':bid,'ask':ask,'funding_rate':None,'source':'HTX_USDT_SWAP'}
                if venue=='Binance':
                    r=await c.get('https://fapi.binance.com/fapi/v1/ticker/bookTicker',params={'symbol':base+'USDT'}); x=r.json()
                    fr=await c.get('https://fapi.binance.com/fapi/v1/premiumIndex',params={'symbol':base+'USDT'}); f=fr.json()
                    bid=float(x.get('bidPrice') or 0); ask=float(x.get('askPrice') or 0)
                    return {'ok':r.is_success and bid>0,'bid':bid,'ask':ask,'funding_rate':float(f.get('lastFundingRate') or 0),'next_funding_time':f.get('nextFundingTime'),'source':'BINANCE_USDT_PERP'}
                if venue=='Bybit':
                    r=await c.get('https://api.bybit.com/v5/market/tickers',params={'category':'linear','symbol':base+'USDT'}); d=r.json(); x=((d.get('result') or {}).get('list') or [{}])[0]
                    bid=float(x.get('bid1Price') or 0); ask=float(x.get('ask1Price') or 0)
                    return {'ok':r.is_success and str(d.get('retCode'))=='0' and bid>0,'bid':bid,'ask':ask,'funding_rate':float(x.get('fundingRate') or 0),'next_funding_time':x.get('nextFundingTime'),'source':'BYBIT_USDT_PERP'}
                if venue=='OKX':
                    inst=base+'-USDT-SWAP'; r=await c.get('https://www.okx.com/api/v5/market/ticker',params={'instId':inst}); d=r.json(); x=(d.get('data') or [{}])[0]
                    fr=await c.get('https://www.okx.com/api/v5/public/funding-rate',params={'instId':inst}); fd=fr.json(); f=(fd.get('data') or [{}])[0]
                    bid=float(x.get('bidPx') or 0); ask=float(x.get('askPx') or 0)
                    return {'ok':r.is_success and d.get('code')=='0' and bid>0,'bid':bid,'ask':ask,'funding_rate':float(f.get('fundingRate') or 0),'funding_time':f.get('fundingTime'),'next_funding_time':f.get('nextFundingTime'),'source':'OKX_USDT_SWAP'}
            return {'ok':False,'reason':'PERP_VENUE_NOT_PROBED'}
        except Exception as exc:return {'ok':False,'reason':'PERP_QUERY_ERROR','error':str(exc)[:160]}
    async def _one(self,row):
        base=str(row.get('base') or '').upper(); quote=str(row.get('quote') or '').upper(); sell=str(row.get('sell_venue') or '')
        market_net=float(row.get('market_net_pct') or row.get('net_pct') or 0); notional=float(row.get('notional') or row.get('quote_in') or 25)
        borrow={'ok':False,'reason':'BORROW_NOT_PROBED'}
        if sell=='HTX':borrow=await self._htx_borrow(base)
        elif sell=='Bitget':borrow=await self._bitget_borrow(base)
        bcost=float(borrow.get('daily_interest_pct') or 0)*self.hold_hours/24.0
        borrow_net=market_net-bcost
        borrow_row={**{k:row.get(k) for k in ('base','quote','buy_venue','sell_venue','signature')},'market_net_pct':market_net,
                    'notional':notional,'borrow':borrow,'borrow_cost_pct':round(bcost,6),'estimated_net_pct':round(borrow_net,6),
                    'status':'BORROW_READY_ANALYSIS' if borrow.get('ok') and borrow_net>0 else ('BORROW_AVAILABLE_LOW_NET' if borrow.get('ok') else borrow.get('reason')),
                    'live_enabled':False}
        perp=await self._perp(sell,base,quote); spot_ask=float(row.get('buy_ask') or 0); perp_bid=float(perp.get('bid') or 0)
        spot_fee=.001; perp_fee=.0006; safety=.001
        if spot_ask>0 and perp_bid>0:
            basis_net=(perp_bid*(1-perp_fee)/(spot_ask*(1+spot_fee))-1.0-safety)*100.0
        else:basis_net=-999.0
        perp_row={**{k:row.get(k) for k in ('base','quote','buy_venue','sell_venue','signature')},'spot_buy_ask':spot_ask,'perp_sell_bid':perp_bid,
                  'entry_basis_net_pct':round(basis_net,6),'perp':perp,'status':'PERP_ENTRY_EDGE' if perp.get('ok') and basis_net>0 else ('PERP_NO_EDGE' if perp.get('ok') else perp.get('reason')),
                  'note':'Entry basis only; closing P/L depends on basis/funding.','live_enabled':False}
        return borrow_row,perp_row

    async def refresh(self):
        if not self.enabled:return self.status()
        try:
            pairs=await asyncio.gather(*[self._one(x) for x in self.pending[:10]],return_exceptions=True)
            b=[]; p=[]
            for x in pairs:
                if isinstance(x,Exception):continue
                b.append(x[0]); p.append(x[1])
            self.borrow_rows=sorted(b,key=lambda x:x.get('estimated_net_pct',-999),reverse=True)
            self.perp_rows=sorted(p,key=lambda x:x.get('entry_basis_net_pct',-999),reverse=True)
            self.last_refresh=time.time(); self.last_error=None
        except Exception as exc:self.last_error=str(exc)[:220]
        return self.status()

    def status(self):
        return {'ok':True,'enabled':self.enabled,'live_enabled':False,'mode':'ANALYSIS_ONLY','hold_hours':self.hold_hours,
                'last_refresh':self.last_refresh,'last_error':self.last_error,'candidate_count':len(self.pending),
                'borrow_ready_count':sum(1 for x in self.borrow_rows if x.get('status')=='BORROW_READY_ANALYSIS'),
                'perp_edge_count':sum(1 for x in self.perp_rows if x.get('status')=='PERP_ENTRY_EDGE'),
                'borrow_opportunities':self.borrow_rows,'perp_opportunities':self.perp_rows}

synthetic_inventory=SyntheticInventory()

