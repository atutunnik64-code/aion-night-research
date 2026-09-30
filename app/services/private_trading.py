from __future__ import annotations
from app.http_shared import SHARED_SSL_CONTEXT
import os,time,json,hmac,hashlib,base64,uuid,asyncio
from pathlib import Path
from datetime import datetime,timezone
from urllib.parse import urlencode
import httpx

class PrivateAdapter:
    name='Base'; base_url=''; fill_verification=False; live_execution_ready=True
    def __init__(self):
        self.key=os.getenv(f'AION_{self.name.upper().replace(".","").replace(" ","_")}_KEY','')
        self.secret=os.getenv(f'AION_{self.name.upper().replace(".","").replace(" ","_")}_SECRET','')
        self.passphrase=os.getenv(f'AION_{self.name.upper().replace(".","").replace(" ","_")}_PASSPHRASE','')
    def ready(self): return bool(self.key and self.secret)
    async def place_ioc(self,symbol,side,qty,price,client_id): raise NotImplementedError
    async def order_status(self,symbol,order_id=None,client_id=None): return {'ok':False,'terminal':True,'status':'UNSUPPORTED','filled_qty':0.0,'filled_quote':0.0}
    async def cancel_order(self,symbol,order_id=None,client_id=None): return {'ok':False,'status':'UNSUPPORTED'}
    async def balances(self): return {}

class BinancePrivate(PrivateAdapter):
    fill_verification=True
    name='BINANCE'; base_url='https://api.binance.com'
    async def _signed(self,method,path,params):
        params=dict(params); params['timestamp']=int(time.time()*1000); params['recvWindow']=3000
        query=urlencode(params,safe='')
        sig=hmac.new(self.secret.encode(),query.encode(),hashlib.sha256).hexdigest()
        headers={'X-MBX-APIKEY':self.key}
        async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=5) as c:
            r=await c.request(method,self.base_url+path+'?'+query+'&signature='+sig,headers=headers); return r
    async def balances(self):
        r=await self._signed('GET','/api/v3/account',{})
        d=r.json(); return {x['asset']:float(x.get('free') or 0) for x in d.get('balances',[]) if float(x.get('free') or 0)>0}
    async def place_ioc(self,symbol,side,qty,price,client_id):
        p={'symbol':symbol.replace('-','').replace('_',''),'side':side.upper(),'type':'LIMIT','timeInForce':'IOC',
           'quantity':format(qty,'.12f').rstrip('0').rstrip('.'),'price':format(price,'.12f').rstrip('0').rstrip('.'),
           'newClientOrderId':client_id[:36],'newOrderRespType':'FULL'}
        r=await self._signed('POST','/api/v3/order',p); d=r.json(); return {'ok':r.is_success,'venue':self.name,'http':r.status_code,'data':d,'order_id':d.get('orderId'),'client_id':d.get('clientOrderId') or client_id}
    async def order_status(self,symbol,order_id=None,client_id=None):
        p={'symbol':symbol.replace('-','').replace('_','')}; p.update({'orderId':order_id} if order_id else {'origClientOrderId':client_id})
        r=await self._signed('GET','/api/v3/order',p); d=r.json(); st=str(d.get('status') or '').upper(); q=float(d.get('executedQty') or 0); qq=float(d.get('cummulativeQuoteQty') or 0)
        return {'ok':r.is_success,'terminal':st in {'FILLED','CANCELED','REJECTED','EXPIRED','EXPIRED_IN_MATCH'},'status':st,'filled_qty':q,'filled_quote':qq,'data':d}
    async def cancel_order(self,symbol,order_id=None,client_id=None):
        p={'symbol':symbol.replace('-','').replace('_','')}; p.update({'orderId':order_id} if order_id else {'origClientOrderId':client_id}); r=await self._signed('DELETE','/api/v3/order',p)
        return {'ok':r.is_success,'status':'CANCEL_SENT','data':r.json()}

class KuCoinPrivate(PrivateAdapter):
    fill_verification=True
    name='KUCOIN'; base_url='https://api.kucoin.com'
    def _headers(self,method,path,body):
        ts=str(int(time.time()*1000)); raw=ts+method.upper()+path+body
        sign=base64.b64encode(hmac.new(self.secret.encode(),raw.encode(),hashlib.sha256).digest()).decode()
        phrase=base64.b64encode(hmac.new(self.secret.encode(),self.passphrase.encode(),hashlib.sha256).digest()).decode()
        return {'KC-API-KEY':self.key,'KC-API-SIGN':sign,'KC-API-TIMESTAMP':ts,
                'KC-API-PASSPHRASE':phrase,'KC-API-KEY-VERSION':'2','Content-Type':'application/json'}
    def ready(self): return bool(self.key and self.secret and self.passphrase)
    async def balances(self):
        path='/api/v1/accounts?type=trade'; headers=self._headers('GET',path,'')
        async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=5) as c:r=await c.get(self.base_url+path,headers=headers)
        d=r.json(); return {x['currency']:float(x.get('available') or 0) for x in d.get('data',[]) if float(x.get('available') or 0)>0}
    async def place_ioc(self,symbol,side,qty,price,client_id):
        path='/api/v1/hf/orders'; payload={'clientOid':client_id,'symbol':symbol.replace('_','-'),'type':'limit',
             'side':side.lower(),'price':str(price),'size':str(qty),'timeInForce':'IOC'}
        body=json.dumps(payload,separators=(',',':'))
        async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=5) as c:
            r=await c.post(self.base_url+path,headers=self._headers('POST',path,body),content=body)
        d=r.json(); return {'ok':r.is_success and d.get('code')=='200000','venue':self.name,'http':r.status_code,'data':d,'order_id':(d.get('data') or {}).get('orderId'),'client_id':client_id}
    async def order_status(self,symbol,order_id=None,client_id=None):
        ident=order_id or client_id; path=f"/api/v1/hf/orders/{ident}" if order_id else f"/api/v1/hf/orders/client-order/{ident}"; pathq=path+'?symbol='+symbol.replace('_','-')
        async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=5) as c:r=await c.get(self.base_url+pathq,headers=self._headers('GET',pathq,''))
        d=r.json(); x=d.get('data') or {}; active=bool(x.get('active')); q=float(x.get('dealSize') or 0); qq=float(x.get('dealFunds') or 0); st='ACTIVE' if active else ('FILLED' if q>0 and float(x.get('remainSize') or 0)<=0 else 'DONE')
        return {'ok':r.is_success and d.get('code')=='200000','terminal':not active,'status':st,'filled_qty':q,'filled_quote':qq,'data':d}
    async def cancel_order(self,symbol,order_id=None,client_id=None):
        ident=order_id or client_id; path=f"/api/v1/hf/orders/{ident}" if order_id else f"/api/v1/hf/orders/client-order/{ident}"; pathq=path+'?symbol='+symbol.replace('_','-')
        async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=5) as c:r=await c.delete(self.base_url+pathq,headers=self._headers('DELETE',pathq,'')); d=r.json()
        return {'ok':r.is_success and d.get('code')=='200000','status':'CANCEL_SENT','data':d}

class BybitPrivate(PrivateAdapter):
    fill_verification=True
    name='BYBIT'; base_url='https://api.bybit.com'
    def _now_ms(self):
        return int(time.time()*1000)+int(getattr(self,'_time_offset_ms',0) or 0)
    async def _ensure_time(self):
        if time.monotonic()-float(getattr(self,'_time_sync_mono',0) or 0)<60:return
        try:
            async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=4) as c:r=await c.get(self.base_url+'/v5/market/time')
            d=r.json(); server=int(d.get('time') or int((d.get('result') or {}).get('timeNano') or 0)//1_000_000)
            if server:self._time_offset_ms=server-int(time.time()*1000); self._time_sync_mono=time.monotonic()
        except Exception:pass
    def _headers(self,body):
        ts=str(self._now_ms()); recv='5000'; raw=ts+self.key+recv+body
        sign=hmac.new(self.secret.encode(),raw.encode(),hashlib.sha256).hexdigest()
        return {'X-BAPI-API-KEY':self.key,'X-BAPI-TIMESTAMP':ts,'X-BAPI-RECV-WINDOW':recv,
                'X-BAPI-SIGN':sign,'Content-Type':'application/json'}
    async def balances(self):
        await self._ensure_time()
        path='/v5/account/wallet-balance'; qs='accountType=UNIFIED'; ts=str(self._now_ms()); recv='5000'
        sign=hmac.new(self.secret.encode(),(ts+self.key+recv+qs).encode(),hashlib.sha256).hexdigest()
        headers={'X-BAPI-API-KEY':self.key,'X-BAPI-TIMESTAMP':ts,'X-BAPI-RECV-WINDOW':recv,'X-BAPI-SIGN':sign}
        async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=5) as c:r=await c.get(self.base_url+path+'?'+qs,headers=headers)
        d=r.json()
        if (not r.is_success) or d.get('retCode')!=0: raise RuntimeError(f"BYBIT_AUTH {d.get('retCode')} {d.get('retMsg')}")
        out={}
        for acct in d.get('result',{}).get('list',[]):
            for x in acct.get('coin',[]):
                bal=float(x.get('walletBalance') or 0)
                if bal>0:out[x.get('coin')]=bal
        return out
    async def place_ioc(self,symbol,side,qty,price,client_id):
        await self._ensure_time()
        path='/v5/order/create'; payload={'category':'spot','symbol':symbol.replace('-','').replace('_',''),
            'side':side.title(),'orderType':'Limit','qty':str(qty),'price':str(price),'timeInForce':'IOC','orderLinkId':client_id[:36]}
        body=json.dumps(payload,separators=(',',':'))
        async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=5) as c:r=await c.post(self.base_url+path,headers=self._headers(body),content=body)
        d=r.json(); x=d.get('result') or {}; return {'ok':r.is_success and d.get('retCode')==0,'venue':self.name,'http':r.status_code,'data':d,'order_id':x.get('orderId'),'client_id':x.get('orderLinkId') or client_id}
    async def _signed_get(self,path,qs):
        await self._ensure_time()
        ts=str(self._now_ms()); recv='5000'; sign=hmac.new(self.secret.encode(),(ts+self.key+recv+qs).encode(),hashlib.sha256).hexdigest(); h={'X-BAPI-API-KEY':self.key,'X-BAPI-TIMESTAMP':ts,'X-BAPI-RECV-WINDOW':recv,'X-BAPI-SIGN':sign}
        async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=5) as c:return await c.get(self.base_url+path+'?'+qs,headers=h)
    async def order_status(self,symbol,order_id=None,client_id=None):
        sym=symbol.replace('-','').replace('_',''); qs='category=spot&symbol='+sym+('&orderId='+str(order_id) if order_id else '&orderLinkId='+str(client_id)); r=await self._signed_get('/v5/order/realtime',qs); d=r.json(); x=((d.get('result') or {}).get('list') or [{}])[0]
        st=str(x.get('orderStatus') or '').upper(); q=float(x.get('cumExecQty') or 0); qq=float(x.get('cumExecValue') or 0); return {'ok':r.is_success and d.get('retCode')==0,'terminal':st in {'FILLED','CANCELLED','REJECTED','DEACTIVATED'},'status':st,'filled_qty':q,'filled_quote':qq,'data':d}
    async def cancel_order(self,symbol,order_id=None,client_id=None):
        await self._ensure_time()
        path='/v5/order/cancel'; payload={'category':'spot','symbol':symbol.replace('-','').replace('_','')}; payload.update({'orderId':order_id} if order_id else {'orderLinkId':client_id}); body=json.dumps(payload,separators=(',',':'))
        async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=5) as c:r=await c.post(self.base_url+path,headers=self._headers(body),content=body); d=r.json()
        return {'ok':r.is_success and d.get('retCode')==0,'status':'CANCEL_SENT','data':d}

class OKXPrivate(PrivateAdapter):
    fill_verification=True
    name='OKX'; base_url='https://www.okx.com'
    def ready(self): return bool(self.key and self.secret and self.passphrase)
    def _iso_now(self):
        off=float(getattr(self,'_time_offset_ms',0) or 0)/1000.0
        return datetime.fromtimestamp(time.time()+off,timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z')
    async def _ensure_time(self):
        if time.monotonic()-float(getattr(self,'_time_sync_mono',0) or 0)<60:return
        try:
            async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=4) as c:r=await c.get(self.base_url+'/api/v5/public/time')
            d=r.json(); rows=d.get('data') or []; server=int((rows[0] if rows else {}).get('ts') or 0)
            if server:self._time_offset_ms=server-int(time.time()*1000); self._time_sync_mono=time.monotonic()
        except Exception:pass
    def _headers(self,method,path,body):
        ts=self._iso_now()
        raw=ts+method.upper()+path+body
        sign=base64.b64encode(hmac.new(self.secret.encode(),raw.encode(),hashlib.sha256).digest()).decode()
        return {'OK-ACCESS-KEY':self.key,'OK-ACCESS-SIGN':sign,'OK-ACCESS-TIMESTAMP':ts,
                'OK-ACCESS-PASSPHRASE':self.passphrase,'Content-Type':'application/json'}
    async def balances(self):
        await self._ensure_time()
        path='/api/v5/account/balance'
        async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=5) as c:r=await c.get(self.base_url+path,headers=self._headers('GET',path,''))
        d=r.json()
        if (not r.is_success) or str(d.get('code'))!='0': raise RuntimeError(f"OKX_AUTH {d.get('code')} {d.get('msg')}")
        out={}
        for acct in d.get('data',[]):
            for x in acct.get('details',[]):
                bal=float(x.get('availBal') or x.get('cashBal') or 0)
                if bal>0:out[x.get('ccy')]=bal
        return out
    async def place_ioc(self,symbol,side,qty,price,client_id):
        await self._ensure_time()
        path='/api/v5/trade/order'; payload={'instId':symbol.replace('_','-'),'tdMode':'cash','clOrdId':client_id[:32],
            'side':side.lower(),'ordType':'ioc','px':str(price),'sz':str(qty)}
        body=json.dumps(payload,separators=(',',':'))
        async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=5) as c:r=await c.post(self.base_url+path,headers=self._headers('POST',path,body),content=body)
        d=r.json(); ok=r.is_success and str(d.get('code'))=='0'; x=(d.get('data') or [{}])[0]; return {'ok':ok,'venue':self.name,'http':r.status_code,'data':d,'order_id':x.get('ordId'),'client_id':x.get('clOrdId') or client_id}
    async def order_status(self,symbol,order_id=None,client_id=None):
        await self._ensure_time()
        inst=symbol.replace('_','-'); qs='instId='+inst+('&ordId='+str(order_id) if order_id else '&clOrdId='+str(client_id)); path='/api/v5/trade/order?'+qs
        async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=5) as c:r=await c.get(self.base_url+path,headers=self._headers('GET',path,'')); d=r.json(); x=(d.get('data') or [{}])[0]
        st=str(x.get('state') or '').lower(); q=float(x.get('accFillSz') or 0); avg=float(x.get('avgPx') or 0); return {'ok':r.is_success and str(d.get('code'))=='0','terminal':st in {'filled','canceled','mmp_canceled'},'status':st.upper(),'filled_qty':q,'filled_quote':q*avg,'data':d}
    async def cancel_order(self,symbol,order_id=None,client_id=None):
        await self._ensure_time()
        path='/api/v5/trade/cancel-order'; payload={'instId':symbol.replace('_','-')}; payload.update({'ordId':order_id} if order_id else {'clOrdId':client_id}); body=json.dumps(payload,separators=(',',':'))
        async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=5) as c:r=await c.post(self.base_url+path,headers=self._headers('POST',path,body),content=body); d=r.json()
        return {'ok':r.is_success and str(d.get('code'))=='0','status':'CANCEL_SENT','data':d}

class BitgetPrivate(PrivateAdapter):
    fill_verification=True
    name='BITGET'; base_url='https://api.bitget.com'
    def ready(self): return bool(self.key and self.secret and self.passphrase)
    def _now_ms(self):
        return int(time.time()*1000)+int(getattr(self,'_time_offset_ms',0) or 0)
    async def _ensure_time(self):
        if time.monotonic()-float(getattr(self,'_time_sync_mono',0) or 0)<60 and hasattr(self,'_time_offset_ms'):return
        last_exc=None
        for attempt in range(3):
            try:
                async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=6) as c:r=await c.get(self.base_url+'/api/v2/public/time')
                d=r.json(); server=int((d.get('data') or {}).get('serverTime') or d.get('requestTime') or 0)
                if server:
                    self._time_offset_ms=server-int(time.time()*1000); self._time_sync_mono=time.monotonic(); return
            except (httpx.TimeoutException,httpx.NetworkError,ValueError,TypeError) as exc:
                last_exc=exc
            if attempt<2: await asyncio.sleep(0.25)
        if hasattr(self,'_time_offset_ms'):
            self._time_sync_mono=time.monotonic(); return
        raise RuntimeError(f'BITGET_TIME_SYNC_FAILED {type(last_exc).__name__ if last_exc else "NO_SERVER_TIME"}')
    def _headers(self,method,path,body):
        ts=str(self._now_ms()); raw=ts+method.upper()+path+body
        sign=base64.b64encode(hmac.new(self.secret.encode(),raw.encode(),hashlib.sha256).digest()).decode()
        return {'ACCESS-KEY':self.key,'ACCESS-SIGN':sign,'ACCESS-TIMESTAMP':ts,
                'ACCESS-PASSPHRASE':self.passphrase,'Content-Type':'application/json','locale':'en-US'}
    async def balances(self):
        r=await self._get_signed('/api/v3/account/assets','')
        d=r.json()
        if (not r.is_success) or d.get('code')!='00000': raise RuntimeError(f"BITGET_AUTH {d.get('code')} {d.get('msg')}")
        rows=((d.get('data') or {}).get('assets') or [])
        return {x.get('coin'):float(x.get('available') or x.get('balance') or 0) for x in rows if float(x.get('available') or x.get('balance') or 0)>0}
    async def place_ioc(self,symbol,side,qty,price,client_id):
        await self._ensure_time()
        path='/api/v3/trade/place-order'; payload={'category':'SPOT','symbol':symbol.replace('-','').replace('_',''),'side':side.lower(),
            'orderType':'limit','timeInForce':'ioc','price':str(price),'qty':str(qty),'clientOid':client_id[:40]}
        body=json.dumps(payload,separators=(',',':'))
        async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=5) as c:r=await c.post(self.base_url+path,headers=self._headers('POST',path,body),content=body)
        d=r.json(); x=d.get('data') or {}; return {'ok':r.is_success and d.get('code')=='00000','venue':self.name,'http':r.status_code,'data':d,'order_id':x.get('orderId'),'client_id':x.get('clientOid') or client_id}
    async def _get_signed(self,path,qs):
        await self._ensure_time()
        full=path+('?' + qs if qs else '')
        last_exc=None
        for attempt in range(2):
            ts=str(self._now_ms()); raw=ts+'GET'+full; sign=base64.b64encode(hmac.new(self.secret.encode(),raw.encode(),hashlib.sha256).digest()).decode(); h={'ACCESS-KEY':self.key,'ACCESS-SIGN':sign,'ACCESS-TIMESTAMP':ts,'ACCESS-PASSPHRASE':self.passphrase,'locale':'en-US'}
            try:
                async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=8) as c:return await c.get(self.base_url+full,headers=h)
            except (httpx.TimeoutException,httpx.NetworkError) as exc:
                last_exc=exc
                if attempt==0: await asyncio.sleep(0.2)
        raise RuntimeError(f'BITGET_NETWORK {type(last_exc).__name__}')
    async def order_status(self,symbol,order_id=None,client_id=None):
        qs='category=SPOT&'+(('orderId='+str(order_id)) if order_id else ('clientOid='+str(client_id))); r=await self._get_signed('/api/v3/trade/order-info',qs); d=r.json(); x=d.get('data') or {}; st=str(x.get('orderStatus') or '').lower(); q=float(x.get('cumExecQty') or 0); qq=float(x.get('cumExecValue') or 0)
        fees=[]
        for z in (x.get('feeDetail') or []):
            try:
                cur=str(z.get('feeCoin') or '').upper(); amt=float(z.get('fee') or 0)
                if cur:fees.append({'currency':cur,'amount':amt,'source':'BITGET_FEE_DETAIL'})
            except Exception:pass
        return {'ok':r.is_success and d.get('code')=='00000','terminal':st in {'filled','cancelled','canceled','rejected'},'status':st.upper(),'filled_qty':q,'filled_quote':qq,'fee_items':fees,'data':d}
    async def cancel_order(self,symbol,order_id=None,client_id=None):
        await self._ensure_time()
        path='/api/v3/trade/cancel-order'; payload={'category':'SPOT'}
        payload.update({'orderId':order_id} if order_id else {'clientOid':client_id})
        body=json.dumps(payload,separators=(',',':'))
        async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=5) as c:
            r=await c.post(self.base_url+path,headers=self._headers('POST',path,body),content=body)
        d=r.json()
        return {'ok':r.is_success and d.get('code')=='00000','status':'CANCEL_SENT','data':d}


class CoinWPrivate(PrivateAdapter):
    name='COINW'; base_url='https://api.coinw.com'; fill_verification=True; live_execution_ready=False; emulated_ioc=True; ioc_timeout=0.35
    async def _private(self,command,params=None):
        params=dict(params or {}); params['api_key']=self.key
        raw=''.join(f'{k}={v}&' for k,v in sorted(params.items()))+'secret_key='+self.secret
        sign=hashlib.md5(raw.encode('utf-8')).hexdigest().upper()
        path=f'/api/v1/private?command={command}'; url=self.base_url+path+'&sign='+sign+'&'+urlencode(params)
        r=None; last_exc=None
        for attempt in range(2):
            try:
                async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=10) as c:r=await c.post(url,content=b'',headers={'Content-type':'application/json'})
                break
            except (httpx.TimeoutException,httpx.NetworkError) as exc:
                last_exc=exc
                if attempt==0:await asyncio.sleep(.25)
        if r is None:raise RuntimeError(f'COINW_NETWORK {type(last_exc).__name__}')
        try:d=r.json()
        except Exception:raise RuntimeError(f'COINW_HTTP_{r.status_code}')
        if (not r.is_success) or str(d.get('code'))!='200' or d.get('success') is False:raise RuntimeError(f"COINW_AUTH {d.get('code')} {d.get('msg')}")
        return d
    async def balances(self):
        d=await self._private('returnBalances',{}); out={}
        for k,v in (d.get('data') or {}).items():
            try:bal=float(v or 0)
            except Exception:continue
            if bal>0:out[str(k).upper()]=bal
        return out

    async def place_ioc(self,symbol,side,qty,price,client_id):
        sym=symbol.replace('-','_').upper(); params={'symbol':sym,'type':'0' if str(side).lower()=='buy' else '1',
            'amount':format(float(qty),'.12f').rstrip('0').rstrip('.'),'rate':format(float(price),'.12f').rstrip('0').rstrip('.'),
            'isMarket':'false','out_trade_no':str(client_id)[:50]}
        d=await self._private('doTrade',params); x=d.get('data') or {}; oid=x.get('orderNumber')
        return {'ok':bool(oid),'venue':'CoinW','data':d,'order_id':str(oid) if oid is not None else None,'client_id':client_id,'emulated_ioc':True}
    async def order_status(self,symbol,order_id=None,client_id=None):
        if not order_id:return {'ok':False,'terminal':True,'status':'MISSING_ORDER_ID','filled_qty':0.0,'filled_quote':0.0}
        d=await self._private('returnOrderTrades',{'orderNumber':str(order_id)}); x=d.get('data') or {}; st=int(x.get('status') or 0)
        return {'ok':True,'terminal':st in {3,4},'status':{1:'OPEN',2:'PARTIAL',3:'FILLED',4:'CANCELED'}.get(st,str(st)),
                'filled_qty':float(x.get('success_total') or 0),'filled_quote':float(x.get('success_amount') or 0),
                'fee_amount':float(x.get('fee') or 0),'data':d}
    async def cancel_order(self,symbol,order_id=None,client_id=None):
        if not order_id:return {'ok':False,'status':'MISSING_ORDER_ID'}
        d=await self._private('cancelOrder',{'orderNumber':str(order_id)})
        return {'ok':True,'status':'CANCEL_SENT','data':d}

class HTXPrivate(PrivateAdapter):
    name='HTX'; base_url='https://api.huobi.pro'; fill_verification=True
    def __init__(self):
        super().__init__(); self.account_id=os.getenv('AION_HTX_ACCOUNT_ID','')
    def ready(self): return bool(self.key and self.secret and self.account_id)
    def _auth_query(self,method,path,extra=None):
        params={'AccessKeyId':self.key,'SignatureMethod':'HmacSHA256','SignatureVersion':'2',
                'Timestamp':datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')}
        params.update(extra or {})
        qs=urlencode(sorted(params.items())); host='api.huobi.pro'
        raw='\n'.join([method.upper(),host,path,qs])
        sig=base64.b64encode(hmac.new(self.secret.encode(),raw.encode(),hashlib.sha256).digest()).decode()
        return qs+'&Signature='+urlencode({'x':sig})[2:]
    async def balances(self):
        path=f'/v1/account/accounts/{self.account_id}/balance'
        async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=5) as c:r=await c.get(self.base_url+path+'?'+self._auth_query('GET',path))
        d=r.json()
        if (not r.is_success) or d.get('status')!='ok':
            raise RuntimeError(f"HTX_AUTH {d.get('err-code')} {d.get('err-msg')}")
        out={}
        for x in (d.get('data') or {}).get('list',[]):
            if x.get('type')!='trade':continue
            bal=float(x.get('balance') or 0)
            if bal>0:out[str(x.get('currency','')).upper()]=bal
        return out
    async def place_ioc(self,symbol,side,qty,price,client_id):
        path='/v1/order/orders/place'; sym=symbol.replace('-','').replace('_','').lower()
        payload={'account-id':self.account_id,'symbol':sym,'type':f'{side.lower()}-ioc','amount':str(qty),
                 'price':str(price),'source':'spot-api','client-order-id':client_id[:64]}
        async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=5) as c:
            r=await c.post(self.base_url+path+'?'+self._auth_query('POST',path),json=payload)
        d=r.json(); ok=r.is_success and d.get('status')=='ok'
        return {'ok':ok,'venue':self.name,'http':r.status_code,'data':d,'order_id':d.get('data') if ok else None,'client_id':client_id}
    async def order_status(self,symbol,order_id=None,client_id=None):
        if order_id:
            path=f'/v1/order/orders/{order_id}'; extra=None
        elif client_id:
            path='/v1/order/orders/getClientOrder'; extra={'clientOrderId':client_id}
        else:
            return {'ok':False,'terminal':True,'status':'MISSING_ORDER_ID','filled_qty':0.0,'filled_quote':0.0}
        async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=5) as c:
            r=await c.get(self.base_url+path+'?'+self._auth_query('GET',path,extra))
        d=r.json(); x=d.get('data') or {}; st=str(x.get('state') or '').lower()
        q=float(x.get('filled-amount') or 0); qq=float(x.get('filled-cash-amount') or 0)
        terminal=st in {'filled','canceled','partial-canceled','partially-canceled'}; fees=[]
        if terminal and order_id and q>0:
            try:
                mp=f'/v1/order/orders/{order_id}/matchresults'
                async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=5) as c:mr=await c.get(self.base_url+mp+'?'+self._auth_query('GET',mp))
                md=mr.json()
                for z in (md.get('data') or []):
                    cur=str(z.get('fee-currency') or '').upper(); amt=float(z.get('filled-fees') or 0); dcur=str(z.get('fee-deduct-currency') or '').upper(); points=float(z.get('filled-points') or 0)
                    if dcur and points!=0:fees.append({'currency':dcur,'amount':points,'source':'HTX_FEE_DEDUCTION'})
                    elif cur:fees.append({'currency':cur,'amount':amt,'source':'HTX_MATCHRESULTS'})
            except Exception:pass
        return {'ok':r.is_success and d.get('status')=='ok','terminal':terminal,'status':st.upper(),'filled_qty':q,'filled_quote':qq,'fee_items':fees,'data':d}
    async def cancel_order(self,symbol,order_id=None,client_id=None):
        if order_id:
            path=f'/v1/order/orders/{order_id}/submitcancel'; payload=None
        elif client_id:
            path='/v1/order/orders/submitCancelClientOrder'; payload={'client-order-id':client_id}
        else:
            return {'ok':False,'status':'MISSING_ORDER_ID'}
        async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=5) as c:
            r=await c.post(self.base_url+path+'?'+self._auth_query('POST',path),json=payload)
        d=r.json(); return {'ok':r.is_success and d.get('status')=='ok','status':'CANCEL_SENT' if d.get('status')=='ok' else 'CANCEL_FAILED','data':d}

ADAPTERS={
    'Binance':BinancePrivate(),'KuCoin':KuCoinPrivate(),'Bybit':BybitPrivate(),
    'OKX':OKXPrivate(),'Bitget':BitgetPrivate(),'CoinW':CoinWPrivate(),'HTX':HTXPrivate(),
}
LIVE_VERIFICATION={}

def readiness():
    return {name:{'ready':a.ready(),'trade_only_required':True,'fill_verification':bool(a.fill_verification),
                  'live_execution_ready':bool(getattr(a,'live_execution_ready',True)),
                  'verified':bool((LIVE_VERIFICATION.get(name) or {}).get('verified'))} for name,a in ADAPTERS.items()}


def reload_live_credentials():
    env_path=Path(__file__).parents[2]/'data'/'secrets.env'
    if env_path.exists():
        for raw in env_path.read_text(encoding='utf-8-sig').splitlines():
            line=raw.strip()
            if not line or line.startswith('#') or '=' not in line:continue
            k,v=line.split('=',1); os.environ[k.strip()]=v.strip()
    for name,a in ADAPTERS.items():
        pref='AION_'+str(a.name).upper().replace('.','').replace(' ','_')
        a.key=os.getenv(pref+'_KEY',''); a.secret=os.getenv(pref+'_SECRET',''); a.passphrase=os.getenv(pref+'_PASSPHRASE','')
        if name=='HTX':a.account_id=os.getenv('AION_HTX_ACCOUNT_ID','')
        LIVE_VERIFICATION[name]={'verified':False,'status':'NOT_VERIFIED'}
    return readiness()

async def verify_live_credentials():
    reload_live_credentials(); out={}
    for name,a in ADAPTERS.items():
        if not a.ready():
            LIVE_VERIFICATION[name]={'verified':False,'status':'KEYS_MISSING'}
            out[name]={'configured':False,'verified':False,'status':'KEYS_MISSING','fill_verification':bool(a.fill_verification)}; continue
        if not a.fill_verification:
            LIVE_VERIFICATION[name]={'verified':False,'status':'FILL_VERIFICATION_UNSUPPORTED'}
            out[name]={'configured':True,'verified':False,'status':'FILL_VERIFICATION_UNSUPPORTED','fill_verification':False}; continue
        try:
            balances=await a.balances(); ok=isinstance(balances,dict)
            LIVE_VERIFICATION[name]={'verified':bool(ok),'status':'AUTH_OK' if ok else 'AUTH_FAILED'}
            out[name]={'configured':True,'verified':bool(ok),'status':'AUTH_OK' if ok else 'AUTH_FAILED','asset_count':len(balances) if ok else 0,'fill_verification':True}
        except Exception as exc:
            LIVE_VERIFICATION[name]={'verified':False,'status':'AUTH_FAILED'}
            out[name]={'configured':True,'verified':False,'status':'AUTH_FAILED','error':str(exc)[:180],'fill_verification':True}
    return out
