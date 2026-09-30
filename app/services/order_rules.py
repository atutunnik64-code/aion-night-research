from __future__ import annotations
from app.http_shared import SHARED_SSL_CONTEXT
import time, asyncio
from dataclasses import dataclass, asdict
from decimal import Decimal, ROUND_DOWN, InvalidOperation
import httpx

@dataclass
class OrderRule:
    venue: str
    symbol: str
    enabled: bool
    price_step: float = 0.0
    qty_step: float = 0.0
    min_qty: float = 0.0
    max_qty: float = 0.0
    min_notional: float = 0.0
    max_notional: float = 0.0
    source: str = 'public_instrument_rules'

class OrderRulesRegistry:
    def __init__(self):
        self.cache = {}
        self.ttl = 1800.0
        self.lock = asyncio.Lock()
        self.cache_hits = 0; self.fetches = 0; self.failures = 0; self.last_prewarm = 0.0

    @staticmethod
    def _f(v, default=0.0):
        try: return float(v)
        except (TypeError, ValueError): return default
    @staticmethod
    def _floor(value, step):
        if not step or step <= 0: return float(value)
        try:
            v = Decimal(str(value)); s = Decimal(str(step))
            return float((v / s).to_integral_value(rounding=ROUND_DOWN) * s)
        except (InvalidOperation, ValueError, ZeroDivisionError):
            return 0.0

    async def get(self, venue, symbol):
        key = (str(venue), str(symbol).upper())
        hit = self.cache.get(key)
        if hit and time.time() - hit[0] < self.ttl:
            self.cache_hits += 1
            return hit[1]
        async with self.lock:
            hit = self.cache.get(key)
            if hit and time.time() - hit[0] < self.ttl:
                self.cache_hits += 1
                return hit[1]
            self.fetches += 1
            try:
                async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=7, follow_redirects=True) as c:
                    rule = await self._fetch(c, venue, symbol)
            except Exception as exc:
                self.failures += 1
                rule = OrderRule(str(venue), str(symbol), False, source=f'fetch_error:{type(exc).__name__}')
            self.cache[key] = (time.time(), rule)
            return rule

    async def _fetch(self, c, venue, symbol):
        v = str(venue); raw = str(symbol or '').upper()
        if v == 'Binance': return await self._binance(c, raw.replace('-','').replace('_',''))
        if v == 'KuCoin': return await self._kucoin(c, raw.replace('_','-'))
        if v == 'Bybit': return await self._bybit(c, raw.replace('-','').replace('_',''))
        if v == 'OKX': return await self._okx(c, raw.replace('_','-'))
        if v == 'Bitget': return await self._bitget(c, raw.replace('-','').replace('_',''))
        if v == 'HTX': return await self._htx(c, raw.replace('-','').replace('_','').lower())
        if v == 'CoinW': return await self._coinw(c, raw.replace('-','_'))
        return OrderRule(v, raw, False, source='unsupported_private_venue')

    async def _binance(self, c, symbol):
        r = await c.get('https://api.binance.com/api/v3/exchangeInfo', params={'symbol': symbol}); r.raise_for_status()
        x = (r.json().get('symbols') or [{}])[0]
        fs = {z.get('filterType'): z for z in x.get('filters') or []}
        price = fs.get('PRICE_FILTER', {}); lot = fs.get('LOT_SIZE', {})
        notion = fs.get('NOTIONAL') or fs.get('MIN_NOTIONAL') or {}
        return OrderRule('Binance', symbol, x.get('status') == 'TRADING' and bool(x.get('isSpotTradingAllowed', True)),
            self._f(price.get('tickSize')), self._f(lot.get('stepSize')), self._f(lot.get('minQty')), self._f(lot.get('maxQty')),
            self._f(notion.get('minNotional')), self._f(notion.get('maxNotional')))

    async def _kucoin(self, c, symbol):
        r = await c.get('https://api.kucoin.com/api/v2/symbols'); r.raise_for_status()
        x = next((z for z in (r.json().get('data') or []) if str(z.get('symbol','')).upper() == symbol.upper()), None)
        if not x: return OrderRule('KuCoin', symbol, False, source='symbol_not_found')
        return OrderRule('KuCoin', symbol, bool(x.get('enableTrading', False)), self._f(x.get('priceIncrement')),
            self._f(x.get('baseIncrement')), self._f(x.get('baseMinSize')), self._f(x.get('baseMaxSize')),
            self._f(x.get('minFunds') or x.get('quoteMinSize')), self._f(x.get('quoteMaxSize')))

    async def _bybit(self, c, symbol):
        r = await c.get('https://api.bybit.com/v5/market/instruments-info', params={'category':'spot','symbol':symbol}); r.raise_for_status()
        x = (((r.json().get('result') or {}).get('list')) or [{}])[0]
        lot = x.get('lotSizeFilter') or {}; price = x.get('priceFilter') or {}
        return OrderRule('Bybit', symbol, x.get('status') == 'Trading', self._f(price.get('tickSize')),
            self._f(lot.get('basePrecision') or lot.get('minOrderQty')), self._f(lot.get('minOrderQty')),
            self._f(lot.get('maxOrderQty') or lot.get('maxLimitOrderQty')), self._f(lot.get('minOrderAmt')),
            self._f(lot.get('maxOrderAmt')))

    async def _okx(self, c, symbol):
        r = await c.get('https://www.okx.com/api/v5/public/instruments', params={'instType':'SPOT','instId':symbol}); r.raise_for_status()
        x = (r.json().get('data') or [{}])[0]
        return OrderRule('OKX', symbol, x.get('state') == 'live', self._f(x.get('tickSz')), self._f(x.get('lotSz')),
            self._f(x.get('minSz')), self._f(x.get('maxLmtSz') or x.get('maxMktSz')), 0.0,
            self._f(x.get('maxLmtAmt') or x.get('maxMktAmt')))

    async def _bitget(self, c, symbol):
        r = await c.get('https://api.bitget.com/api/v2/spot/public/symbols', params={'symbol':symbol}); r.raise_for_status()
        x = (r.json().get('data') or [{}])[0]
        qprec = int(self._f(x.get('quantityPrecision'), 0)); pprec = int(self._f(x.get('pricePrecision'), 0))
        return OrderRule('Bitget', symbol, str(x.get('status')).lower() == 'online', 10 ** (-pprec) if pprec >= 0 else 0.0,
            10 ** (-qprec) if qprec >= 0 else 0.0, self._f(x.get('minTradeAmount')), self._f(x.get('maxTradeAmount')),
            self._f(x.get('minTradeUSDT')), self._f(x.get('maxLimitOrderValue') or x.get('maxMarketOrderValue')))

    async def _coinw(self, c, symbol):
        r=await c.get('https://api.coinw.com/api/v1/public',params={'command':'returnSymbol'}); r.raise_for_status(); d=r.json()
        target=str(symbol or '').upper().replace('-','_')
        x=next((z for z in (d.get('data') or []) if str(z.get('currencyPair','')).upper()==target),None)
        if not x:return OrderRule('CoinW',target,False,source='symbol_not_found')
        pp=int(self._f(x.get('pricePrecision'),0)); qp=int(self._f(x.get('countPrecision'),0))
        return OrderRule('CoinW',target,int(self._f(x.get('state'),0))==1,10**(-pp),10**(-qp),
            self._f(x.get('minBuyCount')),self._f(x.get('maxBuyCount')),self._f(x.get('minBuyAmount')),self._f(x.get('maxBuyAmount')),source='coinw_returnSymbol')

    async def _htx(self, c, symbol):
        r = await c.get('https://api.huobi.pro/v1/settings/common/symbols'); r.raise_for_status()
        data = r.json().get('data') or []
        x = next((z for z in data if str(z.get('symbol','')).lower() == symbol.lower()), None)
        if not x: return OrderRule('HTX', symbol, False, source='symbol_not_found')
        ap = int(self._f(x.get('tap', x.get('amount-precision')), 0)); pp = int(self._f(x.get('tpp', x.get('price-precision')), 0))
        return OrderRule('HTX', symbol, x.get('state') == 'online' and bool(x.get('te', True)),
            10 ** (-pp) if pp >= 0 else 0.0, 10 ** (-ap) if ap >= 0 else 0.0,
            0.0, 0.0, 0.0, 0.0)

    def _apply_rule(self, leg, rule):
        out = dict(leg); out['order_rule'] = asdict(rule)
        if not rule.enabled:
            return False, 'PAIR_DISABLED_OR_RULE_UNAVAILABLE', out
        qty = self._floor(self._f(leg.get('qty')), rule.qty_step)
        price = self._floor(self._f(leg.get('price')), rule.price_step)
        notional = qty * price
        out['qty'] = qty; out['price'] = price; out['notional'] = notional
        if qty <= 0 or price <= 0: return False, 'NON_POSITIVE_ORDER', out
        if rule.min_qty and qty < rule.min_qty: return False, 'MIN_QTY', out
        if rule.max_qty and qty > rule.max_qty: return False, 'MAX_QTY', out
        if rule.min_notional and notional < rule.min_notional: return False, 'MIN_NOTIONAL', out
        if rule.max_notional and notional > rule.max_notional: return False, 'MAX_NOTIONAL', out
        return True, 'OK', out

    def cached(self, venue, symbol):
        key=(str(venue),str(symbol).upper())
        hit=self.cache.get(key)
        if not hit or time.time()-hit[0]>=self.ttl:return None
        self.cache_hits+=1
        return hit[1]

    async def preflight_leg(self, leg):
        rule = await self.get(leg.get('venue'), leg.get('symbol'))
        return self._apply_rule(leg, rule)

    def preflight_cached(self, legs):
        checked=[]
        for leg in legs:
            rule=self.cached(leg.get('venue'),leg.get('symbol'))
            if rule is None:
                out=dict(leg); out['order_rule']=None
                checked.append((False,'RULE_CACHE_MISS',out)); continue
            checked.append(self._apply_rule(leg,rule))
        normalized=[x[2] for x in checked]
        errors=[{'leg':i,'reason':x[1]} for i,x in enumerate(checked) if not x[0]]
        return {'ok':not errors,'legs':normalized,'errors':errors,'source':'RULE_CACHE'}

    async def preflight(self, legs):
        checked = await asyncio.gather(*[self.preflight_leg(x) for x in legs])
        normalized = [x[2] for x in checked]
        errors = [{'leg':i, 'reason':x[1]} for i,x in enumerate(checked) if not x[0]]
        return {'ok': not errors, 'legs': normalized, 'errors': errors, 'source':'RULE_FETCH_OR_CACHE'}

    async def prewarm(self, legs):
        unique={(str(x.get('venue') or ''),str(x.get('symbol') or '')) for x in (legs or []) if x.get('venue') and x.get('symbol')}
        if not unique:return {'ok':True,'requested':0,'enabled':0}
        rows=await asyncio.gather(*[self.get(v,s) for v,s in unique],return_exceptions=True)
        self.last_prewarm=time.time(); enabled=sum(1 for x in rows if not isinstance(x,Exception) and getattr(x,'enabled',False))
        return {'ok':True,'requested':len(unique),'enabled':enabled}

    def status(self):
        now=time.time(); fresh=sum(1 for ts,_ in self.cache.values() if now-ts<self.ttl)
        return {'cached':len(self.cache),'fresh':fresh,'ttl_s':self.ttl,'cache_hits':self.cache_hits,'fetches':self.fetches,'failures':self.failures,'last_prewarm':self.last_prewarm}

order_rules_registry = OrderRulesRegistry()
