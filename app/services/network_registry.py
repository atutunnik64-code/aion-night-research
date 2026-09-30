from __future__ import annotations
from app.http_shared import SHARED_SSL_CONTEXT
import asyncio, time
import httpx
from dataclasses import dataclass, asdict

PUBLIC_VENUES = {'Binance','KuCoin','Bitget','HTX','BitMart','CoinEx'}
NATIVE_ALIASES = {'BTC':'BTC','ETH':'ETH','SOL':'SOL','TRX':'TRX','BNB':'BSC','ONE':'ONE','RVN':'RVN','RONIN':'RONIN','WAVES':'WAVES','XRP':'XRP','XLM':'XLM','LTC':'LTC','BCH':'BCH','DOGE':'DOGE','TON':'TON','SUI':'SUI','APT':'APT','NEAR':'NEAR','ADA':'ADA','ATOM':'ATOM','IOTX':'IOTX','INJ':'INJ','RUNE':'RUNE','ZIL':'ZIL','MINA':'MINA','XNO':'XNO','OSMO':'OSMO'}

@dataclass
class NetworkInfo:
    venue: str
    asset: str
    network: str
    contract: str | None
    deposit: bool
    withdraw: bool
    withdraw_fee: float
    min_withdraw: float
    min_deposit: float
    raw_network: str


def _f(value, default=0.0):
    try: return float(value)
    except (TypeError, ValueError): return default
def _canon_network(*parts: str) -> str:
    text=' '.join(str(x or '').upper() for x in parts)
    aliases=[
        (('ERC20','ETHEREUM',' ETH ','ETH-','ETH_'),'ETH'),(('TRC20','TRON','TRX'),'TRX'),
        (('BEP20','BSC','BNB SMART'),'BSC'),(('ARBITRUM',' ARB ','ARC20'),'ARB'),
        (('OPTIMISM',' OP ','OPT'),'OP'),(('POLYGON','MATIC'),'POLYGON'),(('SOLANA',' SOL'),'SOL'),
        (('APTOS',' APT'),'APT'),(('RONIN',),'RONIN'),(('WAVES',),'WAVES'),(('HARMONY',' ONE'),'ONE'),
        (('RAVENCOIN',' RVN'),'RVN'),(('BITCOIN',' BTC'),'BTC'),(('LITECOIN',' LTC'),'LTC'),
        (('DOGECOIN','DOGE'),'DOGE'),(('XRP',),'XRP'),(('STELLAR','XLM'),'XLM'),(('TON',),'TON'),
        (('SUI',),'SUI'),(('NEAR',),'NEAR'),(('AVAX C','AVALANCHE C','AVAXC'),'AVAXC'),(('BASE',),'BASE'),
        (('ZILLIQA',' ZIL ','ZIL-','ZIL_'),'ZIL'),(('COSMOS',' ATOM ','ATOM-','ATOM_'),'ATOM'),
        (('IOTEX',' IOTX ','IOTX-','IOTX_'),'IOTX'),(('INJECTIVE',' INJ ','INJ-','INJ_'),'INJ'),
        (('THORCHAIN',' RUNE ','RUNE-','RUNE_'),'RUNE'),(('CARDANO',' ADA ','ADA-','ADA_'),'ADA'),
        (('STARKNET',),'STARKNET'),(('TERRA CLASSIC','TERRACLASSIC',' LUNC ','LUNC-','LUNC_'),'LUNC'),
        (('MINA',),'MINA'),(('NANO',' XNO '),'XNO'),(('OSMOSIS',' OSMO '),'OSMO'),
    ]
    padded=f' {text} '
    for needles, value in aliases:
        if any(n in padded for n in needles): return value
    cleaned=''.join(ch for ch in text if ch.isalnum())
    return cleaned[:32]

def _contract(value):
    s=str(value or '').strip()
    if not s or s.lower() in {'null','none','-'}: return None
    if s.startswith(('0x','0X')):
        body=s[2:].lower()
        if body and all(ch in '0123456789abcdef' for ch in body):
            return '0x'+(body.lstrip('0') or '0')
        return s.lower()
    return s

def _truth(value):
    if isinstance(value,bool): return value
    return str(value).lower() in {'true','1','yes','allowed','enable','enabled'}
class NetworkRegistry:
    def __init__(self):
        self.rows: dict[tuple[str,str], list[NetworkInfo]] = {}
        self.loaded_at=0.0
        self.ttl=180.0
        self.coinex_identity={}
        self.coinex_loaded=set()

    async def refresh(self, force=False):
        if not force and self.rows and time.time()-self.loaded_at < self.ttl: return
        async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=25,follow_redirects=True) as c:
            results=await asyncio.gather(
                c.get('https://www.binance.com/bapi/capital/v1/public/capital/getNetworkCoinAll?'),
                c.get('https://api.kucoin.com/api/v3/currencies'),
                c.get('https://api.bitget.com/api/v2/spot/public/coins'),
                c.get('https://api.huobi.pro/v2/reference/currencies'),
                c.get('https://api-cloud.bitmart.com/account/v1/currencies'),
                c.get('https://api.coinex.com/v2/assets/info'),
                return_exceptions=True)
        rows={}
        parsers=(self._binance,self._kucoin,self._bitget,self._htx,self._bitmart,self._coinex_info)
        for result,parser in zip(results,parsers):
            if isinstance(result,Exception): continue
            try: parser(result.json(),rows)
            except Exception: continue
        self.rows=rows; self.loaded_at=time.time()

    @staticmethod
    def _add(rows, info: NetworkInfo):
        rows.setdefault((info.venue,info.asset),[]).append(info)
    def _binance(self,d,rows):
        for coin in d.get('data') or []:
            asset=str(coin.get('coin','')).upper()
            for x in coin.get('networkList') or []:
                info=NetworkInfo('Binance',asset,_canon_network(x.get('network'),x.get('name')),_contract(x.get('contractAddress')),
                    bool(x.get('depositEnable')),bool(x.get('withdrawEnable')),_f(x.get('withdrawFee')),
                    _f(x.get('withdrawMin')),_f(x.get('depositDust')),str(x.get('network') or x.get('name') or ''))
                self._add(rows,info)

    def _kucoin(self,d,rows):
        for coin in d.get('data',[]):
            asset=str(coin.get('currency','')).upper()
            for x in coin.get('chains') or []:
                info=NetworkInfo('KuCoin',asset,_canon_network(x.get('chainId'),x.get('chainName')),_contract(x.get('contractAddress')),
                    bool(x.get('isDepositEnabled')),bool(x.get('isWithdrawEnabled')),
                    _f(x.get('withdrawMinFee',x.get('withdrawalMinFee'))),_f(x.get('withdrawMinSize',x.get('withdrawalMinSize'))),_f(x.get('depositMinSize')),
                    str(x.get('chainName') or x.get('chainId') or ''))
                self._add(rows,info)

    def _bitget(self,d,rows):
        for coin in d.get('data',[]):
            asset=str(coin.get('coin','')).upper()
            for x in coin.get('chains') or []:
                info=NetworkInfo('Bitget',asset,_canon_network(x.get('chain')),_contract(x.get('contractAddress')),
                    _truth(x.get('rechargeable')),_truth(x.get('withdrawable')),_f(x.get('withdrawFee'))+_f(x.get('extraWithdrawFee')),
                    _f(x.get('minWithdrawAmount')),_f(x.get('minDepositAmount')),str(x.get('chain') or ''))
                self._add(rows,info)
    def _htx(self,d,rows):
        for coin in d.get('data',[]):
            asset=str(coin.get('currency','')).upper()
            for x in coin.get('chains') or []:
                info=NetworkInfo('HTX',asset,_canon_network(x.get('baseChain'),x.get('displayName'),x.get('chain')),_contract(x.get('contractAddress')),
                    str(x.get('depositStatus','')).lower()=='allowed',str(x.get('withdrawStatus','')).lower()=='allowed',
                    _f(x.get('transactFeeWithdraw')),_f(x.get('minWithdrawAmt')),_f(x.get('minDepositAmt')),
                    str(x.get('displayName') or x.get('chain') or ''))
                self._add(rows,info)

    def _bitmart(self,d,rows):
        for x in d.get('data',{}).get('currencies',[]):
            asset=str(x.get('currency','')).upper()
            info=NetworkInfo('BitMart',asset,_canon_network(x.get('network')),_contract(x.get('contract_address')),
                bool(x.get('deposit_enabled')),bool(x.get('withdraw_enabled')),_f(x.get('withdraw_fee')),
                _f(x.get('withdraw_minsize')),_f(x.get('recharge_minsize')),str(x.get('network') or ''))
            self._add(rows,info)

    def _coinex_info(self,d,rows):
        if d.get('code') != 0:return
        for coin in d.get('data') or []:
            asset=str(coin.get('short_name','')).upper()
            for x in coin.get('chain_info') or []:
                network=_canon_network(x.get('chain_name'))
                identity=_contract(x.get('identity'))
                if asset and network:self.coinex_identity[(asset,network)]=identity

    async def ensure_coinex_assets(self,assets):
        ordered=[]
        for a in assets:
            a=str(a or '').upper()
            if a and a not in self.coinex_loaded and a not in ordered:ordered.append(a)
        ordered=ordered[:60]
        if not ordered:return
        sem=asyncio.Semaphore(8)
        async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=15,follow_redirects=True) as c:
            async def fetch(asset):
                async with sem:
                    try:
                        r=await c.get('https://api.coinex.com/v2/assets/deposit-withdraw-config',params={'ccy':asset}); r.raise_for_status(); return asset,r.json()
                    except Exception:return asset,None
            results=await asyncio.gather(*[fetch(a) for a in ordered])
        for asset,d in results:
            if not d or d.get('code')!=0:continue
            for x in (d.get('data') or {}).get('chains') or []:
                network=_canon_network(x.get('chain')); contract=self.coinex_identity.get((asset,network))
                info=NetworkInfo('CoinEx',asset,network,contract,bool(x.get('deposit_enabled')),bool(x.get('withdraw_enabled')),_f(x.get('withdrawal_fee')),_f(x.get('min_withdraw_amount')),_f(x.get('min_deposit_amount')),str(x.get('chain') or ''))
                self._add(self.rows,info)
            self.coinex_loaded.add(asset)

    @staticmethod
    def _identity_ok(asset,a:NetworkInfo,b:NetworkInfo):
        if a.network!=b.network: return False,None
        if a.contract and b.contract:
            return (a.contract==b.contract),('CONTRACT_MATCH' if a.contract==b.contract else 'CONTRACT_MISMATCH')
        native=NATIVE_ALIASES.get(asset.upper())
        if not a.contract and not b.contract and native and a.network==native:
            return True,'NATIVE_NETWORK_MATCH'
        return False,'IDENTITY_UNPROVEN'
    def best_transfer(self,src,dst,asset,amount):
        src_rows=self.rows.get((src,asset.upper()),[]); dst_rows=self.rows.get((dst,asset.upper()),[])
        if not src_rows or not dst_rows:
            return {'ok':False,'reason':'NETWORK_METADATA_UNAVAILABLE','src':src,'dst':dst,'asset':asset}
        candidates=[]; identity_issue=False
        for a in src_rows:
            for b in dst_rows:
                same,identity=self._identity_ok(asset,a,b)
                if not same:
                    if identity=='CONTRACT_MISMATCH': identity_issue=True
                    continue
                if not a.withdraw or not b.deposit: continue
                if amount < max(a.min_withdraw,b.min_deposit): continue
                candidates.append((a.withdraw_fee,a,b,identity))
        if not candidates:
            return {'ok':False,'reason':'CONTRACT_MISMATCH' if identity_issue else 'NO_OPEN_COMMON_NETWORK','src':src,'dst':dst,'asset':asset}
        fee,a,b,identity=min(candidates,key=lambda x:x[0])
        return {'ok':True,'src':src,'dst':dst,'asset':asset,'network':a.network,'raw_src_network':a.raw_network,
                'raw_dst_network':b.raw_network,'identity':identity,'contract':a.contract or b.contract,
                'fee':fee,'min_withdraw':a.min_withdraw,'min_deposit':b.min_deposit,
                'withdraw_open':a.withdraw,'deposit_open':b.deposit}

    def venue_supported(self,venue): return venue in PUBLIC_VENUES
    def summary(self):
        return {'venues':sorted(PUBLIC_VENUES),'assets':len({a for _,a in self.rows}),'network_rows':sum(len(v) for v in self.rows.values()),'loaded_at':self.loaded_at}

network_registry=NetworkRegistry()
