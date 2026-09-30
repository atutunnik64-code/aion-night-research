from __future__ import annotations
import asyncio,json,time,urllib.parse,urllib.request
from pathlib import Path

ROOT=Path(__file__).parents[2];DATA=ROOT/'data'
RAW=DATA/'liquidity_migration_perp_snapshots_v1.jsonl';STATE=DATA/'liquidity_migration_perp_collector_v1.json'
SYMS=['BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','DOGEUSDT','ADAUSDT','LINKUSDT','AVAXUSDT'];INTERVAL=30.0

def _get(url,params=None):
    q=('?'+urllib.parse.urlencode(params)) if params else ''
    req=urllib.request.Request(url+q,headers={'User-Agent':'AION-Crypto-Radar/0.11.58'})
    with urllib.request.urlopen(req,timeout=12) as r:return json.loads(r.read().decode('utf-8'))

def _book(bids,asks):
    b=[(float(x[0]),float(x[1])) for x in bids[:20]];a=[(float(x[0]),float(x[1])) for x in asks[:20]]
    if not b or not a:return None
    bid=b[0][0];ask=a[0][0];mid=(bid+ask)/2;bd=sum(p*q for p,q in b);ad=sum(p*q for p,q in a);tot=bd+ad
    return {'bid':bid,'ask':ask,'mid':mid,'spread_bps':((ask/bid-1)*10000 if bid else None),
            'bid_depth_quote':bd,'ask_depth_quote':ad,'depth_quote':tot,
            'imbalance':((bd-ad)/tot if tot else 0.0)}

def _append(row):
    with RAW.open('a',encoding='utf-8') as f:f.write(json.dumps(row,ensure_ascii=False,separators=(',',':'))+'\n')

def _binance(sym):
    d=_get('https://fapi.binance.com/fapi/v1/depth',{'symbol':sym,'limit':20});p=_get('https://fapi.binance.com/fapi/v1/premiumIndex',{'symbol':sym});fi=_get('https://fapi.binance.com/fapi/v1/fundingInfo')
    z=_book(d.get('bids') or [],d.get('asks') or []);meta=next((x for x in fi if x.get('symbol')==sym),None) or {}
    z['funding_rate']=float(p.get('lastFundingRate') or 0);z['next_funding_time']=int(p.get('nextFundingTime') or 0);z['funding_interval_hours']=float(meta.get('fundingIntervalHours') or 8);return z

def _bybit(sym):
    d=_get('https://api.bybit.com/v5/market/orderbook',{'category':'linear','symbol':sym,'limit':25});r=d.get('result') or {}
    z=_book(r.get('b') or [],r.get('a') or []);t=_get('https://api.bybit.com/v5/market/tickers',{'category':'linear','symbol':sym});x=((t.get('result') or {}).get('list') or [{}])[0]
    ii=_get('https://api.bybit.com/v5/market/instruments-info',{'category':'linear','symbol':sym});ix=((ii.get('result') or {}).get('list') or [{}])[0]
    z['funding_rate']=float(x.get('fundingRate') or 0);z['next_funding_time']=int(x.get('nextFundingTime') or 0);z['funding_interval_hours']=max(1.0,float(ix.get('fundingInterval') or 480)/60.0);return z

def _okx(sym):
    inst=sym.replace('USDT','-USDT-SWAP');d=_get('https://www.okx.com/api/v5/market/books',{'instId':inst,'sz':20});r=(d.get('data') or [{}])[0]
    z=_book(r.get('bids') or [],r.get('asks') or []);f=_get('https://www.okx.com/api/v5/public/funding-rate',{'instId':inst});x=(f.get('data') or [{}])[0]
    ft=int(x.get('fundingTime') or 0);nft=int(x.get('nextFundingTime') or 0);z['funding_rate']=float(x.get('fundingRate') or 0);z['funding_time']=ft;z['next_funding_time']=nft;z['funding_interval_hours']=max(1.0,(nft-ft)/3600000.0) if nft>ft else 8.0;return z

class LiquidityMigrationPerpCollector:
    def __init__(self):
        self.enabled=True;self.live_enabled=False;self.task=None;self.last_error=None;self.last_refresh=None
        try:self.state=json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:self.state={'version':'LIQUIDITY_MIGRATION_PERP_COLLECTOR_V1','snapshot_count':0,'started_at':time.time(),'last':None}
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    def _snapshot_sync(self):
        row={'ts':time.time(),'market':'USDT_PERPETUAL','symbols':{},'errors':{}}
        for sym in SYMS:
            venues={}
            for name,fn in [('Binance',_binance),('Bybit',_bybit),('OKX',_okx)]:
                try:venues[name]=fn(sym)
                except Exception as exc:row['errors'][f'{sym}:{name}']=str(exc)[:180]
            valid={k:v for k,v in venues.items() if isinstance(v,dict) and float(v.get('depth_quote') or 0)>0}
            total=sum(float(v['depth_quote']) for v in valid.values())
            for v in valid.values():v['depth_share']=float(v['depth_quote'])/total if total else 0.0
            mids=[float(v['mid']) for v in valid.values() if float(v.get('mid') or 0)>0];med=sorted(mids)[len(mids)//2] if mids else 0
            dispersion=((max(mids)-min(mids))/med*10000) if len(mids)>=2 and med else None
            row['symbols'][sym]={'venues':valid,'venue_count':len(valid),'dispersion_bps':dispersion}
        _append(row);self.state['snapshot_count']=int(self.state.get('snapshot_count') or 0)+1
        self.state['last']=row;self.state['last_refresh']=row['ts'];self._save();return row
    async def refresh(self):
        try:r=await asyncio.to_thread(self._snapshot_sync);self.last_error=None
        except Exception as exc:r=None;self.last_error=str(exc)[:500]
        self.last_refresh=time.time();return r
    def status(self):
        last=self.state.get('last') or {};return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'FUTURE_ONLY_COLLECT',
            'strategy':'venue_fragmentation_liquidity_shift','market':'USDT_PERPETUAL','venues':['Binance','Bybit','OKX'],'symbols':SYMS,
            'live_enabled':False,'paper_only':True,'interval_seconds':INTERVAL,'snapshot_count':int(self.state.get('snapshot_count') or 0),
            'last_refresh':self.state.get('last_refresh'),'coverage':{s:(last.get('symbols') or {}).get(s,{}).get('venue_count',0) for s in SYMS},
            'last_errors':last.get('errors') or {},'last_error':self.last_error,
            'policy':{'no_grid':True,'no_martingale':True,'no_dca':True,'no_live_orders':True,'funding_collected':True}}
    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name='liquidity-migration-perp-collector')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None
    async def _loop(self):
        await asyncio.sleep(5)
        while True:
            if self.enabled:await self.refresh()
            await asyncio.sleep(INTERVAL)

liquidity_migration_perp_collector=LiquidityMigrationPerpCollector()
