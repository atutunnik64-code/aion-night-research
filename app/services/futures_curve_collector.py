from __future__ import annotations
import asyncio,json,time,urllib.parse,urllib.request
from pathlib import Path

ROOT=Path(__file__).parents[2];DATA=ROOT/'data'
RAW=DATA/'futures_curve_snapshots_v1.jsonl';STATE=DATA/'futures_curve_collector_v1.json'
ASSETS=['BTC','ETH'];INTERVAL=300.0
H={'User-Agent':'AION-Crypto-Radar/0.11.58'}

def _get(url,params=None):
    q=('?'+urllib.parse.urlencode(params)) if params else ''
    with urllib.request.urlopen(urllib.request.Request(url+q,headers=H),timeout=15) as r:return json.loads(r.read().decode())
def _append(x):
    with RAW.open('a',encoding='utf-8') as f:f.write(json.dumps(x,ensure_ascii=False,separators=(',',':'))+'\n')
def _ann(mark,index,expiry_ms,now_ms):
    if mark<=0 or index<=0:return None
    days=max((expiry_ms-now_ms)/86400000.0,1/24);basis=(mark/index-1.0)*100
    return {'basis_pct':basis,'annualized_basis_pct':basis*365.0/days,'days_to_expiry':days}

def _contracts():
    b=_get('https://fapi.binance.com/fapi/v1/exchangeInfo');o=_get('https://www.okx.com/api/v5/public/instruments',{'instType':'FUTURES'})
    out={}
    for asset in ASSETS:
        bs=[x for x in b.get('symbols',[]) if x.get('baseAsset')==asset and x.get('quoteAsset')=='USDT' and x.get('contractType') in {'CURRENT_QUARTER','NEXT_QUARTER'}]
        bm={x.get('contractType'):x for x in bs};rows=[]
        for label in ('CURRENT_QUARTER','NEXT_QUARTER'):
            z=bm.get(label)
            if not z:continue
            exp=int(z.get('deliveryDate') or 0)
            ox=[x for x in o.get('data',[]) if str(x.get('instId') or '').startswith(asset+'-USD_UM-') and int(x.get('expTime') or 0)==exp]
            if ox:rows.append({'label':label,'expiry_ms':exp,'binance':z['symbol'],'okx':ox[0]['instId']})
        out[asset]=rows
    return out
def _snapshot_sync():
    now=time.time();now_ms=int(now*1000);contracts=_contracts();row={'ts':now,'mode':'FUTURE_ONLY_COLLECT','assets':{},'errors':{}}
    for asset,items in contracts.items():
        idx_okx=None
        try:idx_okx=float(_get('https://www.okx.com/api/v5/market/index-tickers',{'instId':asset+'-USD'})['data'][0]['idxPx'])
        except Exception as exc:row['errors'][asset+':OKX_INDEX']=str(exc)[:180]
        aset={}
        for c in items:
            z={'expiry_ms':c['expiry_ms'],'binance_symbol':c['binance'],'okx_inst_id':c['okx']}
            try:
                b=_get('https://fapi.binance.com/fapi/v1/premiumIndex',{'symbol':c['binance']});bm=float(b.get('markPrice') or 0);bi=float(b.get('indexPrice') or 0)
                z['binance']={'mark':bm,'index':bi,**(_ann(bm,bi,c['expiry_ms'],now_ms) or {})}
            except Exception as exc:row['errors'][c['binance']]=str(exc)[:180]
            try:
                om=float(_get('https://www.okx.com/api/v5/public/mark-price',{'instType':'FUTURES','instId':c['okx']})['data'][0]['markPx'])
                z['okx']={'mark':om,'index':idx_okx,**(_ann(om,idx_okx or 0,c['expiry_ms'],now_ms) or {})}
            except Exception as exc:row['errors'][c['okx']]=str(exc)[:180]
            b=z.get('binance') or {};o=z.get('okx') or {}
            if b.get('annualized_basis_pct') is not None and o.get('annualized_basis_pct') is not None:
                z['crossvenue_ann_basis_diff_pct']=float(b['annualized_basis_pct'])-float(o['annualized_basis_pct'])
            aset[c['label']]=z
        if 'CURRENT_QUARTER' in aset and 'NEXT_QUARTER' in aset:
            for venue in ('binance','okx'):
                a=(aset['CURRENT_QUARTER'].get(venue) or {}).get('annualized_basis_pct');b=(aset['NEXT_QUARTER'].get(venue) or {}).get('annualized_basis_pct')
                if a is not None and b is not None:aset['curve_'+venue]={'next_minus_current_ann_pct':float(b)-float(a)}
        row['assets'][asset]=aset
    _append(row);return row
class FuturesCurveCollector:
    def __init__(self):
        self.enabled=True;self.live_enabled=False;self.task=None;self.last_error=None;self.last_refresh=None
        try:self.state=json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:self.state={'version':'FUTURES_CURVE_COLLECTOR_V1','started_at':time.time(),'snapshot_count':0,'last':None}
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    async def refresh(self):
        try:
            row=await asyncio.to_thread(_snapshot_sync);self.state['snapshot_count']=int(self.state.get('snapshot_count') or 0)+1;self.state['last']=row;self.state['last_refresh']=row['ts'];self._save();self.last_error=None
        except Exception as exc:self.last_error=str(exc)[:500]
        self.last_refresh=time.time();return self.status()
    def status(self):
        last=self.state.get('last') or {};return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'FUTURE_ONLY_COLLECT','strategy':'DATED_FUTURES_CURVE_DISLOCATION_V1',
            'live_enabled':False,'paper_only':True,'assets':ASSETS,'venues':['Binance','OKX'],'interval_seconds':INTERVAL,
            'snapshot_count':int(self.state.get('snapshot_count') or 0),'last':last,'last_refresh':self.state.get('last_refresh'),'last_error':self.last_error,
            'gate':{'required_baseline_snapshots':288,'required_collection_hours':24,'ready_for_shadow_rule':False},
            'policy':{'same_expiry_crossvenue_comparison':True,'annualized_basis_normalized_to_venue_index':True,'no_live_orders':True,'no_historical_selection':True}}
    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name='futures-curve-collector')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None
    async def _loop(self):
        while True:
            if self.enabled:await self.refresh()
            await asyncio.sleep(INTERVAL)

futures_curve_collector=FuturesCurveCollector()
