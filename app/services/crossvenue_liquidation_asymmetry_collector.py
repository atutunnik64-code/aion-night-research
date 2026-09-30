from __future__ import annotations
import asyncio,json,time,urllib.parse,urllib.request
from pathlib import Path
import websockets

ROOT=Path(__file__).parents[2]; DATA=ROOT/'data'
BY_RAW=DATA/'crossvenue_bybit_liquidations_v1.jsonl'
BY_CTX=DATA/'crossvenue_bybit_liq_context_v1.jsonl'
STATE=DATA/'crossvenue_liquidation_asymmetry_collector_v1.json'
WS='wss://stream.bybit.com/v5/public/linear'
CORE=['BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','BNBUSDT','DOGEUSDT','ADAUSDT','LINKUSDT','AVAXUSDT','LTCUSDT','SUIUSDT','AAVEUSDT']

def _append(path,row):
    with path.open('a',encoding='utf-8') as f:f.write(json.dumps(row,ensure_ascii=False,separators=(',',':'))+'\n')

def _get(url,params=None):
    q=('?'+urllib.parse.urlencode(params)) if params else ''
    req=urllib.request.Request(url+q,headers={'User-Agent':'AION-Crypto-Radar/0.11.58'})
    with urllib.request.urlopen(req,timeout=12) as r:return json.loads(r.read().decode('utf-8'))

def _blank():
    return {'version':'CROSSVENUE_LIQUIDATION_ASYMMETRY_COLLECTOR_V1','started_at':time.time(),
            'bybit_events':0,'bybit_notional_usdt':0.0,'context_count':0,'last_event_at':None,'last_context_at':None,'recent':[]}

class CrossVenueLiquidationAsymmetryCollector:
    def __init__(self):
        self.enabled=True;self.live_enabled=False;self.ws_task=None;self.ctx_task=None;self.connected=False;self.last_error=None
        try:self.state=json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:self.state=_blank()
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    def _record(self,msg):
        for d in msg.get('data') or []:
            sym=str(d.get('s') or '')
            if sym not in CORE:continue
            px=float(d.get('p') or 0);qty=float(d.get('v') or 0);notional=px*qty
            pos_side=str(d.get('S') or '')
            pressure='SELL' if pos_side=='Buy' else ('BUY' if pos_side=='Sell' else '')
            if not pressure or px<=0 or qty<=0:continue
            row={'event_time':int(d.get('T') or msg.get('ts') or time.time()*1000),'symbol':sym,
                 'position_side':pos_side,'pressure_side':pressure,'price':px,'qty':qty,'notional_usdt':notional}
            _append(BY_RAW,row);self.state['bybit_events']=int(self.state.get('bybit_events') or 0)+1
            self.state['bybit_notional_usdt']=float(self.state.get('bybit_notional_usdt') or 0)+notional
            self.state['last_event_at']=time.time();self.state.setdefault('recent',[]).append(row)
            self.state['recent']=self.state['recent'][-100:];self._save()
    async def _ws_loop(self):
        backoff=2
        while True:
            try:
                async with websockets.connect(WS,ping_interval=20,ping_timeout=20,open_timeout=20,close_timeout=10) as ws:
                    await ws.send(json.dumps({'op':'subscribe','args':[f'allLiquidation.{s}' for s in CORE]}))
                    self.connected=True;self.last_error=None;backoff=2
                    async for raw in ws:
                        msg=json.loads(raw)
                        if str(msg.get('topic') or '').startswith('allLiquidation.'):self._record(msg)
            except asyncio.CancelledError:raise
            except Exception as exc:
                self.connected=False;self.last_error='WS: '+str(exc)[:300];await asyncio.sleep(backoff);backoff=min(60,backoff*2)
    def _ctx_symbol(self,sym):
        r=_get('https://api.bybit.com/v5/market/tickers',{'category':'linear','symbol':sym})
        d=((r.get('result') or {}).get('list') or [{}])[0]
        return {'ts':time.time(),'symbol':sym,'venue':'Bybit','open_interest':float(d.get('openInterest') or 0),
                'mark_price':float(d.get('markPrice') or 0),'index_price':float(d.get('indexPrice') or 0),
                'funding_rate':float(d.get('fundingRate') or 0),'next_funding_time':int(d.get('nextFundingTime') or 0)}
    def _ctx_sync(self):
        rows=[]
        for sym in CORE:
            try:
                x=self._ctx_symbol(sym);_append(BY_CTX,x);rows.append(x)
            except Exception as exc:self.last_error=f'CTX {sym}: {str(exc)[:180]}'
        self.state['context_count']=int(self.state.get('context_count') or 0)+len(rows)
        self.state['last_context_at']=time.time();self.state['last_context']=rows;self._save();return rows
    async def _ctx_loop(self):
        await asyncio.sleep(2)
        while True:
            try:await asyncio.to_thread(self._ctx_sync)
            except asyncio.CancelledError:raise
            except Exception as exc:self.last_error='CTX: '+str(exc)[:300]
            await asyncio.sleep(60)
    def status(self):
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'FUTURE_ONLY_COLLECT',
                'strategy':'CROSSVENUE_LIQUIDATION_ASYMMETRY_V1','live_enabled':False,'paper_only':True,
                'bybit_connected':self.connected,'symbols':CORE,'bybit_events':int(self.state.get('bybit_events') or 0),
                'bybit_notional_usdt':float(self.state.get('bybit_notional_usdt') or 0),'context_count':int(self.state.get('context_count') or 0),
                'last_event_at':self.state.get('last_event_at'),'last_context_at':self.state.get('last_context_at'),
                'recent':(self.state.get('recent') or [])[-12:],'last_error':self.last_error,
                'policy':{'no_grid':True,'no_martingale':True,'no_dca':True,'no_live_orders':True,'no_historical_selection':True}}
    async def start(self):
        if not self.ws_task or self.ws_task.done():self.ws_task=asyncio.create_task(self._ws_loop(),name='crossvenue-bybit-liquidations')
        if not self.ctx_task or self.ctx_task.done():self.ctx_task=asyncio.create_task(self._ctx_loop(),name='crossvenue-bybit-liq-context')
    async def stop(self):
        for t in (self.ws_task,self.ctx_task):
            if t and not t.done():t.cancel()
        for t in (self.ws_task,self.ctx_task):
            if t:
                try:await t
                except BaseException:pass
        self.ws_task=None;self.ctx_task=None;self.connected=False

crossvenue_liquidation_asymmetry_collector=CrossVenueLiquidationAsymmetryCollector()

