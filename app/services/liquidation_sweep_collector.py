from __future__ import annotations
import asyncio,json,time,urllib.parse,urllib.request
from pathlib import Path
import websockets

ROOT=Path(__file__).parents[2]
DATA=ROOT/'data'
RAW=DATA/'liquidation_force_orders_v1.jsonl'
SNAPS=DATA/'liquidation_context_snapshots_v1.jsonl'
STATE=DATA/'liquidation_sweep_collector_v1.json'
WS='wss://fstream.binance.com/market/ws/!forceOrder@arr'
CORE=['BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','BNBUSDT','DOGEUSDT','ADAUSDT','LINKUSDT','AVAXUSDT','LTCUSDT','SUIUSDT','AAVEUSDT']

def _get(url,params=None):
    q=('?'+urllib.parse.urlencode(params)) if params else ''
    req=urllib.request.Request(url+q,headers={'User-Agent':'AION-Crypto-Radar/0.11.58'})
    with urllib.request.urlopen(req,timeout=15) as r:return json.loads(r.read().decode('utf-8'))

def _append(path,row):
    with path.open('a',encoding='utf-8') as f:f.write(json.dumps(row,ensure_ascii=False,separators=(',',':'))+'\n')

def _blank():
    return {'version':'LIQUIDATION_SWEEP_COLLECTOR_V1','started_at':time.time(),'event_count':0,'notional_usdt':0.0,
            'symbols':{},'recent':[],'snapshot_count':0,'last_event_at':None,'last_snapshot_at':None}
class LiquidationSweepCollector:
    def __init__(self):
        self.enabled=True;self.live_enabled=False;self.ws_task=None;self.snap_task=None;self.last_error=None;self.connected=False
        try:self.state=json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:self.state=_blank()
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    def _record_event(self,msg):
        x=msg.get('data',msg);o=x.get('o') or {};sym=str(o.get('s') or '')
        if not sym.endswith('USDT'):return
        px=float(o.get('ap') or o.get('p') or 0);qty=float(o.get('q') or 0);notional=px*qty
        row={'event_time':int(x.get('E') or time.time()*1000),'trade_time':int(o.get('T') or 0),'symbol':sym,
             'side':o.get('S'),'price':px,'qty':qty,'notional_usdt':notional,'status':o.get('X'),'order_type':o.get('o')}
        _append(RAW,row);self.state['event_count']=int(self.state.get('event_count') or 0)+1
        if not self.state.get('first_event_at'): self.state['first_event_at']=time.time()
        self.state['notional_usdt']=float(self.state.get('notional_usdt') or 0)+notional;self.state['last_event_at']=time.time()
        z=self.state.setdefault('symbols',{}).setdefault(sym,{'events':0,'notional_usdt':0.0,'last_side':None,'last_event_at':None})
        z['events']+=1;z['notional_usdt']+=notional;z['last_side']=row['side'];z['last_event_at']=self.state['last_event_at']
        self.state.setdefault('recent',[]).append(row);self.state['recent']=self.state['recent'][-100:];self._save()
    async def _ws_loop(self):
        backoff=2
        while True:
            try:
                async with websockets.connect(WS,ping_interval=150,ping_timeout=600,open_timeout=20,close_timeout=10) as ws:
                    self.connected=True;self.last_error=None;backoff=2
                    async for raw in ws:self._record_event(json.loads(raw))
            except asyncio.CancelledError:raise
            except Exception as exc:self.connected=False;self.last_error='WS: '+str(exc)[:300];await asyncio.sleep(backoff);backoff=min(60,backoff*2)
    def _snapshot_symbol(self,sym):
        oi=_get('https://fapi.binance.com/fapi/v1/openInterest',{'symbol':sym})
        pm=_get('https://fapi.binance.com/fapi/v1/premiumIndex',{'symbol':sym})
        mark=float(pm.get('markPrice') or 0);index=float(pm.get('indexPrice') or 0)
        return {'ts':time.time(),'symbol':sym,'open_interest':float(oi.get('openInterest') or 0),'mark_price':mark,
                'index_price':index,'mark_index_pct':((mark/index-1)*100 if index else None),
                'funding_rate':float(pm.get('lastFundingRate') or 0),'next_funding_time':int(pm.get('nextFundingTime') or 0)}
    def _snapshot_sync(self):
        rows=[]
        recent_syms=[]
        for e in reversed(self.state.get('recent') or []):
            sym=str(e.get('symbol') or '')
            if sym and sym not in CORE and sym not in recent_syms: recent_syms.append(sym)
            if len(recent_syms)>=8: break
        symbols=CORE+recent_syms
        for sym in symbols:
            try:
                r=self._snapshot_symbol(sym);_append(SNAPS,r);rows.append(r)
            except Exception as exc:self.last_error=f'{sym}: {str(exc)[:180]}'
        self.state['snapshot_count']=int(self.state.get('snapshot_count') or 0)+len(rows)
        self.state['last_snapshot_at']=time.time();self.state['last_context']=rows;self._save();return rows
    async def _snapshot_loop(self):
        await asyncio.sleep(3)
        while True:
            try:await asyncio.to_thread(self._snapshot_sync)
            except asyncio.CancelledError:raise
            except Exception as exc:self.last_error='SNAP: '+str(exc)[:300]
            await asyncio.sleep(60)
    def status(self):
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'FUTURE_ONLY_COLLECT','strategy':'liquidation_sweep_recovery',
                'live_enabled':False,'paper_only':True,'connected':self.connected,'source':'BINANCE_USDTM_PUBLIC_FORCE_ORDER',
                'event_count':int(self.state.get('event_count') or 0),'notional_usdt':float(self.state.get('notional_usdt') or 0),
                'snapshot_count':int(self.state.get('snapshot_count') or 0),'tracked_symbols':len(self.state.get('symbols') or {}),
                'first_event_at':self.state.get('first_event_at'),'last_event_at':self.state.get('last_event_at'),'last_snapshot_at':self.state.get('last_snapshot_at'),
                'recent':(self.state.get('recent') or [])[-12:],'last_error':self.last_error,
                'gate':{'minimum_evaluable_events':100,'minimum_collection_hours':48,'rule_already_frozen':True},
                'policy':{'no_grid':True,'no_martingale':True,'no_dca':True,'no_live_orders':True,'no_backfill_selection':True}}
    async def start(self):
        if not self.enabled:return
        if not self.ws_task or self.ws_task.done():self.ws_task=asyncio.create_task(self._ws_loop(),name='liquidation-forceorder-ws')
        if not self.snap_task or self.snap_task.done():self.snap_task=asyncio.create_task(self._snapshot_loop(),name='liquidation-context-snapshots')
    async def stop(self):
        for t in (self.ws_task,self.snap_task):
            if t and not t.done():t.cancel()
        for t in (self.ws_task,self.snap_task):
            if t:
                try:await t
                except BaseException:pass
        self.ws_task=None;self.snap_task=None;self.connected=False

liquidation_sweep_collector=LiquidationSweepCollector()
