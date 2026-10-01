from __future__ import annotations
import asyncio,json,time
from pathlib import Path
import websockets

ROOT=Path(__file__).parents[2]; DATA=ROOT/'data'
BY_RAW=DATA/'crossvenue_bybit_liquidations_v1.jsonl'
BY_CTX=DATA/'crossvenue_bybit_liq_context_v1.jsonl'
STATE=DATA/'crossvenue_liquidation_asymmetry_collector_v1.json'
WS='wss://stream.bybit.com/v5/public/linear'
CORE=['BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','BNBUSDT','DOGEUSDT','ADAUSDT','LINKUSDT','AVAXUSDT','LTCUSDT','SUIUSDT','AAVEUSDT']
CTX_WRITE_SEC=60.0


def _append(path,row):
    with path.open('a',encoding='utf-8') as f:f.write(json.dumps(row,ensure_ascii=False,separators=(',',':'))+'\n')


def _blank():
    return {'version':'CROSSVENUE_LIQUIDATION_ASYMMETRY_COLLECTOR_V2_WS_CONTEXT','started_at':time.time(),
            'bybit_events':0,'bybit_notional_usdt':0.0,'context_count':0,'last_event_at':None,'last_context_at':None,'recent':[],
            'ticker_messages':0,'ws_reconnects':0}


class CrossVenueLiquidationAsymmetryCollector:
    def __init__(self):
        self.enabled=True;self.live_enabled=False;self.ws_task=None;self.connected=False;self.last_error=None
        self.ctx_latest={};self.ctx_last_written={}
        try:self.state=json.loads(STATE.read_text(encoding='utf-8-sig'))
        except Exception:self.state=_blank()
        self.state['version']='CROSSVENUE_LIQUIDATION_ASYMMETRY_COLLECTOR_V2_WS_CONTEXT'
    def _save(self):
        tmp=STATE.with_suffix(STATE.suffix+'.tmp');tmp.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8');tmp.replace(STATE)
    @staticmethod
    def _f(v):
        try:return float(v or 0)
        except Exception:return 0.0
    def _record_liq(self,msg):
        for d in msg.get('data') or []:
            sym=str(d.get('s') or '')
            if sym not in CORE:continue
            px=self._f(d.get('p'));qty=self._f(d.get('v'));notional=px*qty
            pos_side=str(d.get('S') or '')
            pressure='SELL' if pos_side=='Buy' else ('BUY' if pos_side=='Sell' else '')
            if not pressure or px<=0 or qty<=0:continue
            row={'event_time':int(d.get('T') or msg.get('ts') or time.time()*1000),'symbol':sym,
                 'position_side':pos_side,'pressure_side':pressure,'price':px,'qty':qty,'notional_usdt':notional}
            _append(BY_RAW,row);self.state['bybit_events']=int(self.state.get('bybit_events') or 0)+1
            self.state['bybit_notional_usdt']=float(self.state.get('bybit_notional_usdt') or 0)+notional
            self.state['last_event_at']=time.time();self.state.setdefault('recent',[]).append(row)
            self.state['recent']=self.state['recent'][-100:]
    def _record_ticker(self,msg):
        topic=str(msg.get('topic') or '')
        if not topic.startswith('tickers.'):return
        sym=topic.split('.',1)[1]
        if sym not in CORE:return
        d=msg.get('data') or {}
        if isinstance(d,list):d=d[0] if d else {}
        old=self.ctx_latest.get(sym) or {}
        # Linear ticker is delta-capable: retain fields omitted from deltas.
        merged=dict(old);merged.update({k:v for k,v in d.items() if v not in (None,'')})
        merged['ts']=self._f(msg.get('ts'))/1000.0 or time.time();self.ctx_latest[sym]=merged
        self.state['ticker_messages']=int(self.state.get('ticker_messages') or 0)+1
        now=time.time();prev=float(self.ctx_last_written.get(sym) or 0)
        if now-prev<CTX_WRITE_SEC:return
        oi=self._f(merged.get('openInterest'));mark=self._f(merged.get('markPrice'));index=self._f(merged.get('indexPrice'))
        if oi<=0 or mark<=0:return
        row={'ts':merged['ts'],'symbol':sym,'venue':'Bybit','open_interest':oi,'mark_price':mark,'index_price':index,
             'funding_rate':self._f(merged.get('fundingRate')),'next_funding_time':int(self._f(merged.get('nextFundingTime'))),
             'funding_interval_hours':self._f(merged.get('fundingIntervalHour')),'source':'BYBIT_LINEAR_WS'}
        _append(BY_CTX,row);self.ctx_last_written[sym]=now
        self.state['context_count']=int(self.state.get('context_count') or 0)+1
        self.state['last_context_at']=now
        recent_ctx=list(self.state.get('last_context') or [])
        recent_ctx=[x for x in recent_ctx if x.get('symbol')!=sym];recent_ctx.append(row)
        self.state['last_context']=recent_ctx[-len(CORE):]
    async def _ws_loop(self):
        backoff=2
        while True:
            try:
                async with websockets.connect(WS,ping_interval=None,open_timeout=20,close_timeout=10,max_size=4_000_000) as ws:
                    topics=[f'allLiquidation.{s}' for s in CORE]+[f'tickers.{s}' for s in CORE]
                    await ws.send(json.dumps({'op':'subscribe','args':topics}))
                    self.connected=True;self.last_error=None;backoff=2;self.state['ws_reconnects']=int(self.state.get('ws_reconnects') or 0)+1
                    last_ping=time.time();last_save=0.0
                    while True:
                        timeout=max(1.0,20-(time.time()-last_ping))
                        try:raw=await asyncio.wait_for(ws.recv(),timeout=timeout)
                        except asyncio.TimeoutError:
                            await ws.send(json.dumps({'op':'ping'}));last_ping=time.time();continue
                        msg=json.loads(raw)
                        topic=str(msg.get('topic') or '')
                        if topic.startswith('allLiquidation.'):self._record_liq(msg)
                        elif topic.startswith('tickers.'):self._record_ticker(msg)
                        now=time.time()
                        if now-last_save>=30:self._save();last_save=now
            except asyncio.CancelledError:raise
            except Exception as exc:
                self.connected=False;self.last_error='WS: '+str(exc)[:300];self.state['last_error']=self.last_error;self.state['last_error_ts']=time.time();self._save()
                await asyncio.sleep(backoff);backoff=min(60,backoff*2)
    def status(self):
        fresh=sum(1 for x in self.ctx_latest.values() if time.time()-float(x.get('ts') or 0)<30)
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'FUTURE_ONLY_COLLECT',
                'strategy':'CROSSVENUE_LIQUIDATION_ASYMMETRY_V2_WS_CONTEXT','live_enabled':False,'paper_only':True,
                'bybit_connected':self.connected,'symbols':CORE,'bybit_events':int(self.state.get('bybit_events') or 0),
                'bybit_notional_usdt':float(self.state.get('bybit_notional_usdt') or 0),'context_count':int(self.state.get('context_count') or 0),
                'fresh_context_symbols':fresh,'ticker_messages':int(self.state.get('ticker_messages') or 0),
                'last_event_at':self.state.get('last_event_at'),'last_context_at':self.state.get('last_context_at'),
                'recent':(self.state.get('recent') or [])[-12:],'last_error':self.last_error,
                'source':WS,'policy':{'context_via_websocket':True,'no_grid':True,'no_martingale':True,'no_dca':True,'no_live_orders':True,'no_historical_selection':True}}
    async def start(self):
        if not self.ws_task or self.ws_task.done():self.ws_task=asyncio.create_task(self._ws_loop(),name='crossvenue-bybit-linear-ws-context')
    async def stop(self):
        if self.ws_task and not self.ws_task.done():self.ws_task.cancel()
        if self.ws_task:
            try:await self.ws_task
            except BaseException:pass
        self.ws_task=None;self.connected=False;self._save()

crossvenue_liquidation_asymmetry_collector=CrossVenueLiquidationAsymmetryCollector()
