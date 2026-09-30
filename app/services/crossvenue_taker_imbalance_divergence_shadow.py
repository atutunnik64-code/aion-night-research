from __future__ import annotations
import asyncio,json,time
from collections import defaultdict
from pathlib import Path

ROOT=Path(__file__).parents[2];DATA=ROOT/'data'
FLOW=DATA/'crossvenue_perp_taker_buckets_v1.jsonl'
BIN_CTX=DATA/'liquidation_context_snapshots_v1.jsonl';BY_CTX=DATA/'crossvenue_bybit_liq_context_v1.jsonl'
STATE=DATA/'crossvenue_taker_imbalance_divergence_shadow_v1.json'
SYMS=['BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','BNBUSDT','DOGEUSDT','ADAUSDT','LINKUSDT','AVAXUSDT','LTCUSDT','SUIUSDT','AAVEUSDT']
MIN_VENUE_VOLUME=250_000.;MIN_ABS_IMBALANCE=.20;MIN_DIVERGENCE=.60;MIN_OI_MOVE_PCT=.50
HOLD_SEC=3600;COOLDOWN_SEC=7200;COST=.0012;ALLOC=.05
MIN_HOURS=72.;MIN_RESOLVED=30;MIN_SYMBOLS=6;MIN_PER_BIN_SIDE=5

def _tail(path,limit=80000):
    if not path.exists():return []
    raw=path.read_text(encoding='utf-8').splitlines()[-limit:];out=[]
    for line in raw:
        try:out.append(json.loads(line))
        except Exception:pass
    return out

def _blank():
    now=time.time();return {'version':'CROSSVENUE_TAKER_IMBALANCE_DIVERGENCE_V1','rule_frozen_at':now,
        'last_processed_bucket':int(now//60),'last_signal_ts':{},'pending':[],'resolved':[],
        'binance_equity':100.0,'binance_peak':100.0,'binance_max_dd_pct':0.0,
        'bybit_equity':100.0,'bybit_peak':100.0,'bybit_max_dd_pct':0.0}

def _ctx_index(rows):
    out=defaultdict(list)
    for x in rows:
        s=str(x.get('symbol') or '')
        if s:out[s].append(x)
    for s in out:out[s].sort(key=lambda x:float(x.get('ts') or 0))
    return out

def _at_before(idx,sym,ts):
    for x in reversed(idx.get(sym) or []):
        if float(x.get('ts') or 0)<=ts:return x
    return None

def _at_after(idx,sym,ts):
    for x in idx.get(sym) or []:
        if float(x.get('ts') or 0)>=ts:return x
    return None

def _oi_change(idx,sym,ts):
    now=_at_before(idx,sym,ts);old=_at_before(idx,sym,ts-900)
    if not now or not old:return None
    a=float(old.get('open_interest') or 0);b=float(now.get('open_interest') or 0)
    return ((b/a)-1.0)*100 if a>0 and b>0 else None

class CrossVenueTakerImbalanceDivergenceShadow:
    def __init__(self):
        self.enabled=True;self.live_enabled=False;self.task=None;self.last_error=None;self.last_refresh=None
        try:self.state=json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:self.state=_blank()
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    def _detect(self,flow,bc,yc):
        paired=defaultdict(dict)
        for x in flow:
            s=str(x.get('symbol') or '');v=str(x.get('venue') or '');b=int(x.get('bucket') or 0)
            if s in SYMS and v in {'Binance','Bybit'}:paired[(s,b)][v]=x
        bi=_ctx_index(bc);yi=_ctx_index(yc);last=int(self.state.get('last_processed_bucket') or 0);new=0
        for (sym,bucket),z in sorted(paired.items(),key=lambda kv:kv[0][1]):
            if bucket<=last or 'Binance' not in z or 'Bybit' not in z:continue
            b=z['Binance'];y=z['Bybit'];bv=float(b.get('quote_volume') or 0);yv=float(y.get('quote_volume') or 0)
            if bv<MIN_VENUE_VOLUME or yv<MIN_VENUE_VOLUME:continue
            bx=float(b.get('imbalance') or 0);yx=float(y.get('imbalance') or 0)
            if bx*yx>=0 or abs(bx)<MIN_ABS_IMBALANCE or abs(yx)<MIN_ABS_IMBALANCE or abs(bx-yx)<MIN_DIVERGENCE:continue
            ts=(bucket+1)*60;boi=_oi_change(bi,sym,ts);yoi=_oi_change(yi,sym,ts)
            if boi is None or yoi is None or max(abs(boi),abs(yoi))<MIN_OI_MOVE_PCT:continue
            prev=float((self.state.get('last_signal_ts') or {}).get(sym) or 0)
            if ts-prev<COOLDOWN_SEC:continue
            m=_at_before(bi,sym,ts);entry=float((m or {}).get('mark_price') or 0)
            if entry<=0:continue
            bside=1.0 if bx>0 else -1.0;yside=1.0 if yx>0 else -1.0
            self.state.setdefault('pending',[]).append({'id':f'{sym}:{bucket}','symbol':sym,'bucket':bucket,'ts':ts,'due_ts':ts+HOLD_SEC,
                'binance_imbalance':bx,'bybit_imbalance':yx,'divergence':abs(bx-yx),'binance_quote_volume':bv,'bybit_quote_volume':yv,
                'binance_oi_15m_pct':boi,'bybit_oi_15m_pct':yoi,'entry':entry,'binance_side':bside,'bybit_side':yside,
                'funding_rate':float(m.get('funding_rate') or 0),'next_funding_time':int(m.get('next_funding_time') or 0)})
            self.state.setdefault('last_signal_ts',{})[sym]=ts;new+=1
        buckets=[k[1] for k in paired]
        if buckets:self.state['last_processed_bucket']=max(last,max(buckets))
        return new

    def _resolve(self,bc):
        idx=_ctx_index(bc);keep=[]
        for p in self.state.get('pending') or []:
            ex=_at_after(idx,p['symbol'],float(p['due_ts']))
            px=float((ex or {}).get('mark_price') or 0)
            if px<=0:keep.append(p);continue
            ret=px/float(p['entry'])-1.0
            crossed=int(p.get('next_funding_time') or 0)>0 and float(p['ts'])*1000<int(p['next_funding_time'])<=float(ex.get('ts') or 0)*1000
            out={**p,'exit':px,'exit_ts':float(ex.get('ts') or 0),'funding_crossed':crossed}
            for name in ('binance','bybit'):
                side=float(p[name+'_side']);fund=side*float(p.get('funding_rate') or 0) if crossed else 0.0
                net=side*ret-COST-fund;out[name+'_net_return_pct']=net*100
                eqk=name+'_equity';peakk=name+'_peak';ddk=name+'_max_dd_pct'
                eq=float(self.state.get(eqk) or 100)*(1+ALLOC*net);peak=max(float(self.state.get(peakk) or 100),eq)
                self.state[eqk]=eq;self.state[peakk]=peak;self.state[ddk]=min(float(self.state.get(ddk) or 0),(eq/peak-1)*100)
                out[name+'_paper_equity']=eq
            self.state.setdefault('resolved',[]).append(out);self.state['resolved']=self.state['resolved'][-1000:]
        self.state['pending']=keep

    def _refresh_sync(self):
        flow=_tail(FLOW,50000);bc=_tail(BIN_CTX,80000);yc=_tail(BY_CTX,80000)
        new=self._detect(flow,bc,yc);self._resolve(bc);self.state['last_new_events']=new;self._save();return flow,bc,yc
    async def refresh(self):
        try:flow,bc,yc=await asyncio.to_thread(self._refresh_sync);self.last_error=None
        except Exception as exc:flow=[];bc=[];yc=[];self.last_error=str(exc)[:500]
        self.last_refresh=time.time();return self.status(flow,bc,yc)
    def status(self,flow=None,bc=None,yc=None):
        flow=_tail(FLOW,50000) if flow is None else flow;bc=_tail(BIN_CTX,80000) if bc is None else bc;yc=_tail(BY_CTX,80000) if yc is None else yc
        r=self.state.get('resolved') or [];freeze=float(self.state.get('rule_frozen_at') or time.time())
        ends=[float(x.get('ts') or 0) for x in flow];hours=max(0.0,((max(ends) if ends else freeze)-freeze)/3600)
        syms=sorted({x.get('symbol') for x in r if x.get('symbol')});bsides={s:sum(1 for x in r if float(x.get('binance_side') or 0)==s) for s in (-1.0,1.0)}
        bw=sum(1 for x in r if float(x.get('binance_net_return_pct') or 0)>0);yw=sum(1 for x in r if float(x.get('bybit_net_return_pct') or 0)>0)
        gate={'collection_hours':hours,'required_hours':MIN_HOURS,'resolved_events':len(r),'required_resolved_events':MIN_RESOLVED,
            'resolved_symbols':len(syms),'required_symbols':MIN_SYMBOLS,'binance_side_events':{'SELL':bsides[-1.0],'BUY':bsides[1.0]},
            'required_per_binance_side':MIN_PER_BIN_SIDE,'ready_for_review':bool(hours>=MIN_HOURS and len(r)>=MIN_RESOLVED and len(syms)>=MIN_SYMBOLS and all(v>=MIN_PER_BIN_SIDE for v in bsides.values()))}
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'FUTURE_ONLY_SHADOW','strategy':'CROSSVENUE_TAKER_IMBALANCE_DIVERGENCE_V1',
            'live_enabled':False,'paper_only':True,'market':'USDT_PERPETUAL','rule_frozen_at':self.state.get('rule_frozen_at'),
            'locked_rule':{'bucket_seconds':60,'min_venue_quote_volume':MIN_VENUE_VOLUME,'min_abs_imbalance':MIN_ABS_IMBALANCE,
                'min_crossvenue_divergence':MIN_DIVERGENCE,'min_abs_oi_move_15m_pct':MIN_OI_MOVE_PCT,'hold_hours':1,'cooldown_hours':2,
                'round_trip_cost':COST,'allocation_fraction':ALLOC,'hypotheses':['FOLLOW_BINANCE_FLOW','FOLLOW_BYBIT_FLOW']},
            'pending_count':len(self.state.get('pending') or []),'resolved_count':len(r),'future_gate':gate,
            'binance':{'equity':self.state.get('binance_equity',100.0),'return_pct':float(self.state.get('binance_equity',100.0))-100,
                'max_dd_pct':self.state.get('binance_max_dd_pct',0.0),'wins':bw,'win_rate':bw/len(r) if r else None},
            'bybit':{'equity':self.state.get('bybit_equity',100.0),'return_pct':float(self.state.get('bybit_equity',100.0))-100,
                'max_dd_pct':self.state.get('bybit_max_dd_pct',0.0),'wins':yw,'win_rate':yw/len(r) if r else None},
            'promotion_eligible':False,'recent_resolved':r[-12:],'last_refresh':self.last_refresh,'last_error':self.last_error,
            'policy':{'no_grid':True,'no_martingale':True,'no_dca':True,'no_historical_selection':True,'no_parameter_tuning':True,'live_requires_separate_review':True}}
    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name='crossvenue-taker-divergence-shadow')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None
    async def _loop(self):
        while True:
            if self.enabled:await self.refresh()
            await asyncio.sleep(120 if self.last_error else 300)

crossvenue_taker_imbalance_divergence_shadow=CrossVenueTakerImbalanceDivergenceShadow()
