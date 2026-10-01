from __future__ import annotations
import asyncio,json,time
from pathlib import Path

ROOT=Path(__file__).parents[2]; DATA=ROOT/'data'
RAW=DATA/'crossvenue_bybit_liquidations_v1.jsonl'; CTX=DATA/'crossvenue_bybit_liq_context_v1.jsonl'
STATE=DATA/'bybit_liquidation_regime_shadow_v1.json'
CORE=['BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','BNBUSDT','DOGEUSDT','ADAUSDT','LINKUSDT','AVAXUSDT','LTCUSDT','SUIUSDT','AAVEUSDT']
WINDOW_SEC=60; MIN_NOTIONAL=250000.0; MIN_OI_DROP_PCT=.20
HOLD_SEC=3600; COOLDOWN_SEC=7200; ROUND_TRIP_COST=.0012; ALLOC=.05
MIN_HOURS=72.; MIN_RESOLVED=30; MIN_SYMBOLS=6; STRESS=(2.0,3.0)

def _lines(path,limit=80000):
    if not path.exists(): return []
    out=[]
    for line in path.read_text(encoding='utf-8').splitlines()[-limit:]:
        try: out.append(json.loads(line))
        except Exception: pass
    return out

def _blank():
    now=time.time();return {'version':'BYBIT_LIQUIDATION_REGIME_V1','rule_frozen_at':now,'last_bucket':int(now//WINDOW_SEC),
        'last_signal_ts':{},'pending':[],'resolved':[],'continuation_equity':100.0,'continuation_peak':100.0,
        'continuation_max_dd_pct':0.0,'recovery_equity':100.0,'recovery_peak':100.0,'recovery_max_dd_pct':0.0}

def _ctx_pair(rows,sym,ts):
    z=[x for x in rows if x.get('symbol')==sym and float(x.get('ts') or 0)<=ts]
    if not z:return None,None
    now=z[-1]; old=[x for x in z if float(x.get('ts') or 0)<=ts-900]
    return now,(old[-1] if old else None)

def _exit(rows,sym,due):
    for x in rows:
        if x.get('symbol')==sym and float(x.get('ts') or 0)>=due:return x
    return None

def _stress(rows,name,m):
    eq=100.0
    for x in rows:
        base=float(x.get(name+'_net_return_pct') or 0)/100.0
        cost=float(x.get('round_trip_cost') or ROUND_TRIP_COST)
        eq*=1.0+ALLOC*(base-(m-1.0)*cost)
    return eq-100.0

class BybitLiquidationRegimeShadowV1:
    def __init__(self):
        self.enabled=True;self.live_enabled=False;self.task=None;self.last_error=None;self.last_refresh=None
        try:self.state=json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:self.state=_blank()
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    def _detect(self,events,ctx):
        agg={}
        for e in events:
            sym=str(e.get('symbol') or ''); pressure=str(e.get('pressure_side') or '')
            if sym not in CORE or pressure not in {'BUY','SELL'}:continue
            ts=float(e.get('event_time') or 0)/1000.0; bucket=int(ts//WINDOW_SEC)
            k=(sym,bucket,pressure);z=agg.setdefault(k,{'notional':0.0,'events':0})
            z['notional']+=float(e.get('notional_usdt') or 0);z['events']+=1
        last=int(self.state.get('last_bucket') or 0);new=0
        for (sym,bucket,pressure),z in sorted(agg.items(),key=lambda kv:kv[0][1]):
            if bucket<=last or z['notional']<MIN_NOTIONAL:continue
            ts=(bucket+1)*WINDOW_SEC;now,old=_ctx_pair(ctx,sym,ts)
            if not now or not old:continue
            oi0=float(old.get('open_interest') or 0);oi1=float(now.get('open_interest') or 0)
            if oi0<=0 or oi1<=0:continue
            oi_pct=(oi1/oi0-1.0)*100
            if oi_pct>-MIN_OI_DROP_PCT:continue
            lk=f'{sym}:{pressure}';prev=float((self.state.get('last_signal_ts') or {}).get(lk) or 0)
            if ts-prev<COOLDOWN_SEC:continue
            entry=float(now.get('mark_price') or 0)
            if entry<=0:continue
            side=1.0 if pressure=='BUY' else -1.0
            self.state.setdefault('pending',[]).append({'id':f'{sym}:{bucket}:{pressure}','symbol':sym,'ts':ts,'due_ts':ts+HOLD_SEC,
                'pressure_side':pressure,'liquidation_notional':z['notional'],'liquidation_events':z['events'],'oi_15m_pct':oi_pct,
                'entry':entry,'continuation_side':side,'recovery_side':-side,'funding_rate':float(now.get('funding_rate') or 0),
                'next_funding_time':int(now.get('next_funding_time') or 0),'round_trip_cost':ROUND_TRIP_COST,'allocation_fraction':ALLOC})
            self.state.setdefault('last_signal_ts',{})[lk]=ts;new+=1
        if agg:self.state['last_bucket']=max(last,max(k[1] for k in agg))
        return new
    def _resolve(self,ctx):
        keep=[]
        for p in self.state.get('pending') or []:
            ex=_exit(ctx,p['symbol'],float(p['due_ts']))
            if not ex:keep.append(p);continue
            px=float(ex.get('mark_price') or 0)
            if px<=0:keep.append(p);continue
            ret=px/float(p['entry'])-1.0;crossed=int(p.get('next_funding_time') or 0)>0 and float(p['ts'])*1000<int(p['next_funding_time'])<=float(ex.get('ts') or 0)*1000
            out={**p,'exit':px,'exit_ts':float(ex.get('ts') or 0),'funding_crossed':crossed}
            for name in ('continuation','recovery'):
                side=float(p[name+'_side']);fund=side*float(p.get('funding_rate') or 0) if crossed else 0.0
                net=side*ret-ROUND_TRIP_COST-fund;out[name+'_net_return_pct']=net*100
                eqk=name+'_equity';pk=name+'_peak';dd=name+'_max_dd_pct';eq=float(self.state.get(eqk) or 100)*(1+ALLOC*net)
                peak=max(float(self.state.get(pk) or 100),eq);self.state[eqk]=eq;self.state[pk]=peak;self.state[dd]=min(float(self.state.get(dd) or 0),(eq/peak-1)*100)
            self.state.setdefault('resolved',[]).append(out);self.state['resolved']=self.state['resolved'][-1000:]
        self.state['pending']=keep
    def _refresh_sync(self):
        e=_lines(RAW);c=_lines(CTX);self.state['last_new_events']=self._detect(e,c);self._resolve(c);self._save();return c
    async def refresh(self):
        try:c=await asyncio.to_thread(self._refresh_sync);self.last_error=None
        except Exception as exc:c=[];self.last_error=str(exc)[:500]
        self.last_refresh=time.time();return self.status(c)
    def status(self,c=None):
        c=_lines(CTX) if c is None else c;r=self.state.get('resolved') or [];freeze=float(self.state.get('rule_frozen_at') or time.time())
        end=max([float(x.get('ts') or 0) for x in c],default=freeze);hours=max(0.0,(end-freeze)/3600);syms=len({x.get('symbol') for x in r if x.get('symbol')})
        cw=sum(float(x.get('continuation_net_return_pct') or 0)>0 for x in r);rw=sum(float(x.get('recovery_net_return_pct') or 0)>0 for x in r)
        cs={f'{int(m)}x':_stress(r,'continuation',m) for m in STRESS};rs={f'{int(m)}x':_stress(r,'recovery',m) for m in STRESS}
        sample=hours>=MIN_HOURS and len(r)>=MIN_RESOLVED and syms>=MIN_SYMBOLS
        return {'ok':self.last_error is None,'strategy':'BYBIT_LIQUIDATION_REGIME_V1','mode':'FUTURE_ONLY_SHADOW','live_enabled':False,'paper_only':True,
            'locked_rule':{'window_sec':WINDOW_SEC,'min_liquidation_notional':MIN_NOTIONAL,'min_oi_drop_pct_15m':MIN_OI_DROP_PCT,'hold_hours':1,'cooldown_hours':2,'round_trip_cost':ROUND_TRIP_COST,
            'hypotheses':['CONTINUATION','RECOVERY']},'pending_count':len(self.state.get('pending') or []),'resolved_count':len(r),
            'continuation':{'return_pct':float(self.state.get('continuation_equity') or 100)-100,'wins':cw,'cost_stress_return_pct':cs},
            'recovery':{'return_pct':float(self.state.get('recovery_equity') or 100)-100,'wins':rw,'cost_stress_return_pct':rs},
            'future_gate':{'collection_hours':hours,'resolved':len(r),'resolved_symbols':syms,'sample_ready':sample,'ready_for_review':bool(sample and ((cs['2x']>0 and cs['3x']>0) or (rs['2x']>0 and rs['3x']>0)))},
            'promotion_eligible':False,'last_error':self.last_error,'last_refresh':self.last_refresh,
            'policy':{'no_grid':True,'no_martingale':True,'no_dca':True,'no_parameter_tuning':True}}
    async def start(self):
        if self.task and not self.task.done():return
        await self.refresh();self.task=asyncio.create_task(self._loop())
    async def stop(self):
        if self.task and not self.task.done():self.task.cancel()
        self.task=None
    async def _loop(self):
        while True:
            await asyncio.sleep(60)
            if self.enabled:await self.refresh()

bybit_liquidation_regime_shadow_v1=BybitLiquidationRegimeShadowV1()
