from __future__ import annotations
import asyncio,json,time
from pathlib import Path

ROOT=Path(__file__).parents[2]; DATA=ROOT/'data'
BIN_RAW=DATA/'liquidation_force_orders_v1.jsonl'; BY_RAW=DATA/'crossvenue_bybit_liquidations_v1.jsonl'
BIN_CTX=DATA/'liquidation_context_snapshots_v1.jsonl'; BY_CTX=DATA/'crossvenue_bybit_liq_context_v1.jsonl'
STATE=DATA/'crossvenue_liquidation_asymmetry_shadow_v1.json'
CORE=['BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','BNBUSDT','DOGEUSDT','ADAUSDT','LINKUSDT','AVAXUSDT','LTCUSDT','SUIUSDT','AAVEUSDT']
WINDOW_SEC=60;MIN_COMBINED_NOTIONAL=250000.0;MIN_DOMINANCE=.60;MIN_OI_DROP_PCT=.20
HOLD_SEC=3600;COOLDOWN_SEC=7200;ROUND_TRIP_COST=.0012;ALLOC=.05
MIN_HOURS=72.;MIN_RESOLVED=30;MIN_SYMBOLS=6;MIN_PER_DOMINANT_VENUE=5
STRESS_COST_MULTS=(2.0,3.0)

def _lines(path,limit=80000):
    if not path.exists():return []
    out=[]
    for line in path.read_text(encoding='utf-8').splitlines()[-limit:]:
        try:out.append(json.loads(line))
        except Exception:pass
    return out

def _blank():
    now=time.time();return {'version':'CROSSVENUE_LIQUIDATION_ASYMMETRY_SHADOW_V1','rule_frozen_at':now,
        'last_processed_bucket':int(now//WINDOW_SEC),'last_signal_ts':{},'pending':[],'resolved':[],
        'continuation_equity':100.0,'continuation_peak':100.0,'continuation_max_dd_pct':0.0,
        'recovery_equity':100.0,'recovery_peak':100.0,'recovery_max_dd_pct':0.0}

def _stress_return_pct(rows,name,cost_mult):
    eq=100.0
    for x in rows:
        entry=float(x.get('entry') or 0);exit_px=float(x.get('exit') or 0)
        if entry<=0 or exit_px<=0:continue
        side=float(x.get(name+'_side') or 0);gross=side*(exit_px/entry-1.0)
        funding=side*float(x.get('funding_rate') or 0) if x.get('funding_crossed') else 0.0
        cost=float(x.get('round_trip_cost') or ROUND_TRIP_COST)*float(cost_mult)
        alloc=float(x.get('allocation_fraction') or ALLOC)
        eq*=1.0+alloc*(gross-cost-funding)
    return eq-100.0

def _bin_pressure(e):
    s=str(e.get('side') or '')
    return s if s in {'BUY','SELL'} else ''

def _aggregate(events,venue):
    out={}
    for e in events:
        sym=str(e.get('symbol') or '')
        if sym not in CORE:continue
        ms=float(e.get('event_time') or 0);ts=ms/1000.0
        pressure=_bin_pressure(e) if venue=='Binance' else str(e.get('pressure_side') or '')
        if pressure not in {'BUY','SELL'}:continue
        bucket=int(ts//WINDOW_SEC);k=(sym,bucket,pressure)
        z=out.setdefault(k,{'notional':0.0,'events':0})
        z['notional']+=float(e.get('notional_usdt') or 0);z['events']+=1
    return out

def _ctx_pair(rows,sym,ts):
    z=[x for x in rows if x.get('symbol')==sym and float(x.get('ts') or 0)<=ts]
    if not z:return None,None
    now=z[-1];old=[x for x in z if float(x.get('ts') or 0)<=ts-900]
    return now,(old[-1] if old else None)

def _exit_row(rows,sym,due):
    for x in rows:
        if x.get('symbol')==sym and float(x.get('ts') or 0)>=due:return x
    return None

class CrossVenueLiquidationAsymmetryShadow:
    def __init__(self):
        self.enabled=True;self.live_enabled=False;self.task=None;self.last_error=None;self.last_refresh=None
        try:self.state=json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:self.state=_blank()
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    def _detect(self,bin_events,by_events,bin_ctx,by_ctx):
        b=_aggregate(bin_events,'Binance');y=_aggregate(by_events,'Bybit')
        keys=sorted(set(b)|set(y),key=lambda k:k[1]);last=int(self.state.get('last_processed_bucket') or 0);new=0
        for sym,bucket,pressure in keys:
            if bucket<=last:continue
            ts=(bucket+1)*WINDOW_SEC
            bn=float((b.get((sym,bucket,pressure)) or {}).get('notional') or 0)
            yn=float((y.get((sym,bucket,pressure)) or {}).get('notional') or 0)
            total=bn+yn
            if total<MIN_COMBINED_NOTIONAL:continue
            dominance=abs(bn-yn)/total if total else 0.0
            if dominance<MIN_DOMINANCE:continue
            dominant='Binance' if bn>yn else 'Bybit'
            crows=bin_ctx if dominant=='Binance' else by_ctx
            now,old=_ctx_pair(crows,sym,ts)
            if not now or not old:continue
            oi0=float(old.get('open_interest') or 0);oi1=float(now.get('open_interest') or 0)
            if oi0<=0 or oi1<=0:continue
            oi_pct=(oi1/oi0-1.0)*100
            if oi_pct>-MIN_OI_DROP_PCT:continue
            bnow,_=_ctx_pair(bin_ctx,sym,ts)
            if not bnow:continue
            entry=float(bnow.get('mark_price') or 0)
            if entry<=0:continue
            lk=f'{sym}:{pressure}';prev=float((self.state.get('last_signal_ts') or {}).get(lk) or 0)
            if ts-prev<COOLDOWN_SEC:continue
            pressure_side=1.0 if pressure=='BUY' else -1.0
            self.state.setdefault('pending',[]).append({'id':f'{sym}:{bucket}:{pressure}','symbol':sym,'ts':ts,'due_ts':ts+HOLD_SEC,
                'pressure_side':pressure,'dominant_venue':dominant,'binance_notional':bn,'bybit_notional':yn,
                'combined_notional':total,'dominance':dominance,'dominant_oi_15m_pct':oi_pct,'entry':entry,
                'continuation_side':pressure_side,'recovery_side':-pressure_side,'funding_rate':float(bnow.get('funding_rate') or 0),
                'next_funding_time':int(bnow.get('next_funding_time') or 0),'round_trip_cost':ROUND_TRIP_COST,'allocation_fraction':ALLOC})
            self.state.setdefault('last_signal_ts',{})[lk]=ts;new+=1
        if keys:self.state['last_processed_bucket']=max(last,max(k[1] for k in keys))
        return new
    def _resolve(self,bin_ctx):
        keep=[]
        for p in self.state.get('pending') or []:
            ex=_exit_row(bin_ctx,p['symbol'],float(p['due_ts']))
            if not ex:keep.append(p);continue
            exit_px=float(ex.get('mark_price') or 0)
            if exit_px<=0:keep.append(p);continue
            ret=exit_px/float(p['entry'])-1.0
            crossed=int(p.get('next_funding_time') or 0)>0 and float(p['ts'])*1000<int(p['next_funding_time'])<=float(ex.get('ts') or 0)*1000
            out={**p,'exit':exit_px,'exit_ts':float(ex.get('ts') or 0),'funding_crossed':crossed}
            for name in ('continuation','recovery'):
                side=float(p[name+'_side']);f=side*float(p.get('funding_rate') or 0) if crossed else 0.0
                net=side*ret-ROUND_TRIP_COST-f;out[name+'_net_return_pct']=net*100
                eqk=name+'_equity';peakk=name+'_peak';ddk=name+'_max_dd_pct'
                eq=float(self.state.get(eqk) or 100)*(1+ALLOC*net);peak=max(float(self.state.get(peakk) or 100),eq)
                self.state[eqk]=eq;self.state[peakk]=peak;self.state[ddk]=min(float(self.state.get(ddk) or 0),(eq/peak-1)*100)
                out[name+'_paper_equity']=eq
            self.state.setdefault('resolved',[]).append(out);self.state['resolved']=self.state['resolved'][-1000:]
        self.state['pending']=keep
    def _refresh_sync(self):
        be=_lines(BIN_RAW);ye=_lines(BY_RAW);bc=_lines(BIN_CTX);yc=_lines(BY_CTX)
        new=self._detect(be,ye,bc,yc);self._resolve(bc);self.state['last_new_events']=new;self._save();return bc,yc
    async def refresh(self):
        try:bc,yc=await asyncio.to_thread(self._refresh_sync);self.last_error=None
        except Exception as exc:bc=[];yc=[];self.last_error=str(exc)[:500]
        self.last_refresh=time.time();return self.status(bc,yc)
    def status(self,bc=None,yc=None):
        bc=_lines(BIN_CTX) if bc is None else bc;yc=_lines(BY_CTX) if yc is None else yc;r=self.state.get('resolved') or []
        freeze=float(self.state.get('rule_frozen_at') or time.time());ends=[float(x.get('ts') or 0) for x in bc+yc]
        hours=max(0.0,((max(ends) if ends else freeze)-freeze)/3600)
        syms=sorted({x.get('symbol') for x in r if x.get('symbol')});venues={v:sum(1 for x in r if x.get('dominant_venue')==v) for v in ('Binance','Bybit')}
        cw=sum(1 for x in r if float(x.get('continuation_net_return_pct') or 0)>0);rw=sum(1 for x in r if float(x.get('recovery_net_return_pct') or 0)>0)
        continuation_stress={f'{int(m)}x':_stress_return_pct(r,'continuation',m) for m in STRESS_COST_MULTS}
        recovery_stress={f'{int(m)}x':_stress_return_pct(r,'recovery',m) for m in STRESS_COST_MULTS}
        sample_ready=bool(hours>=MIN_HOURS and len(r)>=MIN_RESOLVED and len(syms)>=MIN_SYMBOLS and all(venues[v]>=MIN_PER_DOMINANT_VENUE for v in venues))
        hypothesis_ready={'continuation':bool(sample_ready and continuation_stress['2x']>0 and continuation_stress['3x']>0),
                          'recovery':bool(sample_ready and recovery_stress['2x']>0 and recovery_stress['3x']>0)}
        gate={'collection_hours':hours,'required_hours':MIN_HOURS,'resolved_events':len(r),'required_resolved_events':MIN_RESOLVED,
              'resolved_symbols':len(syms),'required_symbols':MIN_SYMBOLS,'dominant_venues':venues,'required_per_dominant_venue':MIN_PER_DOMINANT_VENUE,
              'sample_ready':sample_ready,'continuation_stress_return_pct':continuation_stress,'recovery_stress_return_pct':recovery_stress,
              'hypothesis_ready':hypothesis_ready,'selection_policy':'NO_AUTOMATIC_WINNER',
              'ready_for_review':bool(hypothesis_ready['continuation'] or hypothesis_ready['recovery'])}
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'FUTURE_ONLY_SHADOW','strategy':'CROSSVENUE_LIQUIDATION_ASYMMETRY_V1',
            'live_enabled':False,'paper_only':True,'rule_frozen_at':self.state.get('rule_frozen_at'),
            'locked_rule':{'window_sec':WINDOW_SEC,'min_combined_notional':MIN_COMBINED_NOTIONAL,'min_dominance':MIN_DOMINANCE,
                'min_dominant_oi_drop_pct_15m':MIN_OI_DROP_PCT,'hold_hours':HOLD_SEC/3600,'cooldown_hours':COOLDOWN_SEC/3600,
                'round_trip_cost':ROUND_TRIP_COST,'allocation_fraction':ALLOC,'hypotheses':['CONTINUATION','RECOVERY']},
            'pending_count':len(self.state.get('pending') or []),'resolved_count':len(r),'resolved_symbols':syms,'future_gate':gate,
            'continuation':{'equity':self.state.get('continuation_equity',100.0),'return_pct':float(self.state.get('continuation_equity',100.0))-100,
                'max_dd_pct':self.state.get('continuation_max_dd_pct',0.0),'wins':cw,'win_rate':cw/len(r) if r else None,
                'cost_stress_return_pct':continuation_stress},
            'recovery':{'equity':self.state.get('recovery_equity',100.0),'return_pct':float(self.state.get('recovery_equity',100.0))-100,
                'max_dd_pct':self.state.get('recovery_max_dd_pct',0.0),'wins':rw,'win_rate':rw/len(r) if r else None,
                'cost_stress_return_pct':recovery_stress},
            'promotion_eligible':False,'recent_resolved':r[-12:],'last_refresh':self.last_refresh,'last_error':self.last_error,
            'policy':{'no_grid':True,'no_martingale':True,'no_dca':True,'no_historical_selection':True,'no_parameter_tuning':True,'live_requires_separate_review':True}}
    async def start(self):
        if self.task and not self.task.done():return
        await self.refresh();self.task=asyncio.create_task(self._loop(),name='crossvenue-liquidation-asymmetry-shadow')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None
    async def _loop(self):
        while True:
            await asyncio.sleep(60)
            if self.enabled:await self.refresh()

crossvenue_liquidation_asymmetry_shadow=CrossVenueLiquidationAsymmetryShadow()
