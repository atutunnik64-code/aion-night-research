from __future__ import annotations
import asyncio,json,time
from pathlib import Path
from collections import defaultdict

ROOT=Path(__file__).parents[2];DATA=ROOT/'data'
BIN_RAW=DATA/'liquidation_force_orders_v1.jsonl'
BY_RAW=DATA/'crossvenue_bybit_liquidations_v1.jsonl'
BIN_CTX=DATA/'liquidation_context_snapshots_v1.jsonl'
STATE=DATA/'major_alt_liquidation_contagion_shadow_v1.json'
MAJORS={'BTCUSDT','ETHUSDT','SOLUSDT'}
ALTS=['XRPUSDT','BNBUSDT','DOGEUSDT','ADAUSDT','LINKUSDT','AVAXUSDT','LTCUSDT','SUIUSDT','AAVEUSDT']
WINDOW_SEC=60;MIN_MAJOR_NOTIONAL=1_000_000.;MIN_DOMINANCE=.70
HOLD_SEC=3600;COOLDOWN_SEC=7200;COST=.0012;ALLOC=.05;MIN_ALT_COVERAGE=6
MIN_HOURS=72.;MIN_RESOLVED=30;MIN_PER_PRESSURE=5

def _lines(path,limit=100000):
    if not path.exists():return []
    with path.open('rb') as f:
        f.seek(0,2);pos=f.tell();buf=b'';block=1<<20
        while pos>0 and buf.count(b'\n')<=limit:
            n=min(block,pos);pos-=n;f.seek(pos);buf=f.read(n)+buf
    out=[]
    for raw in buf.splitlines()[-limit:]:
        try:out.append(json.loads(raw.decode('utf-8')))
        except Exception:pass
    return out

def _blank():
    now=time.time();return {'version':'LIQUIDATION_CONTAGION_MAJOR_ALT_V1','rule_frozen_at':now,
        'last_processed_bucket':int(now//WINDOW_SEC),'last_signal_ts':0.0,'pending':[],'resolved':[],
        'contagion_equity':100.0,'contagion_peak':100.0,'contagion_max_dd_pct':0.0,
        'relief_equity':100.0,'relief_peak':100.0,'relief_max_dd_pct':0.0}

def _pressure(e,venue):
    if venue=='Binance':
        s=str(e.get('side') or '')
        return s if s in {'BUY','SELL'} else ''
    s=str(e.get('pressure_side') or '')
    return s if s in {'BUY','SELL'} else ''

def _aggregate(events,venue,out):
    for e in events:
        sym=str(e.get('symbol') or '')
        if sym not in MAJORS:continue
        ts=float(e.get('event_time') or 0)/1000.;pressure=_pressure(e,venue)
        if ts<=0 or not pressure:continue
        bucket=int(ts//WINDOW_SEC);z=out.setdefault(bucket,{'BUY':0.0,'SELL':0.0,'majors':set(),'venues':set()})
        z[pressure]+=float(e.get('notional_usdt') or 0);z['majors'].add(sym);z['venues'].add(venue)
    return out

def _index_ctx(rows):
    out=defaultdict(list)
    for x in rows:
        s=str(x.get('symbol') or '')
        if s:out[s].append(x)
    return out

def _before(idx,sym,ts):
    for x in reversed(idx.get(sym) or []):
        if float(x.get('ts') or 0)<=ts:return x
    return None

def _after(idx,sym,ts):
    for x in idx.get(sym) or []:
        if float(x.get('ts') or 0)>=ts:return x
    return None

class MajorAltLiquidationContagionShadow:
    def __init__(self):
        self.enabled=True;self.live_enabled=False;self.task=None;self.last_error=None;self.last_refresh=None
        try:self.state=json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:self.state=_blank()
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    def _detect(self,bin_events,by_events,ctx):
        agg={};_aggregate(bin_events,'Binance',agg);_aggregate(by_events,'Bybit',agg)
        idx=_index_ctx(ctx);last=int(self.state.get('last_processed_bucket') or 0);new=0
        for bucket in sorted(agg):
            if bucket<=last:continue
            z=agg[bucket];total=float(z['BUY']+z['SELL'])
            if total<MIN_MAJOR_NOTIONAL:continue
            pressure='BUY' if z['BUY']>=z['SELL'] else 'SELL';dominant=float(z[pressure])
            dominance=dominant/total if total else 0.0
            if dominance<MIN_DOMINANCE:continue
            ts=(bucket+1)*WINDOW_SEC
            if ts-float(self.state.get('last_signal_ts') or 0)<COOLDOWN_SEC:continue
            entries={};funding={};nft={}
            for sym in ALTS:
                x=_before(idx,sym,ts)
                px=float((x or {}).get('mark_price') or 0)
                if px<=0:continue
                entries[sym]=px;funding[sym]=float(x.get('funding_rate') or 0);nft[sym]=int(x.get('next_funding_time') or 0)
            if len(entries)<MIN_ALT_COVERAGE:continue
            side=1.0 if pressure=='BUY' else -1.0
            self.state.setdefault('pending',[]).append({'id':f'{bucket}:{pressure}','ts':ts,'due_ts':ts+HOLD_SEC,
                'pressure':pressure,'buy_notional':float(z['BUY']),'sell_notional':float(z['SELL']),'major_notional':total,
                'dominance':dominance,'major_symbols':sorted(z['majors']),'venues':sorted(z['venues']),
                'entries':entries,'funding':funding,'next_funding_time':nft,'contagion_side':side,'relief_side':-side})
            self.state['last_signal_ts']=ts;new+=1
        if agg:self.state['last_processed_bucket']=max(last,max(agg))
        return new
    def _resolve(self,ctx):
        idx=_index_ctx(ctx);keep=[]
        for p in self.state.get('pending') or []:
            legs=[]
            for sym,entry in (p.get('entries') or {}).items():
                ex=_after(idx,sym,float(p['due_ts']))
                px=float((ex or {}).get('mark_price') or 0)
                if px<=0:continue
                crossed=int((p.get('next_funding_time') or {}).get(sym) or 0)>0 and float(p['ts'])*1000<int(p['next_funding_time'][sym])<=float(ex.get('ts') or 0)*1000
                legs.append({'symbol':sym,'ret':px/float(entry)-1.0,'exit':px,'exit_ts':float(ex.get('ts') or 0),
                    'funding':float((p.get('funding') or {}).get(sym) or 0),'funding_crossed':crossed})
            if len(legs)<MIN_ALT_COVERAGE:keep.append(p);continue
            out={**p,'legs':legs,'alt_coverage':len(legs),'resolved_ts':time.time()}
            for name in ('contagion','relief'):
                side=float(p[name+'_side']);nets=[]
                for leg in legs:
                    f=side*float(leg['funding']) if leg['funding_crossed'] else 0.0
                    nets.append(side*float(leg['ret'])-COST-f)
                net=sum(nets)/len(nets);out[name+'_net_return_pct']=net*100
                eqk=name+'_equity';peakk=name+'_peak';ddk=name+'_max_dd_pct'
                eq=float(self.state.get(eqk) or 100)*(1+ALLOC*net);peak=max(float(self.state.get(peakk) or 100),eq)
                self.state[eqk]=eq;self.state[peakk]=peak;self.state[ddk]=min(float(self.state.get(ddk) or 0),(eq/peak-1)*100)
                out[name+'_paper_equity']=eq
            self.state.setdefault('resolved',[]).append(out);self.state['resolved']=self.state['resolved'][-1000:]
        self.state['pending']=keep
    def _refresh_sync(self):
        be=_lines(BIN_RAW);ye=_lines(BY_RAW);ctx=_lines(BIN_CTX,80000)
        new=self._detect(be,ye,ctx);self._resolve(ctx);self.state['last_new_events']=new;self._save();return ctx
    async def refresh(self):
        try:ctx=await asyncio.to_thread(self._refresh_sync);self.last_error=None
        except Exception as exc:ctx=[];self.last_error=str(exc)[:500]
        self.last_refresh=time.time();return self.status(ctx)
    def status(self,ctx=None):
        ctx=_lines(BIN_CTX,80000) if ctx is None else ctx;r=self.state.get('resolved') or []
        freeze=float(self.state.get('rule_frozen_at') or time.time());ends=[float(x.get('ts') or 0) for x in ctx]
        hours=max(0.0,((max(ends) if ends else freeze)-freeze)/3600)
        pressures={p:sum(1 for x in r if x.get('pressure')==p) for p in ('BUY','SELL')}
        cw=sum(1 for x in r if float(x.get('contagion_net_return_pct') or 0)>0);rw=sum(1 for x in r if float(x.get('relief_net_return_pct') or 0)>0)
        gate={'collection_hours':hours,'required_hours':MIN_HOURS,'resolved_events':len(r),'required_resolved_events':MIN_RESOLVED,
            'pressure_events':pressures,'required_per_pressure':MIN_PER_PRESSURE,
            'ready_for_review':bool(hours>=MIN_HOURS and len(r)>=MIN_RESOLVED and all(pressures[p]>=MIN_PER_PRESSURE for p in pressures))}
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'FUTURE_ONLY_SHADOW','strategy':'LIQUIDATION_CONTAGION_MAJOR_ALT_V1',
            'live_enabled':False,'paper_only':True,'rule_frozen_at':self.state.get('rule_frozen_at'),
            'locked_rule':{'window_sec':WINDOW_SEC,'major_symbols':sorted(MAJORS),'alt_symbols':ALTS,'min_major_notional':MIN_MAJOR_NOTIONAL,
                'min_dominance':MIN_DOMINANCE,'min_alt_coverage':MIN_ALT_COVERAGE,'hold_hours':HOLD_SEC/3600,'cooldown_hours':COOLDOWN_SEC/3600,
                'round_trip_cost':COST,'allocation_fraction':ALLOC,'hypotheses':['CONTAGION','RELIEF']},
            'pending_count':len(self.state.get('pending') or []),'resolved_count':len(r),'future_gate':gate,
            'contagion':{'equity':self.state.get('contagion_equity',100.0),'return_pct':float(self.state.get('contagion_equity',100.0))-100,
                'max_dd_pct':self.state.get('contagion_max_dd_pct',0.0),'wins':cw,'win_rate':cw/len(r) if r else None},
            'relief':{'equity':self.state.get('relief_equity',100.0),'return_pct':float(self.state.get('relief_equity',100.0))-100,
                'max_dd_pct':self.state.get('relief_max_dd_pct',0.0),'wins':rw,'win_rate':rw/len(r) if r else None},
            'promotion_eligible':False,'recent_resolved':r[-12:],'last_refresh':self.last_refresh,'last_error':self.last_error,
            'policy':{'no_grid':True,'no_martingale':True,'no_dca':True,'no_historical_selection':True,
                'no_parameter_tuning':True,'live_requires_separate_review':True}}
    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name='major-alt-liquidation-contagion-shadow')
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

major_alt_liquidation_contagion_shadow=MajorAltLiquidationContagionShadow()
