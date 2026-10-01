from __future__ import annotations
import asyncio,json,time
from pathlib import Path

ROOT=Path(__file__).parents[2]; DATA=ROOT/'data'
SNAPS=DATA/'crossvenue_bybit_liq_context_v1.jsonl'
STATE=DATA/'bybit_price_shock_shadow_v1.json'
CORE=['BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','BNBUSDT','DOGEUSDT','ADAUSDT','LINKUSDT','AVAXUSDT','LTCUSDT','SUIUSDT','AAVEUSDT']
LOOKBACK_SEC=900; MIN_ABS_MOVE_PCT=1.0; HOLD_SEC=3600; COOLDOWN_SEC=7200
ROUND_TRIP_COST=.0012; ALLOC=.05; MIN_HOURS=72.; MIN_RESOLVED=30; MIN_SYMBOLS=6
STRESS=(2.0,3.0)

def _rows():
    if not SNAPS.exists(): return []
    out=[]
    for line in SNAPS.read_text(encoding='utf-8').splitlines()[-50000:]:
        try:
            x=json.loads(line)
            if str(x.get('symbol') or '') in CORE and float(x.get('mark_price') or 0)>0: out.append(x)
        except Exception: pass
    return sorted(out,key=lambda x:float(x.get('ts') or 0))

def _blank():
    now=time.time()
    return {'version':'BYBIT_PRICE_SHOCK_V1','rule_frozen_at':now,'last_processed_ts':now,'last_event_ts':{},'pending':[],'resolved':[],
            'continuation_equity':100.0,'continuation_peak':100.0,'continuation_max_dd_pct':0.0,
            'reversal_equity':100.0,'reversal_peak':100.0,'reversal_max_dd_pct':0.0}

def _old(sr,target):
    for x in reversed(sr):
        if float(x.get('ts') or 0)<=target:return x
    return None

def _exit(sr,due):
    for x in sr:
        if float(x.get('ts') or 0)>=due:return x
    return None

def _stress(rows,name,mult):
    eq=100.0
    for x in rows:
        entry=float(x.get('entry') or 0); exit_px=float(x.get('exit') or 0)
        if entry<=0 or exit_px<=0: continue
        side=float(x.get(name+'_side') or 0); gross=side*(exit_px/entry-1.0)
        eq*=1.0+ALLOC*(gross-ROUND_TRIP_COST*float(mult))
    return eq-100.0

class BybitPriceShockShadowV1:
    def __init__(self):
        self.enabled=True; self.live_enabled=False; self.task=None; self.last_error=None; self.last_refresh=None
        try:self.state=json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:self.state=_blank()
    def _save(self): STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    def _detect(self,rows):
        by={s:[] for s in CORE}
        for x in rows:
            s=str(x.get('symbol') or '')
            if s in by: by[s].append(x)
        last=float(self.state.get('last_processed_ts') or self.state.get('rule_frozen_at') or 0); new=0; max_ts=last
        for sym,sr in by.items():
            for x in sr:
                ts=float(x.get('ts') or 0); max_ts=max(max_ts,ts)
                if ts<=last or ts<float(self.state.get('rule_frozen_at') or 0): continue
                old=_old(sr,ts-LOOKBACK_SEC)
                if not old: continue
                p0=float(old.get('mark_price') or 0); p1=float(x.get('mark_price') or 0)
                if p0<=0 or p1<=0: continue
                move=(p1/p0-1.0)*100.0
                if abs(move)<MIN_ABS_MOVE_PCT: continue
                prev=float((self.state.get('last_event_ts') or {}).get(sym) or 0)
                if ts-prev<COOLDOWN_SEC: continue
                direction=1.0 if move>0 else -1.0
                self.state.setdefault('pending',[]).append({'id':f'{sym}:{int(ts)}','symbol':sym,'venue':'Bybit','ts':ts,'due_ts':ts+HOLD_SEC,
                    'move_15m_pct':move,'entry':p1,'continuation_side':direction,'reversal_side':-direction,'round_trip_cost':ROUND_TRIP_COST})
                self.state.setdefault('last_event_ts',{})[sym]=ts; new+=1
        self.state['last_processed_ts']=max_ts; return new
    def _resolve(self,rows):
        by={s:[] for s in CORE}
        for x in rows:
            s=str(x.get('symbol') or '')
            if s in by: by[s].append(x)
        keep=[]
        for p in self.state.get('pending') or []:
            ex=_exit(by.get(p['symbol']) or [],float(p['due_ts']))
            if not ex: keep.append(p); continue
            px=float(ex.get('mark_price') or 0)
            if px<=0: keep.append(p); continue
            ret=px/float(p['entry'])-1.0; out={**p,'exit':px,'exit_ts':float(ex.get('ts') or 0)}
            for name in ('continuation','reversal'):
                net=float(p[name+'_side'])*ret-ROUND_TRIP_COST; out[name+'_net_return_pct']=net*100
                eqk=name+'_equity'; pk=name+'_peak'; ddk=name+'_max_dd_pct'
                eq=float(self.state.get(eqk) or 100)*(1+ALLOC*net); peak=max(float(self.state.get(pk) or 100),eq)
                self.state[eqk]=eq; self.state[pk]=peak; self.state[ddk]=min(float(self.state.get(ddk) or 0),(eq/peak-1)*100)
                out[name+'_paper_equity']=eq
            self.state.setdefault('resolved',[]).append(out); self.state['resolved']=self.state['resolved'][-1000:]
        self.state['pending']=keep
    def _refresh_sync(self):
        rows=_rows(); self.state['last_new_events']=self._detect(rows); self._resolve(rows); self._save(); return rows
    async def refresh(self):
        try: rows=await asyncio.to_thread(self._refresh_sync); self.last_error=None
        except Exception as exc: rows=[]; self.last_error=str(exc)[:500]
        self.last_refresh=time.time(); return self.status(rows)
    def status(self,rows=None):
        rows=_rows() if rows is None else rows; r=self.state.get('resolved') or []
        start=float(self.state.get('rule_frozen_at') or time.time()); end=max([float(x.get('ts') or 0) for x in rows],default=start)
        hours=max(0.0,(end-start)/3600); syms=sorted({x.get('symbol') for x in r if x.get('symbol')})
        out={'ok':self.last_error is None,'strategy':'BYBIT_PRICE_SHOCK_V1','mode':'FUTURE_ONLY_SHADOW','venue':'Bybit','live_enabled':False,'paper_only':True,
             'rule_frozen_at':self.state.get('rule_frozen_at'),'locked_rule':{'lookback_minutes':15,'min_abs_move_pct':MIN_ABS_MOVE_PCT,'hold_hours':1,
             'cooldown_hours':2,'round_trip_cost':ROUND_TRIP_COST,'allocation_fraction':ALLOC},'pending_count':len(self.state.get('pending') or []),'resolved_count':len(r),
             'resolved_symbols':syms,'future_gate':{'collection_hours':hours,'required_hours':MIN_HOURS,'resolved_events':len(r),'required_resolved_events':MIN_RESOLVED,
             'resolved_symbols':len(syms),'required_symbols':MIN_SYMBOLS,'sample_ready':bool(hours>=MIN_HOURS and len(r)>=MIN_RESOLVED and len(syms)>=MIN_SYMBOLS)},
             'promotion_eligible':False,'last_new_events':self.state.get('last_new_events',0),'last_error':self.last_error,
             'policy':{'no_grid':True,'no_martingale':True,'no_dca':True,'no_parameter_tuning':True,'live_requires_separate_review':True}}
        for name in ('continuation','reversal'):
            wins=sum(1 for x in r if float(x.get(name+'_net_return_pct') or 0)>0)
            st={f'{int(m)}x':_stress(r,name,m) for m in STRESS}
            out[name]={'return_pct':float(self.state.get(name+'_equity',100))-100,'equity':self.state.get(name+'_equity',100),
                       'max_dd_pct':self.state.get(name+'_max_dd_pct',0),'wins':wins,'win_rate':wins/len(r) if r else None,'cost_stress_return_pct':st}
        return out
    async def start(self):
        if self.task and not self.task.done(): return
        await self.refresh(); self.task=asyncio.create_task(self._loop(),name='bybit-price-shock-v1')
    async def stop(self):
        if self.task and not self.task.done(): self.task.cancel(); await asyncio.gather(self.task,return_exceptions=True)
        self.task=None
    async def _loop(self):
        while True:
            await asyncio.sleep(60)
            if self.enabled: await self.refresh()

bybit_price_shock_shadow_v1=BybitPriceShockShadowV1()
