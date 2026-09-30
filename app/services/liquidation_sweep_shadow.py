from __future__ import annotations
import asyncio,json,time
from pathlib import Path

ROOT=Path(__file__).parents[2];DATA=ROOT/'data'
RAW=DATA/'liquidation_force_orders_v1.jsonl';SNAPS=DATA/'liquidation_context_snapshots_v1.jsonl'
STATE=DATA/'liquidation_sweep_shadow_v1.json'
RULE_FROZEN_AT=1790613000.0
MIN_COLLECTION_HOURS=48.0;MIN_RAW_EVENTS=100
SWEEP_WINDOW_SEC=60;MIN_SWEEP_NOTIONAL=250000.0;MIN_SWEEP_EVENTS=2
MIN_OI_DROP_PCT=.20;MIN_MARK_INDEX_ABS_PCT=.02;HOLD_HOURS=6;ROUND_TRIP_COST=.0012;ALLOC=.05

def _lines(path):
    if not path.exists():return []
    out=[]
    for line in path.read_text(encoding='utf-8').splitlines():
        try:out.append(json.loads(line))
        except Exception:pass
    return out

def _blank():
    return {'version':'LIQUIDATION_SWEEP_SHADOW_V1','rule_frozen_at':RULE_FROZEN_AT,'pending':[],'resolved':[],
            'seen_sweeps':[],'equity':100.0,'peak':100.0,'max_dd_pct':0.0,'last_processed_ms':0}

class LiquidationSweepShadow:
    def __init__(self):
        self.enabled=True;self.live_enabled=False;self.task=None;self.last_error=None;self.last_refresh=None
        try:self.state=json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:self.state=_blank()
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    @staticmethod
    def _collection_gate(events):
        if not events:return False,0.0
        ts=[float(x.get('event_time') or 0)/1000 for x in events if x.get('event_time')]
        hours=((max(ts)-min(ts))/3600) if len(ts)>1 else 0.0
        return bool(len(events)>=MIN_RAW_EVENTS and hours>=MIN_COLLECTION_HOURS),hours
    @staticmethod
    def _ctx(snaps,sym,ts):
        z=[x for x in snaps if x.get('symbol')==sym and float(x.get('ts') or 0)<=ts]
        if not z:return None,None
        now=z[-1];old=[x for x in z if float(x.get('ts') or 0)<=ts-900]
        return now,(old[-1] if old else None)
    @staticmethod
    def _sweep_key(sym,bucket,side):return f'{sym}:{bucket}:{side}'
    def _detect(self,events,snaps):
        ready,hours=self._collection_gate(events)
        groups={}
        for e in events:
            ts=float(e.get('event_time') or 0)/1000
            if ts<RULE_FROZEN_AT:continue
            sym=str(e.get('symbol') or '');side=str(e.get('side') or '');bucket=int(ts//SWEEP_WINDOW_SEC)
            if not sym or side not in {'BUY','SELL'}:continue
            g=groups.setdefault((sym,bucket,side),[]);g.append(e)
        seen=set(self.state.get('seen_sweeps') or []);new=0
        for (sym,bucket,side),rows in sorted(groups.items(),key=lambda x:x[0][1]):
            key=self._sweep_key(sym,bucket,side)
            if key in seen:continue
            notional=sum(float(x.get('notional_usdt') or 0) for x in rows);ts=(bucket+1)*SWEEP_WINDOW_SEC
            if len(rows)<MIN_SWEEP_EVENTS or notional<MIN_SWEEP_NOTIONAL:continue
            now,old=self._ctx(snaps,sym,ts)
            if not now or not old:continue
            oi0=float(old.get('open_interest') or 0);oi1=float(now.get('open_interest') or 0)
            oi_drop=((oi1/oi0-1)*100) if oi0 else 0.0;mi=float(now.get('mark_index_pct') or 0)
            if oi_drop>-MIN_OI_DROP_PCT or abs(mi)<MIN_MARK_INDEX_ABS_PCT:continue
            entry=float(now.get('mark_price') or 0)
            if entry<=0:continue
            side_mult=1.0 if side=='SELL' else -1.0
            self.state['pending'].append({'id':key,'symbol':sym,'sweep_ts':ts,'liquidation_side':side,'side':side_mult,
                'notional_usdt':notional,'events':len(rows),'oi_drop_pct_15m':oi_drop,'mark_index_pct':mi,
                'entry':entry,'due_ts':ts+HOLD_HOURS*3600,'cost':ROUND_TRIP_COST,'allocation_fraction':ALLOC})
            seen.add(key);new+=1
        self.state['seen_sweeps']=list(seen)[-2000:]
        return {'ready':ready,'collection_hours':hours,'raw_events':len(events),'new_sweeps':new}
    def _resolve(self,snaps):
        keep=[]
        for p in self.state.get('pending') or []:
            rows=[x for x in snaps if x.get('symbol')==p['symbol'] and float(x.get('ts') or 0)>=float(p['due_ts'])]
            if not rows:keep.append(p);continue
            exit_px=float(rows[0].get('mark_price') or 0)
            if exit_px<=0:keep.append(p);continue
            gross=float(p['side'])*(exit_px/float(p['entry'])-1);net=gross-ROUND_TRIP_COST
            eq=float(self.state.get('equity') or 100)*(1+ALLOC*net);peak=max(float(self.state.get('peak') or 100),eq)
            self.state['equity']=eq;self.state['peak']=peak;self.state['max_dd_pct']=min(float(self.state.get('max_dd_pct') or 0),(eq/peak-1)*100)
            self.state.setdefault('resolved',[]).append({**p,'exit':exit_px,'gross_return_pct':gross*100,
                'net_return_pct':net*100,'resolved_ts':float(rows[0].get('ts') or time.time()),'paper_equity':eq})
            self.state['resolved']=self.state['resolved'][-500:]
        self.state['pending']=keep
    def _refresh_sync(self):
        events=_lines(RAW);snaps=_lines(SNAPS);ctx_syms={str(x.get('symbol') or '') for x in snaps};eval_events=[e for e in events if str(e.get('symbol') or '') in ctx_syms];gate=self._detect(eval_events,snaps);gate['raw_events_total']=len(events);gate['evaluable_events']=len(eval_events);self._resolve(snaps);self.state['gate']=gate;self._save();return self.state
    async def refresh(self):
        try:self.state=await asyncio.to_thread(self._refresh_sync);self.last_error=None
        except Exception as exc:self.last_error=str(exc)[:500]
        self.last_refresh=time.time();return self.status()
    def status(self):
        g=self.state.get('gate') or {};r=self.state.get('resolved') or [];wins=sum(1 for x in r if float(x.get('net_return_pct') or 0)>0)
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'FUTURE_ONLY_SHADOW','strategy':'liquidation_sweep_recovery',
                'live_enabled':False,'paper_only':True,'rule_frozen_at':RULE_FROZEN_AT,
                'locked_rule':{'window_sec':SWEEP_WINDOW_SEC,'min_sweep_notional':MIN_SWEEP_NOTIONAL,'min_events':MIN_SWEEP_EVENTS,
                               'min_oi_drop_pct_15m':MIN_OI_DROP_PCT,'min_mark_index_abs_pct':MIN_MARK_INDEX_ABS_PCT,
                               'hold_hours':HOLD_HOURS,'round_trip_cost':ROUND_TRIP_COST,'allocation_fraction':ALLOC},
                'collection_gate':g,'pending_count':len(self.state.get('pending') or []),'resolved_count':len(r),
                'wins':wins,'win_rate':(wins/len(r) if r else None),'equity':float(self.state.get('equity') or 100),
                'return_pct':float(self.state.get('equity') or 100)-100,'max_dd_pct':float(self.state.get('max_dd_pct') or 0),
                'promotion_eligible':False,'recent_resolved':r[-12:],'last_refresh':self.last_refresh,'last_error':self.last_error,
                'policy':{'no_grid':True,'no_martingale':True,'no_dca':True,'no_holdout_tuning':True,'live_requires_new_gate':True}}
    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name='liquidation-sweep-shadow')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None
    async def _loop(self):
        await asyncio.sleep(20)
        while True:
            if self.enabled:await self.refresh()
            await asyncio.sleep(60)

liquidation_sweep_shadow=LiquidationSweepShadow()
