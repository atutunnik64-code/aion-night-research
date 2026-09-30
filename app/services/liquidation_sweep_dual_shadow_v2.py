from __future__ import annotations
import asyncio,json,time
from pathlib import Path

ROOT=Path(__file__).parents[2]; DATA=ROOT/'data'
RAW=DATA/'liquidation_force_orders_v1.jsonl'; SNAPS=DATA/'liquidation_context_snapshots_v1.jsonl'
STATE=DATA/'liquidation_sweep_dual_shadow_v2.json'
RULE_FROZEN_AT=1790672779.89689
MIN_COLLECTION_HOURS=48.0; MIN_RESOLVED=20
SWEEP_WINDOW_SEC=60; MIN_SWEEP_NOTIONAL=250000.0; MIN_SWEEP_EVENTS=2
MIN_OI_DROP_PCT=.20; MIN_MARK_INDEX_ABS_PCT=.02
HOLD_HOURS=6; ROUND_TRIP_COST=.0012; ALLOC=.05

def _lines(path):
    if not path.exists(): return []
    out=[]
    for line in path.read_text(encoding='utf-8').splitlines():
        try: out.append(json.loads(line))
        except Exception: pass
    return out

def _blank():
    return {'version':'LIQUIDATION_SWEEP_DUAL_SHADOW_V2','rule_frozen_at':RULE_FROZEN_AT,
            'pending':[],'resolved':[],'seen_sweeps':[],
            'recovery_equity':100.0,'recovery_peak':100.0,'recovery_max_dd_pct':0.0,
            'continuation_equity':100.0,'continuation_peak':100.0,'continuation_max_dd_pct':0.0}

class LiquidationSweepDualShadowV2:
    def __init__(self):
        self.enabled=True; self.live_enabled=False; self.task=None; self.last_error=None; self.last_refresh=None
        try: self.state=json.loads(STATE.read_text(encoding='utf-8'))
        except Exception: self.state=_blank()
    def _save(self): STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    @staticmethod
    def _ctx(snaps,sym,ts):
        z=[x for x in snaps if x.get('symbol')==sym and float(x.get('ts') or 0)<=ts]
        if not z: return None,None
        now=z[-1]; old=[x for x in z if float(x.get('ts') or 0)<=ts-900]
        return now,(old[-1] if old else None)
    def _detect(self,events,snaps):
        groups={}
        for e in events:
            ts=float(e.get('event_time') or 0)/1000
            if ts<RULE_FROZEN_AT: continue
            sym=str(e.get('symbol') or ''); side=str(e.get('side') or '')
            if not sym or side not in {'BUY','SELL'}: continue
            groups.setdefault((sym,int(ts//SWEEP_WINDOW_SEC),side),[]).append(e)
        seen=set(self.state.get('seen_sweeps') or []); new=0
        for (sym,bucket,side),rows in sorted(groups.items(),key=lambda x:x[0][1]):
            key=f'{sym}:{bucket}:{side}'
            if key in seen: continue
            notional=sum(float(x.get('notional_usdt') or 0) for x in rows); ts=(bucket+1)*SWEEP_WINDOW_SEC
            if len(rows)<MIN_SWEEP_EVENTS or notional<MIN_SWEEP_NOTIONAL: continue
            now,old=self._ctx(snaps,sym,ts)
            if not now or not old: continue
            oi0=float(old.get('open_interest') or 0); oi1=float(now.get('open_interest') or 0)
            oi_drop=((oi1/oi0-1)*100) if oi0 else 0.0; mi=float(now.get('mark_index_pct') or 0)
            if oi_drop>-MIN_OI_DROP_PCT or abs(mi)<MIN_MARK_INDEX_ABS_PCT: continue
            entry=float(now.get('mark_price') or 0)
            if entry<=0: continue
            liquidation_mult=1.0 if side=='SELL' else -1.0
            self.state['pending'].append({'id':key,'symbol':sym,'sweep_ts':ts,'liquidation_side':side,
                'recovery_side':liquidation_mult,'continuation_side':-liquidation_mult,
                'notional_usdt':notional,'events':len(rows),'oi_drop_pct_15m':oi_drop,
                'mark_index_pct':mi,'entry':entry,'due_ts':ts+HOLD_HOURS*3600})
            seen.add(key); new+=1
        self.state['seen_sweeps']=list(seen)[-2000:]
        return new
    def _update_curve(self,prefix,net):
        eq=float(self.state.get(prefix+'_equity') or 100)*(1+ALLOC*net)
        peak=max(float(self.state.get(prefix+'_peak') or 100),eq)
        self.state[prefix+'_equity']=eq; self.state[prefix+'_peak']=peak
        self.state[prefix+'_max_dd_pct']=min(float(self.state.get(prefix+'_max_dd_pct') or 0),(eq/peak-1)*100)
        return eq
    def _resolve(self,snaps):
        keep=[]
        for p in self.state.get('pending') or []:
            rows=[x for x in snaps if x.get('symbol')==p['symbol'] and float(x.get('ts') or 0)>=float(p['due_ts'])]
            if not rows: keep.append(p); continue
            exit_px=float(rows[0].get('mark_price') or 0)
            if exit_px<=0: keep.append(p); continue
            move=exit_px/float(p['entry'])-1
            recovery_net=float(p['recovery_side'])*move-ROUND_TRIP_COST
            continuation_net=float(p['continuation_side'])*move-ROUND_TRIP_COST
            req=self._update_curve('recovery',recovery_net); ceq=self._update_curve('continuation',continuation_net)
            self.state.setdefault('resolved',[]).append({**p,'exit':exit_px,
                'recovery_net_return_pct':recovery_net*100,'continuation_net_return_pct':continuation_net*100,
                'resolved_ts':float(rows[0].get('ts') or time.time()),
                'recovery_equity':req,'continuation_equity':ceq})
            self.state['resolved']=self.state['resolved'][-500:]
        self.state['pending']=keep
    def _refresh_sync(self):
        events=_lines(RAW); snaps=_lines(SNAPS)
        ctx_syms={str(x.get('symbol') or '') for x in snaps}
        eval_events=[e for e in events if str(e.get('symbol') or '') in ctx_syms]
        new=self._detect(eval_events,snaps); self._resolve(snaps)
        post=[float(e.get('event_time') or 0)/1000 for e in eval_events if float(e.get('event_time') or 0)/1000>=RULE_FROZEN_AT]
        hours=((max(post)-RULE_FROZEN_AT)/3600) if post else 0.0
        self.state['gate']={'ready':hours>=MIN_COLLECTION_HOURS and len(self.state.get('resolved') or [])>=MIN_RESOLVED,
                            'collection_hours':hours,'min_hours':MIN_COLLECTION_HOURS,
                            'resolved':len(self.state.get('resolved') or []),'min_resolved':MIN_RESOLVED,
                            'new_sweeps':new,'evaluable_events':len(eval_events)}
        self._save(); return self.state
    async def refresh(self):
        try: self.state=await asyncio.to_thread(self._refresh_sync); self.last_error=None
        except Exception as exc: self.last_error=str(exc)[:500]
        self.last_refresh=time.time(); return self.status()
    def status(self):
        r=self.state.get('resolved') or []; g=self.state.get('gate') or {}
        rw=sum(1 for x in r if float(x.get('recovery_net_return_pct') or 0)>0)
        cw=sum(1 for x in r if float(x.get('continuation_net_return_pct') or 0)>0)
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'FUTURE_ONLY_SHADOW',
                'strategy':'liquidation_sweep_dual_v2','live_enabled':False,'paper_only':True,
                'rule_frozen_at':RULE_FROZEN_AT,'gate':g,'pending_count':len(self.state.get('pending') or []),
                'resolved_count':len(r),'recovery_return_pct':float(self.state.get('recovery_equity') or 100)-100,
                'continuation_return_pct':float(self.state.get('continuation_equity') or 100)-100,
                'recovery_max_dd_pct':float(self.state.get('recovery_max_dd_pct') or 0),
                'continuation_max_dd_pct':float(self.state.get('continuation_max_dd_pct') or 0),
                'recovery_win_rate':(rw/len(r) if r else None),'continuation_win_rate':(cw/len(r) if r else None),
                'promotion_eligible':bool(g.get('ready')),'recent_resolved':r[-12:],
                'last_refresh':self.last_refresh,'last_error':self.last_error,
                'policy':{'no_grid':True,'no_martingale':True,'no_dca':True,'no_direction_flip_after_results':True}}
    async def start(self):
        if self.task and not self.task.done(): return
        self.task=asyncio.create_task(self._loop(),name='liquidation-sweep-dual-shadow-v2')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try: await self.task
            except BaseException: pass
        self.task=None
    async def _loop(self):
        await asyncio.sleep(25)
        while True:
            if self.enabled: await self.refresh()
            await asyncio.sleep(60)

liquidation_sweep_dual_shadow_v2=LiquidationSweepDualShadowV2()
