from __future__ import annotations
import asyncio,json,time,statistics
from pathlib import Path

ROOT=Path(__file__).parents[2];DATA=ROOT/'data';RAW=DATA/'liquidity_migration_perp_snapshots_v1.jsonl';STATE=DATA/'liquidity_migration_perp_shadow_v1.json'
RULE_FROZEN_AT=1790614800.0
BASELINE_SAMPLES=20;MIN_BASELINE=15;MIN_SHARE_JUMP=.15;MIN_IMBALANCE=.35;MAX_DISPERSION_BPS=3.0;MAX_SPREAD_BPS=2.5
HOLD_SECONDS=900;COOLDOWN_SECONDS=1800;ROUND_TRIP_COST=.0012;ALLOC=.05;MIN_COLLECTION_HOURS=48.;MIN_RESOLVED=20
STRESS_COST_MULTS=(2.0,3.0)
from app.services.liquidity_migration_perp_collector import SYMS

def _lines():
    if not RAW.exists():return []
    out=[]
    for line in RAW.read_text(encoding='utf-8').splitlines():
        try:
            x=json.loads(line)
            if float(x.get('ts') or 0)>=RULE_FROZEN_AT:out.append(x)
        except Exception:pass
    return sorted(out,key=lambda x:float(x.get('ts') or 0))

def _blank():
    return {'version':'LIQUIDITY_MIGRATION_PERP_SHADOW_V1','rule_frozen_at':RULE_FROZEN_AT,'last_processed_ts':0.0,
            'last_signal_ts':{},'pending':[],'resolved':[],'equity':100.0,'peak':100.0,'max_dd_pct':0.0}

def _stress_return_pct(rows,cost_mult):
    eq=100.0
    for x in rows:
        gross=float(x.get('gross_return_pct') or 0)/100.0
        funding=float(x.get('funding_cost_pct') or 0)/100.0
        cost=float(x.get('round_trip_cost') or ROUND_TRIP_COST)*float(cost_mult)
        alloc=float(x.get('allocation_fraction') or ALLOC)
        eq*=1.0+alloc*(gross-cost-funding)
    return eq-100.0

class LiquidityMigrationPerpShadow:
    def __init__(self):
        self.enabled=True;self.live_enabled=False;self.task=None;self.last_error=None;self.last_refresh=None
        try:self.state=json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:self.state=_blank()
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    @staticmethod
    def _sym(row,sym):return (row.get('symbols') or {}).get(sym) or {}
    @staticmethod
    def _binance_mid(row,sym):
        return float((((row.get('symbols') or {}).get(sym) or {}).get('venues') or {}).get('Binance',{}).get('mid') or 0)
    def _detect_one(self,rows,i,sym):
        row=rows[i];now=self._sym(row,sym);venues=now.get('venues') or {}
        if len(venues)<3 or float(now.get('dispersion_bps') or 999)>MAX_DISPERSION_BPS:return None
        hist=rows[max(0,i-BASELINE_SAMPLES):i]
        deltas={}
        for venue,v in venues.items():
            vals=[float(((self._sym(h,sym).get('venues') or {}).get(venue) or {}).get('depth_share')) for h in hist
                  if ((self._sym(h,sym).get('venues') or {}).get(venue) or {}).get('depth_share') is not None]
            if len(vals)>=MIN_BASELINE:deltas[venue]=float(v.get('depth_share') or 0)-statistics.median(vals)
        if not deltas:return None
        venue=max(deltas,key=deltas.get);v=venues[venue];jump=deltas[venue];imb=float(v.get('imbalance') or 0);spread=float(v.get('spread_bps') or 999)
        if jump<MIN_SHARE_JUMP or abs(imb)<MIN_IMBALANCE or spread>MAX_SPREAD_BPS:return None
        ts=float(row.get('ts') or 0);last=float((self.state.get('last_signal_ts') or {}).get(sym) or 0)
        if ts-last<COOLDOWN_SECONDS:return None
        entry=self._binance_mid(row,sym)
        if entry<=0:return None
        bn=(venues.get('Binance') or {});return {'id':f'{sym}:{int(ts)}','symbol':sym,'ts':ts,'due_ts':ts+HOLD_SECONDS,
            'leader_venue':venue,'depth_share_jump':jump,'leader_imbalance':imb,'dispersion_bps':float(now.get('dispersion_bps') or 0),
            'side':1.0 if imb>0 else -1.0,'entry':entry,'funding_rate':float(bn.get('funding_rate') or 0),
            'next_funding_time':int(bn.get('next_funding_time') or 0),'allocation_fraction':ALLOC,'round_trip_cost':ROUND_TRIP_COST}
    def _resolve(self,rows):
        keep=[]
        for p in self.state.get('pending') or []:
            exits=[r for r in rows if float(r.get('ts') or 0)>=float(p['due_ts'])]
            if not exits:keep.append(p);continue
            ex=exits[0];exit_px=self._binance_mid(ex,p['symbol'])
            if exit_px<=0:keep.append(p);continue
            gross=float(p['side'])*(exit_px/float(p['entry'])-1.0)
            crossed=int(p.get('next_funding_time') or 0)>0 and float(p['ts'])*1000<int(p['next_funding_time'])<=float(ex.get('ts') or 0)*1000
            funding=float(p['side'])*float(p.get('funding_rate') or 0) if crossed else 0.0
            net=gross-ROUND_TRIP_COST-funding;eq=float(self.state.get('equity') or 100)*(1+ALLOC*net);peak=max(float(self.state.get('peak') or 100),eq)
            self.state['equity']=eq;self.state['peak']=peak;self.state['max_dd_pct']=min(float(self.state.get('max_dd_pct') or 0),(eq/peak-1)*100)
            self.state.setdefault('resolved',[]).append({**p,'exit':exit_px,'exit_ts':float(ex.get('ts') or 0),'gross_return_pct':gross*100,
                'funding_cost_pct':funding*100,'net_return_pct':net*100,'paper_equity':eq})
            self.state['resolved']=self.state['resolved'][-1000:]
        self.state['pending']=keep

    def _refresh_sync(self):
        rows=_lines();last=float(self.state.get('last_processed_ts') or 0)
        for i,row in enumerate(rows):
            ts=float(row.get('ts') or 0)
            if ts<=last:continue
            for sym in SYMS:
                p=self._detect_one(rows,i,sym)
                if p:
                    self.state.setdefault('pending',[]).append(p);self.state.setdefault('last_signal_ts',{})[sym]=ts
            self.state['last_processed_ts']=ts
        self._resolve(rows);self._save();return rows
    async def refresh(self):
        try:rows=await asyncio.to_thread(self._refresh_sync);self.last_error=None
        except Exception as exc:rows=[];self.last_error=str(exc)[:500]
        self.last_refresh=time.time();return self.status(rows)
    def status(self,rows=None):
        rows=_lines() if rows is None else rows;r=self.state.get('resolved') or [];wins=sum(1 for x in r if float(x.get('net_return_pct') or 0)>0)
        hours=((float(rows[-1]['ts'])-float(rows[0]['ts']))/3600) if len(rows)>1 else 0.0;per={s:sum(1 for x in r if x.get('symbol')==s) for s in SYMS}
        stress={f'{int(m)}x':_stress_return_pct(r,m) for m in STRESS_COST_MULTS}
        breadth_ready=all(per[s]>=3 for s in SYMS)
        sample_ready=bool(hours>=MIN_COLLECTION_HOURS and len(r)>=MIN_RESOLVED and breadth_ready)
        stress_ready=bool(stress['2x']>0 and stress['3x']>0)
        evidence=bool(sample_ready and stress_ready)
        gate={'required_collection_hours':MIN_COLLECTION_HOURS,'collection_hours':hours,'required_resolved':MIN_RESOLVED,
              'resolved':len(r),'required_per_symbol':3,'resolved_by_symbol':per,'breadth_ready':breadth_ready,
              'stress_2x_return_pct':stress['2x'],'stress_3x_return_pct':stress['3x'],'stress_ready':stress_ready,
              'sample_ready':sample_ready,'ready':evidence}
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'FUTURE_ONLY_SHADOW','strategy':'VENUE_FRAGMENTATION_LIQUIDITY_SHIFT_V1',
            'future_only':True,
            'market':'BINANCE_USDT_PERPETUAL_EXECUTION_WITH_CROSSVENUE_SIGNAL','live_enabled':False,'paper_only':True,'rule_frozen_at':RULE_FROZEN_AT,
            'locked_config':{'baseline_samples':BASELINE_SAMPLES,'min_share_jump':MIN_SHARE_JUMP,'min_abs_imbalance':MIN_IMBALANCE,'max_dispersion_bps':MAX_DISPERSION_BPS,'max_spread_bps':MAX_SPREAD_BPS,'hold_seconds':HOLD_SECONDS,'cooldown_seconds':COOLDOWN_SECONDS,'round_trip_cost':ROUND_TRIP_COST,'allocation_fraction':ALLOC},
            'locked_rule':{'baseline_samples':BASELINE_SAMPLES,'min_share_jump':MIN_SHARE_JUMP,'min_abs_imbalance':MIN_IMBALANCE,
                'max_dispersion_bps':MAX_DISPERSION_BPS,'max_spread_bps':MAX_SPREAD_BPS,'hold_seconds':HOLD_SECONDS,
                'cooldown_seconds':COOLDOWN_SECONDS,'round_trip_cost':ROUND_TRIP_COST,'allocation_fraction':ALLOC,'funding_charged_if_crossed':True},
            'collection_hours':hours,'snapshot_count':len(rows),'pending_count':len(self.state.get('pending') or []),'resolved_count':len(r),
            'latest':{'bar_ts':(float(rows[-1].get('ts') or 0) if rows else None),'source':'EXACT_CROSSVENUE_PERP_SNAPSHOTS'},
            'resolved_by_symbol':per,'wins':wins,'win_rate':(wins/len(r) if r else None),'equity':float(self.state.get('equity') or 100),
            'return_pct':float(self.state.get('equity') or 100)-100,'max_dd_pct':float(self.state.get('max_dd_pct') or 0),
            'cost_stress_return_pct':stress,'future_gate':gate,'evidence_gate_ready':evidence,'promotion_eligible':False,
            'futures_evaluated':True,'futures_promotion_eligible':True,
            'recent_resolved':r[-12:],'last_refresh':self.last_refresh,'last_error':self.last_error,
            'policy':{'no_grid':True,'no_martingale':True,'no_dca':True,'no_backfill_selection':True,'live_requires_separate_review':True}}
    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name='liquidity-migration-perp-shadow')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None
    async def _loop(self):
        await asyncio.sleep(40)
        while True:
            if self.enabled:await self.refresh()
            await asyncio.sleep(30)

liquidity_migration_perp_shadow=LiquidityMigrationPerpShadow()
