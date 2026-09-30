from __future__ import annotations
import asyncio,json,time,statistics
from pathlib import Path

ROOT=Path(__file__).parents[2];DATA=ROOT/'data'
RAW=DATA/'futures_curve_snapshots_v1.jsonl';STATE=DATA/'futures_curve_shadow_v1.json'
ASSETS=['BTC','ETH'];BASELINE=288;Z_THR=2.5;HOLD_SEC=21600;COOLDOWN_SEC=43200
ROUND_TRIP_COST=.0020;ALLOC=.05;MIN_HOURS=72.;MIN_RESOLVED=20;MIN_PER_ASSET=5

def _rows():
    if not RAW.exists():return []
    out=[]
    for line in RAW.read_text(encoding='utf-8').splitlines()[-5000:]:
        try:out.append(json.loads(line))
        except Exception:pass
    return sorted(out,key=lambda x:float(x.get('ts') or 0))
def _blank():
    now=time.time();return {'version':'FUTURES_CURVE_SHADOW_V1','rule_frozen_at':now,'last_processed_ts':now,
        'last_signal_ts':{},'pending':[],'resolved':[],'equity':100.0,'peak':100.0,'max_dd_pct':0.0}
def _point(row,asset):
    a=(row.get('assets') or {}).get(asset) or {};c=a.get('CURRENT_QUARTER') or {};n=a.get('NEXT_QUARTER') or {}
    try:return {'ts':float(row.get('ts') or 0),'cur_diff':float(c['crossvenue_ann_basis_diff_pct']),'next_diff':float(n['crossvenue_ann_basis_diff_pct']),
        'bmark':float(c['binance']['mark']),'omark':float(c['okx']['mark'])}
    except Exception:return None
class FuturesCurveShadow:
    def __init__(self):
        self.enabled=True;self.live_enabled=False;self.task=None;self.last_error=None;self.last_refresh=None
        try:self.state=json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:self.state=_blank()
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    def _detect(self,rows):
        last=float(self.state.get('last_processed_ts') or self.state.get('rule_frozen_at') or 0);freeze=float(self.state.get('rule_frozen_at') or 0);max_ts=last;new=0
        by={a:[] for a in ASSETS}
        for r in rows:
            for a in ASSETS:
                p=_point(r,a)
                if p and p['ts']>=freeze:by[a].append(p)
        for asset,pts in by.items():
            for i,p in enumerate(pts):
                ts=p['ts'];max_ts=max(max_ts,ts)
                if ts<=last or i<BASELINE:continue
                hist=[x['cur_diff'] for x in pts[max(0,i-BASELINE):i]]
                if len(hist)<BASELINE:continue
                med=statistics.median(hist);mad=statistics.median([abs(x-med) for x in hist]);scale=1.4826*mad
                if scale<=1e-9:continue
                z=(p['cur_diff']-med)/scale
                if abs(z)<Z_THR or p['cur_diff']*p['next_diff']<=0:continue
                prev=float((self.state.get('last_signal_ts') or {}).get(asset) or 0)
                if ts-prev<COOLDOWN_SEC:continue
                sign=1.0 if p['cur_diff']>0 else -1.0
                self.state.setdefault('pending',[]).append({'id':f'{asset}:{int(ts)}','asset':asset,'ts':ts,'due_ts':ts+HOLD_SEC,
                    'z':z,'current_ann_diff_pct':p['cur_diff'],'next_ann_diff_pct':p['next_diff'],'binance_entry':p['bmark'],'okx_entry':p['omark'],
                    'binance_side':-sign,'okx_side':sign,'allocation_fraction':ALLOC,'round_trip_cost':ROUND_TRIP_COST})
                self.state.setdefault('last_signal_ts',{})[asset]=ts;new+=1
        self.state['last_processed_ts']=max_ts;return new
    @staticmethod
    def _exit_point(rows,asset,due):
        for r in rows:
            if float(r.get('ts') or 0)<due:continue
            p=_point(r,asset)
            if p:return p
        return None
    def _resolve(self,rows):
        keep=[]
        for p in self.state.get('pending') or []:
            ex=self._exit_point(rows,p['asset'],float(p['due_ts']))
            if not ex:keep.append(p);continue
            bret=ex['bmark']/float(p['binance_entry'])-1.0;oret=ex['omark']/float(p['okx_entry'])-1.0
            gross=.5*float(p['binance_side'])*bret+.5*float(p['okx_side'])*oret;net=gross-ROUND_TRIP_COST
            eq=float(self.state.get('equity') or 100)*(1+ALLOC*net);peak=max(float(self.state.get('peak') or 100),eq)
            self.state['equity']=eq;self.state['peak']=peak;self.state['max_dd_pct']=min(float(self.state.get('max_dd_pct') or 0),(eq/peak-1)*100)
            self.state.setdefault('resolved',[]).append({**p,'binance_exit':ex['bmark'],'okx_exit':ex['omark'],'exit_ts':ex['ts'],
                'gross_return_pct':gross*100,'net_return_pct':net*100,'paper_equity':eq})
            self.state['resolved']=self.state['resolved'][-500:]
        self.state['pending']=keep
    def _refresh_sync(self):
        rows=_rows();new=self._detect(rows);self._resolve(rows);self.state['last_new_events']=new;self._save();return rows
    async def refresh(self):
        try:rows=await asyncio.to_thread(self._refresh_sync);self.last_error=None
        except Exception as exc:rows=[];self.last_error=str(exc)[:500]
        self.last_refresh=time.time();return self.status(rows)
    def status(self,rows=None):
        rows=_rows() if rows is None else rows;r=self.state.get('resolved') or []
        start=float(self.state.get('rule_frozen_at') or time.time());end=max([float(x.get('ts') or 0) for x in rows],default=start)
        hours=max(0.0,(end-start)/3600);per={a:sum(1 for x in r if x.get('asset')==a) for a in ASSETS};wins=sum(1 for x in r if float(x.get('net_return_pct') or 0)>0)
        gate={'collection_hours':hours,'required_hours':MIN_HOURS,'resolved_events':len(r),'required_resolved_events':MIN_RESOLVED,
              'resolved_by_asset':per,'required_per_asset':MIN_PER_ASSET,
              'ready_for_review':bool(hours>=MIN_HOURS and len(r)>=MIN_RESOLVED and all(per[a]>=MIN_PER_ASSET for a in ASSETS))}
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'FUTURE_ONLY_SHADOW','strategy':'DATED_FUTURES_CURVE_DISLOCATION_V1',
            'live_enabled':False,'paper_only':True,'rule_frozen_at':self.state.get('rule_frozen_at'),
            'locked_rule':{'baseline_snapshots':BASELINE,'robust_z_threshold':Z_THR,'same_sign_current_next_required':True,
                           'hold_hours':HOLD_SEC/3600,'cooldown_hours':COOLDOWN_SEC/3600,'round_trip_cost':ROUND_TRIP_COST,'allocation_fraction':ALLOC},
            'pending_count':len(self.state.get('pending') or []),'resolved_count':len(r),'wins':wins,'win_rate':wins/len(r) if r else None,
            'equity':float(self.state.get('equity') or 100),'return_pct':float(self.state.get('equity') or 100)-100,'max_dd_pct':float(self.state.get('max_dd_pct') or 0),
            'future_gate':gate,'promotion_eligible':False,'recent_resolved':r[-12:],'last_refresh':self.last_refresh,'last_error':self.last_error,
            'policy':{'same_expiry_normalized_basis':True,'no_historical_selection':True,'no_live_orders':True,'execution_contract_spec_review_required':True}}
    async def start(self):
        if self.task and not self.task.done():return
        await self.refresh();self.task=asyncio.create_task(self._loop(),name='futures-curve-shadow')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None
    async def _loop(self):
        while True:
            await asyncio.sleep(300)
            if self.enabled:await self.refresh()

futures_curve_shadow=FuturesCurveShadow()
