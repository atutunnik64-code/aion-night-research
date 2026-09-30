from __future__ import annotations
import json,time
from collections import defaultdict
from app.services.execution_engine import execution_engine
from app.services.live_executor import live_executor

class BatchSeriesLedger:
    def __init__(self):
        self.max_gap_seconds=120.0
        self.last_refresh=None; self.last_error=None; self.latest={}

    @staticmethod
    def _parse(r):
        try:d=json.loads(r['payload'] or '{}')
        except Exception:return None
        row=d.get('row') or {}; plan=row.get('batch_ready_plan') or {}
        sig=str(row.get('signature') or r['signature'] or '')
        venues=set(row.get('venues') or [])
        for z in row.get('steps') or []:
            if z.get('venue'):venues.add(str(z['venue']))
        need=max(1,int(plan.get('cycles') or 1))
        if not sig or len(venues)<2:return None
        return {'id':int(r['id']),'ts':float(r['ts']),'signature':sig,
                'venues':tuple(sorted(venues)),'need':need,
                'pnl':float(r['realized_profit'] or 0)}
    def refresh(self,start_id=0):
        try:
            q="SELECT id,ts,signature,realized_profit,payload FROM executions WHERE id>? AND environment='DEMO' AND strategy='BATCH_READY' AND status='FILLED_BOTH' ORDER BY ts,id"
            rows=execution_engine.db.execute(q,(int(start_id),)).fetchall()
            items=[x for x in (self._parse(r) for r in rows) if x]
            groups=defaultdict(list)
            for x in items:groups[x['signature']].append(x)
            live=live_executor.status(); quarantine=set(live.get('live_quarantined_venues') or [])
            allowed=set(live.get('verified_ready') or [])-quarantine
            completed=[]; unfinished=[]
            for sig,seq in groups.items():
                acc=[]; need=None; last_ts=None
                for x in seq:
                    if last_ts is None or x['ts']-last_ts<=self.max_gap_seconds:
                        if not acc:need=x['need']
                        acc.append(x)
                    else:
                        unfinished.extend(acc);acc=[x];need=x['need']
                    last_ts=x['ts']
                    while need and len(acc)>=need:
                        chunk=acc[:need];acc=acc[need:]
                        pnl=sum(z['pnl'] for z in chunk);venues=set(chunk[0]['venues'])
                        completed.append({'signature':sig,'cycles':need,'pnl':pnl,'venues':sorted(venues),
                                          'deployable':bool(venues.issubset(allowed)),'first_id':chunk[0]['id'],'last_id':chunk[-1]['id']})
                        need=acc[0]['need'] if acc else None
                unfinished.extend(acc)
            research=sum(x['pnl'] for x in completed); deploy=sum(x['pnl'] for x in completed if x['deployable'])
            self.latest={'mode':'COMPLETED_BATCH_SERIES','max_gap_seconds':self.max_gap_seconds,
                         'completed_series_count':len(completed),'completed_cycle_count':sum(x['cycles'] for x in completed),
                         'completed_series_pnl':round(research,8),'deployable_series_count':sum(1 for x in completed if x['deployable']),
                         'deployable_series_pnl':round(deploy,8),'unfinished_cycle_count':len(unfinished),
                         'raw_batch_fill_count':len(items),'allowed_venues':sorted(allowed),'quarantined_venues':sorted(quarantine),
                         'recent_completed':completed[-20:]}
            self.last_error=None;self.last_refresh=time.time()
        except Exception as exc:
            self.last_error=str(exc)[:400];self.last_refresh=time.time()
        return self.status()

    def status(self):
        return {'ok':self.last_error is None,'last_refresh':self.last_refresh,'last_error':self.last_error,'latest':self.latest}

batch_series_ledger=BatchSeriesLedger()
