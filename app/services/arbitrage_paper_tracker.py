from __future__ import annotations
import json, time
from pathlib import Path
from app.services.execution_engine import execution_engine
from app.services.live_executor import live_executor
from app.services.natural_rebalance_ledger import natural_rebalance_ledger
from app.services.batch_series_ledger import batch_series_ledger

ROOT=Path(__file__).parents[2]
STATE=ROOT/'data'/'arbitrage_paper_tracker.json'
STRATEGIES=('FULL_NOW','BATCH_READY','CLASSIC','LATENCY')
CLOSED_LOOP={'FULL_NOW'}

class ArbitragePaperTracker:
    def __init__(self):
        self.enabled=True; self.last_error=None; self.last_refresh=None; self.latest={}
        self.state=self._load()
    def _load(self):
        try:return json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:
            r=execution_engine.db.execute('SELECT COALESCE(MAX(id),0) m FROM executions').fetchone()
            x={'start_id':int(r['m'] or 0),'created_at':time.time()}; STATE.write_text(json.dumps(x,indent=2),encoding='utf-8'); return x
    @staticmethod
    def _venues(payload):
        try:
            p=json.loads(payload or '{}'); row=p.get('row') or {}; vals=set(row.get('execution_venues') or [])
            for z in row.get('steps') or []:
                if z.get('venue'):vals.add(str(z['venue']))
            for k in ('buy_venue','sell_venue','slow_venue','fast_venue'):
                if row.get(k):vals.add(str(row[k]))
            return vals
        except Exception:return set()
    def refresh(self):
        try:
            q='SELECT id,ts,strategy,status,realized_profit,payload FROM executions WHERE id>? AND environment=? AND strategy IN (?,?,?,?) ORDER BY id'
            rows=execution_engine.db.execute(q,(int(self.state.get('start_id') or 0),'DEMO',*STRATEGIES)).fetchall()
            live=live_executor.status(); quarantine=set(live.get('live_quarantined_venues') or [])
            allowed=set(live.get('verified_ready') or [])-quarantine
            closed_pnl=0.0; closed_count=0; signal_pnl=0.0; signal_count=0; deploy_pnl=0.0; deploy_count=0; recent=[]
            for r in rows:
                pnl=float(r['realized_profit'] or 0); venues=self._venues(r['payload'])
                try: meta=json.loads(r['payload'] or '{}'); route=meta.get('row') or {}
                except Exception: route={}
                closed=bool(r['strategy']=='FULL_NOW' and str(r['status'])=='FILLED_BOTH' and route.get('full_loop_confirmed'))
                deployable=bool(closed and len(venues)>=2 and venues.issubset(allowed))
                if closed:
                    closed_pnl+=pnl; closed_count+=1
                    if deployable:deploy_pnl+=pnl; deploy_count+=1
                elif r['strategy'] in {'CLASSIC','LATENCY'} and str(r['status'])=='FILLED_BOTH':
                    signal_pnl+=pnl; signal_count+=1
                recent.append({'id':int(r['id']),'ts':r['ts'],'strategy':r['strategy'],'status':r['status'],'pnl':round(pnl,8),
                               'venues':sorted(venues),'closed_loop':closed,'batch_cycle':bool(r['strategy']=='BATCH_READY'),'deployable':deployable})
            natural=(natural_rebalance_ledger.refresh(int(self.state.get('start_id') or 0)).get('latest') or {})
            batch=(batch_series_ledger.refresh(int(self.state.get('start_id') or 0)).get('latest') or {})
            natural_pnl=float(natural.get('natural_closed_loop_pnl') or 0); natural_deploy=float(natural.get('deployable_natural_pnl') or 0)
            batch_pnl=float(batch.get('completed_series_pnl') or 0); batch_deploy=float(batch.get('deployable_series_pnl') or 0)
            residual=float(natural.get('residual_signal_pnl') or signal_pnl); matched=int(natural.get('matched_pair_count') or 0); dep_matches=int(natural.get('deployable_match_count') or 0)
            batch_done=int(batch.get('completed_series_count') or 0); batch_dep=int(batch.get('deployable_series_count') or 0)
            total_closed=closed_pnl+natural_pnl+batch_pnl; total_deploy=deploy_pnl+natural_deploy+batch_deploy; dep_events=deploy_count+dep_matches+batch_dep
            self.latest={'mode':'PAPER_FROM_DEMO_FILLS','research_equity':round(100+total_closed,6),'research_return_pct':round(total_closed,4),
                         'native_closed_loop_pnl':round(closed_pnl,8),'natural_closed_loop_pnl':round(natural_pnl,8),'batch_series_closed_pnl':round(batch_pnl,8),
                         'closed_loop_trade_count':closed_count,'natural_match_count':matched,'batch_series_count':batch_done,
                         'unsettled_signal_pnl':round(residual,6),'signal_fill_count':signal_count,'batch_series':batch,
                         'gross_demo_pnl':round(closed_pnl+signal_pnl,6),'deployable_equity':round(100+total_deploy,6),
                         'deployable_return_pct':round(total_deploy,4),'deployable_trade_count':dep_events,'allocatable':bool(dep_events>=5),
                         'allowed_venues':sorted(allowed),'quarantined_venues':sorted(quarantine),'natural_rebalance':natural,'recent':recent[-20:]}
            self.last_refresh=time.time(); self.last_error=None
        except Exception as exc:self.last_error=str(exc)[:300]
        return self.status()
    def status(self):
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'PAPER_FROM_DEMO_FILLS','last_refresh':self.last_refresh,'last_error':self.last_error,'latest':self.latest}

arbitrage_paper_tracker=ArbitragePaperTracker()